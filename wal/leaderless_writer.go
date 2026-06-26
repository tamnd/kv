package wal

// This file is M5.2: the leaderless double-buffered committer that produces the M5.1 frame
// format (spec 05 §4, decision D7). M5.1 proved the format and its recovery in isolation;
// this is the concurrent writer that fills the log, built alongside the shipped chained log
// and off the live commit path until the M8 flip.
//
// The shape is the two-buffer ping-pong from the design. At any moment one buffer is active
// and accepting appends; the other is idle or being synced. A committer claims a region of
// the active buffer, copies its frame in, and the buffer is sealed and synced as a whole, so
// one fsync makes a whole buffer's worth of committers durable at once. The double-buffering
// overlaps the sync of one buffer with the fill of the next, so a committer never waits behind
// a designated leader's syscall to do its own append. There is no leader: every committer
// claims its own slot, copies its own bytes, and blocks only on its own durability point.
//
// Two honest divergences from the design's letter, both preserving its intent and recorded so
// the implementation doc and a later reader know what shipped:
//
//   - The region claim is a lock-free CAS on a packed (generation, offset) word, not the
//     wait-free atomic.Add the design names. A hard buffer capacity plus a clean seal point
//     cannot both hold under a blind Add: an Add that overruns the capacity leaves the cursor
//     past the buffer with a straddling claim that never fills, so the flusher cannot know the
//     sealed extent. The CAS claim keeps the cursor exactly at the sum of admitted claims, so
//     the seal point is unambiguous, and it keeps the property that actually matters, that no
//     committer waits on a leader. The CAS competes only with other claims and the one flip,
//     and the loser simply retries, so it is lock-free, not wait-free; the wait-free Add is the
//     unbounded ideal and the CAS is its bounded, correct realization.
//   - The swap is serialized by a brief mutex exactly as the design says, but the buffer-pointer
//     transition itself is folded into the same packed-word CAS, so the mutex serializes only
//     the flippers among themselves (the back-pressure wait and the next buffer's preparation),
//     never a committer's claim. A committer's claim never takes the mutex.
//
// The completion watermark reconciles the out-of-order commits into one ordered durable
// frontier. Committers claim LSNs and complete out of order, and buffers make ranges of LSNs
// durable in batches, so the watermark is the highest LSN whose whole prefix is durable. A
// committer is acked only once the watermark covers its LSN, which is exactly the
// contiguous-LSN prefix RecoverLeaderless reconstructs, so the set of acked commits equals the
// set recovery returns: no acked commit is ever lost, and recovery may keep a few in-flight
// commits past the watermark that were never acked, which is safe.

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/tamnd/kv/vfs"
)

// errFrameTooLarge is returned by Commit when a single frame would not fit in one buffer. A
// buffer is the unit of sync, so a frame must fit inside one; an oversized batch is the caller's
// to split, the same constraint the multi-frame transaction marker (reserved, not built) exists
// to lift.
var errFrameTooLarge = errors.New("wal: leaderless frame larger than buffer capacity")

// Packed-state layout: the high bits hold the generation, the low bits hold the in-buffer
// write offset. A claim CASes the whole word, advancing the offset; a flip CASes the whole
// word, bumping the generation and resetting the offset to zero. The generation's low bit
// selects the physical buffer, so a flip alternates buffers. The offset field is wide enough
// for any sane buffer (a terabyte), so a claim never carries into the generation bits.
const (
	llStateShift = 40
	llOffMask    = (uint64(1) << llStateShift) - 1
	// defaultBufBytes is the per-buffer capacity when the caller passes zero. It sets the
	// most committers one fsync can amortize (K): a larger buffer holds more concurrent
	// committers per sync at the cost of more memory and a larger torn-tail on a crash. This is
	// the mechanism's tuning knob; doc 09 sizes it against a real multi-writer benchmark.
	defaultBufBytes = 1 << 20
)

// llFlushReq tells the flusher to write and sync one sealed generation's bytes.
type llFlushReq struct {
	gen    uint64
	base   int64 // file offset the sealed bytes are written at
	sealed int64 // number of bytes to write, the generation's filled extent
}

// llWBuf is one of the two ping-pong buffers.
type llWBuf struct {
	data   []byte
	filled atomic.Int64 // bytes whose frame copy has completed, for the current generation
	base   int64        // file offset of this generation's first byte; touched only under swapMu
}

// Leaderless is the concurrent double-buffered committer. It is safe for many goroutines to
// Commit concurrently. It is not wired onto the live commit path; the db drives the chained
// WAL until the M8 flip.
type Leaderless struct {
	file    vfs.File
	salt    uint64
	baseLSN uint64
	cap     int64
	syncSel func(vfs.File) error

	state atomic.Uint64 // packed (generation, offset)
	lsn   atomic.Uint64 // next LSN to assign
	bufs  [2]llWBuf

	swapMu sync.Mutex // serializes flippers: back-pressure wait, next-buffer prep, the flip CAS

	flushCh chan llFlushReq
	flushWG sync.WaitGroup

	fmu         sync.Mutex // guards flushedGen and its condition
	fcond       *sync.Cond
	flushedGen  [2]uint64 // highest generation fully synced on each buffer
	flushedInit [2]bool   // whether a buffer has had any generation synced yet
	flushErr    atomic.Pointer[error]

	wmu       sync.Mutex // guards the watermark and the completed set
	wcond     *sync.Cond
	watermark uint64          // highest LSN whose whole prefix is durable
	completed map[uint64]bool // durable LSNs not yet folded into the watermark
}

// CreateLeaderless initializes a fresh leaderless -wal file and returns a committer positioned
// to append after the header. baseLSN is the first LSN this generation assigns; bufBytes sets the
// per-buffer capacity (zero uses the default).
func CreateLeaderless(fs vfs.FS, path string, opts Options, baseLSN uint64, bufBytes int) (*Leaderless, error) {
	f, err := fs.Open(path, vfs.OpenReadWrite|vfs.OpenCreate)
	if err != nil {
		return nil, err
	}
	if baseLSN == 0 {
		baseLSN = 1
	}
	if bufBytes <= 0 {
		bufBytes = defaultBufBytes
	}
	sm := opts.Sync
	if sm == syncDefault {
		sm = SyncFull
	}
	l := &Leaderless{
		file:      f,
		salt:      opts.Salt,
		baseLSN:   baseLSN,
		cap:       int64(bufBytes),
		syncSel:   syncSelector(sm),
		flushCh:   make(chan llFlushReq, 4),
		watermark: baseLSN - 1,
		completed: make(map[uint64]bool),
	}
	l.fcond = sync.NewCond(&l.fmu)
	l.wcond = sync.NewCond(&l.wmu)
	l.lsn.Store(baseLSN)
	l.bufs[0].data = make([]byte, bufBytes)
	l.bufs[1].data = make([]byte, bufBytes)
	// Generation 0 lives on buffer 0, starting right after the header.
	l.bufs[0].base = int64(llHeaderSize)
	l.state.Store(0) // generation 0, offset 0

	if err := writeLeaderlessHeader(f, opts.PageSize, opts.Salt, baseLSN); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(vfs.SyncFull); err != nil {
		f.Close()
		return nil, err
	}
	l.flushWG.Add(1)
	go l.flusher()
	return l, nil
}

// syncSelector maps a sync level onto the durability primitive. The per-platform selection
// (D11, fdatasync on Linux against F_FULLFSYNC on macOS) is a later slice; this reuses the vfs
// modes the chained log already drives.
func syncSelector(sm Sync) func(vfs.File) error {
	switch sm {
	case SyncOff, SyncNormal:
		return func(vfs.File) error { return nil }
	case SyncBarrier:
		return func(f vfs.File) error { return f.Sync(vfs.SyncBarrier) }
	default: // SyncFull, SyncExtra
		return func(f vfs.File) error { return f.Sync(vfs.SyncData) }
	}
}

// Commit appends one self-committing kv-batch frame carrying the serialized batch and returns
// its LSN once the commit is durable, that is, once the completion watermark covers it. Many
// goroutines may call it concurrently; each drives its own commit and blocks only on its own
// durability point.
func (l *Leaderless) Commit(version uint64, payload []byte) (uint64, error) {
	size := int64(llFrameHeaderSize + len(payload))
	if size > l.cap {
		return 0, errFrameTooLarge
	}
	gen, off := l.claim(size)
	lsn := l.lsn.Add(1) - 1

	b := &l.bufs[gen&1]
	encodeLLFrame(b.data[off:off+size], FrameKVBatch, lsn, version, l.salt, payload)
	// Publish the copy with a release on filled, so the flusher's acquiring load of filled
	// orders these bytes before it writes the buffer out.
	b.filled.Add(size)

	if err := l.ensureDurable(gen, lsn); err != nil {
		return 0, err
	}
	return lsn, nil
}

// claim reserves a region of the active buffer for a frame of the given size, returning the
// generation and in-buffer offset it got. It is the lock-free claim: a single CAS advances the
// packed offset, and a claim that would overrun the buffer triggers a flip to the fresh buffer
// and retries. It never takes the swap mutex.
func (l *Leaderless) claim(size int64) (gen uint64, off int64) {
	for {
		s := l.state.Load()
		g := s >> llStateShift
		o := int64(s & llOffMask)
		if o+size > l.cap {
			// The active buffer cannot hold this frame: seal it and move to the fresh one, then
			// retry the claim against the new active buffer.
			l.flip(g)
			continue
		}
		if l.state.CompareAndSwap(s, s+uint64(size)) {
			return g, o
		}
		// Lost the CAS to another claim or a flip; retry.
	}
}

// flip seals the active generation expectGen and activates the next one on the other buffer. It
// is serialized by swapMu so only one flipper prepares the next buffer at a time; a caller whose
// generation was already flipped finds the generation advanced and returns without doing
// anything. The write and sync of the sealed buffer happen in the flusher, off this path.
func (l *Leaderless) flip(expectGen uint64) {
	l.swapMu.Lock()
	defer l.swapMu.Unlock()
	for {
		s := l.state.Load()
		gen := s >> llStateShift
		if gen != expectGen {
			// Someone else already flipped past expectGen; the caller's generation is sealed.
			return
		}
		off := int64(s & llOffMask)
		newGen := gen + 1
		tb := newGen & 1
		ob := &l.bufs[gen&1]
		nb := &l.bufs[tb]

		// Back-pressure: the buffer we flip into last held generation newGen-2, and it must be
		// fully synced before we overwrite it, or we would clobber bytes a crash still needs. This
		// wait is what bounds the writer to one buffer ahead of the sync, and it is where the
		// amortization comes from: while it blocks, more committers claim into the still-active
		// generation, so the eventual seal sweeps all of them under one fsync.
		l.fmu.Lock()
		for newGen >= 2 && !(l.flushedInit[tb] && l.flushedGen[tb] >= newGen-2) {
			l.fcond.Wait()
		}
		l.fmu.Unlock()
		if perr := l.flushErr.Load(); perr != nil {
			return // a prior sync failed; stop flipping, the error surfaces to committers
		}

		nb.base = ob.base + off
		nb.filled.Store(0)
		if l.state.CompareAndSwap(s, newGen<<llStateShift) {
			l.flushCh <- llFlushReq{gen: gen, base: ob.base, sealed: off}
			return
		}
		// A claim advanced the offset between our Load and our CAS; re-read and seal the larger
		// extent. nb is untouched by claims until the CAS publishes the new generation, so
		// rewriting nb.base on the retry is safe.
	}
}

// ensureDurable makes sure the committer's generation gets sealed and synced, then advances the
// completion watermark with the committer's LSN and waits until the watermark covers it.
func (l *Leaderless) ensureDurable(gen, lsn uint64) error {
	// If the generation is still active, seal it so its flush is scheduled. If it was already
	// sealed by another committer, flip returns at once.
	if (l.state.Load() >> llStateShift) == gen {
		l.flip(gen)
	}
	// Wait for this generation's buffer to be synced.
	l.fmu.Lock()
	for !(l.flushedInit[gen&1] && l.flushedGen[gen&1] >= gen) {
		if perr := l.flushErr.Load(); perr != nil {
			l.fmu.Unlock()
			return *perr
		}
		l.fcond.Wait()
	}
	l.fmu.Unlock()
	if perr := l.flushErr.Load(); perr != nil {
		return *perr
	}

	// The frame is durable on disk. Record its LSN complete and advance the watermark over the
	// contiguous prefix of completed LSNs, then wait until the watermark covers this LSN, which
	// it does not until every lower LSN is also durable and recorded.
	l.wmu.Lock()
	l.completed[lsn] = true
	for l.completed[l.watermark+1] {
		delete(l.completed, l.watermark+1)
		l.watermark++
	}
	l.wcond.Broadcast()
	for l.watermark < lsn {
		l.wcond.Wait()
	}
	l.wmu.Unlock()
	return nil
}

// flusher is the single goroutine that writes and syncs sealed generations in order. It waits
// for every claim in a sealed generation to finish its copy (filled reaches the sealed extent),
// writes the generation's bytes in one WriteAt, syncs, and records the generation durable so the
// committers waiting on it and the flippers waiting on back-pressure wake.
func (l *Leaderless) flusher() {
	defer l.flushWG.Done()
	for req := range l.flushCh {
		b := &l.bufs[req.gen&1]
		// Spin until every committer that claimed a region in this generation has finished its
		// copy. The window is a memcpy, microseconds; back-pressure guarantees the buffer is not
		// reused for a later generation while we wait, so filled belongs to req.gen throughout.
		for b.filled.Load() < req.sealed {
			runtime.Gosched()
		}
		if req.sealed > 0 {
			if _, err := l.file.WriteAt(b.data[:req.sealed], req.base); err != nil {
				l.failFlush(err)
				return
			}
			if err := l.syncSel(l.file); err != nil {
				l.failFlush(err)
				return
			}
		}
		l.fmu.Lock()
		l.flushedGen[req.gen&1] = req.gen
		l.flushedInit[req.gen&1] = true
		l.fcond.Broadcast()
		l.fmu.Unlock()
	}
}

// failFlush records a fatal flush error and wakes every waiter so each committer's Commit
// returns the error rather than blocking forever. A sync error is non-retryable (fsyncgate):
// the caller must stop writing and reopen.
func (l *Leaderless) failFlush(err error) {
	l.flushErr.CompareAndSwap(nil, &err)
	l.fmu.Lock()
	l.fcond.Broadcast()
	l.fmu.Unlock()
	l.wmu.Lock()
	l.wcond.Broadcast()
	l.wmu.Unlock()
}

// Sync seals and flushes any pending active generation and returns once every commit appended so
// far is durable. A commit that already returned is durable, so this only matters for a buffer
// that has appends not yet swept by a flip, which is the path Close uses.
func (l *Leaderless) Sync() error {
	for {
		s := l.state.Load()
		gen := s >> llStateShift
		off := int64(s & llOffMask)
		if off == 0 {
			break // active generation is empty; nothing unsealed to flush
		}
		l.flip(gen)
		if perr := l.flushErr.Load(); perr != nil {
			return *perr
		}
		// Wait for the sealed generation to sync, then loop in case a concurrent committer opened
		// a new non-empty generation.
		l.fmu.Lock()
		for !(l.flushedInit[gen&1] && l.flushedGen[gen&1] >= gen) {
			if perr := l.flushErr.Load(); perr != nil {
				l.fmu.Unlock()
				return *perr
			}
			l.fcond.Wait()
		}
		l.fmu.Unlock()
	}
	if perr := l.flushErr.Load(); perr != nil {
		return *perr
	}
	return nil
}

// Watermark reports the completion watermark, the highest LSN whose whole prefix is durable.
func (l *Leaderless) Watermark() uint64 {
	l.wmu.Lock()
	defer l.wmu.Unlock()
	return l.watermark
}

// Close flushes any pending generation, stops the flusher, and releases the file. The caller
// must have no Commit in flight.
func (l *Leaderless) Close() error {
	serr := l.Sync()
	close(l.flushCh)
	l.flushWG.Wait()
	cerr := l.file.Close()
	if serr != nil {
		return serr
	}
	return cerr
}
