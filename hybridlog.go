package kv

import (
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// hybridLog is the larger-than-memory log: one growing file whose recent tail is held in
// a fixed in-memory ring, so the working set lives in RAM and the cold prefix lives on
// disk, in one file, with the look and feel of a single-file store.
//
// The idea it keeps from FASTER, and the tax it drops. A logical address is a byte
// offset into one ever-growing address space. The file IS that address space: the bytes
// of logical address a live at file offset a, with no block table, no per-page header,
// and no allocator mapping addresses to scattered blocks. That direct mapping is the
// whole point. The recent window (tail-ringBytes, tail] is mirrored in the ring buffer
// buf, indexed by a&mask, so a hot read never touches the disk. Older addresses are read
// straight from the file at offset a. None of the pager machinery the old engine carried
// exists here.
//
// Three watermarks order the work, all monotonic, all atomic:
//   - tail: next free logical address, advanced by a single fetch-add per append. This is
//     the lock-free reservation; two appenders never block here.
//   - committed: the highest address below which every record is fully written. Reserve
//     does not write the length header; Commit does, after the caller has filled the
//     payload, so a nonzero header means a complete record. Committed advances in address
//     order, which is what lets the flusher stream a clean prefix.
//   - flushed: the highest address the file holds. The background flusher writes the ring
//     range (flushed, committed] to the file and advances flushed. A record below flushed
//     is safe to overwrite in the ring because it is durable on disk.
//
// Backpressure, not a lock, bounds memory. An append may not overwrite a ring slot whose
// previous occupant is not yet flushed, so it waits until flushed catches up. In steady
// state the flusher keeps ahead and the wait never triggers, so the common append path is
// the fetch-add and a copy.
type hybridLog struct {
	buf       []byte
	ringBytes int64
	mask      int64
	f         *os.File
	cf        *os.File // commit side file: the durable-tail watermark, fsynced after the data

	tail      atomic.Int64
	committed atomic.Int64
	flushed   atomic.Int64
	synced    atomic.Int64 // highest address fsynced to disk, the durability watermark

	flushTrigger int64 // wake the flusher only once this many committed-but-unflushed bytes pile up

	flushWake chan struct{}
	closed    chan struct{}
	wg        sync.WaitGroup
}

// flushTick is the backstop interval: even when the unflushed prefix never reaches flushTrigger,
// the flusher wakes this often to drain the tail, so a trickle of writes still reaches the file
// within a bounded delay instead of waiting for the next trigger crossing or for Close.
const flushTick = 2 * time.Millisecond

// maxRecord bounds a single record so a torn length read during a racing ring read is
// rejected by a sanity check instead of driving an out-of-range copy. It also bounds the
// flush chunk. A record larger than this is a misuse the engine does not store.
const maxRecord = 1 << 20

// openHybridLog opens or creates the backing file at path and returns a log whose ring holds
// ringBytes of the recent tail. ringBytes is rounded up to a power of two so the
// address-to-slot map is a mask. It must exceed maxRecord so a record always fits the ring.
// An existing file is not truncated: the log recovers its durable tail from the commit side
// file and refills the ring from disk, so a reopen sees every fsynced record.
func openHybridLog(path string, ringBytes int64) (*hybridLog, error) {
	n := int64(1)
	for n < ringBytes {
		n <<= 1
	}
	if n < maxRecord*2 {
		n = maxRecord * 2
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	cf, err := os.OpenFile(path+".commit", os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		f.Close()
		return nil, err
	}
	l := &hybridLog{
		buf:          make([]byte, n),
		ringBytes:    n,
		mask:         n - 1,
		f:            f,
		cf:           cf,
		flushTrigger: min(n/8, 1<<20), // batch wakeups: a flush covers up to ~1 MiB or an eighth of the ring
		flushWake:    make(chan struct{}, 1),
		closed:       make(chan struct{}),
	}
	if err := l.recover(); err != nil {
		f.Close()
		cf.Close()
		return nil, err
	}
	l.wg.Add(1)
	go l.flushLoop()
	return l, nil
}

// recover restores the watermarks and the resident ring from a prior run. The durable tail is
// the address the commit side file records, which is only advanced after the data below it is
// fsynced, so every byte under it is on the platter. It clamps the watermark to the file size
// in case the side file is ahead of a short write, then refills the ring with the last
// ringBytes of durable data so a hot read right after open serves from memory. Below that
// window reads go to the file, which is intact.
func (l *hybridLog) recover() error {
	var tb [8]byte
	if n, _ := l.cf.ReadAt(tb[:], 0); n == 8 {
		durable := int64(binary.LittleEndian.Uint64(tb[:]))
		fi, err := l.f.Stat()
		if err != nil {
			return err
		}
		if durable > fi.Size() {
			durable = fi.Size() // side file ahead of the data: trust only what is there
		}
		if durable < 0 {
			durable = 0
		}
		l.tail.Store(durable)
		l.committed.Store(durable)
		l.flushed.Store(durable)
		l.synced.Store(durable)
		l.fillRing(durable)
	}
	return nil
}

// fillRing copies the last ringBytes of durable data from the file back into the in-memory
// ring at the logical offsets, so the recovered tail is resident exactly as it would be had it
// just been appended. It undoes the wrap split the same way flushRange does.
func (l *hybridLog) fillRing(durable int64) {
	start := max(durable-l.ringBytes, 0)
	for off := start; off < durable; {
		pos := off & l.mask
		span := durable - off
		if pos+span > l.ringBytes {
			span = l.ringBytes - pos
		}
		l.f.ReadAt(l.buf[pos:pos+span], off)
		off += span
	}
}

// ringWrite copies src into the ring at logical offset off, splitting at the ring end so a
// record that wraps the buffer is written as two contiguous halves. The file never wraps,
// so this split is only about the in-memory mirror.
func (l *hybridLog) ringWrite(off int64, src []byte) {
	pos := off & l.mask
	end := pos + int64(len(src))
	if end <= l.ringBytes {
		copy(l.buf[pos:end], src)
		return
	}
	first := l.ringBytes - pos
	copy(l.buf[pos:], src[:first])
	copy(l.buf[:end-l.ringBytes], src[first:])
}

// ringRead copies len(dst) bytes starting at logical offset off out of the ring, undoing
// the same wrap split. The caller validates the read against the tail afterward.
func (l *hybridLog) ringRead(off int64, dst []byte) {
	pos := off & l.mask
	end := pos + int64(len(dst))
	if end <= l.ringBytes {
		copy(dst, l.buf[pos:end])
		return
	}
	first := l.ringBytes - pos
	copy(dst[:first], l.buf[pos:])
	copy(dst[first:], l.buf[:end-l.ringBytes])
}

// Append writes one record and returns its logical address. It reserves the span with one
// fetch-add, waits only if it would overwrite a not-yet-flushed ring slot, copies the
// payload, then writes the header last so its nonzero value marks the record complete, and
// advances the commit watermark. A wrapping ring cannot hand back one contiguous in-place
// slice, so the store frames its record into a pooled buffer and calls Append; the single
// ring copy here is the cost of the wrap, measured in note 178.
func (l *hybridLog) Append(rec []byte) int64 {
	total := int64(hdrLen + len(rec))
	off := l.tail.Add(total) - total
	for off+total > l.flushed.Load()+l.ringBytes {
		l.wakeFlusher()
		runtime.Gosched()
	}
	var hdr [hdrLen]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(rec)))
	l.ringWrite(off+hdrLen, rec)
	l.ringWrite(off, hdr[:]) // header last: its nonzero value marks the record complete
	l.commit(off, total)
	return off
}

// AppendFrame writes one record framed as [op][klen][key][value] and returns its logical
// address, framing straight into the ring instead of taking a caller-staged buffer. It saves a
// copy over framing into a scratch buffer and handing that to Append: the value, the large part,
// lands in its ring slot in one memmove rather than being copied into a staging buffer first and
// into the ring second. The profiler put that second copy at a third of the write path on a
// 1 KiB value (note 182), so the durable store frames here rather than through Append. The small
// header parts, the op, the key length, and the key, are tiny next to the value. The length
// header is still written last so its nonzero value marks the record complete, the same publish
// order Append uses, and the same backpressure wait guards a not-yet-flushed ring slot.
func (l *hybridLog) AppendFrame(op byte, key, value []byte) int64 {
	payload := int64(recordLen(key, value))
	total := hdrLen + payload
	off := l.tail.Add(total) - total
	for off+total > l.flushed.Load()+l.ringBytes {
		l.wakeFlusher()
		runtime.Gosched()
	}
	base := off + hdrLen
	var meta [opSize + keyLenSize]byte
	meta[0] = op
	binary.LittleEndian.PutUint16(meta[opSize:], uint16(len(key)))
	l.ringWrite(base, meta[:])
	l.ringWrite(base+opSize+keyLenSize, key)
	l.ringWrite(base+opSize+keyLenSize+int64(len(key)), value) // the big copy: value straight to its slot
	var hdr [hdrLen]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(payload))
	l.ringWrite(off, hdr[:]) // header last: its nonzero value marks the record complete
	l.commit(off, total)
	return off
}

// commit advances the committed watermark in address order. The appender whose record
// starts at the current watermark moves it past its own record; a later appender that
// finished first waits its turn, so committed only ever exposes a contiguous fully-written
// prefix. The wait is the cost of an ordered durable prefix and is short, bounded by the
// number of concurrent appenders, and absent under low concurrency.
func (l *hybridLog) commit(off, total int64) {
	for l.committed.Load() != off {
		runtime.Gosched()
	}
	c := off + total
	l.committed.Store(c)
	// Wake the flusher only once enough unflushed bytes have piled up, not on every record. A
	// per-record signal was a cond-signal storm that dominated the write path under profiling: the
	// migrator appended a million records and woke the flusher a million times. Batching the wake
	// to a flushTrigger-sized prefix collapses that to a handful of wakeups per burst, and the
	// ticker backstop in flushLoop still drains a trickle that never reaches the trigger.
	if c-l.flushed.Load() >= l.flushTrigger {
		l.wakeFlusher()
	}
}

func (l *hybridLog) wakeFlusher() {
	select {
	case l.flushWake <- struct{}{}:
	default:
	}
}

// flushLoop streams the committed-but-unflushed prefix to the file and advances flushed,
// which both makes the tail durable and frees ring slots for reuse. It writes at the
// record's logical offset, so the file mirrors the address space exactly.
func (l *hybridLog) flushLoop() {
	defer l.wg.Done()
	ticker := time.NewTicker(flushTick)
	defer ticker.Stop()
	for {
		c := l.committed.Load()
		f := l.flushed.Load()
		if c > f {
			l.flushRange(f, c)
			l.flushed.Store(c)
			l.persist(c)
			continue
		}
		select {
		case <-l.closed:
			// Final drain: persist whatever committed after the last wake.
			if last := l.committed.Load(); last > l.flushed.Load() {
				l.flushRange(l.flushed.Load(), last)
				l.flushed.Store(last)
				l.persist(last)
			}
			return
		case <-l.flushWake:
		case <-ticker.C: // backstop: drain a tail that never reached the trigger
		}
	}
}

// persist makes the data below upTo durable and records the new durable tail. It fsyncs the
// data file first, then writes and fsyncs the watermark in the side file, in that order: the
// data must be on the platter before recovery is allowed to trust the higher tail. This is the
// group commit the lab measured (note 181): one fsync covers the whole batch the flusher just
// wrote, so under load the per-record fsync cost falls by the batch size, and when idle each
// record gets its own fsync, which is the right shape.
func (l *hybridLog) persist(upTo int64) {
	if upTo <= l.synced.Load() {
		return
	}
	l.f.Sync()
	var tb [8]byte
	binary.LittleEndian.PutUint64(tb[:], uint64(upTo))
	l.cf.WriteAt(tb[:], 0)
	l.cf.Sync()
	l.synced.Store(upTo)
}

// flushRange writes logical bytes [from, to) to the file at offset from, splitting the
// read out of the ring at the wrap and writing the file in one or two contiguous pwrites.
func (l *hybridLog) flushRange(from, to int64) {
	for from < to {
		pos := from & l.mask
		span := to - from
		if pos+span > l.ringBytes {
			span = l.ringBytes - pos
		}
		l.f.WriteAt(l.buf[pos:pos+span], from)
		from += span
	}
}

var errShortRecord = errors.New("kv: record past tail")

// At returns the record bytes at logical address addr, copied into dst (grown as needed)
// since the bytes may live in the ring or on disk and a ring slot can be reused under a
// reader. It first tries the ring with a validating read: snapshot the tail, copy, then
// re-check that the address is still inside the window; if the window moved during the
// copy, the record is now on disk (the backpressure invariant guarantees it was flushed
// before being overwritten), so it falls back to a file read. A record comfortably inside
// the window takes the lock-free ring path.
func (l *hybridLog) At(addr int64, dst []byte) ([]byte, error) {
	dst, _, err := l.AtSource(addr, dst)
	return dst, err
}

// AtSource is At plus whether the record was served from disk rather than the resident ring.
// The tier uses the flag to cache only disk-sourced reads: a record already resident in the ring
// is served from memory at full speed, so a second copy of it in the read cache would only spend
// RAM to save nothing, while a disk-sourced read is exactly the one worth caching.
func (l *hybridLog) AtSource(addr int64, dst []byte) ([]byte, bool, error) {
	t := l.tail.Load()
	if addr < 0 || addr+hdrLen > t {
		return nil, false, errShortRecord
	}
	if addr >= t-l.ringBytes {
		// Resident path: read length, sanity-check, read payload, then validate.
		var hb [hdrLen]byte
		l.ringRead(addr, hb[:])
		n := int64(binary.LittleEndian.Uint32(hb[:]))
		if n >= 0 && n <= maxRecord && addr+hdrLen+n <= t {
			dst = grow(dst, int(n))
			l.ringRead(addr+hdrLen, dst)
			if addr >= l.tail.Load()-l.ringBytes {
				return dst, false, nil
			}
		}
		// Window moved during the read, or a torn length: serve from disk below.
	}
	dst, err := l.readDisk(addr, dst)
	return dst, true, err
}

// readDisk reads a record straight from the file at offset addr. The file mirrors the
// address space, so the record is contiguous on disk even if it wrapped in the ring.
func (l *hybridLog) readDisk(addr int64, dst []byte) ([]byte, error) {
	var hb [hdrLen]byte
	if _, err := l.f.ReadAt(hb[:], addr); err != nil {
		return nil, err
	}
	n := int(binary.LittleEndian.Uint32(hb[:]))
	if n < 0 || n > maxRecord {
		return nil, errShortRecord
	}
	dst = grow(dst, n)
	if _, err := l.f.ReadAt(dst, addr+hdrLen); err != nil {
		return nil, err
	}
	return dst, nil
}

// grow returns dst resized to n bytes, reusing its capacity when it already fits so a
// reader that passes a scratch buffer reads without allocating.
func grow(dst []byte, n int) []byte {
	if cap(dst) >= n {
		return dst[:n]
	}
	return make([]byte, n)
}

// Tail returns the current tail, the total logical bytes appended.
func (l *hybridLog) Tail() int64 { return l.tail.Load() }

// Synced returns the durable tail, the address below which every record is fsynced.
func (l *hybridLog) Synced() int64 { return l.synced.Load() }

// Sync forces every committed record durable before it returns, by waking the flusher and
// waiting for the synced watermark to reach the current commit point. A caller that needs a
// write on the platter, a checkpoint barrier or an explicit flush, calls this; the normal path
// leaves durability to the background group commit.
func (l *hybridLog) Sync() error {
	target := l.committed.Load()
	for l.synced.Load() < target {
		l.wakeFlusher()
		runtime.Gosched()
	}
	return nil
}

// Range calls fn for every committed record in address order, passing the record's address and
// its bytes copied into a reused buffer. It is the log replay the store uses on open to rebuild
// the index from the file. Returning false from fn stops the walk early.
func (l *hybridLog) Range(fn func(addr int64, rec []byte) bool) error {
	end := l.committed.Load()
	buf := make([]byte, 0, 256)
	for off := int64(0); off+hdrLen <= end; {
		var err error
		buf, err = l.At(off, buf)
		if err != nil {
			return err
		}
		if !fn(off, buf) {
			return nil
		}
		off += hdrLen + int64(len(buf))
	}
	return nil
}

// Close stops the flusher after a final drain and closes the file. After Close every
// committed record is on disk.
func (l *hybridLog) Close() error {
	close(l.closed)
	l.wakeFlusher()
	l.wg.Wait()
	syncErr := l.f.Sync()
	cfErr := l.cf.Close()
	closeErr := l.f.Close()
	if syncErr != nil {
		return syncErr
	}
	if cfErr != nil {
		return cfErr
	}
	return closeErr
}
