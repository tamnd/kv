package f2

import (
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
)

// errLogFull is returned by append when the shard's log would pass the largest
// address an index slot can encode (slotAddrMask is the address plus one). The
// store must be compacted, which resets each shard's log to a fresh generation at
// offset zero, before it can take more writes. With compaction this is unreachable
// in practice; the guard turns a silently truncated address into a clear error.
var errLogFull = errors.New("f2: shard log address space exhausted, compact the store")

// log is one shard's append-only record store, laid out as fixed-size pages. A
// logical address is a byte offset into the flattened page sequence: page index
// is addr/pageSize, the byte within is addr%pageSize. Pages are allocated whole
// and never moved or resized.
//
// One log type serves two profiles, selected at New:
//
//   - memory-only (no Path): every page stays in RAM, records use the lean
//     uvarint format, a read aliases its page with no copy and no lock. This is
//     the in-memory ceiling.
//   - single file (Path set): pages back onto blocks of one shared file, records
//     use the CRC format so a crash is recoverable. This one design covers both
//     larger-than-memory and durability: ResidentPagesPerShard bounds how much of
//     each shard's log stays in RAM (the rest is dropped and reread from its
//     block on demand), and the Durability dial decides when the file is fsynced.
//     A page that is evicted from RAM is simply a page already written to the
//     file, so eviction is free of any extra copy.
//
// The memory-only and unbudgeted profiles read with no shard lock: they load the
// page directory atomically and index into immutable page refs. Each directory slot
// is an atomic pointer to an immutable pageRef, so a concurrent grow of the
// directory never tears a read, and because neither profile evicts, a reader's slice
// into a page stays valid for the page's life with the garbage collector keeping the
// backing array alive.
//
// The budgeted profile evicts, and to keep eviction allocation-free it recycles
// evicted page buffers (audit L5) rather than dropping them to the collector. A
// recycled buffer is reused for a later page, so a read must not alias one a writer
// could recycle. The budgeted read therefore takes the shard read lock and copies
// its value out under it (shard.getLocked), which excludes the evictor (it holds the
// write lock), so a buffer is recycled only while no reader can be touching it and
// recycling needs no epoch deferral. This mirrors the sibling hashlog engine's
// durable evicting profile.
type log struct {
	pageSize int64
	hdr      int64 // per-page header bytes reserved at the front, single-file only
	reserve  int64 // bytes reserved at the page tail for the AEAD envelope, 0 unless encrypted
	dir      atomic.Pointer[pageDir]
	tail     int64 // next free logical offset, write side only

	df        *durableFile // the one shared file in single-file mode, nil in memory-only
	shardID   int
	gen       uint32  // generation stamped into new page headers, bumped by compaction
	pageBlock []int64 // page index to file block, single-file mode only
	budget    int     // resident page budget, 0 means unbounded
	evict     int     // index of the next page to evict, the front of the window
	npages    int     // pages allocated so far

	// mutableWindow is how many pages at the tail stay unsealed and so eligible for an
	// in-place overwrite, FASTER's mutable region. It is 1 on every profile but the
	// budgeted in-place one, where the surrounding store may widen it (clamped to the
	// resident budget so a window page is never evicted before it is sealed) to keep a
	// hot key rewriting in place across a page roll instead of declining once its record
	// falls behind the tail. A wider window seals a page later, when it leaves the window
	// rather than when the writer moves on, which under Normal defers that page's sync.
	mutableWindow int

	// flushedTail is the page count a checkpoint last flushed: pages below it reached
	// disk in flushTail or commitGeneration, so an in-place overwrite of one would
	// diverge the resident copy from the durable one and is refused. Combined with the
	// seal frontier (npages-mutableWindow), it tells onDisk which window pages are still
	// safe to rewrite. It is touched only under the shard write lock.
	flushedTail int

	// freeBufs is the page-buffer recycle pool (audit L5), filled by eviction and
	// drawn from when a new page rolls. It is touched only under the shard write lock,
	// which is also where eviction runs, so an evicted buffer goes straight into the
	// pool: no budgeted reader can be aliasing it (a budgeted read holds the read lock,
	// excluded by the write lock). It stays empty on the memory-only and unbudgeted
	// profiles, which never evict.
	freeBufs [][]byte
}

// pageRef is one page's location. It is immutable once published: a resident page
// has mem set, an evicted page has mem nil and fileOff naming its bytes in the
// file. Eviction replaces the ref, it never mutates one in place.
type pageRef struct {
	mem     []byte
	fileOff int64
}

// pageDir is the indexable directory of page refs. The refs slice is replaced
// only when it must grow, which doubles; routine eviction swaps individual slots
// with atomic stores and does not touch the directory pointer.
type pageDir struct {
	refs []atomic.Pointer[pageRef]
}

// newLog builds a memory-only log (df nil) or a single-file log (df set). When df
// is set each page reserves a header for recovery.
func newLog(pageSize int, df *durableFile, shardID, budget, window int) *log {
	if window < 1 {
		window = 1
	}
	l := &log{
		pageSize:      int64(pageSize),
		df:            df,
		shardID:       shardID,
		budget:        budget,
		mutableWindow: window,
	}
	if df != nil {
		l.hdr = blockHeaderSize
		l.tail = l.hdr // page 0 reserves its header
		if df.enc != nil {
			l.reserve = cryptoOverhead // the sealed envelope sits in the page tail
		}
	}
	d := &pageDir{refs: make([]atomic.Pointer[pageRef], 0, 8)}
	l.dir.Store(d)
	return l
}

// maxRecord is the largest record this log can store, the page minus any header and
// the AEAD envelope reserved at the tail when the file is encrypted.
func (l *log) maxRecord() int { return int(l.pageSize - l.hdr - l.reserve) }

// append writes one record and returns its logical address and byte length. A
// record never straddles a page: if it would not fit in what is left of the
// current page, the tail jumps to the next page boundary, past that page's
// header, first. The caller holds the shard write lock.
func (l *log) append(key, value []byte, tombstone bool) (int64, int, error) {
	var n int
	if l.df != nil {
		n = durableRecordLen(key, value)
	} else {
		n = recordLen(key, value)
	}
	off := l.tail
	within := off % l.pageSize
	if within+int64(n) > l.pageSize-l.reserve {
		off += l.pageSize - within // to the next page boundary
		off += l.hdr               // past its header
	}
	if uint64(off)+1 > slotAddrMask {
		return 0, 0, errLogFull // the index cannot encode this address
	}
	pi := int(off / l.pageSize)
	page, err := l.pageFor(pi)
	if err != nil {
		return 0, 0, err
	}
	w := off % l.pageSize

	if l.df != nil {
		encodeDurable(page[w:], key, value, tombstone)
		if err := l.writeThrough(pi, page, int(w), n); err != nil {
			return 0, 0, err
		}
	} else {
		ww := w
		ww += int64(binary.PutUvarint(page[ww:], uint64(len(key))))
		ww += int64(binary.PutUvarint(page[ww:], uint64(len(value))))
		copy(page[ww:], key)
		ww += int64(len(key))
		copy(page[ww:], value)
	}

	l.tail = off + int64(n)
	return off, n, nil
}

// pageFor returns the resident byte buffer for page pi, allocating new pages up
// to pi and, when a budget is set, evicting the front of the window to stay
// inside it. Because records never straddle pages, pi is the current page or the
// next one. The caller holds the shard write lock.
func (l *log) pageFor(pi int) ([]byte, error) {
	for l.npages <= pi {
		if err := l.addPage(); err != nil {
			return nil, err
		}
	}
	d := l.dir.Load()
	return d.refs[pi].Load().mem, nil
}

// addPage seals the current tail page (single-file Normal/None flush it to disk),
// appends one fresh resident page, and evicts the front while over budget. The
// caller holds the shard write lock.
func (l *log) addPage() error {
	if l.df != nil {
		// Seal the page that falls out of the mutable window as the new tail rolls in,
		// not the page just left: with a window of one this is the page we are leaving
		// (today's behavior), with a wider window it lags by window-1 pages so the last
		// window pages stay rewritable.
		if toSeal := l.npages - l.mutableWindow; toSeal >= 0 {
			if err := l.sealPage(toSeal); err != nil {
				return err
			}
		}
	}
	d := l.ensureCap(l.npages + 1)
	buf := l.newPageBuf()
	pi := l.npages
	if l.df != nil {
		block := l.df.allocBlock()
		l.pageBlock = append(l.pageBlock, block)
		writeBlockHeader(buf, l.shardID, pi, l.gen)
		if l.df.dial == DurabilityFull {
			// The header must reach disk before any record acknowledged from this
			// page, so a Full crash never leaves a record in an unheadered block.
			if l.df.enc != nil {
				// Under encryption every write is a whole sealed page, so seal and write
				// the empty page now rather than a bare header. This also overwrites any
				// stale ciphertext a reused block carries, which would otherwise open
				// under this block's page number and resurrect dead records.
				if err := l.df.writeData(block, buf); err != nil {
					return err
				}
			} else if _, err := l.df.f.WriteAt(buf[:blockHeaderSize], l.df.blockOffset(block)); err != nil {
				return err
			}
			if err := l.df.sync(); err != nil {
				return err
			}
		}
	}
	ref := &pageRef{mem: buf}
	d.refs[pi].Store(ref)
	l.npages++
	// No flushedTail reset is needed: the fresh tail page's index is npages-1, at or
	// above flushedTail, so onDisk already reports it unwritten and rewritable.
	for l.budget > 0 && l.npages-l.evict > l.budget {
		l.evictFront()
	}
	return nil
}

// onDisk reports whether page pi's current bytes are already on disk, so an in-place
// rewrite of them would diverge the resident copy from the durable one. A page is on
// disk once it is sealed (it has fallen below the seal frontier npages-mutableWindow) or
// once a checkpoint flushed it (it sits below flushedTail). The window pages between the
// two frontiers are the ones a rewrite may still land on. The caller holds the shard
// write lock.
func (l *log) onDisk(pi int) bool {
	frontier := l.npages - l.mutableWindow
	if l.flushedTail > frontier {
		frontier = l.flushedTail
	}
	return pi < frontier
}

// overwriteInPlace rewrites the same-size record at logical address off with key and
// value over its existing byte span, FASTER's in-place update. It succeeds only when the
// record is in the mutable window (the last mutableWindow pages), that page is resident,
// and the page has not been written to disk, so the rewrite never mutates bytes a sealed
// or flushed page already carries (the ARIES in-place durable mutation an append-only log
// exists to avoid). The caller holds the shard write lock and has confirmed the new value
// matches the old record's size, so the re-encode lands in the same span and the record's
// address does not move. It returns whether it rewrote.
func (l *log) overwriteInPlace(off int64, key, value []byte) bool {
	pi := int(off / l.pageSize)
	if pi < l.npages-l.mutableWindow || pi >= l.npages {
		return false // outside the mutable window: sealed below it, or not yet allocated
	}
	if l.onDisk(pi) {
		return false // already on disk; mutating it would diverge from the durable copy
	}
	ref := l.dir.Load().refs[pi].Load()
	if ref.mem == nil {
		return false // window page not resident (it always is when window <= budget, stay defensive)
	}
	w := off % l.pageSize
	encodeDurable(ref.mem[w:], key, value, false)
	return true
}

// newPageBuf returns a zeroed page buffer, drawing from the recycle pool a budgeted
// log fills on eviction before falling back to a fresh allocation. A recycled buffer
// is wiped so a stale record left from its previous page never decodes back as live:
// recovery walks a page until a record fails to decode, and an old record carries a
// valid CRC, so unzeroed trailing bytes would resurrect dead data. The caller holds
// the shard write lock.
func (l *log) newPageBuf() []byte {
	if n := len(l.freeBufs); n > 0 {
		buf := l.freeBufs[n-1]
		l.freeBufs[n-1] = nil
		l.freeBufs = l.freeBufs[:n-1]
		for i := range buf {
			buf[i] = 0
		}
		return buf
	}
	return make([]byte, l.pageSize)
}

// writeThrough flushes a single record to disk and, under Full, fsyncs it, so an
// acknowledged Set is on stable storage before it returns. Under Normal and None
// the bytes wait in the resident page and reach disk when the page is sealed. The
// caller holds the shard write lock.
func (l *log) writeThrough(pi int, page []byte, w, n int) error {
	if l.df.dial != DurabilityFull {
		return nil
	}
	if l.df.enc != nil {
		// Encryption seals the whole records region as one envelope, so a single
		// record cannot be written in place: write and seal the whole page instead.
		if err := l.df.writeData(l.pageBlock[pi], page); err != nil {
			return err
		}
		return l.df.sync()
	}
	off := l.df.blockOffset(l.pageBlock[pi]) + int64(w)
	if _, err := l.df.f.WriteAt(page[w:w+n], off); err != nil {
		return err
	}
	return l.df.sync()
}

// sealPage writes a full page to its block and, under Normal, fsyncs it. Sealing
// happens when the writer moves on to the next page, so the page is complete. The
// caller holds the shard write lock.
func (l *log) sealPage(pi int) error {
	if l.df.dial == DurabilityFull {
		return nil // already written through
	}
	d := l.dir.Load()
	ref := d.refs[pi].Load()
	if ref.mem == nil {
		return nil // already evicted, hence already on disk
	}
	if err := l.df.writeData(l.pageBlock[pi], ref.mem); err != nil {
		return err
	}
	if l.df.dial == DurabilityNormal {
		// Defer the device barrier to the byte cadence rather than fsyncing this one
		// sealed page: a smaller page seals more often, so a per-seal barrier would
		// scale the fsync count with the page-roll rate (redesign-v2 doc 09).
		return l.df.sealSync(int64(len(ref.mem)))
	}
	return nil
}

// flushTail writes the current tail page to disk, used by Checkpoint and Close so
// records sitting only in the resident tail page reach stable storage. The caller
// holds the shard write lock.
func (l *log) flushTail() error {
	if l.df == nil || l.npages == 0 {
		return nil
	}
	// Flush every unsealed window page, not just the last one: with a wider window the
	// pages behind the tail hold records the snapshot's index points at, so they must
	// reach disk for recovery to read them back. Pages below the seal frontier are
	// already on disk. With a window of one this flushes the tail alone, as before.
	d := l.dir.Load()
	start := l.npages - l.mutableWindow
	if start < 0 {
		start = 0
	}
	for pi := start; pi < l.npages; pi++ {
		ref := d.refs[pi].Load()
		if ref.mem == nil {
			continue // evicted, hence already on disk
		}
		if err := l.df.writeData(l.pageBlock[pi], ref.mem); err != nil {
			return err
		}
	}
	// The window's bytes are now on disk, so an in-place overwrite of them would diverge
	// the resident copy from the durable one; refuse in-place on them until they roll out.
	l.flushedTail = l.npages
	return nil
}

// evictFront drops the front resident page from RAM. The page is already on disk
// in its block, so eviction only repoints the ref to the block offset; a later
// read of that page preads it back. The atomic store is what makes this safe for
// a concurrent reader. The evicted buffer goes straight into the recycle pool
// (audit L5): eviction runs under the shard write lock, and a budgeted read holds
// the read lock, so no reader can be aliasing the buffer here, and a fresh page
// roll can wipe and reuse it instead of allocating. The caller holds the shard
// write lock.
func (l *log) evictFront() {
	d := l.dir.Load()
	ref := d.refs[l.evict].Load()
	fileOff := l.df.blockOffset(l.pageBlock[l.evict])
	d.refs[l.evict].Store(&pageRef{fileOff: fileOff})
	if ref.mem != nil {
		l.freeBufs = append(l.freeBufs, ref.mem)
	}
	l.evict++
}

// ensureCap returns a directory whose refs slice has room for n pages, doubling
// and republishing it if needed. The caller holds the shard write lock.
func (l *log) ensureCap(n int) *pageDir {
	d := l.dir.Load()
	if n <= len(d.refs) {
		return d
	}
	newLen := len(d.refs) * 2
	if newLen < 8 {
		newLen = 8
	}
	for newLen < n {
		newLen *= 2
	}
	nd := &pageDir{refs: make([]atomic.Pointer[pageRef], newLen)}
	for i := range d.refs {
		nd.refs[i].Store(d.refs[i].Load())
	}
	l.dir.Store(nd)
	return nd
}

// packResident appends one record into a fresh generation under construction,
// keeping every page resident and touching no disk. A compaction builds the whole
// new log this way and then writes it out in one pass, so the shard lock is held
// for an in-memory copy rather than a record-by-record disk write. It mirrors
// append's no-straddle page jump and returns the record's logical address. The
// caller holds the shard write lock and the log is single-file (df set).
func (l *log) packResident(key, value []byte) int64 {
	n := durableRecordLen(key, value)
	off := l.tail
	within := off % l.pageSize
	if within+int64(n) > l.pageSize-l.reserve {
		off += l.pageSize - within // to the next page boundary
		off += l.hdr               // past its header
	}
	pi := int(off / l.pageSize)
	for l.npages <= pi {
		l.addPageResident()
	}
	page := l.dir.Load().refs[pi].Load().mem
	w := off % l.pageSize
	encodeDurable(page[w:], key, value, false)
	l.tail = off + int64(n)
	return off
}

// addPageResident appends one fresh resident page to a generation under
// construction, allocating its file block and stamping its header but writing
// nothing to disk and never evicting. It is the build-time counterpart of addPage,
// which seals and may evict as it goes; here the whole generation is held resident
// until commitGeneration writes it and evictToBudget trims it. The caller holds the
// shard write lock.
func (l *log) addPageResident() {
	d := l.ensureCap(l.npages + 1)
	buf := l.newPageBuf()
	pi := l.npages
	block := l.df.allocBlock()
	l.pageBlock = append(l.pageBlock, block)
	writeBlockHeader(buf, l.shardID, pi, l.gen)
	d.refs[pi].Store(&pageRef{mem: buf})
	l.npages++
}

// commitGeneration writes a freshly built generation to disk with page 0 last:
// pages 1..m are written and fsynced first, then page 0 is written and fsynced, so
// a durable page 0 proves every page of this generation already reached disk and is
// the commit marker recovery keys on. Under the None dial it writes in the same
// order but skips the fsyncs, since None makes no crash promise. The caller holds
// the shard write lock.
func (l *log) commitGeneration() error {
	d := l.dir.Load()
	for pi := 1; pi < l.npages; pi++ {
		buf := d.refs[pi].Load().mem
		if err := l.df.writeData(l.pageBlock[pi], buf); err != nil {
			return err
		}
	}
	if l.npages > 1 && l.df.dial != DurabilityNone {
		if err := l.df.sync(); err != nil {
			return err
		}
	}
	buf0 := d.refs[0].Load().mem
	if err := l.df.writeData(l.pageBlock[0], buf0); err != nil {
		return err
	}
	// The whole rebuilt generation is now on disk; refuse in-place on every page until a
	// fresh one rolls, the same fence flushTail sets.
	l.flushedTail = l.npages
	if l.df.dial != DurabilityNone {
		return l.df.sync()
	}
	return nil
}

// evictToBudget trims a freshly committed generation down to the resident budget,
// dropping the front pages from RAM. The pages are already on disk from
// commitGeneration, so eviction only repoints the ref to the block offset. With no
// budget it keeps every page resident. The caller holds the shard write lock.
func (l *log) evictToBudget() {
	if l.budget <= 0 {
		return
	}
	for l.npages-l.evict > l.budget {
		l.evictFront()
	}
}

// read returns the key and value of the record at addr. For a resident page the
// slices alias the page and must not be mutated; for an evicted page they are
// owned copies. Either way no shard lock is taken.
func (l *log) read(addr int64) (key, value []byte) {
	d := l.dir.Load()
	pi := addr / l.pageSize
	within := addr % l.pageSize
	ref := d.refs[pi].Load()
	if ref.mem != nil {
		return l.decodeAt(ref.mem[within:])
	}
	return l.readEvicted(ref.fileOff + within)
}

// decodeAt splits a record at the start of b into key and value slices, picking
// the durable or lean format for this log.
func (l *log) decodeAt(b []byte) (key, value []byte) {
	if l.df != nil {
		k, v, _, _, ok := decodeDurable(b)
		if !ok {
			return nil, nil // bad CRC or short buffer: report nothing, never wrong bytes
		}
		return k, v
	}
	return decodeRecord(b)
}

// readEvicted preads a record from the file into an owned buffer. It does one
// probe read sized to cover most records in a single syscall; only a record
// larger than the probe needs a second exact read. The probe may overrun into
// the next record or the page tail, which is harmless because a record never
// straddles a page and decode consumes only its own bytes.
const evictProbe = 512

// probePool recycles the fixed-size scratch buffers readEvicted preads into. The
// probe is transient: its decoded key and value are copied into an exactly sized
// owned slice before the probe goes back to the pool, so a returned slice never
// aliases a buffer another reader will reuse. Pooling turns the steady-state cold
// read from a 512-byte slab allocation per call into a single allocation sized to
// the record, which is the dominant cost once a working set spills to the file.
var probePool = sync.Pool{New: func() any { b := make([]byte, evictProbe); return &b }}

func (l *log) readEvicted(at int64) (key, value []byte) {
	if l.df.enc != nil {
		// Encryption seals the whole records region, so a record cannot be read in
		// isolation: read and open the whole page, then decode the record at its offset.
		return l.readEvictedSealed(at)
	}
	bp := probePool.Get().(*[]byte)
	buf := (*bp)[:evictProbe]
	n, _ := l.df.f.ReadAt(buf, at)
	buf = buf[:n]
	total := durableRecordSpan(buf)
	if total <= 0 {
		probePool.Put(bp)
		return nil, nil
	}
	if total > len(buf) {
		// Record is larger than the probe: read it exactly into a fresh owned slice
		// and decode that. This is the rare path, so it does not warrant pooling, and
		// the slices it returns already own their backing array.
		probePool.Put(bp)
		full := make([]byte, total)
		m, _ := l.df.f.ReadAt(full, at)
		if m < total {
			return nil, nil // short read: do not decode a truncated record
		}
		return l.decodeAt(full)
	}
	k, v := l.decodeAt(buf)
	if k == nil {
		probePool.Put(bp)
		return nil, nil
	}
	// Copy the decoded key and value into one owned slice before recycling the
	// probe. The result is sized to the record, not the 512-byte probe.
	out := make([]byte, len(k)+len(v))
	copy(out, k)
	copy(out[len(k):], v)
	key, value = out[:len(k):len(k)], out[len(k):]
	probePool.Put(bp)
	return key, value
}

// readEvictedSealed reads the whole page that holds the record at file offset at,
// decrypts its records region, and decodes the record at its within-page offset. It is
// the encrypted counterpart of readEvicted's probe path: a sealed record cannot be read
// in isolation, so the unit is the page. The page number is the file block index, derived
// from at the same way the eviction ref stored it (blockOffset + within).
func (l *log) readEvictedSealed(at int64) (key, value []byte) {
	block := (at - dataStart) / l.pageSize
	within := (at - dataStart) % l.pageSize
	buf := make([]byte, l.pageSize)
	if _, err := l.df.readData(block, buf); err != nil {
		return nil, nil // short read or failed open: report nothing, never ciphertext
	}
	k, v := l.decodeAt(buf[within:])
	if k == nil {
		return nil, nil
	}
	out := make([]byte, len(k)+len(v))
	copy(out, k)
	copy(out[len(k):], v)
	return out[:len(k):len(k)], out[len(k):]
}

// durableRecordSpan returns the total on-log byte length of the durable record at
// the start of b, reading only its length fields, or 0 if b is too short to hold
// even the lengths. It lets readEvicted size an exact read without a full decode.
func durableRecordSpan(b []byte) int {
	if len(b) < 1 {
		return 0
	}
	p := 1 // skip the flags byte
	kl, a := binary.Uvarint(b[p:])
	if a <= 0 {
		return 0
	}
	p += a
	vl, c := binary.Uvarint(b[p:])
	if c <= 0 {
		return 0
	}
	p += c
	return p + int(kl) + int(vl) + 4
}

// recordBytes returns the on-log size of the record at addr, for stranded-byte
// accounting. The caller holds the shard write lock.
func (l *log) recordBytes(addr int64) int {
	key, value := l.read(addr)
	return l.recordLenKV(key, value)
}

// recordLenKV returns the on-log size of an already-decoded key/value pair in
// this log's format, so an overwrite or delete that just read the old record can
// account its stranded bytes without reading it a second time.
func (l *log) recordLenKV(key, value []byte) int {
	if l.df != nil {
		return durableRecordLen(key, value)
	}
	return recordLen(key, value)
}

// decodeRecord splits a lean record buffer into key and value slices.
func decodeRecord(b []byte) (key, value []byte) {
	klen, n := binary.Uvarint(b)
	b = b[n:]
	vlen, n := binary.Uvarint(b)
	b = b[n:]
	return b[:klen], b[klen : klen+vlen]
}

// recordLen is the on-log size of a lean record.
func recordLen(key, value []byte) int {
	return uvarintLen(uint64(len(key))) + uvarintLen(uint64(len(value))) + len(key) + len(value)
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}
