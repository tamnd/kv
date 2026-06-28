package f2

import (
	"encoding/binary"
	"sync/atomic"
)

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
// In both profiles the read path takes no shard lock: it loads the page directory
// atomically and indexes into immutable page refs. Each directory slot is an
// atomic pointer to an immutable pageRef, so eviction (swapping a resident ref
// for an evicted one) never tears a concurrent read, and a reader holding a slice
// into an evicted page stays correct because the garbage collector keeps the
// backing array alive.
type log struct {
	pageSize int64
	hdr      int64 // per-page header bytes reserved at the front, single-file only
	dir      atomic.Pointer[pageDir]
	tail     int64 // next free logical offset, write side only

	df        *durableFile // the one shared file in single-file mode, nil in memory-only
	shardID   int
	pageBlock []int64 // page index to file block, single-file mode only
	budget    int     // resident page budget, 0 means unbounded
	evict     int     // index of the next page to evict, the front of the window
	npages    int     // pages allocated so far
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
func newLog(pageSize int, df *durableFile, shardID, budget int) *log {
	l := &log{
		pageSize: int64(pageSize),
		df:       df,
		shardID:  shardID,
		budget:   budget,
	}
	if df != nil {
		l.hdr = blockHeaderSize
		l.tail = l.hdr // page 0 reserves its header
	}
	d := &pageDir{refs: make([]atomic.Pointer[pageRef], 0, 8)}
	l.dir.Store(d)
	return l
}

// maxRecord is the largest record this log can store, the page minus any header.
func (l *log) maxRecord() int { return int(l.pageSize - l.hdr) }

// append writes one record and returns its logical address and byte length. A
// record never straddles a page: if it would not fit in what is left of the
// current page, the tail jumps to the next page boundary, past that page's
// header, first. The caller holds the shard write lock.
func (l *log) append(key, value []byte, tombstone bool) (int64, int) {
	var n int
	if l.df != nil {
		n = durableRecordLen(key, value)
	} else {
		n = recordLen(key, value)
	}
	off := l.tail
	within := off % l.pageSize
	if within+int64(n) > l.pageSize {
		off += l.pageSize - within // to the next page boundary
		off += l.hdr               // past its header
	}
	pi := int(off / l.pageSize)
	page := l.pageFor(pi)
	w := off % l.pageSize

	if l.df != nil {
		encodeDurable(page[w:], key, value, tombstone)
		l.writeThrough(pi, page, int(w), n)
	} else {
		ww := w
		ww += int64(binary.PutUvarint(page[ww:], uint64(len(key))))
		ww += int64(binary.PutUvarint(page[ww:], uint64(len(value))))
		copy(page[ww:], key)
		ww += int64(len(key))
		copy(page[ww:], value)
	}

	l.tail = off + int64(n)
	return off, n
}

// pageFor returns the resident byte buffer for page pi, allocating new pages up
// to pi and, when a budget is set, evicting the front of the window to stay
// inside it. Because records never straddle pages, pi is the current page or the
// next one. The caller holds the shard write lock.
func (l *log) pageFor(pi int) []byte {
	for l.npages <= pi {
		l.addPage()
	}
	d := l.dir.Load()
	return d.refs[pi].Load().mem
}

// addPage seals the current tail page (single-file Normal/None flush it to disk),
// appends one fresh resident page, and evicts the front while over budget. The
// caller holds the shard write lock.
func (l *log) addPage() {
	if l.df != nil && l.npages > 0 {
		l.sealPage(l.npages - 1) // flush the page we are leaving
	}
	d := l.ensureCap(l.npages + 1)
	buf := make([]byte, l.pageSize)
	pi := l.npages
	if l.df != nil {
		block := l.df.allocBlock()
		l.pageBlock = append(l.pageBlock, block)
		writeBlockHeader(buf, l.shardID, pi)
		if l.df.dial == DurabilityFull {
			// The header must reach disk before any record acknowledged from this
			// page, so a Full crash never leaves a record in an unheadered block.
			_, _ = l.df.f.WriteAt(buf[:blockHeaderSize], l.df.blockOffset(block))
			_ = l.df.f.Sync()
		}
	}
	ref := &pageRef{mem: buf}
	d.refs[pi].Store(ref)
	l.npages++
	for l.budget > 0 && l.npages-l.evict > l.budget {
		l.evictFront()
	}
}

// writeThrough flushes a single record to disk and, under Full, fsyncs it, so an
// acknowledged Set is on stable storage before it returns. Under Normal and None
// the bytes wait in the resident page and reach disk when the page is sealed. The
// caller holds the shard write lock.
func (l *log) writeThrough(pi int, page []byte, w, n int) {
	if l.df.dial != DurabilityFull {
		return
	}
	off := l.df.blockOffset(l.pageBlock[pi]) + int64(w)
	_, _ = l.df.f.WriteAt(page[w:w+n], off)
	_ = l.df.f.Sync()
}

// sealPage writes a full page to its block and, under Normal, fsyncs it. Sealing
// happens when the writer moves on to the next page, so the page is complete. The
// caller holds the shard write lock.
func (l *log) sealPage(pi int) {
	if l.df.dial == DurabilityFull {
		return // already written through
	}
	d := l.dir.Load()
	ref := d.refs[pi].Load()
	if ref.mem == nil {
		return // already evicted, hence already on disk
	}
	_, _ = l.df.f.WriteAt(ref.mem, l.df.blockOffset(l.pageBlock[pi]))
	if l.df.dial == DurabilityNormal {
		_ = l.df.f.Sync()
	}
}

// flushTail writes the current tail page to disk, used by Checkpoint and Close so
// records sitting only in the resident tail page reach stable storage. The caller
// holds the shard write lock.
func (l *log) flushTail() {
	if l.df == nil || l.npages == 0 {
		return
	}
	pi := l.npages - 1
	d := l.dir.Load()
	ref := d.refs[pi].Load()
	if ref.mem == nil {
		return
	}
	_, _ = l.df.f.WriteAt(ref.mem, l.df.blockOffset(l.pageBlock[pi]))
}

// evictFront drops the front resident page from RAM. The page is already on disk
// in its block, so eviction only repoints the ref to the block offset; a later
// read of that page preads it back. The atomic store is what makes this safe for
// a concurrent reader. The caller holds the shard write lock.
func (l *log) evictFront() {
	d := l.dir.Load()
	fileOff := l.df.blockOffset(l.pageBlock[l.evict])
	d.refs[l.evict].Store(&pageRef{fileOff: fileOff})
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
		k, v, _, _, _ := decodeDurable(b)
		return k, v
	}
	return decodeRecord(b)
}

// readEvicted preads a record from the file into an owned buffer. It does one
// probe read sized to cover most records in a single syscall and allocation; only
// a record larger than the probe needs a second exact read. The probe may overrun
// into the next record or the page tail, which is harmless because a record never
// straddles a page and decode consumes only its own bytes.
const evictProbe = 512

func (l *log) readEvicted(at int64) (key, value []byte) {
	buf := make([]byte, evictProbe)
	n, _ := l.df.f.ReadAt(buf, at)
	buf = buf[:n]
	if total := durableRecordSpan(buf); total > 0 && total <= len(buf) {
		return l.decodeAt(buf) // the probe already holds the whole record
	} else if total > len(buf) {
		full := make([]byte, total)
		_, _ = l.df.f.ReadAt(full, at)
		return l.decodeAt(full)
	}
	return nil, nil
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
