package f2

import (
	"encoding/binary"
	"os"
	"sync/atomic"
)

// log is one shard's append-only record store, laid out as fixed-size pages. A
// logical address is a byte offset into the flattened page sequence: page index
// is addr/pageSize, the byte within is addr%pageSize. Pages are allocated whole
// and never moved or resized.
//
// The store has two profiles that share this one log type. In the full-resident
// profile every page stays in RAM, so a reader slices a record straight out of
// its page with no copy and no lock. In the larger-than-memory profile the log
// keeps only the newest budget of pages resident and spills the rest, oldest
// first, to a per-shard scratch file; a read of a spilled record preads it from
// the file into a fresh buffer. Either way the read path takes no shard lock: it
// loads the page directory atomically and indexes into immutable page refs.
//
// Page refs make the spill safe without an epoch reclaimer. Each directory slot
// is an atomic pointer to an immutable pageRef. To spill a page the writer writes
// its bytes to the file and atomically swaps the slot to point at a new ref that
// names the file offset; a reader sees either the old resident ref or the new
// spilled one, never a torn value. A reader that already holds a slice into an
// evicted page keeps it valid because Go's garbage collector will not reclaim the
// backing array while the slice is reachable, which is the role FASTER's epoch
// protection plays in a manually managed runtime.
type log struct {
	pageSize int64
	dir      atomic.Pointer[pageDir] // page directory, grown by doubling
	tail     int64                   // next free logical offset, write side only

	// Spill state, all write side, guarded by the shard lock. spill is nil in the
	// full-resident profile and the rest is unused.
	spill     *os.File
	spillTail int64 // next free byte offset in the spill file
	evict     int   // index of the next page to spill, the front of the window
	budget    int   // resident page budget, 0 means unbounded
	npages    int   // pages allocated so far
}

// pageRef is one page's location. It is immutable once published: a resident page
// has mem set, a spilled page has mem nil and fileOff naming its bytes in the
// spill file. Spilling replaces the ref, it never mutates one in place.
type pageRef struct {
	mem     []byte
	fileOff int64
}

// pageDir is the indexable directory of page refs. The refs slice is replaced
// only when it must grow, which doubles, so growth is amortized constant; routine
// spilling swaps individual slots in place with atomic stores and does not touch
// the directory pointer.
type pageDir struct {
	refs []atomic.Pointer[pageRef]
}

// newLog builds a memory-only log when spill is nil, or a spilling log bounded to
// budget resident pages when spill names a scratch file.
func newLog(pageSize int, spill *os.File, budget int) *log {
	l := &log{pageSize: int64(pageSize), spill: spill, budget: budget}
	d := &pageDir{refs: make([]atomic.Pointer[pageRef], 0, 8)}
	l.dir.Store(d)
	return l
}

// append writes key and value as one record and returns its logical address and
// byte length. A record never straddles a page: if it would not fit in what is
// left of the current page, the tail jumps to the next page boundary first. The
// caller holds the shard write lock, so the tail, the directory, and the spill
// state all advance serially.
func (l *log) append(key, value []byte) (int64, int) {
	n := recordLen(key, value)
	off := l.tail
	within := off % l.pageSize
	if within+int64(n) > l.pageSize {
		off += l.pageSize - within // pad to the next page boundary
	}
	pi := int(off / l.pageSize)
	page := l.pageFor(pi)
	w := off % l.pageSize

	w += int64(binary.PutUvarint(page[w:], uint64(len(key))))
	w += int64(binary.PutUvarint(page[w:], uint64(len(value))))
	copy(page[w:], key)
	w += int64(len(key))
	copy(page[w:], value)

	l.tail = off + int64(n)
	return off, n
}

// pageFor returns the resident byte buffer for page pi, allocating new pages up
// to pi and spilling the front of the window to stay inside the budget. Because
// records never straddle pages, pi is the current page or the next one, so at
// most one page is allocated per call. The caller holds the shard write lock.
func (l *log) pageFor(pi int) []byte {
	for l.npages <= pi {
		l.addPage()
	}
	d := l.dir.Load()
	return d.refs[pi].Load().mem
}

// addPage appends one fresh resident page and, if that pushes the resident window
// past the budget, spills the oldest still-resident page. The caller holds the
// shard write lock.
func (l *log) addPage() {
	d := l.ensureCap(l.npages + 1)
	ref := &pageRef{mem: make([]byte, l.pageSize)}
	d.refs[l.npages].Store(ref)
	l.npages++
	// Keep at most budget pages resident, including the tail just added. budget 0
	// means unbounded, so nothing is ever spilled.
	for l.budget > 0 && l.npages-l.evict > l.budget {
		l.spillFront()
	}
}

// spillFront writes the front resident page to the scratch file and swaps its
// directory slot to a spilled ref. The atomic store is what makes this safe for a
// concurrent reader. The caller holds the shard write lock.
func (l *log) spillFront() {
	d := l.dir.Load()
	old := d.refs[l.evict].Load()
	// A best-effort write; a scratch-file write error leaves the page resident, so
	// reads stay correct and the budget is simply exceeded until space frees.
	if _, err := l.spill.WriteAt(old.mem, l.spillTail); err != nil {
		return
	}
	spilled := &pageRef{fileOff: l.spillTail}
	d.refs[l.evict].Store(spilled)
	l.spillTail += l.pageSize
	l.evict++
}

// ensureCap returns a directory whose refs slice has room for n pages, doubling
// and republishing it if needed. Growth copies the existing atomic slots by
// loading and storing each, so a concurrent reader that holds the old directory
// keeps reading valid refs while the new one is built. The caller holds the shard
// write lock.
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
// slices alias the page and must not be mutated; for a spilled page they are
// owned copies the caller may keep. Either way no shard lock is taken.
func (l *log) read(addr int64) (key, value []byte) {
	d := l.dir.Load()
	pi := addr / l.pageSize
	within := addr % l.pageSize
	ref := d.refs[pi].Load()
	if ref.mem != nil {
		return decodeRecord(ref.mem[within:])
	}
	return l.readSpilled(ref.fileOff + within)
}

// readSpilled preads a record from the scratch file. It reads a small header
// window first to learn the key and value lengths, then reads the exact record
// bytes into an owned buffer. The record never straddles a page, so the bytes are
// all within the page the offset lands in.
func (l *log) readSpilled(at int64) (key, value []byte) {
	var hdr [2 * binary.MaxVarintLen64]byte
	n, _ := l.spill.ReadAt(hdr[:], at)
	klen, a := binary.Uvarint(hdr[:n])
	vlen, b := binary.Uvarint(hdr[a:n])
	hbytes := a + b
	total := hbytes + int(klen) + int(vlen)
	buf := make([]byte, total)
	_, _ = l.spill.ReadAt(buf, at)
	return decodeRecord(buf)
}

// recordBytes returns the on-log size of the record at addr, used to account
// stranded bytes when an overwrite or delete orphans it. The caller holds the
// shard write lock.
func (l *log) recordBytes(addr int64) int {
	key, value := l.read(addr)
	return recordLen(key, value)
}

// decodeRecord splits a record buffer into its key and value slices, aliasing the
// buffer.
func decodeRecord(b []byte) (key, value []byte) {
	klen, n := binary.Uvarint(b)
	b = b[n:]
	vlen, n := binary.Uvarint(b)
	b = b[n:]
	return b[:klen], b[klen : klen+vlen]
}

// recordLen is the on-log size a record will occupy: the two uvarint length
// prefixes plus the key and value bytes.
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
