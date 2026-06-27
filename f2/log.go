package f2

import (
	"encoding/binary"
	"sync/atomic"
)

// log is one shard's append-only record store, laid out as fixed-size pages. A
// logical address is a byte offset into the flattened page sequence: page index
// is addr/pageSize, the byte within is addr%pageSize. Pages are allocated whole
// and never moved or resized, so a record's bytes stay put for the life of the
// store, which is what lets a reader alias them without a copy or a lock.
//
// The page directory sits behind an atomic pointer. A writer that needs a new
// page builds a longer directory slice and publishes it with one atomic store; a
// reader loads the directory once and indexes into it. Because a record is only
// ever read after its slot was published, and a slot is published only after the
// record's bytes were written, a reader never observes a half-written record.
type log struct {
	pageSize int64
	dir      atomic.Pointer[[][]byte] // the page directory, published atomically
	tail     int64                    // next free logical offset, write side only
}

func newLog(pageSize int) *log {
	l := &log{pageSize: int64(pageSize)}
	first := [][]byte{make([]byte, pageSize)}
	l.dir.Store(&first)
	return l
}

// append writes key and value as one record and returns its logical address and
// byte length. A record never straddles a page: if it would not fit in what is
// left of the current page, the tail jumps to the next page boundary first. The
// caller holds the shard write lock, so tail and the directory move serially.
func (l *log) append(key, value []byte) (int64, int) {
	n := recordLen(key, value)
	off := l.tail
	within := off % l.pageSize
	if within+int64(n) > l.pageSize {
		// Not enough room left on this page; pad to the next page boundary.
		off += l.pageSize - within
	}
	pi := off / l.pageSize
	l.ensurePage(int(pi))
	dir := *l.dir.Load()
	page := dir[pi]
	w := off % l.pageSize

	w += int64(binary.PutUvarint(page[w:], uint64(len(key))))
	w += int64(binary.PutUvarint(page[w:], uint64(len(value))))
	copy(page[w:], key)
	w += int64(len(key))
	copy(page[w:], value)

	l.tail = off + int64(n)
	return off, n
}

// ensurePage makes sure a page exists at index pi, extending the directory if
// needed. New pages are appended one at a time as the tail advances, so pi is at
// most one past the current end. The caller holds the shard write lock.
func (l *log) ensurePage(pi int) {
	dir := *l.dir.Load()
	if pi < len(dir) {
		return
	}
	nd := make([][]byte, pi+1)
	copy(nd, dir)
	for i := len(dir); i <= pi; i++ {
		nd[i] = make([]byte, l.pageSize)
	}
	l.dir.Store(&nd)
}

// read returns the key and value of the record at addr as slices aliasing the
// log page. In the full-resident profile the page is immutable, so the slices
// stay valid and must not be mutated by the caller.
func (l *log) read(addr int64) (key, value []byte) {
	dir := *l.dir.Load()
	page := dir[addr/l.pageSize]
	off := addr % l.pageSize
	klen, n := binary.Uvarint(page[off:])
	off += int64(n)
	vlen, n := binary.Uvarint(page[off:])
	off += int64(n)
	key = page[off : off+int64(klen)]
	off += int64(klen)
	value = page[off : off+int64(vlen)]
	return key, value
}

// recordBytes returns the on-log size of the record at addr, used to account
// stranded bytes when an overwrite or delete orphans it. The caller holds the
// shard write lock.
func (l *log) recordBytes(addr int64) int {
	dir := *l.dir.Load()
	page := dir[addr/l.pageSize]
	off := addr % l.pageSize
	klen, n := binary.Uvarint(page[off:])
	hdr := n
	vlen, n := binary.Uvarint(page[off+int64(n):])
	hdr += n
	return hdr + int(klen) + int(vlen)
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
