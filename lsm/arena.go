package lsm

import "encoding/binary"

// arena is a bump allocator backing the memtable's skip list (spec 06 §2). The
// whole memtable is a handful of large allocations rather than one allocation per
// node, which keeps the skip list off the garbage collector's per-object radar:
// the GC sees one []byte, not millions of nodes (the Pebble/Badger lesson, ADR-8).
//
// Allocations are handed out as uint32 byte offsets into buf, never as Go
// pointers, so the arena may grow by copying into a larger backing slice without
// invalidating any reference: an offset is an index, and an index survives a
// reallocation. Offset 0 is reserved as the nil sentinel, so a real allocation
// never starts at 0 and a zero forward pointer unambiguously means "no node".
type arena struct {
	buf []byte
	// n is the high-water allocation cursor: buf[:n] is allocated, buf[n:] is free.
	n uint32
}

// newArena returns an arena sized to an initial capacity. It burns offset 0 so the
// nil sentinel is distinguishable from a genuine allocation at the start of the
// buffer.
func newArena(capacity int) *arena {
	if capacity < 64 {
		capacity = 64
	}
	a := &arena{buf: make([]byte, capacity), n: 1}
	return a
}

// alloc reserves size bytes and returns the offset of the first. It grows the
// backing slice geometrically when the bump cursor would overflow; because callers
// hold offsets, never pointers, the copy into the larger slice is transparent.
func (a *arena) alloc(size int) uint32 {
	off := a.n
	end := int(off) + size
	if end > len(a.buf) {
		grown := len(a.buf) * 2
		for grown < end {
			grown *= 2
		}
		bigger := make([]byte, grown)
		copy(bigger, a.buf[:a.n])
		a.buf = bigger
	}
	a.n = uint32(end)
	return off
}

// size reports the bytes the arena has handed out, the memtable's in-memory
// footprint used to decide when to seal it (spec 06 §2).
func (a *arena) size() int { return int(a.n) }

// the small fixed-width accessors below read and write the integer fields of a
// node header at a given offset. They centralize the endianness so the skip list
// never touches binary.LittleEndian directly.

func (a *arena) putU32(off uint32, v uint32) {
	binary.LittleEndian.PutUint32(a.buf[off:off+4], v)
}

func (a *arena) getU32(off uint32) uint32 {
	return binary.LittleEndian.Uint32(a.buf[off : off+4])
}
