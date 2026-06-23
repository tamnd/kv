package lsm

import "encoding/binary"

// The memtable's nodes live in two never-moving bump arenas: this byte arena holds a
// node's header, key, and value, and the parallel uint32Arena holds its forward-pointer
// tower. "Never-moving" is the load-bearing property: a node, once allocated, keeps its
// bytes at a fixed address for the life of the memtable, so a reader may hold a slice
// into the arena without a lock even while other goroutines allocate (perf/03 W1,
// perf/07). The earlier arena was one []byte grown by reallocation, and a realloc copies
// the bytes to a fresh backing array, which is sound only while no reader holds a slice
// into the old one; the concurrent memtable is exactly that reader, so reallocation is
// out and the arena is a list of fixed blocks instead.
//
// blockShift sets the chunk size to 1 MiB, the same figure defaultArenaCap used as the
// old arena's starting size, so a tiny database still pays one 1 MiB block and no more.
// A uint32 offset partitions into a block index (the bits above blockShift) and a
// within-block offset (the low blockShift bits), so resolving an offset to bytes is a
// shift and a mask, no search.
const (
	blockShift = 20
	blockSize  = 1 << blockShift
	blockMask  = blockSize - 1
)

// arena is a never-moving bump allocator over a list of fixed 1 MiB blocks. Allocations
// bump within the current block; one that would cross the block's end starts a fresh
// block, wasting the short tail. A single allocation larger than a block gets its own
// right-sized block, so an oversized value written to the memtable before flush still
// lands contiguously. Blocks are allocated once and never moved or freed until the whole
// memtable is dropped.
//
// Allocations are addressed by uint32 offset, never by Go pointer, so the value is an
// index naming (block, within) that survives any number of later allocations. Offset 0
// is the nil sentinel: the first block burns its byte 0 so a real allocation never starts
// there and a zero forward pointer unambiguously means "no node". Every stored offset is
// the start of an allocation, and a start always sits at within < blockSize (a normal
// allocation fits inside one block; an oversized one starts at within 0 of its own
// block), so the block index decodes correctly even when an oversized block physically
// spills past blockSize: bytes are only ever sliced forward from a start offset on that
// same block.
// The bump target is always the last block in blocks; within is the next free byte in it,
// so the invariant "blocks[len(blocks)-1] exists and has within bytes used" holds after
// every call and a global offset is (lastBlockIndex << blockShift) + within with within
// always below blockSize.
type arena struct {
	blocks [][]byte
	within uint32 // next free byte within the last (bump) block
	used   int    // bytes actually handed out, the seal-threshold footprint
}

// newArena returns an arena with its first 1 MiB block in place and byte 0 burned as the
// nil sentinel. The capacity argument is kept for call-site compatibility but no longer
// sizes anything: blocks are fixed at blockSize so the offset arithmetic stays a shift
// and a mask.
func newArena(capacity int) *arena {
	_ = capacity
	a := &arena{}
	a.blocks = append(a.blocks, make([]byte, blockSize))
	a.within = 1
	a.used = 1
	return a
}

// alloc reserves size bytes and returns the global offset of the first. Within the current
// block it is a pointer bump; when the allocation would cross the block's end it opens a
// fresh block and bumps there. An allocation larger than a block gets its own right-sized
// block, followed by filler indices that cover the physical spill so the next bump block's
// virtual range never overlaps it, then a fresh bump block. Because callers hold offsets,
// never pointers, every new block is invisible to existing references.
func (a *arena) alloc(size int) uint32 {
	a.used += size
	if size > blockSize {
		oidx := uint32(len(a.blocks))
		a.blocks = append(a.blocks, make([]byte, size))
		// The oversized block physically spans ceil(size/blockSize) block indices; reserve
		// the extra ones with nil fillers so no later allocation is handed a virtual offset
		// that decodes into the oversized block's tail.
		span := uint32((size + blockSize - 1) >> blockShift)
		for k := uint32(1); k < span; k++ {
			a.blocks = append(a.blocks, nil)
		}
		a.blocks = append(a.blocks, make([]byte, blockSize)) // fresh bump block after it
		a.within = 0
		return oidx << blockShift
	}
	if int(a.within)+size > blockSize {
		a.blocks = append(a.blocks, make([]byte, blockSize))
		a.within = 0
	}
	off := (uint32(len(a.blocks)-1) << blockShift) + a.within
	a.within += uint32(size)
	return off
}

// size reports the bytes the arena has handed out, the memtable's footprint used to
// decide when to seal it. It counts allocated bytes, not the wasted block tails, so the
// seal fires on real data rather than on internal fragmentation.
func (a *arena) size() int { return a.used }

// bytesAt returns the n-byte slice that starts at off. The slice points into the block's
// fixed backing array, so it is stable for the life of the memtable and may be held by a
// reader. The allocation that produced off reserved n contiguous bytes in one block, so
// the slice never straddles a block boundary.
func (a *arena) bytesAt(off uint32, n int) []byte {
	within := off & blockMask
	buf := a.blocks[off>>blockShift]
	return buf[within : int(within)+n]
}

// putU32 and getU32 read and write a little-endian uint32 header field at off. The field
// was reserved by a single alloc, so its four bytes lie within one block.
func (a *arena) putU32(off uint32, v uint32) {
	within := off & blockMask
	binary.LittleEndian.PutUint32(a.blocks[off>>blockShift][within:within+4], v)
}

func (a *arena) getU32(off uint32) uint32 {
	within := off & blockMask
	return binary.LittleEndian.Uint32(a.blocks[off>>blockShift][within : within+4])
}

// u32Shift chunks the tower arena into 1 MiB blocks of 256K uint32 slots. A tower is at
// most maxHeight slots, far smaller than a block, so the tower arena never needs the
// oversized path the byte arena carries; an allocation always fits in the current block.
const (
	u32Shift = 18
	u32Size  = 1 << u32Shift
	u32Mask  = u32Size - 1
)

// uint32Arena is the never-moving home of every node's forward-pointer tower, held apart
// from the byte arena for one reason: atomic access. The concurrent skip list reads and
// writes tower slots with sync/atomic, which requires each slot to be 4-byte aligned, and
// a []uint32 element is naturally aligned where a tower packed into the byte arena at an
// arbitrary node offset is not. Slots are uint32 offsets the same shape as the byte arena
// uses, with slot 0 burned so a zero tower entry means nil. Blocks are zero-filled by
// make and never reused, so a freshly allocated tower reads as all-nil without explicit
// clearing.
type uint32Arena struct {
	blocks [][]uint32
	cur    uint32
}

// newUint32Arena returns a tower arena with its first block in place and slot 0 burned as
// the nil sentinel.
func newUint32Arena() *uint32Arena {
	u := &uint32Arena{}
	u.blocks = append(u.blocks, make([]uint32, u32Size))
	u.cur = 1
	return u
}

// alloc reserves n contiguous slots and returns the index of the first. A tower is small
// enough that a single allocation always fits in one block, so a request that would cross
// the block end simply opens a fresh block and allocates at its start.
func (u *uint32Arena) alloc(n int) uint32 {
	within := u.cur & u32Mask
	if int(within)+n > u32Size {
		idx := uint32(len(u.blocks))
		u.blocks = append(u.blocks, make([]uint32, u32Size))
		base := idx << u32Shift
		u.cur = base + uint32(n)
		return base
	}
	base := u.cur
	u.cur += uint32(n)
	return base
}

// slot returns a pointer to the uint32 at index i, the cell the skip list reads and writes
// (atomically, in the concurrent version). The pointer is stable for the memtable's life
// because the block never moves.
func (u *uint32Arena) slot(i uint32) *uint32 {
	return &u.blocks[i>>u32Shift][i&u32Mask]
}
