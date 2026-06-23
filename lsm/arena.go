package lsm

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
)

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
// Allocation is lock-free on its common path. The parallel-apply path inserts from many
// goroutines at once, and a mutex around every allocation would serialize them and erase
// the win: a profile of an early build showed the insert wall-clock pinned by lock
// handoff on the allocator, with the skip-list descent that was meant to spread across
// cores waiting its turn behind the lock. So the bump is a single atomic cursor advanced
// by compare-and-swap, and the only mutex is taken on the rare event of appending a new
// block (once per megabyte) or laying down an oversized allocation. The cursor packs the
// current block index in its high 32 bits and the next free offset within that block in
// its low 32, so one CAS both reserves the bytes and reports where they are. Readers never
// touch the cursor at all: they navigate the skip list and resolve an offset against the
// block snapshot, which is published through an atomic pointer and only ever grows.
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

// packCursor and the unpackers move a (blockIndex, within) pair through the single atomic
// cursor word. within is kept explicit, never derived by masking the offset, so the
// exact-boundary case where an allocation fills a block to its final byte leaves within at
// blockSize and forces the next allocation onto a new block, instead of silently decoding a
// within of zero in a block that was never grown.
func packCursor(blockIdx, within uint32) uint64 { return uint64(blockIdx)<<32 | uint64(within) }
func cursorBlock(c uint64) uint32               { return uint32(c >> 32) }
func cursorWithin(c uint64) uint32              { return uint32(c) }

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
//
// cur is the atomic bump cursor; blocks is the published, only-growing block snapshot; used
// is the running footprint, bumped atomically. mu serializes only the two rare structural
// events, growing the block list and laying down an oversized block, so the snapshot is
// never replaced concurrently.
type arena struct {
	mu     sync.Mutex
	blocks atomic.Pointer[[][]byte] // immutable snapshot, replaced under mu on grow
	cur    atomic.Uint64            // packed (blockIndex, within), advanced lock-free by CAS
	used   atomic.Int64             // bytes handed out, the seal-threshold footprint
}

// newArena returns an arena with its first 1 MiB block in place and byte 0 burned as the
// nil sentinel. The capacity argument is kept for call-site compatibility but no longer
// sizes anything: blocks are fixed at blockSize so the offset arithmetic stays a shift
// and a mask.
func newArena(capacity int) *arena {
	_ = capacity
	a := &arena{}
	blocks := [][]byte{make([]byte, blockSize)}
	a.blocks.Store(&blocks)
	a.cur.Store(packCursor(0, 1)) // block 0, byte 0 burned
	a.used.Store(1)
	return a
}

// alloc reserves size bytes and returns the global offset of the first. The common path is
// a lock-free CAS that advances the cursor within the current block. When the allocation
// would cross the block end, grow appends a fresh block under mu and the loop retries on the
// new block; an allocation larger than a block takes the oversized path, which lays down its
// own right-sized block under mu.
func (a *arena) alloc(size int) uint32 {
	if size > blockSize {
		return a.allocOversized(size)
	}
	for {
		old := a.cur.Load()
		blockIdx, within := cursorBlock(old), cursorWithin(old)
		if int(within)+size <= blockSize {
			if a.cur.CompareAndSwap(old, packCursor(blockIdx, within+uint32(size))) {
				a.used.Add(int64(size))
				return blockIdx<<blockShift | within
			}
			continue // lost the race, reload and retry
		}
		a.grow(blockIdx) // current block is full; append the next one
	}
}

// grow appends one fresh block after the block fullIdx, then points the cursor at its start.
// It double-checks under mu that the block list has not already grown past fullIdx, so two
// allocators that both find the same block full append only one block between them. The
// cursor is published after the block, so a winner of the next CAS names a block that exists.
func (a *arena) grow(fullIdx uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cur := *a.blocks.Load()
	if uint32(len(cur)-1) > fullIdx {
		return // another allocator already appended; retry will find the room
	}
	next := make([][]byte, len(cur), len(cur)+1)
	copy(next, cur)
	next = append(next, make([]byte, blockSize))
	a.blocks.Store(&next)
	a.cur.Store(packCursor(uint32(len(next)-1), 0))
}

// allocOversized lays down a single allocation larger than a block as its own right-sized
// block, followed by nil filler indices that cover its physical spill so no later offset
// decodes into its tail, then a fresh bump block the cursor is moved onto. It runs under mu
// because it replaces the block snapshot; a concurrent CAS bump either loses to the cursor
// store and retries on the new bump block, or wins first and keeps its offset in the old
// block, which still exists. Either way no two allocations overlap.
func (a *arena) allocOversized(size int) uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	cur := *a.blocks.Load()
	oidx := uint32(len(cur))
	next := make([][]byte, len(cur), len(cur)+2)
	copy(next, cur)
	next = append(next, make([]byte, size))
	span := uint32((size + blockSize - 1) >> blockShift)
	for k := uint32(1); k < span; k++ {
		next = append(next, nil)
	}
	next = append(next, make([]byte, blockSize)) // fresh bump block after it
	a.blocks.Store(&next)
	a.cur.Store(packCursor(uint32(len(next)-1), 0))
	a.used.Add(int64(size))
	return oidx << blockShift
}

// size reports the bytes the arena has handed out, the memtable's footprint used to
// decide when to seal it. It counts allocated bytes, not the wasted block tails, so the
// seal fires on real data rather than on internal fragmentation. It is an atomic load,
// since used is bumped atomically by concurrent allocators.
func (a *arena) size() int { return int(a.used.Load()) }

// bytesAt returns the n-byte slice that starts at off. The slice points into the block's
// fixed backing array, so it is stable for the life of the memtable and may be held by a
// reader. The allocation that produced off reserved n contiguous bytes in one block, so
// the slice never straddles a block boundary. The snapshot load is atomic and lock-free;
// the block it indexes was published before the node carrying off was, so it is present.
func (a *arena) bytesAt(off uint32, n int) []byte {
	blocks := *a.blocks.Load()
	within := off & blockMask
	buf := blocks[off>>blockShift]
	return buf[within : int(within)+n]
}

// putU32 and getU32 read and write a little-endian uint32 header field at off. The field
// was reserved by a single alloc, so its four bytes lie within one block.
func (a *arena) putU32(off uint32, v uint32) {
	blocks := *a.blocks.Load()
	within := off & blockMask
	binary.LittleEndian.PutUint32(blocks[off>>blockShift][within:within+4], v)
}

func (a *arena) getU32(off uint32) uint32 {
	blocks := *a.blocks.Load()
	within := off & blockMask
	return binary.LittleEndian.Uint32(blocks[off>>blockShift][within : within+4])
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
//
// It bumps lock-free exactly like the byte arena: an atomic packed cursor advanced by CAS,
// with a mutex taken only to append a block. Keeping the within counter explicit in the
// cursor (rather than masking the offset) is what makes the exact-boundary case correct,
// where a tower fills a block to its last slot; the next allocation then sees within at
// u32Size and grows, instead of decoding a base into a block that was never allocated.
type uint32Arena struct {
	mu     sync.Mutex
	blocks atomic.Pointer[[][]uint32] // immutable snapshot, replaced under mu on grow
	cur    atomic.Uint64              // packed (blockIndex, within), advanced lock-free by CAS
}

// newUint32Arena returns a tower arena with its first block in place and slot 0 burned as
// the nil sentinel.
func newUint32Arena() *uint32Arena {
	u := &uint32Arena{}
	blocks := [][]uint32{make([]uint32, u32Size)}
	u.blocks.Store(&blocks)
	u.cur.Store(packCursor(0, 1)) // block 0, slot 0 burned
	return u
}

// alloc reserves n contiguous slots and returns the index of the first. A tower is small
// enough that a single allocation always fits in one block, so a request that would cross
// the block end grows a fresh block and the loop retries. The common path is a lock-free
// CAS; only the block append takes mu.
func (u *uint32Arena) alloc(n int) uint32 {
	for {
		old := u.cur.Load()
		blockIdx, within := cursorBlock(old), cursorWithin(old)
		if int(within)+n <= u32Size {
			if u.cur.CompareAndSwap(old, packCursor(blockIdx, within+uint32(n))) {
				return blockIdx<<u32Shift | within
			}
			continue
		}
		u.grow(blockIdx)
	}
}

// grow appends one fresh tower block after fullIdx and moves the cursor onto it, double-
// checking under mu so concurrent growers append only one block between them.
func (u *uint32Arena) grow(fullIdx uint32) {
	u.mu.Lock()
	defer u.mu.Unlock()
	cur := *u.blocks.Load()
	if uint32(len(cur)-1) > fullIdx {
		return
	}
	next := make([][]uint32, len(cur), len(cur)+1)
	copy(next, cur)
	next = append(next, make([]uint32, u32Size))
	u.blocks.Store(&next)
	u.cur.Store(packCursor(uint32(len(next)-1), 0))
}

// slot returns a pointer to the uint32 at index i. The pointer is stable for the
// memtable's life because the block never moves, and it is 4-byte aligned because it is a
// []uint32 element, so the atomic accessors below may operate on it. The snapshot load is
// atomic and lock-free.
func (u *uint32Arena) slot(i uint32) *uint32 {
	blocks := *u.blocks.Load()
	return &blocks[i>>u32Shift][i&u32Mask]
}

// load, store, and cas are the atomic forward-pointer operations the concurrent skip list
// links and reads tower slots with. A concurrent insert publishes a node by compare-and-
// swapping its predecessor's slot from the old successor to the new node, and every reader
// loads slots atomically, so a reader either sees the old link or the fully-initialized new
// node, never a torn pointer (perf/03 W1).
func (u *uint32Arena) load(i uint32) uint32     { return atomic.LoadUint32(u.slot(i)) }
func (u *uint32Arena) store(i uint32, v uint32) { atomic.StoreUint32(u.slot(i), v) }
func (u *uint32Arena) cas(i, old, newv uint32) bool {
	return atomic.CompareAndSwapUint32(u.slot(i), old, newv)
}
