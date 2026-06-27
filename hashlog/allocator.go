package hashlog

import "sync"

// allocator owns the extent pool (doc 03 section 4). It hands out extents to shards
// that are appending and to checkpoints that are writing snapshots, and it takes
// extents back from the compactor. Its state is persisted in the superblock slot so
// a crash cannot leak an extent (free in memory, never recorded) or double-allocate
// one (recorded free, also handed out).
//
// The free list is an array-backed LIFO stack of free extent ids held in memory:
// allocation is a pop, freeing is a push, both O(1) with no I/O, so the allocator
// stays off the hot append path (only a roll allocates, and a roll is rare relative
// to appends). LIFO rather than FIFO because a recently freed extent is likely still
// in the OS page cache, so reusing it soon is cache-friendly; liveness is enforced
// by epochs (doc 07), not by free-list order.
type allocator struct {
	mu    sync.Mutex
	free  []int64 // stack of free extent ids; alloc pops the end, free pushes the end
	count int64   // number of extents the file currently holds
}

// newAllocator builds an allocator with a given extent count and free stack, as
// reconstructed from a superblock slot on open.
func newAllocator(count int64, free []int64) *allocator {
	a := &allocator{count: count}
	if len(free) > 0 {
		a.free = append(a.free, free...)
	}
	return a
}

// alloc returns an extent id for a caller that is about to write into it. It pops a
// freed id when one is available, otherwise grows the pool by one and returns the
// new id. The caller (the durableFile) is responsible for making the new extent's
// bytes exist in the file before writing records into it; the allocator only owns
// the id bookkeeping (doc 03 section 4). grew reports whether the pool grew, so the
// caller knows it must extend the file rather than reuse existing bytes.
func (a *allocator) alloc() (id int64, grew bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n := len(a.free); n > 0 {
		id = a.free[n-1]
		a.free = a.free[:n-1]
		return id, false
	}
	id = a.count
	a.count++
	return id, true
}

// freeExtent pushes an extent id back onto the free stack, making it eligible for a
// later alloc. The caller guarantees the extent holds no live reference when it is
// freed (the copy-then-repoint-then-epoch-drain ordering of doc 03 section 4, owned
// by the compactor at M8); the allocator only records the id.
func (a *allocator) freeExtent(id int64) {
	a.mu.Lock()
	a.free = append(a.free, id)
	a.mu.Unlock()
}

// counts returns the extent count and a copy of the free stack, for persisting into
// a superblock slot. The copy is taken under the lock so a concurrent alloc or free
// cannot tear the snapshot.
func (a *allocator) counts() (count int64, free []int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	free = make([]int64, len(a.free))
	copy(free, a.free)
	return a.count, free
}

// inUse returns the number of extents currently handed out: the total minus the
// free stack. It is the conservation check's "in use" term: len(free) + inUse must
// equal count (doc 03 section 9 I7).
func (a *allocator) inUse() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.count - int64(len(a.free))
}
