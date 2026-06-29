package hashlog

import (
	"sort"
	"sync"
)

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

// allocRun returns the first id of n contiguous extents, for a caller that wants one
// contiguous byte region (the index snapshot, doc 05 section 4: written across a run
// and read back as one region, no extent chain). It first looks for a run of n
// consecutive free ids and carves it out; failing that it grows the pool by n from the
// tail. grew reports whether the pool grew, so the caller knows it must extend the
// file. A contiguous run keeps the snapshot a single ReadAt at recovery and a single
// WriteAt at checkpoint.
func (a *allocator) allocRun(n int64) (first int64, grew bool) {
	if n <= 0 {
		n = 1
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	// Fast path: the run a checkpoint most often wants is the one the previous snapshot
	// just freed. freeRun pushes a run's ids ascending onto the tail of the stack, so in
	// the steady state (a periodic snapshot of roughly constant size) the last n stack
	// entries are exactly a contiguous ascending run. Carve them straight off the tail in
	// O(n), no sort and no scratch allocation. Any n consecutive ids form a valid
	// contiguous run, so this is correct even when the tail is not the most-recently
	// freed run; removing the tail slice preserves the LIFO order of what remains.
	if m := int64(len(a.free)); m >= n {
		if tail := a.free[m-n:]; isAscendingRun(tail) {
			first = tail[0]
			a.free = a.free[:m-n]
			return first, false
		}
	}

	// General path: the tail was not a ready run (a snapshot that changed size), so scan
	// for any contiguous run of n free ids and carve it out rather than grow the file.
	// This is off the steady-state path, so the sort is acceptable; the carved ids are a
	// known contiguous range, so the rebuild filters by range with no scratch map.
	if int64(len(a.free)) >= n {
		sorted := append([]int64(nil), a.free...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		runStart := 0
		for i := 1; i <= len(sorted); i++ {
			if i < len(sorted) && sorted[i] == sorted[i-1]+1 {
				continue
			}
			if int64(i-runStart) >= n {
				first = sorted[runStart]
				kept := make([]int64, 0, len(a.free)-int(n))
				for _, id := range a.free {
					if id < first || id >= first+n {
						kept = append(kept, id)
					}
				}
				a.free = kept
				return first, false
			}
			runStart = i
		}
	}

	first = a.count
	a.count += n
	return first, true
}

// isAscendingRun reports whether ids is a strictly +1 ascending sequence, that is a
// contiguous extent run in id order. The free stack never holds a duplicate id (the
// conservation invariant I7), so a +1 step is the only thing to check.
func isAscendingRun(ids []int64) bool {
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1]+1 {
			return false
		}
	}
	return true
}

// freeRun pushes a contiguous run of n extent ids back onto the free stack. The caller
// guarantees the run holds no live reference (the checkpoint frees a superseded
// snapshot's extents, which no live reader ever reads, doc 05 section 4).
func (a *allocator) freeRun(first, count int64) {
	a.mu.Lock()
	for k := int64(0); k < count; k++ {
		a.free = append(a.free, first+k)
	}
	a.mu.Unlock()
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
