package betree

// This file is the second slice of M6, the fixed-size slab allocator over the raw arena (doc 05
// section 5, D10). The arena bumps arbitrary byte runs and never takes any back; this layer carves
// the arena into uniform slabs, the node-page-sized units the buffer pool and node buffers live in,
// and keeps a free list so a slab handed back can be reused instead of bumping a fresh one from the
// arena. It is the allocator the epoch reclaimer returns retired node slabs to: when section 2's
// reclaimer computes the safe epoch and a retired node's epoch is older than it, the node's slab goes
// back here, onto the free list, and the next allocation reuses it. Built alongside the engine, off
// the live path; nothing allocates nodes from it yet. The sync.Pool for hot-path transients is the
// next M6 slice.
//
// Why a free list rather than bump-only. A long-running engine churns nodes endlessly: a split
// retires one node and makes two, a merge retires two and makes one, a buffer-pool frame is evicted
// and refilled. Bump-only allocation would walk the arena cursor forward forever and exhaust the
// region even though the live node count is steady, because freed space is never revisited. The free
// list closes that loop: a freed slab is the cheapest possible next allocation, so steady-state churn
// reuses a bounded set of slabs and the arena cursor stops advancing once the working set is mapped.
//
// The reuse discipline. A slab is allocated, used as a node page, retired through the epoch machinery,
// and only freed here once no optimistic reader can still be inside it (section 2 guarantees that
// before the slab is handed back). So by the time a slab reaches freeSlab, it is genuinely dead, and
// reuse cannot hand live bytes to two owners. This layer does not re-check that; it trusts the epoch
// reclaimer to have already proven the slab safe, exactly as a manual allocator trusts its caller not
// to free memory still in use. What this layer owns is that a freed slab is reused before a fresh one
// is bumped, and that alloc and free are safe under concurrency.
//
// The free list is a plain slice of offsets under one mutex, not a lock-free intrusive stack. The
// slab path is far cooler than the per-byte arena bump: a slab is a whole node page, so allocations
// happen per node split or eviction, not per write, and a mutex there is correct and cheap relative
// to the page-sized work each allocation does. A lock-free intrusive free list (the next pointer
// stored in the dead slab's own bytes, Treiber-style) is possible and would remove even that mutex,
// but it carries the ABA hazard that needs a tagged head to close, and the honest call is that the
// slab path does not need it. Per-shard free lists, where disjoint shards never contend on even this
// mutex, arrive with M7; this is the single-domain version.

import "sync"

// slabAlloc carves an arena into fixed-size slabs and recycles freed ones. It is safe for concurrent
// use. A slab is identified by its arena offset, the same integer handle the arena hands out, so the
// pointer-free discipline carries through unchanged: a slab is reached by offset arithmetic, never a
// Go pointer.
type slabAlloc struct {
	a    *arena
	slab uint64 // the fixed slab size in bytes, the node page size in the integrated engine

	mu    sync.Mutex
	free  []arenaOff // dead slabs available for reuse, most-recently-freed last (LIFO)
	bumps uint64     // slabs ever bumped fresh from the arena, for diagnostics
}

// newSlabAlloc builds a slab allocator over an arena with the given uniform slab size. The slab size
// is rounded up to the arena's alignment so every slab starts and ends aligned, which keeps a node
// header's atomically-accessed version word aligned. A zero slab size is treated as one alignment
// unit so the allocator is always usable.
func newSlabAlloc(a *arena, slabSize uint64) *slabAlloc {
	if slabSize == 0 {
		slabSize = arenaAlign
	}
	slabSize = (slabSize + arenaAlign - 1) &^ (arenaAlign - 1)
	return &slabAlloc{a: a, slab: slabSize}
}

// alloc returns a slab, reusing a freed one if any is available and otherwise bumping a fresh one
// from the arena. It returns (arenaNil, false) only when the free list is empty and the arena has no
// room left for another slab. Reuse is LIFO, so the most-recently-freed slab comes back first, which
// is the warmest in cache.
func (s *slabAlloc) alloc() (arenaOff, bool) {
	s.mu.Lock()
	if n := len(s.free); n > 0 {
		off := s.free[n-1]
		s.free = s.free[:n-1]
		s.mu.Unlock()
		return off, true
	}
	s.mu.Unlock()
	// No freed slab to reuse; bump a fresh one. The arena's own bump is lock-free, so this is done
	// outside the free-list mutex.
	off, ok := s.a.alloc(s.slab)
	if !ok {
		return arenaNil, false
	}
	s.mu.Lock()
	s.bumps++
	s.mu.Unlock()
	return off, true
}

// freeSlab returns a slab to the free list for reuse. The caller must have proven the slab dead (the
// epoch reclaimer does this before calling) and must pass an offset this allocator handed out; a
// foreign or already-freed offset would corrupt the reuse invariant, the same contract any manual
// free carries.
func (s *slabAlloc) freeSlab(off arenaOff) {
	s.mu.Lock()
	s.free = append(s.free, off)
	s.mu.Unlock()
}

// bytes returns the slab's byte slice, capped to the slab size so a writer cannot run past it into
// the next slab. The slice aliases the arena and is valid until the arena is closed.
func (s *slabAlloc) bytes(off arenaOff) []byte {
	return s.a.bytesAt(off, s.slab)
}

// slabSize reports the fixed slab size in bytes.
func (s *slabAlloc) slabSize() uint64 { return s.slab }

// freeCount reports how many freed slabs are waiting for reuse. It is a point-in-time read for tests
// and diagnostics.
func (s *slabAlloc) freeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.free)
}

// bumpCount reports how many slabs were ever bumped fresh from the arena, as opposed to reused. A
// steady-state workload should see this stop climbing once the working set is mapped, the signal that
// the free list is doing its job. It is for diagnostics.
func (s *slabAlloc) bumpCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bumps
}
