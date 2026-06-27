package betree

// This file is M2's epoch-based reclamation (doc 05 section 2, decision D4), the partner
// of the optimistic lock in olc.go. Optimistic reading only works if the bytes a reader
// is validating still exist: a writer that unlinks a node and hands its page straight
// back to the pager freelist could see that page reused and overwritten under a reader
// that is still mid-read of it, and the version check cannot save a reader whose page has
// already been rebound to a different node. Epoch reclamation closes that window. A page
// a writer retires is not freed immediately; it is tagged with the current epoch and held
// until no reader that could still be looking at it remains. Only then does its page go
// back to the pager.
//
// How the window is measured. There is one global epoch counter and a set of guards, one
// per active reader. A reader enters a critical section by publishing the current global
// epoch into its guard and leaves by publishing a sentinel that says it holds nothing.
// The reclaimer frees a retired page tagged at epoch e once the minimum epoch published
// by any active guard has passed e, because every reader still running then began after
// the page was already unlinked and so cannot hold a pointer to it. A page is unreachable
// from the live tree the instant it is retired (the writer has already swung the pointer
// that named it), so the only references that can remain are in readers that were already
// descending; the epoch test is exactly the condition that all of those have finished.
//
// What it reclaims today. The betree's structural changes currently keep the original
// page number and allocate new pages rather than freeing any, so in normal operation the
// retired list stays empty and the reclaimer has nothing to do. It is built and tested
// now because M2 is where the safe-reclamation machinery belongs: it is the single
// chokepoint every future page free routes through (version GC, node merges, the
// copy-on-write paths later milestones add), and standing it up here with its own stress
// test means those milestones inherit a reclaimer that is already proven rather than
// bolting one on under them.

import (
	"sync"
	"sync/atomic"
)

// inactiveEpoch is the sentinel a guard publishes when it holds no reference. It is the
// maximum epoch so an idle guard never lowers the minimum and so never holds a retired
// page alive.
const inactiveEpoch = ^uint64(0)

// reclaimSlots is the number of preallocated guard slots a reader can claim without taking
// a lock or allocating. It is sized well above the concurrency the engine runs at (the
// write path caps concurrent groups in the low tens and reads fan out no wider), so in
// practice every register lands in a slot and the overflow map below is never touched. A
// reader that finds all slots taken falls back to the map rather than blocking or failing,
// so the cap is a fast-path budget, not a hard reader limit.
const reclaimSlots = 256

// reclaimer is the epoch-based memory reclaimer. Its zero value is not usable; construct
// it with newReclaimer.
//
// Guard registration is the per-read hot path: every db.View opens a reader, registers a
// guard, and unregisters it on close, so register/unregister run once per read across the
// point, scan, and mixed workloads. They claim and release a slot from a fixed lock-free
// array with a single compare-and-swap and a single store, paying no lock and no
// allocation. The mutex and the overflow map below cover only the cold paths: the rare
// reader that arrives when every slot is taken, and the writer-side retire/reclaim list
// that no reader touches.
type reclaimer struct {
	global atomic.Uint64

	// slots is the lock-free fast path. A reader claims a free slot with a CAS on its state
	// word and releases it with a store; an idle or free slot publishes inactiveEpoch and so
	// never lowers the reclaim minimum.
	slots [reclaimSlots]guard

	mu       sync.Mutex
	overflow map[uint64]*guard // guards that did not fit a slot; empty in normal operation
	nextID   uint64
	retired  []retired
}

// retired is one page waiting to be freed: the epoch at which a writer unlinked it and
// the action that returns it to the pager once no reader can still see it. free is a
// closure rather than a bare page number so the reclaimer stays ignorant of the pager and
// the same path can drop a version-table entry, return a page, or both.
type retired struct {
	epoch uint64
	free  func()
}

// guard is one reader's handle into the reclaimer. A reader pins it for the span of a
// read and unpins it after, and the published epoch in between is what pins the retired
// pages the reader might be looking at. A guard is registered once and reused across many
// pin/unpin cycles so the steady-state read pays one atomic store to enter and one to
// leave, with no allocation and no lock.
//
// A guard lives either in the reclaimer's slot array or, when the array was full at
// register time, in the overflow map. state is meaningful only for slot guards: it is the
// claim word a register CAS-flips from free (0) to taken (1) and an unregister stores back
// to free. id is meaningful only for overflow guards: it is the map key unregister deletes.
type guard struct {
	r      *reclaimer
	id     uint64
	ep     atomic.Uint64
	state  atomic.Uint32 // slot guards only: 0 free, 1 taken
	inSlot bool
}

const (
	slotFree  = 0
	slotTaken = 1
)

func newReclaimer() *reclaimer {
	r := &reclaimer{overflow: make(map[uint64]*guard)}
	// Every slot starts free, owned by r, and publishing inactiveEpoch so a never-claimed
	// slot never lowers the reclaim minimum below the live readers. Without this an unused
	// slot would hold the zero epoch and pin every retired page forever.
	for i := range r.slots {
		g := &r.slots[i]
		g.r = r
		g.inSlot = true
		g.ep.Store(inactiveEpoch)
	}
	return r
}

// register hands a reader a guard for its lifetime. It first sweeps the slot array for a
// free slot and claims it with a single CAS, the path every read takes in normal operation
// (the array is sized past the engine's concurrency). Only when every slot is taken does it
// fall back to the locked overflow map, so the common case pays no lock and no allocation.
// The guard starts inactive (holding nothing); the reader pins it before its first read.
func (r *reclaimer) register() *guard {
	for i := range r.slots {
		g := &r.slots[i]
		// A free slot already publishes inactiveEpoch (newReclaimer seeds it, unregister
		// restores it), so the CAS alone makes the slot ours; no ep store is needed.
		if g.state.Load() == slotFree && g.state.CompareAndSwap(slotFree, slotTaken) {
			return g
		}
	}
	r.mu.Lock()
	id := r.nextID
	r.nextID++
	g := &guard{r: r, id: id}
	g.ep.Store(inactiveEpoch)
	r.overflow[id] = g
	r.mu.Unlock()
	return g
}

// unregister drops a guard the reader will not use again. The guard must be unpinned (not
// in a critical section) when it is dropped, which it always is because a reader unpins
// before it discards its handle. A slot guard is released by restoring its inactive epoch
// and then storing its state back to free, so the next register that claims this slot finds
// it already holding inactiveEpoch. An overflow guard is removed from the map under the lock.
func (r *reclaimer) unregister(g *guard) {
	if g.inSlot {
		g.ep.Store(inactiveEpoch)
		g.state.Store(slotFree)
		return
	}
	r.mu.Lock()
	delete(r.overflow, g.id)
	r.mu.Unlock()
}

// pin enters a read critical section by publishing the current global epoch. From here
// until unpin the reader holds alive every page retired at an epoch at or after the one
// it just published. The load of global and the store into the guard are both atomic, so
// a reclaimer scanning guards sees either the old inactive sentinel or the freshly
// published epoch, never a torn value.
func (g *guard) pin() {
	g.ep.Store(g.r.global.Load())
}

// unpin leaves the read critical section, publishing the inactive sentinel so the guard
// stops holding anything alive. It does not itself free pages; reclamation runs from the
// writer side after a retire, so a reader never pays for another goroutine's garbage.
//
// The publish is a Swap, not a Store, because unpin doubles as the optimistic read
// protocol's read-side barrier (paged.go snapshotRange). That protocol gathers a view under
// no latch and validates it by re-reading the generation after the gather; for the check to
// be sound the gather's page reads must be ordered before that re-read. A plain atomic load
// of the generation is only an acquire, which stops later reads from sinking above it but
// not the gather's earlier reads from sinking below it, so on a weak-memory CPU the re-read
// could observe an unchanged even generation while a gather read still races a writer's
// in-place page rewrite and decodes a half-written or freshly zeroed page. An atomic
// read-modify-write is both an acquire and a release: its release half orders the gather's
// reads before it and its acquire half orders the generation re-read after it, so the two
// compose to put every gather read before the re-read. unpin runs between the gather and the
// re-read on every optimistic attempt, and the guard's own epoch word is a reader-private
// line, so the barrier rides on a store the reader already makes and adds no traffic on the
// shared generation line the way a barrier on the generation itself would.
func (g *guard) unpin() {
	g.ep.Swap(inactiveEpoch)
}

// advance bumps the global epoch and returns the new value. A writer calls it after a
// batch of retirements so readers that arrive next publish a higher epoch, which lets the
// minimum rise past the just-retired pages once the older readers drain. Without an
// advance the minimum could sit forever at the epoch of a page that was retired, and
// nothing would ever free.
func (r *reclaimer) advance() uint64 {
	return r.global.Add(1)
}

// retire records that a page became unreachable at the current epoch and will be freed by
// free once no reader can still see it. It does not free anything itself; the caller
// drives reclamation with reclaim, typically right after retiring a batch and advancing
// the epoch.
func (r *reclaimer) retire(free func()) {
	e := r.global.Load()
	r.mu.Lock()
	r.retired = append(r.retired, retired{epoch: e, free: free})
	r.mu.Unlock()
}

// reclaim frees every retired page whose epoch the slowest active reader has passed. It
// computes the minimum epoch published by any active guard (or the current global epoch
// if no reader is active, since a reader that pins next cannot reach a node already
// unlinked) and frees each retired page tagged strictly below that minimum. The frees run
// after the lock is dropped so a free closure that calls back into the pager does not nest
// under the reclaimer's lock.
func (r *reclaimer) reclaim() {
	r.mu.Lock()
	min := r.minActiveLocked()
	keep := r.retired[:0]
	var free []func()
	for _, rt := range r.retired {
		if rt.epoch < min {
			free = append(free, rt.free)
		} else {
			keep = append(keep, rt)
		}
	}
	// Re-slice onto a fresh backing array only when something was kept and something was
	// freed, so the kept entries are not aliased by a later append over freed slots.
	if len(free) > 0 {
		r.retired = append([]retired(nil), keep...)
	}
	r.mu.Unlock()
	for _, f := range free {
		f()
	}
}

// minActiveLocked returns the minimum epoch any active guard is holding, or the current
// global epoch when no guard is active. A guard publishing the inactive sentinel (a free
// slot, an idle slot, or an unpinned overflow guard) holds the maximum epoch and so never
// lowers the minimum, which lets the slot scan ignore each slot's claim state and read only
// its epoch. The slot epochs are read with plain atomic loads and need no lock; the caller
// holds r.mu for the overflow map walk. A torn read here can only return a value at or below
// the true minimum, which keeps a retired page alive a little longer and never frees one
// early, so the scan is safe without serialising against register and unregister.
func (r *reclaimer) minActiveLocked() uint64 {
	min := inactiveEpoch
	for i := range r.slots {
		if e := r.slots[i].ep.Load(); e < min {
			min = e
		}
	}
	for _, g := range r.overflow {
		if e := g.ep.Load(); e < min {
			min = e
		}
	}
	if min == inactiveEpoch {
		return r.global.Load()
	}
	return min
}

// pendingRetired reports how many retired pages are still waiting to be freed. It exists
// for the reclamation stress test to assert the list drains, and for a future close path
// to confirm nothing is stranded.
func (r *reclaimer) pendingRetired() int {
	r.mu.Lock()
	n := len(r.retired)
	r.mu.Unlock()
	return n
}
