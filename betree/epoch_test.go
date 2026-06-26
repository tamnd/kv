package betree

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestEpochReclaimNoReaders checks the base case: with no reader active, a retired page is
// freed by the next reclaim, because a reader that pins after the retire cannot reach a
// node already unlinked.
func TestEpochReclaimNoReaders(t *testing.T) {
	r := newReclaimer()
	var freed atomic.Bool
	r.advance()
	r.retire(func() { freed.Store(true) })
	r.advance()
	r.reclaim()
	if !freed.Load() {
		t.Fatal("a page retired with no active reader should be freed by reclaim")
	}
	if n := r.pendingRetired(); n != 0 {
		t.Fatalf("retired list should be empty, have %d", n)
	}
}

// TestEpochGuardHoldsPage checks that a reader pinned before a retire holds the page alive
// until it unpins, and that the page frees on the next reclaim after the unpin.
func TestEpochGuardHoldsPage(t *testing.T) {
	r := newReclaimer()
	g := r.register()
	defer r.unregister(g)

	g.pin()
	var freed atomic.Bool
	r.retire(func() { freed.Store(true) })
	r.advance()
	r.reclaim()
	if freed.Load() {
		t.Fatal("a page retired while a reader is pinned must not be freed")
	}

	g.unpin()
	r.reclaim()
	if !freed.Load() {
		t.Fatal("after the reader unpins the page should be freed")
	}
}

// TestEpochReclaimStress is the M2 gate's dedicated reclamation stress. Many readers pin,
// snapshot the set of objects live at that moment, verify none of those objects is freed
// while they stay pinned, then unpin, while a writer continuously publishes objects,
// unlinks them, retires their frees, advances the epoch, and reclaims. A use-after-free
// (an object freed while a reader that snapshotted it is still pinned) is caught directly
// by the freed flag, and -race covers the rest. The final drain asserts the reclaimer
// leaves nothing stranded.
func TestEpochReclaimStress(t *testing.T) {
	r := newReclaimer()

	type object struct{ freed atomic.Bool }
	// live is an immutable, atomically swapped snapshot of the currently linked objects, so
	// a reader Loads one consistent slice and the writer publishes a new slice on each edit.
	var live atomic.Pointer[[]*object]
	empty := []*object{}
	live.Store(&empty)

	const readers = 8
	const rounds = 4000
	var stop atomic.Bool
	var wg sync.WaitGroup

	var uaf atomic.Int64
	var checks atomic.Int64
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g := r.register()
			defer r.unregister(g)
			for !stop.Load() {
				g.pin()
				snap := *live.Load()
				// Hold the snapshot across a few yields so a buggy reclaimer has a window to
				// free one of these objects out from under us.
				for k := 0; k < 3; k++ {
					for _, o := range snap {
						if o.freed.Load() {
							uaf.Add(1)
						}
						checks.Add(1)
					}
					runtime.Gosched()
				}
				g.unpin()
			}
		}()
	}

	// Single writer: grow the live set, then unlink the oldest and retire it.
	wg.Add(1)
	go func() {
		defer wg.Done()
		cur := []*object{}
		for i := 0; i < rounds; i++ {
			o := &object{}
			cur = append(cur, o)
			next := append([]*object(nil), cur...)
			live.Store(&next)
			if len(cur) > 16 {
				victim := cur[0]
				cur = append([]*object(nil), cur[1:]...)
				pub := append([]*object(nil), cur...)
				live.Store(&pub) // unlink: no new reader can snapshot the victim past here
				r.advance()
				r.retire(func() { victim.freed.Store(true) })
				r.reclaim()
			}
			if i%64 == 0 {
				runtime.Gosched()
			}
		}
		stop.Store(true)
	}()

	wg.Wait()

	if uaf.Load() != 0 {
		t.Fatalf("epoch reclamation freed a page %d times while a reader still held it", uaf.Load())
	}
	if checks.Load() == 0 {
		t.Fatal("readers checked nothing; the stress exercised no overlap")
	}

	// Drain: with all readers gone, every retired page must be reclaimable.
	r.advance()
	r.reclaim()
	if n := r.pendingRetired(); n != 0 {
		t.Fatalf("after drain the retired list should be empty, have %d", n)
	}
}
