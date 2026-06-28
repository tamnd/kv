package hashlog

import (
	"math/rand"
	"testing"
)

func TestAllocatorLIFO(t *testing.T) {
	a := newAllocator(0, nil)
	// First three allocs grow the pool to ids 0,1,2.
	for want := int64(0); want < 3; want++ {
		id, grew := a.alloc()
		if id != want || !grew {
			t.Fatalf("alloc %d: got id=%d grew=%v", want, id, grew)
		}
	}
	// Free 1 then 0; the next two allocs pop LIFO: 0, then 1.
	a.freeExtent(1)
	a.freeExtent(0)
	id, grew := a.alloc()
	if id != 0 || grew {
		t.Fatalf("LIFO alloc: got id=%d grew=%v, want 0 reused", id, grew)
	}
	id, grew = a.alloc()
	if id != 1 || grew {
		t.Fatalf("LIFO alloc: got id=%d grew=%v, want 1 reused", id, grew)
	}
}

// TestAllocatorConservation drives a randomized allocate/free sequence and asserts
// conservation (I7): every extent is either in use or free, never lost, never
// double-counted, so len(free) + inUse == count at every step.
func TestAllocatorConservation(t *testing.T) {
	a := newAllocator(0, nil)
	rng := rand.New(rand.NewSource(1))
	live := map[int64]bool{} // ids currently handed out

	check := func(step int) {
		count, free := a.counts()
		if int64(len(free))+a.inUse() != count {
			t.Fatalf("step %d: conservation broken: free=%d inUse=%d count=%d",
				step, len(free), a.inUse(), count)
		}
		if a.inUse() != int64(len(live)) {
			t.Fatalf("step %d: inUse=%d but %d live ids tracked", step, a.inUse(), len(live))
		}
		// No id is both live and free.
		freeset := map[int64]bool{}
		for _, id := range free {
			if freeset[id] {
				t.Fatalf("step %d: id %d double-counted on free stack", step, id)
			}
			freeset[id] = true
			if live[id] {
				t.Fatalf("step %d: id %d is both live and free", step, id)
			}
		}
	}

	for step := 0; step < 5000; step++ {
		if len(live) > 0 && rng.Intn(2) == 0 {
			// Free a random live id.
			var pick int64
			for id := range live {
				pick = id
				break
			}
			delete(live, pick)
			a.freeExtent(pick)
		} else {
			id, _ := a.alloc()
			if live[id] {
				t.Fatalf("step %d: alloc returned live id %d", step, id)
			}
			live[id] = true
		}
		check(step)
	}
}

func TestAllocatorPersistRoundTrip(t *testing.T) {
	a := newAllocator(0, nil)
	for i := 0; i < 10; i++ {
		a.alloc()
	}
	a.freeExtent(3)
	a.freeExtent(7)
	count, free := a.counts()

	b := newAllocator(count, free)
	count2, free2 := b.counts()
	if count2 != count {
		t.Fatalf("count round-trip: got %d, want %d", count2, count)
	}
	if len(free2) != len(free) {
		t.Fatalf("free round-trip: got %v, want %v", free2, free)
	}
	for i := range free {
		if free2[i] != free[i] {
			t.Fatalf("free[%d]: got %d, want %d", i, free2[i], free[i])
		}
	}
	// The reconstructed allocator reuses the freed ids before growing.
	id, grew := b.alloc()
	if grew {
		t.Fatalf("reconstructed allocator grew with %d free ids available", len(free))
	}
	if id != 7 {
		t.Fatalf("reconstructed LIFO alloc: got %d, want 7", id)
	}
}
