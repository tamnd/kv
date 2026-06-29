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

// TestAllocRunReusesFreedTail pins the S8 fast path: a checkpoint frees a run and then
// asks for a run of the same size, and the allocator hands back the freed run off the
// tail without growing the pool, the steady-state snapshot recycle.
func TestAllocRunReusesFreedTail(t *testing.T) {
	a := newAllocator(0, nil)
	first, grew := a.allocRun(5)
	if first != 0 || !grew {
		t.Fatalf("first run: got first=%d grew=%v, want 0 grown", first, grew)
	}
	a.freeRun(first, 5)
	got, grew := a.allocRun(5)
	if grew {
		t.Fatalf("re-alloc grew the pool with a freed run of the same size available")
	}
	if got != first {
		t.Fatalf("re-alloc got first=%d, want the freed run %d", got, first)
	}
	if _, free := a.counts(); len(free) != 0 {
		t.Fatalf("free stack has %d ids after carving the whole freed run, want 0", len(free))
	}
}

// TestAllocRunReclaimsNonTailRun pins that the general path still finds a contiguous run
// that is not sitting at the tail of the stack, so a snapshot whose size changed reclaims
// space rather than growing the file.
func TestAllocRunReclaimsNonTailRun(t *testing.T) {
	// Free stack order [2,3,4,9,7]: a contiguous run 2,3,4 exists but the tail (9,7) is
	// not a ready run, so the fast path misses and the general path must find 2,3,4.
	a := newAllocator(10, []int64{2, 3, 4, 9, 7})
	first, grew := a.allocRun(3)
	if grew {
		t.Fatalf("allocRun grew the pool with a contiguous run 2,3,4 available")
	}
	if first != 2 {
		t.Fatalf("allocRun got first=%d, want 2", first)
	}
	_, free := a.counts()
	if len(free) != 2 {
		t.Fatalf("free stack has %d ids after carving a run of 3 from 5, want 2", len(free))
	}
	for _, id := range free {
		if id == 2 || id == 3 || id == 4 {
			t.Fatalf("carved id %d still on the free stack", id)
		}
	}
}

// TestAllocRunGrowsWhenNoRun pins that with no contiguous run of the wanted size the
// allocator tail-allocates from the pool end and reports the growth.
func TestAllocRunGrowsWhenNoRun(t *testing.T) {
	// Scattered free ids, no two consecutive, so no run of 2 exists.
	a := newAllocator(10, []int64{1, 4, 8})
	first, grew := a.allocRun(2)
	if !grew {
		t.Fatalf("allocRun did not grow with no contiguous run available")
	}
	if first != 10 {
		t.Fatalf("allocRun got first=%d, want 10 (tail of a 10-extent pool)", first)
	}
	if _, free := a.counts(); len(free) != 3 {
		t.Fatalf("free stack changed to %d ids on a grow, want the original 3", len(free))
	}
}

// TestAllocRunConservation drives randomized run allocate/free and asserts no extent is
// lost or double-counted across the fast and general paths.
func TestAllocRunConservation(t *testing.T) {
	a := newAllocator(0, nil)
	rng := rand.New(rand.NewSource(7))
	type run struct{ first, n int64 }
	var live []run
	liveIDs := map[int64]bool{}

	for step := 0; step < 3000; step++ {
		if len(live) > 0 && rng.Intn(2) == 0 {
			i := rng.Intn(len(live))
			r := live[i]
			live[i] = live[len(live)-1]
			live = live[:len(live)-1]
			for k := int64(0); k < r.n; k++ {
				delete(liveIDs, r.first+k)
			}
			a.freeRun(r.first, r.n)
		} else {
			n := int64(rng.Intn(6) + 1)
			first, _ := a.allocRun(n)
			for k := int64(0); k < n; k++ {
				id := first + k
				if liveIDs[id] {
					t.Fatalf("step %d: allocRun returned live id %d", step, id)
				}
				liveIDs[id] = true
			}
			live = append(live, run{first, n})
		}
		count, free := a.counts()
		if int64(len(free))+a.inUse() != count {
			t.Fatalf("step %d: conservation broken: free=%d inUse=%d count=%d", step, len(free), a.inUse(), count)
		}
		seen := map[int64]bool{}
		for _, id := range free {
			if seen[id] {
				t.Fatalf("step %d: id %d double-counted on free stack", step, id)
			}
			seen[id] = true
			if liveIDs[id] {
				t.Fatalf("step %d: id %d both live and free", step, id)
			}
		}
	}
}

// BenchmarkAllocRunRecycle measures the steady-state snapshot recycle: free a run, then
// re-alloc the same size, over a non-trivial free stack. The S8 fast path carves the
// freed run off the tail with no sort and no allocation, so the reported allocs/op is
// zero; the old path sorted a copy of the whole free stack and built a map every call.
func BenchmarkAllocRunRecycle(b *testing.B) {
	// A free stack with a long scattered prefix plus a ready tail run, so a sort-based
	// path would pay for the whole stack while the fast path touches only the tail.
	const prefix = 4096
	free := make([]int64, 0, prefix)
	for i := int64(0); i < prefix; i++ {
		free = append(free, i*2+1) // odd ids, no contiguous run among them
	}
	a := newAllocator(prefix*2+64, free)
	const runLen = 16
	first, _ := a.allocRun(runLen)
	a.freeRun(first, runLen)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, _ := a.allocRun(runLen)
		a.freeRun(f, runLen)
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
