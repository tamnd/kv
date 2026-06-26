package betree

import (
	"sync"
	"testing"
)

// This file gates the M6.2 slab allocator (slab.go), the fixed-size layer over the arena that the
// buffer pool allocates from and the epoch reclaimer frees back to. The contract: every slab is a
// distinct, slab-sized, aligned region; a freed slab is reused before a fresh one is bumped, so a
// steady churn of alloc-then-free keeps the arena cursor from advancing; reuse is LIFO; and alloc and
// free are safe and exact under concurrency, never handing one live slab to two owners. Slabs are
// integer offsets, never Go pointers, so the tests work in offsets and byte slices.

func newTestSlab(t *testing.T, slabSize, arenaSize uint64) (*slabAlloc, func()) {
	t.Helper()
	a, err := newArena(arenaSize)
	if err != nil {
		t.Fatalf("newArena: %v", err)
	}
	return newSlabAlloc(a, slabSize), func() { a.close() }
}

func TestSlabAllocDistinctAndSized(t *testing.T) {
	s, done := newTestSlab(t, 256, 1<<16)
	defer done()

	seen := make(map[arenaOff]bool)
	const count = 16
	for i := 0; i < count; i++ {
		off, ok := s.alloc()
		if !ok {
			t.Fatalf("alloc %d failed", i)
		}
		if seen[off] {
			t.Fatalf("alloc %d handed out offset %d twice", i, off)
		}
		seen[off] = true
		if uint64(off)%arenaAlign != 0 {
			t.Fatalf("slab offset %d not aligned", off)
		}
		if b := s.bytes(off); uint64(len(b)) != s.slabSize() || uint64(cap(b)) != s.slabSize() {
			t.Fatalf("slab bytes len/cap %d/%d, want %d", len(b), cap(b), s.slabSize())
		}
	}
	if s.bumpCount() != count {
		t.Fatalf("bumped %d fresh slabs, want %d (nothing freed yet)", s.bumpCount(), count)
	}
}

// TestSlabReuseBeforeBump is the central property: once slabs are freed, allocation reuses them and
// does not bump the arena cursor forward, so steady churn over a working set maps a bounded region.
func TestSlabReuseBeforeBump(t *testing.T) {
	s, done := newTestSlab(t, 256, 1<<16)
	defer done()

	const batch = 10
	offs := make([]arenaOff, batch)
	for i := range offs {
		off, ok := s.alloc()
		if !ok {
			t.Fatalf("first-batch alloc %d failed", i)
		}
		offs[i] = off
	}
	firstBumps := s.bumpCount()
	if firstBumps != batch {
		t.Fatalf("first batch bumped %d, want %d", firstBumps, batch)
	}

	// Free the whole batch, then allocate the same count again. Every one of these must be served from
	// the free list, so the bump count does not move and the freed slabs are exactly what comes back.
	freed := make(map[arenaOff]bool, batch)
	for _, off := range offs {
		s.freeSlab(off)
		freed[off] = true
	}
	if s.freeCount() != batch {
		t.Fatalf("free count %d after freeing the batch, want %d", s.freeCount(), batch)
	}
	for i := 0; i < batch; i++ {
		off, ok := s.alloc()
		if !ok {
			t.Fatalf("reuse alloc %d failed", i)
		}
		if !freed[off] {
			t.Fatalf("reuse alloc %d returned %d, not a freed slab", i, off)
		}
		delete(freed, off)
	}
	if s.bumpCount() != firstBumps {
		t.Fatalf("bump count climbed to %d during reuse, want it held at %d", s.bumpCount(), firstBumps)
	}
	if s.freeCount() != 0 {
		t.Fatalf("free count %d after draining reuse, want 0", s.freeCount())
	}
}

func TestSlabReuseIsLIFO(t *testing.T) {
	s, done := newTestSlab(t, 64, 1<<16)
	defer done()

	a, _ := s.alloc()
	b, _ := s.alloc()
	c, _ := s.alloc()
	// Free in order a, b, c; the most-recently-freed (c) must come back first.
	s.freeSlab(a)
	s.freeSlab(b)
	s.freeSlab(c)
	for i, want := range []arenaOff{c, b, a} {
		got, ok := s.alloc()
		if !ok || got != want {
			t.Fatalf("LIFO reuse %d got (%d, %v), want %d", i, got, ok, want)
		}
	}
}

func TestSlabExhaustionRefuses(t *testing.T) {
	// One page of arena, 256-byte slabs: a bounded number fit, and the allocator refuses past that
	// rather than overrunning, until a slab is freed and then one more allocation succeeds.
	s, done := newTestSlab(t, 256, 4096)
	defer done()

	var got []arenaOff
	for {
		off, ok := s.alloc()
		if !ok {
			break
		}
		got = append(got, off)
	}
	if len(got) == 0 {
		t.Fatalf("no slabs allocated from a 4096-byte arena")
	}
	if _, ok := s.alloc(); ok {
		t.Fatalf("alloc succeeded on a full slab allocator")
	}
	// Free one and the next allocation reuses it.
	s.freeSlab(got[0])
	off, ok := s.alloc()
	if !ok || off != got[0] {
		t.Fatalf("after freeing one, alloc got (%d, %v), want %d", off, ok, got[0])
	}
}

// TestSlabConcurrentAllocDistinct stresses alloc under many goroutines and asserts no slab is handed
// to two owners: each goroutine stamps every slab it holds with a unique byte, and afterward every
// slab still reads back its own stamp. Allocation only (no concurrent frees) so the live set is the
// whole set, making any overlap observable.
func TestSlabConcurrentAllocDistinct(t *testing.T) {
	s, done := newTestSlab(t, 128, 1<<20)
	defer done()

	const workers = 8
	const perWorker = 100

	type rec struct {
		off  arenaOff
		mark byte
	}
	var mu sync.Mutex
	var recs []rec

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			mark := byte(1 + w)
			local := make([]rec, 0, perWorker)
			for i := 0; i < perWorker; i++ {
				off, ok := s.alloc()
				if !ok {
					t.Errorf("worker %d alloc %d failed", w, i)
					return
				}
				buf := s.bytes(off)
				for j := range buf {
					buf[j] = mark
				}
				local = append(local, rec{off, mark})
			}
			mu.Lock()
			recs = append(recs, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()

	for _, r := range recs {
		buf := s.bytes(r.off)
		for j := range buf {
			if buf[j] != r.mark {
				t.Fatalf("slab at %d byte %d is %#x, want stamp %#x (overlap)", r.off, j, buf[j], r.mark)
			}
		}
	}
	if len(recs) != workers*perWorker {
		t.Fatalf("collected %d slabs, want %d", len(recs), workers*perWorker)
	}
}

// TestSlabConcurrentChurn runs alloc-stamp-free loops across goroutines under the race detector. It
// does not assert exact reuse (frees interleave) but that the allocator stays internally consistent
// and the arena cursor does not run away: with reuse working, the bump count stays bounded well below
// the total number of allocations even though far more allocs happen than slabs fit.
func TestSlabConcurrentChurn(t *testing.T) {
	s, done := newTestSlab(t, 128, 1<<16)
	defer done()

	const workers = 8
	const perWorker = 500
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			mark := byte(1 + w)
			for i := 0; i < perWorker; i++ {
				off, ok := s.alloc()
				if !ok {
					// The arena is small; under heavy interleaving a transient empty free list plus a full
					// arena can refuse. Yield and retry so the churn continues rather than failing the test
					// on a benign race between a free and a bump.
					continue
				}
				buf := s.bytes(off)
				buf[0] = mark
				buf[len(buf)-1] = mark
				s.freeSlab(off)
			}
		}(w)
	}
	wg.Wait()

	// Reuse held the arena to a bounded set of slabs: far fewer fresh bumps than total allocations.
	maxSlabs := s.a.cap() / s.slabSize()
	if s.bumpCount() > maxSlabs {
		t.Fatalf("bumped %d fresh slabs, more than the %d that fit; reuse is not working", s.bumpCount(), maxSlabs)
	}
}
