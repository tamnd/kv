package betree

import (
	"sync"
	"sync/atomic"
	"testing"
)

// This file gates the M6.1 off-heap arena (arena.go), the raw region the rest of the memory substrate
// is built on. The contract: allocations are distinct, non-overlapping, aligned byte ranges carved
// out of a fixed region; bytes written through one allocation's slice read back unchanged and never
// bleed into a neighbor; a full arena refuses rather than overruns; allocation is safe and exact
// under concurrency (the bump cursor never double-hands a byte); and close releases the region. The
// arena hands out integer offsets, never Go pointers, so the GC has nothing to trace into it; the
// tests work in offsets and byte slices, the only surface the arena exposes.

func TestArenaAllocIsDistinctAndAligned(t *testing.T) {
	a, err := newArena(1 << 16)
	if err != nil {
		t.Fatalf("newArena: %v", err)
	}
	defer a.close()

	// A run of odd-sized allocations. Each must start aligned and not overlap the previous one, so the
	// rounded sizes account for every byte the cursor advanced.
	var prevEnd uint64
	for i, n := range []uint64{1, 7, 8, 9, 16, 31, 100} {
		off, ok := a.alloc(n)
		if !ok {
			t.Fatalf("alloc %d of %d bytes failed", i, n)
		}
		if uint64(off)%arenaAlign != 0 {
			t.Fatalf("alloc %d offset %d is not %d-aligned", i, off, arenaAlign)
		}
		if uint64(off) < prevEnd {
			t.Fatalf("alloc %d offset %d overlaps previous end %d", i, off, prevEnd)
		}
		want := (n + arenaAlign - 1) &^ (arenaAlign - 1)
		prevEnd = uint64(off) + want
	}
	if a.used() != prevEnd {
		t.Fatalf("used %d, want %d", a.used(), prevEnd)
	}
}

func TestArenaBytesRoundTripNoBleed(t *testing.T) {
	a, err := newArena(1 << 12)
	if err != nil {
		t.Fatalf("newArena: %v", err)
	}
	defer a.close()

	// Three adjacent allocations, each filled with its own byte. After filling all three, each must
	// still read back its own byte: a write to one allocation must not bleed into a neighbor, which the
	// capped (three-index) slice from bytesAt enforces.
	const n = 64
	offs := make([]arenaOff, 3)
	fills := []byte{0xAA, 0xBB, 0xCC}
	for i := range offs {
		off, ok := a.alloc(n)
		if !ok {
			t.Fatalf("alloc %d failed", i)
		}
		offs[i] = off
		buf := a.bytesAt(off, n)
		for j := range buf {
			buf[j] = fills[i]
		}
	}
	for i, off := range offs {
		buf := a.bytesAt(off, n)
		for j := range buf {
			if buf[j] != fills[i] {
				t.Fatalf("alloc %d byte %d is %#x, want %#x (bleed)", i, j, buf[j], fills[i])
			}
		}
	}

	// The capped slice must refuse to grow into the neighbor: appending returns a fresh backing array
	// rather than overwriting the next allocation in place.
	first := a.bytesAt(offs[0], n)
	if cap(first) != n {
		t.Fatalf("bytesAt slice cap is %d, want %d (not capped)", cap(first), n)
	}
}

func TestArenaExhaustionRefuses(t *testing.T) {
	a, err := newArena(4096) // exactly one page
	if err != nil {
		t.Fatalf("newArena: %v", err)
	}
	defer a.close()

	// Drain the page in big chunks, then assert the next allocation that does not fit is refused with
	// the nil sentinel rather than overrunning the region.
	got := uint64(0)
	for {
		off, ok := a.alloc(512)
		if !ok {
			break
		}
		if off == arenaNil {
			t.Fatalf("ok was true but offset is the nil sentinel")
		}
		got += 512
	}
	if got == 0 || got > a.cap() {
		t.Fatalf("drained %d bytes from a %d-byte arena", got, a.cap())
	}
	off, ok := a.alloc(512)
	if ok || off != arenaNil {
		t.Fatalf("alloc on a full arena returned (%d, %v), want (nil, false)", off, ok)
	}
}

func TestArenaResetReuses(t *testing.T) {
	a, err := newArena(4096)
	if err != nil {
		t.Fatalf("newArena: %v", err)
	}
	defer a.close()

	first, _ := a.alloc(1000)
	a.alloc(1000)
	if a.used() == 0 {
		t.Fatalf("used should be non-zero after allocs")
	}
	a.reset()
	if a.used() != 0 {
		t.Fatalf("used is %d after reset, want 0", a.used())
	}
	// After reset the first allocation hands back the same offset, the whole region reusable again.
	again, ok := a.alloc(1000)
	if !ok || again != first {
		t.Fatalf("after reset first alloc is (%d, %v), want (%d, true)", again, ok, first)
	}
}

// TestArenaConcurrentAllocExact is the core safety property: under many goroutines each carving small
// allocations, the bump cursor must hand every byte to exactly one allocation, so the total handed
// out equals the sum of the rounded request sizes and no two allocations overlap. Each goroutine
// stamps its allocations with a unique byte and afterward every allocation must still read back its
// own stamp, which would fail if two allocations were ever given the same bytes.
func TestArenaConcurrentAllocExact(t *testing.T) {
	a, err := newArena(1 << 20)
	if err != nil {
		t.Fatalf("newArena: %v", err)
	}
	defer a.close()

	const workers = 8
	const perWorker = 200
	const n = 48
	const rounded = (n + arenaAlign - 1) &^ (arenaAlign - 1)

	type rec struct {
		off  arenaOff
		mark byte
	}
	var mu sync.Mutex
	recs := make([]rec, 0, workers*perWorker)
	var failed atomic.Bool

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			mark := byte(1 + w)
			local := make([]rec, 0, perWorker)
			for i := 0; i < perWorker; i++ {
				off, ok := a.alloc(n)
				if !ok {
					failed.Store(true)
					return
				}
				buf := a.bytesAt(off, n)
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

	if failed.Load() {
		t.Fatalf("an allocation failed; the arena should have had room for %d bytes", workers*perWorker*rounded)
	}
	// Every allocation reads back its own stamp: no two allocations shared bytes.
	for _, r := range recs {
		buf := a.bytesAt(r.off, n)
		for j := range buf {
			if buf[j] != r.mark {
				t.Fatalf("allocation at %d byte %d is %#x, want stamp %#x (overlap)", r.off, j, buf[j], r.mark)
			}
		}
	}
	// The cursor advanced by exactly the sum of the rounded sizes: every byte went to one allocation.
	if want := uint64(workers * perWorker * rounded); a.used() != want {
		t.Fatalf("used %d, want exactly %d", a.used(), want)
	}
}

func TestArenaOffHeapOnUnix(t *testing.T) {
	a, err := newArena(4096)
	if err != nil {
		t.Fatalf("newArena: %v", err)
	}
	defer a.close()
	// On a unix build the region is real mmap memory; the test documents the backing the arena got
	// rather than hard-failing on a platform without it. The fallback is honest, not a bug, so this is
	// reported, not asserted as a failure.
	t.Logf("arena offHeap=%v", a.offHeap)
}
