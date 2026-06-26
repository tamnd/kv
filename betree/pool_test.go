package betree

import (
	"sync"
	"testing"
)

// This file gates the M6.3 pooled transients (pool.go). The contract: the pool hands back a
// ready-to-use, clean object; it resets contents on put so no stale bytes leak into the next user; it
// recycles rather than allocating in steady state (the whole point, proved with AllocsPerRun); and it
// is safe under concurrency. The pointer-not-value discipline is structural (the pool stores *T) so
// the tests exercise the behavior that discipline buys: near-zero allocation on a hot get/put loop.

func TestPoolGetResetsAndRecycles(t *testing.T) {
	type box struct {
		n    int
		data []byte
	}
	p := newPool(
		func() *box { return &box{data: make([]byte, 0, 8)} },
		func(b *box) { b.n = 0; b.data = b.data[:0] },
	)

	b := p.get()
	if b == nil {
		t.Fatal("get returned nil")
	}
	// Dirty it, then return it. Whatever comes back next must be clean, because put resets.
	b.n = 42
	b.data = append(b.data, 1, 2, 3)
	p.put(b)

	c := p.get()
	if c.n != 0 || len(c.data) != 0 {
		t.Fatalf("recycled object not reset: n=%d len=%d, want 0/0", c.n, len(c.data))
	}
	// Capacity is preserved across reset, so reuse does not reallocate the backing array.
	if cap(c.data) == 0 {
		t.Fatalf("recycled buffer lost its capacity")
	}
	p.put(c)
}

func TestPoolPutNilIsIgnored(t *testing.T) {
	p := newPool(func() *int { x := 7; return &x }, nil)
	p.put(nil) // must not panic
	if v := p.get(); v == nil {
		t.Fatal("get after put(nil) returned nil")
	}
}

func TestScratchPoolBuffer(t *testing.T) {
	sp := newScratchPool(16)
	if sp.capHint() != 16 {
		t.Fatalf("capHint %d, want 16", sp.capHint())
	}
	ref := sp.get()
	buf := *ref
	if len(buf) != 0 {
		t.Fatalf("fresh scratch buffer length %d, want 0", len(buf))
	}
	if cap(buf) != 16 {
		t.Fatalf("fresh scratch buffer cap %d, want 16", cap(buf))
	}
	buf = append(buf, []byte("hello")...)
	*ref = buf
	sp.put(ref)

	// The next buffer is reset to length zero, ready to append into again.
	ref2 := sp.get()
	if len(*ref2) != 0 {
		t.Fatalf("recycled scratch buffer length %d, want 0", len(*ref2))
	}
	sp.put(ref2)
}

// TestScratchPoolGrowthCarriesBack checks the get/append/set-back/put protocol: a buffer that grows
// past its starting capacity keeps the larger backing array in the pool, so the pool warms up to the
// sizes its users actually need rather than reallocating every time.
func TestScratchPoolGrowthCarriesBack(t *testing.T) {
	sp := newScratchPool(4)
	ref := sp.get()
	buf := *ref
	buf = append(buf, make([]byte, 64)...) // forces a reallocation past cap 4
	*ref = buf
	grownCap := cap(*ref)
	if grownCap < 64 {
		t.Fatalf("grown buffer cap %d, want at least 64", grownCap)
	}
	sp.put(ref)
	// The next get returns a clean, usable buffer. The invariant asserted is protocol soundness: a
	// zero-length slice over at least the starting capacity. Whether the grown buffer itself comes back
	// (carrying its larger capacity) is a sync.Pool optimization, not a guarantee, since Get may return
	// a fresh New object at any time and always does under -race, so that is observed rather than
	// required.
	ref2 := sp.get()
	if len(*ref2) != 0 {
		t.Fatalf("recycled buffer length %d, want 0", len(*ref2))
	}
	if cap(*ref2) < sp.capHint() {
		t.Fatalf("recycled buffer cap %d, want at least the starting %d", cap(*ref2), sp.capHint())
	}
	t.Logf("after growth to cap %d, next get cap is %d (carry-back is best-effort)", grownCap, cap(*ref2))
}

// TestPoolRecyclesWithoutAllocating is the property pooling exists for: a steady get/put loop does not
// allocate, because it recycles the same object. AllocsPerRun runs the body many times and averages
// the allocations; after warmup a recycled get/put is allocation-free.
func TestPoolRecyclesWithoutAllocating(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool does not recycle under the race detector, so the hot path allocates there")
	}
	sp := newScratchPool(32)
	// Warm the pool so the first allocation is not counted.
	warm := sp.get()
	sp.put(warm)

	allocs := testing.AllocsPerRun(1000, func() {
		ref := sp.get()
		buf := *ref
		buf = append(buf, 'x')
		*ref = buf
		sp.put(ref)
	})
	if allocs > 0 {
		t.Fatalf("steady get/put averaged %.2f allocs, want 0 (pool should recycle)", allocs)
	}
}

func TestScratchPoolConcurrent(t *testing.T) {
	sp := newScratchPool(64)
	const workers = 8
	const perWorker = 2000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			mark := byte(1 + w)
			for i := 0; i < perWorker; i++ {
				ref := sp.get()
				buf := *ref
				// Each user writes its own mark and reads it straight back; a buffer shared between two
				// goroutines at once would corrupt this, so it doubles as an aliasing check under -race.
				buf = append(buf, mark, mark, mark)
				if buf[0] != mark || buf[2] != mark {
					t.Errorf("worker %d scratch buffer corrupted", w)
					*ref = buf
					sp.put(ref)
					return
				}
				*ref = buf
				sp.put(ref)
			}
		}(w)
	}
	wg.Wait()
}
