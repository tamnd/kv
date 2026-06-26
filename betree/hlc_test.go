package betree

import (
	"sync"
	"sync/atomic"
	"testing"
)

// This file gates the M5.3 hybrid logical clock (hlc.go), the read-timestamp source and the
// max-plus-one engine the decentralized commit-timestamp assigner will be built on. The clock's
// contract is short and absolute: it never runs backward, every issued timestamp is strictly
// greater than every timestamp issued or absorbed before it, it tracks the wall clock when the
// wall clock leads, and it absorbs an observed timestamp so the next issued one exceeds it. These
// tests pin each clause, with a driven physical clock so the logical-counter and clock-regression
// paths are exercised deterministically rather than left to chance.

// fakeClock is a test-controlled physical-time source in microseconds. The test moves it
// explicitly, including holding it still to force the logical counter to do the work and stepping
// it backward to prove the clock still never regresses.
type fakeClock struct{ micros atomic.Uint64 }

func (c *fakeClock) now() uint64  { return c.micros.Load() }
func (c *fakeClock) set(v uint64) { c.micros.Store(v) }
func (c *fakeClock) add(d uint64) { c.micros.Add(d) }

func TestHLCPacking(t *testing.T) {
	ts := packHLC(123456, 789)
	if ts.physical() != 123456 {
		t.Fatalf("physical() = %d, want 123456", ts.physical())
	}
	if ts.logical() != 789 {
		t.Fatalf("logical() = %d, want 789", ts.logical())
	}
	// A logical value past the max borrows into the physical component and stays strictly greater
	// than the instant it advanced from.
	carried := packHLC(10, hlcMaxLogical+1)
	if carried.physical() != 11 || carried.logical() != 0 {
		t.Fatalf("carry packed to (%d,%d), want (11,0)", carried.physical(), carried.logical())
	}
	if uint64(carried) <= uint64(packHLC(10, hlcMaxLogical)) {
		t.Fatal("carried timestamp is not strictly greater than the instant it advanced from")
	}
}

func TestHLCNowMonotonicAndTracksPhysical(t *testing.T) {
	clk := &fakeClock{}
	clk.set(1000)
	h := newHLC(clk.now)

	first := h.Now()
	if first.physical() != 1000 || first.logical() != 0 {
		t.Fatalf("first Now() = (%d,%d), want (1000,0)", first.physical(), first.logical())
	}
	// Advancing the wall clock adopts it and resets the logical counter.
	clk.set(2000)
	second := h.Now()
	if second.physical() != 2000 || second.logical() != 0 {
		t.Fatalf("second Now() = (%d,%d), want (2000,0)", second.physical(), second.logical())
	}
	if uint64(second) <= uint64(first) {
		t.Fatal("Now() did not strictly increase across a clock advance")
	}
}

func TestHLCStalledClockAdvancesLogical(t *testing.T) {
	clk := &fakeClock{}
	clk.set(5000)
	h := newHLC(clk.now)

	prev := h.Now()
	// The wall clock is frozen, so every further Now() must still strictly increase by advancing
	// the logical counter while the physical component holds.
	for i := 0; i < 1000; i++ {
		cur := h.Now()
		if uint64(cur) <= uint64(prev) {
			t.Fatalf("Now() at step %d did not increase under a stalled clock: %d <= %d", i, uint64(cur), uint64(prev))
		}
		if cur.physical() != 5000 {
			t.Fatalf("physical drifted to %d under a stalled clock", cur.physical())
		}
		prev = cur
	}
}

func TestHLCNeverRegressesUnderBackwardClock(t *testing.T) {
	clk := &fakeClock{}
	clk.set(9000)
	h := newHLC(clk.now)

	high := h.Now()
	// A clock that steps backward (NTP correction, a stalled VM resuming) must never let the clock
	// regress: Now() holds the higher physical instant and advances logically instead.
	clk.set(3000)
	for i := 0; i < 100; i++ {
		cur := h.Now()
		if uint64(cur) <= uint64(high) {
			t.Fatalf("Now() regressed under a backward clock at step %d: %d <= %d", i, uint64(cur), uint64(high))
		}
		if cur.physical() < 9000 {
			t.Fatalf("physical regressed to %d under a backward clock", cur.physical())
		}
		high = cur
	}
}

func TestHLCUpdateIsMaxPlusOne(t *testing.T) {
	clk := &fakeClock{}
	clk.set(1000)
	h := newHLC(clk.now)

	local := h.Now()
	// An observed timestamp far in the future must be absorbed: the returned value exceeds both the
	// observed stamp and the clock's prior value, the max-plus-one step.
	observed := packHLC(50000, 7)
	got := h.Update(observed)
	if uint64(got) <= uint64(observed) {
		t.Fatalf("Update did not exceed the observed stamp: %d <= %d", uint64(got), uint64(observed))
	}
	if uint64(got) <= uint64(local) {
		t.Fatalf("Update did not exceed the prior local stamp: %d <= %d", uint64(got), uint64(local))
	}
	// Causality holds going forward: even with the wall clock still behind the observed instant,
	// the next Now() stays above the absorbed future, because Update moved the clock past it.
	next := h.Now()
	if uint64(next) <= uint64(observed) {
		t.Fatalf("clock fell back below an absorbed future after Update: %d <= %d", uint64(next), uint64(observed))
	}
}

func TestHLCUpdateSamePhysicalTakesHigherLogical(t *testing.T) {
	clk := &fakeClock{}
	clk.set(2000)
	h := newHLC(clk.now)

	// Drive the local logical counter up under a frozen clock.
	var local hlcTime
	for i := 0; i < 5; i++ {
		local = h.Now()
	}
	// Observe a stamp in the same physical instant but with a higher logical counter: the result
	// must take one past the higher of the two logical values.
	observed := packHLC(2000, local.logical()+10)
	got := h.Update(observed)
	if got.physical() != 2000 {
		t.Fatalf("Update changed physical to %d, want 2000", got.physical())
	}
	if got.logical() != observed.logical()+1 {
		t.Fatalf("Update logical = %d, want %d (one past the higher observed)", got.logical(), observed.logical()+1)
	}
}

func TestHLCUpdateStaleObservedStillAdvances(t *testing.T) {
	clk := &fakeClock{}
	clk.set(8000)
	h := newHLC(clk.now)
	local := h.Now()
	// A stale observed stamp (strictly behind the local clock) must not pull the clock back, but
	// Update still issues a fresh strictly-greater timestamp.
	got := h.Update(packHLC(10, 1))
	if uint64(got) <= uint64(local) {
		t.Fatalf("Update with a stale stamp did not advance: %d <= %d", uint64(got), uint64(local))
	}
	if got.physical() < 8000 {
		t.Fatalf("a stale observed stamp pulled physical back to %d", got.physical())
	}
}

func TestHLCConcurrentDistinctAndMonotone(t *testing.T) {
	clk := &fakeClock{}
	clk.set(1)
	h := newHLC(clk.now)

	// A background goroutine nudges the physical clock so both the adopt-wall-clock and
	// advance-logical paths run under contention.
	stop := make(chan struct{})
	var ticker sync.WaitGroup
	ticker.Add(1)
	go func() {
		defer ticker.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clk.add(1)
			}
		}
	}()

	const workers = 16
	const perWorker = 2000
	results := make([][]hlcTime, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			out := make([]hlcTime, 0, perWorker)
			for i := 0; i < perWorker; i++ {
				if i%4 == 0 {
					out = append(out, h.Update(packHLC(uint64(i), uint64(w))))
				} else {
					out = append(out, h.Now())
				}
			}
			results[w] = out
		}(w)
	}
	wg.Wait()
	close(stop)
	ticker.Wait()

	// Every issued timestamp across every worker is distinct: each is published by exactly one
	// successful CAS, and the word strictly increases, so no two issues collide.
	seen := make(map[hlcTime]struct{}, workers*perWorker)
	for w := range results {
		for _, ts := range results[w] {
			if _, dup := seen[ts]; dup {
				t.Fatalf("duplicate timestamp %d issued to two callers", uint64(ts))
			}
			seen[ts] = struct{}{}
		}
		// Within a single worker, calls are sequential, so its own stream must be strictly increasing.
		for i := 1; i < len(results[w]); i++ {
			if uint64(results[w][i]) <= uint64(results[w][i-1]) {
				t.Fatalf("worker %d stream not monotone at %d: %d <= %d", w, i, uint64(results[w][i]), uint64(results[w][i-1]))
			}
		}
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("issued %d distinct timestamps, want %d", len(seen), workers*perWorker)
	}
}
