package betree

import (
	"sync"
	"sync/atomic"
	"testing"
)

// This file gates the M5.3 durability-coupled read frontier (visibility.go), the piece that ties a
// reader's snapshot timestamp to durability so it never observes a commit whose WAL frame is still
// in flight. The contract has three parts. Exclusion: the frontier is never at or above the commit
// timestamp of a commit that is admitted but not resolved, so a reader at the frontier cannot see an
// unresolved commit. Advance: once a contiguous prefix of admissions resolves the frontier moves up,
// and when the in-flight set drains empty it reports the highest commit timestamp actually committed.
// Monotonicity: the frontier never regresses, even under out-of-order resolution and heavy
// concurrency, which matters most because the frontier is the one point the out-of-order leaderless
// completions are reconciled into and a regressing read frontier would let a reader's snapshot move
// backward. The admission-first design is what holds exclusion and monotonicity together: a commit
// is registered with a lower-bound admission timestamp under the lock before it has a commit
// timestamp, so the in-flight set always bounds every commit that holds a timestamp.

// drainHeap pops a copy of a tsHeap into ascending order, so a test can assert the heap really is a
// heap (its pops come out sorted) independent of the visibility logic above it.
func drainHeap(h tsHeap) []hlcTime {
	cp := make(tsHeap, len(h))
	copy(cp, h)
	out := make([]hlcTime, 0, len(cp))
	for cp.len() > 0 {
		out = append(out, cp.pop())
	}
	return out
}

func TestTsHeapOrders(t *testing.T) {
	var h tsHeap
	// Push in a deliberately jumbled order; the heap must still surface the minimum first and pop in
	// ascending order.
	for _, v := range []hlcTime{30, 10, 50, 20, 40, 5, 25} {
		h.push(v)
	}
	if h.min() != 5 {
		t.Fatalf("heap min is %d, want 5", h.min())
	}
	got := drainHeap(h)
	want := []hlcTime{5, 10, 20, 25, 30, 40, 50}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("heap drained %v, want %v", got, want)
		}
	}
}

func TestVisibilityEmptyFrontierIsZero(t *testing.T) {
	v := newVisibility(newHLC((&fakeClock{}).now))
	// A tracker with no commit has nothing durable, so a reader sees the empty store at frontier 0.
	if f := v.frontier(); f != 0 {
		t.Fatalf("empty tracker frontier is %d, want 0", f)
	}
}

func TestVisibilitySingleCommitGatesUntilResolved(t *testing.T) {
	clk := &fakeClock{}
	clk.set(1000)
	v := newVisibility(newHLC(clk.now))

	// Admit a commit. Its admission timestamp is the in-flight lower bound; the frontier sits one
	// below it, so the commit (whose eventual commit timestamp is at least the admission) is excluded.
	adm := v.admit()
	if f := v.frontier(); f != adm-1 {
		t.Fatalf("frontier %d with one admission %d in flight, want %d", f, adm, adm-1)
	}
	if v.inFlight() != 1 {
		t.Fatalf("in-flight count %d, want 1", v.inFlight())
	}
	// It resolves at a commit timestamp at or above its admission. The set drains empty and the
	// frontier reaches that commit timestamp.
	commitTs := adm + 5
	v.resolve(adm, commitTs)
	if f := v.frontier(); f != commitTs {
		t.Fatalf("frontier %d after resolve, want the commit timestamp %d", f, commitTs)
	}
	if v.inFlight() != 0 {
		t.Fatalf("in-flight count %d after resolve, want 0", v.inFlight())
	}
}

func TestVisibilityExcludesInFlightCommitTimestamp(t *testing.T) {
	clk := &fakeClock{}
	clk.set(1)
	v := newVisibility(newHLC(clk.now))

	// Two commits in flight. The frontier is below the older one's admission, hence below both
	// commits' eventual commit timestamps no matter what they turn out to be.
	a1 := v.admit()
	a2 := v.admit()
	if a2 <= a1 {
		t.Fatalf("admissions not increasing: %d then %d", a1, a2)
	}
	if f := v.frontier(); f >= a1 {
		t.Fatalf("frontier %d reaches the oldest admission %d", f, a1)
	}
	// The younger one resolves first at a high commit timestamp. The frontier must hold below a1: the
	// older commit could still commit at anything from a1 upward, so its level cannot be exposed yet.
	v.resolve(a2, a2+1000)
	if f := v.frontier(); f != a1-1 {
		t.Fatalf("frontier %d after out-of-order resolve, want %d (still gated by the older admission)", f, a1-1)
	}
	// The older one resolves. Now nothing is in flight and the frontier exposes the highest commit
	// timestamp seen, which is the younger commit's a2+1000.
	v.resolve(a1, a1+1)
	if f := v.frontier(); f != a2+1000 {
		t.Fatalf("frontier %d after draining empty, want the max resolved commit timestamp %d", f, a2+1000)
	}
}

func TestVisibilityFrontierNeverRegresses(t *testing.T) {
	clk := &fakeClock{}
	clk.set(1)
	v := newVisibility(newHLC(clk.now))

	prev := v.frontier()
	check := func() {
		f := v.frontier()
		if f < prev {
			t.Fatalf("frontier regressed from %d to %d", prev, f)
		}
		prev = f
	}
	a1 := v.admit()
	check()
	a2 := v.admit()
	check()
	v.resolve(a2, a2+50) // out of order; frontier holds below a1
	check()
	a3 := v.admit()
	check()
	v.resolve(a1, a1+1) // drains a1 then a2; frontier rises but stays below a3
	check()
	if f := v.frontier(); f != a3-1 {
		t.Fatalf("frontier %d with only a3 in flight, want %d", f, a3-1)
	}
	v.resolve(a3, a3+9) // empty; frontier to the highest resolved commit timestamp
	check()
	want := a3 + 9
	if a2+50 > want {
		want = a2 + 50
	}
	if prev != want {
		t.Fatalf("final frontier %d, want %d", prev, want)
	}
}

func TestVisibilityExclusionUnderConcurrency(t *testing.T) {
	clk := newHLC(nil)
	v := newVisibility(clk)
	// The core safety property: at any moment, the frontier is strictly below the oldest admission in
	// flight, hence below every in-flight commit's eventual commit timestamp. A watcher samples the
	// frontier and the live admission floor together under one mutex so the two observations are
	// consistent, and asserts the frontier never reaches the floor.
	var liveMu sync.Mutex
	live := make(map[hlcTime]bool)
	floor := func() (hlcTime, bool) {
		var min hlcTime
		first := true
		for ts := range live {
			if first || ts < min {
				min, first = ts, false
			}
		}
		return min, !first
	}

	const writers = 8
	const perWriter = 400
	var wg sync.WaitGroup

	stop := make(chan struct{})
	var watcher sync.WaitGroup
	watcher.Add(1)
	go func() {
		defer watcher.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			liveMu.Lock()
			f := v.frontier()
			min, any := floor()
			if any && f >= min {
				t.Errorf("frontier %d reached or passed the oldest in-flight admission %d", f, min)
				liveMu.Unlock()
				return
			}
			liveMu.Unlock()
		}
	}()

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// admit and publish into the live floor under one lock so the watcher can never see an
				// admission that is registered in the tracker but missing from the floor (or vice versa).
				liveMu.Lock()
				adm := v.admit()
				live[adm] = true
				liveMu.Unlock()

				// Draw the commit timestamp from the same clock, the realistic path: the commit timestamp
				// is a value the clock issued (one past the footprint, modeled here by Update over nothing),
				// at or above this commit's admission, so a later admission's Now always exceeds it.
				ct := clk.Update(0)
				liveMu.Lock()
				delete(live, adm)
				v.resolve(adm, ct)
				liveMu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(stop)
	watcher.Wait()

	if n := v.inFlight(); n != 0 {
		t.Fatalf("in-flight count %d at end, want 0", n)
	}
}

func TestVisibilityMonotoneUnderConcurrency(t *testing.T) {
	clk := newHLC(nil)
	v := newVisibility(clk)
	// A second concurrency angle: independent of exact values, the frontier a single observer reads
	// must never go backward while many committers admit and resolve out of order. The admit-resolve
	// pair is kept ordered per committer, which is the discipline the integration guarantees (a commit
	// is registered before it can resolve), so the only interleaving under test is across committers.
	// The commit timestamp is drawn from the same clock as the admission, so it is a value the clock
	// actually issued, the contract resolve requires for the empty-set frontier to stay monotonic.
	const writers = 8
	const perWriter = 500
	var started atomic.Int64
	var wg sync.WaitGroup

	stop := make(chan struct{})
	var watcher sync.WaitGroup
	watcher.Add(1)
	go func() {
		defer watcher.Done()
		var prev hlcTime
		for {
			select {
			case <-stop:
				return
			default:
			}
			f := v.frontier()
			if f < prev {
				t.Errorf("frontier regressed from %d to %d under concurrency", prev, f)
				return
			}
			prev = f
		}
	}()

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				adm := v.admit()
				started.Add(1)
				v.resolve(adm, clk.Update(0))
			}
		}()
	}
	wg.Wait()
	close(stop)
	watcher.Wait()

	if got := started.Load(); got != writers*perWriter {
		t.Fatalf("admitted %d, want %d", got, writers*perWriter)
	}
	if v.inFlight() != 0 {
		t.Fatalf("in-flight count %d at end, want 0", v.inFlight())
	}
}
