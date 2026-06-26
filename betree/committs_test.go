package betree

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// This file gates the M5.3 decentralized commit-timestamp assigner (committs.go), the piece that
// replaces the oracle's global commit-version counter with max-plus-one over a transaction's
// conflict footprint. The contract has two halves. The ordering half: a committer's timestamp is
// strictly past every timestamp it observed, so conflicting transactions that share a key are
// threaded into a consistent order while disjoint ones are not forced to order. The safety half:
// two writers to one key serialize, so the second observes the first's stamp and orders after it,
// never landing two unordered commits on one key. These tests pin both, including under heavy
// concurrency where the per-key write lock is what holds the safety half together.

// keys turns string literals into the [][]byte the assigner takes.
func keys(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func TestCommitTsExceedsFootprint(t *testing.T) {
	clk := &fakeClock{}
	clk.set(1000)
	a := newAssigner(newHLC(clk.now))

	// A first blind write observes nothing and gets a fresh timestamp.
	ts1 := a.Commit(nil, keys("a"))
	if a.stampOf([]byte("a")) != ts1 {
		t.Fatalf("key a stamped %d, want the commit ts %d", a.stampOf([]byte("a")), ts1)
	}
	// A second writer to the same key observes ts1 under the write lock and must exceed it.
	ts2 := a.Commit(nil, keys("a"))
	if uint64(ts2) <= uint64(ts1) {
		t.Fatalf("second writer to key a did not exceed the first: %d <= %d", uint64(ts2), uint64(ts1))
	}
	if a.stampOf([]byte("a")) != ts2 {
		t.Fatalf("key a stamped %d after second write, want %d", a.stampOf([]byte("a")), ts2)
	}
}

func TestCommitTsCausalThroughSharedKey(t *testing.T) {
	clk := &fakeClock{}
	clk.set(1)
	a := newAssigner(newHLC(clk.now))

	// A writes k. B reads k and writes m. Because B observed k's stamp, B must order after A even
	// though they write different keys: the read footprint carries the dependency.
	tsA := a.Commit(nil, keys("k"))
	tsB := a.Commit(keys("k"), keys("m"))
	if uint64(tsB) <= uint64(tsA) {
		t.Fatalf("B read A's key but did not order after it: %d <= %d", uint64(tsB), uint64(tsA))
	}
	// A transaction that reads neither key is independent: it shares no footprint, so the assigner
	// imposes no key-derived order. It still gets a valid fresh timestamp.
	tsC := a.Commit(nil, keys("z"))
	if tsC == 0 {
		t.Fatal("an independent commit got the zero timestamp")
	}
}

func TestCommitTsDependencyChain(t *testing.T) {
	clk := &fakeClock{}
	clk.set(1)
	a := newAssigner(newHLC(clk.now))

	// A chain A -> B -> C where each reads the previous writer's key produces strictly increasing
	// timestamps, the relative order threaded entirely through the shared keys.
	tsA := a.Commit(nil, keys("k1"))
	tsB := a.Commit(keys("k1"), keys("k2"))
	tsC := a.Commit(keys("k2"), keys("k3"))
	if !(uint64(tsA) < uint64(tsB) && uint64(tsB) < uint64(tsC)) {
		t.Fatalf("dependency chain not strictly increasing: %d, %d, %d", uint64(tsA), uint64(tsB), uint64(tsC))
	}
}

func TestReadTimestampDominatesCommits(t *testing.T) {
	clk := &fakeClock{}
	clk.set(500)
	a := newAssigner(newHLC(clk.now))

	ts := a.Commit(nil, keys("a"))
	// A read timestamp taken after a commit must be at or above it, so the reader's snapshot
	// includes that commit's order point and is not behind the latest commit.
	rt := a.readTimestamp()
	if uint64(rt) < uint64(ts) {
		t.Fatalf("read timestamp %d is behind a prior commit %d", uint64(rt), uint64(ts))
	}
}

func TestCommitTsMultiKeyWriteSet(t *testing.T) {
	clk := &fakeClock{}
	clk.set(1)
	a := newAssigner(newHLC(clk.now))

	// Seed two keys at different timestamps.
	a.Commit(nil, keys("a"))
	tsB := a.Commit(nil, keys("b"))
	// A writer over both, plus a duplicate of one, must observe the higher of the two seeds and
	// exceed it, and stamp every written key (including the duplicate) with the one commit ts.
	ts := a.Commit(nil, keys("a", "b", "a"))
	if uint64(ts) <= uint64(tsB) {
		t.Fatalf("multi-key writer did not exceed the highest seed: %d <= %d", uint64(ts), uint64(tsB))
	}
	if a.stampOf([]byte("a")) != ts || a.stampOf([]byte("b")) != ts {
		t.Fatalf("multi-key writer did not stamp both keys with %d: a=%d b=%d", uint64(ts), uint64(a.stampOf([]byte("a"))), uint64(a.stampOf([]byte("b"))))
	}
}

func TestCommitTsConcurrentSharedKeyOrdered(t *testing.T) {
	clk := &fakeClock{}
	clk.set(1)
	// Nudge the clock so both the adopt-wall-clock and advance-logical paths run.
	stop := make(chan struct{})
	var tick sync.WaitGroup
	tick.Add(1)
	go func() {
		defer tick.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clk.add(1)
			}
		}
	}()
	a := newAssigner(newHLC(clk.now))

	const writers = 16
	const perWriter = 200
	all := make([][]hlcTime, writers)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			out := make([]hlcTime, 0, perWriter)
			for i := 0; i < perWriter; i++ {
				// Every committer writes the one shared hot key, so each must serialize on its write
				// lock, observe the previous stamp, and exceed it.
				out = append(out, a.Commit(nil, keys("hot")))
			}
			all[w] = out
		}(w)
	}
	wg.Wait()
	close(stop)
	tick.Wait()

	// Every timestamp issued is distinct, and the hot key's final stamp is the maximum of them: a
	// serialized run of writers on one key leaves the key at the last (highest) committer's stamp.
	seen := make(map[hlcTime]struct{}, writers*perWriter)
	var max hlcTime
	for w := range all {
		for _, ts := range all[w] {
			if _, dup := seen[ts]; dup {
				t.Fatalf("duplicate commit timestamp %d on the hot key", uint64(ts))
			}
			seen[ts] = struct{}{}
			if ts > max {
				max = ts
			}
		}
	}
	if len(seen) != writers*perWriter {
		t.Fatalf("issued %d distinct timestamps, want %d", len(seen), writers*perWriter)
	}
	if a.stampOf([]byte("hot")) != max {
		t.Fatalf("hot key final stamp %d is not the max issued %d", uint64(a.stampOf([]byte("hot"))), uint64(max))
	}
}

func TestCommitTsConcurrentDisjointNoDeadlock(t *testing.T) {
	a := newAssigner(newHLC(nil)) // real clock; this test is about liveness and stamping, not values

	const writers = 16
	const perWriter = 500
	var committed atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// Each writer owns a private key space, so no two writers ever share a key: the per-key
				// locks never contend, which is the disjoint-committer case the design exists to keep
				// cheap. A multi-key write set with an intentional duplicate exercises the dedup in the
				// lock path under concurrency.
				k := fmt.Sprintf("w%d-k%d", w, i)
				ts := a.Commit(nil, keys(k, k))
				if a.stampOf([]byte(k)) != ts {
					t.Errorf("disjoint key %s stamped %d, want %d", k, uint64(a.stampOf([]byte(k))), uint64(ts))
					return
				}
				committed.Add(1)
			}
		}(w)
	}
	wg.Wait()
	if got := committed.Load(); got != writers*perWriter {
		t.Fatalf("committed %d, want %d", got, writers*perWriter)
	}
}
