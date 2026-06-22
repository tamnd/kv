package lsm

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// TestTieredBottomAddsRuns confirms a push into the tiered bottom adds its output beside
// the runs already there rather than merging into them: two versions of one key, written
// in separate flushes, end up as two overlapping bottom runs with both versions intact. A
// leveled merge would have folded them into one segment.
func TestTieredBottomAddsRuns(t *testing.T) {
	l := newLSM(t)
	l.l0Trigger = 1
	l.tierFanout = 1 << 30 // never self-merge during the test

	applyBatch(t, l, 1, func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v1")) })
	l.flushActive(t)
	compact(t, l, 0)
	if len(l.levelsLocked()[1]) != 1 {
		t.Fatalf("expected one bottom run, got shape %v", levelShape(l))
	}
	run1 := l.levelsLocked()[1][0]

	applyBatch(t, l, 2, func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v2")) })
	l.flushActive(t)
	compact(t, l, 0)

	if len(l.levelsLocked()[1]) != 2 {
		t.Fatalf("tiered add merged instead of adding a run, got shape %v", levelShape(l))
	}
	present := false
	for _, s := range l.levelsLocked()[1] {
		if s == run1 {
			present = true
		}
	}
	if !present {
		t.Fatalf("the existing bottom run was rewritten, not left in place")
	}
	if ov := l.maxOverlapLocked(1); ov != 2 {
		t.Fatalf("two stacked runs report overlap %d, want 2", ov)
	}
	// The newest version wins across the runs, and the older one still resolves at its
	// snapshot, so the add stranded nothing.
	if v, ok := getAt(t, l, "k", 9); !ok || string(v) != "v2" {
		t.Fatalf("Get(k)@9 = (%q,%v), want v2", v, ok)
	}
	if v, ok := getAt(t, l, "k", 1); !ok || string(v) != "v1" {
		t.Fatalf("Get(k)@1 = (%q,%v), want v1", v, ok)
	}
}

// TestMaxOverlapCountsRunsNotPieces confirms the metric the bottom self-merges on counts
// sorted runs, not segments: one run cut into many pieces reports overlap 1, and a second
// run over the same span reports overlap 2 however many pieces each is cut into. This is
// what keeps a descended bottom, which is one disjoint run cut into many small segments,
// from looking like a stack of runs and self-merging forever.
func TestMaxOverlapCountsRunsNotPieces(t *testing.T) {
	l := newLSM(t)
	l.l0Trigger = 1
	l.segTargetBytes = 1   // cut each run into one-key pieces
	l.tierFanout = 1 << 30 // never self-merge during the test

	applyBatch(t, l, 1, func(b *engine.WriteBatch) {
		b.Set([]byte("a"), []byte("1"))
		b.Set([]byte("m"), []byte("1"))
		b.Set([]byte("z"), []byte("1"))
	})
	l.flushActive(t)
	compact(t, l, 0)
	if got := len(l.levelsLocked()[1]); got != 3 {
		t.Fatalf("expected three pieces of one run, got %d (shape %v)", got, levelShape(l))
	}
	if ov := l.maxOverlapLocked(1); ov != 1 {
		t.Fatalf("one disjoint run cut into pieces reports overlap %d, want 1", ov)
	}

	applyBatch(t, l, 2, func(b *engine.WriteBatch) {
		b.Set([]byte("a"), []byte("2"))
		b.Set([]byte("m"), []byte("2"))
		b.Set([]byte("z"), []byte("2"))
	})
	l.flushActive(t)
	compact(t, l, 0)
	if got := len(l.levelsLocked()[1]); got != 6 {
		t.Fatalf("expected six pieces across two runs, got %d (shape %v)", got, levelShape(l))
	}
	if ov := l.maxOverlapLocked(1); ov != 2 {
		t.Fatalf("two stacked runs report overlap %d, want 2", ov)
	}
}

// TestTieredSelfMergeAtFanout confirms the tiered bottom folds its runs back into one
// disjoint run once the overlap reaches tierFanout, dropping the dead versions as it goes.
func TestTieredSelfMergeAtFanout(t *testing.T) {
	l := newLSM(t)
	l.l0Trigger = 1
	l.tierFanout = 3

	for v := 1; v <= 3; v++ {
		val := fmt.Sprintf("v%d", v)
		applyBatch(t, l, uint64(v), func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte(val)) })
		l.flushActive(t)
		compact(t, l, 0) // tiered add, one run per push
	}
	if got := len(l.levelsLocked()[1]); got != 3 {
		t.Fatalf("expected three tiered runs, got %d (shape %v)", got, levelShape(l))
	}
	if ov := l.maxOverlapLocked(1); ov != 3 {
		t.Fatalf("three stacked runs report overlap %d, want 3", ov)
	}

	// The overlap has reached the fanout, so the next Maintain self-merges. At a watermark
	// above every version the history collapses to the newest.
	compact(t, l, 100)
	if got := len(l.levelsLocked()[1]); got != 1 {
		t.Fatalf("self-merge left %d runs, want one disjoint run (shape %v)", got, levelShape(l))
	}
	if ov := l.maxOverlapLocked(1); ov != 1 {
		t.Fatalf("after self-merge overlap is %d, want 1", ov)
	}
	if got := l.levelsLocked()[1][0].numCells; got != 1 {
		t.Fatalf("self-merge kept %d cells, want 1 (newest version only)", got)
	}
	if v, ok := getAt(t, l, "k", 100); !ok || string(v) != "v3" {
		t.Fatalf("Get(k)@100 = (%q,%v), want v3", v, ok)
	}
}

// TestTieredBottomDescends confirms the bottom grows the tree a level once it outgrows its
// size target: the whole bottom moves down a step, leaving a leveled level above a new
// tiered bottom, and every key still resolves.
func TestTieredBottomDescends(t *testing.T) {
	l := newLSM(t)
	l.l0Trigger = 1
	l.l1TargetBytes = 1 // any bottom content is over target, forcing a descend
	l.tierFanout = 1 << 30

	const n = 40
	for i := 0; i < n; i++ {
		applyBatch(t, l, uint64(i+1), func(b *engine.WriteBatch) {
			b.Set([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i)))
		})
		l.flushActive(t)
	}
	drainCompaction(t, l, 0)

	if deepestLevel(l) < 2 {
		t.Fatalf("expected the bottom to descend below L1, got shape %v", levelShape(l))
	}
	assertLeveledInvariant(t, l)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%03d", i)
		if v, ok := getAt(t, l, key, uint64(n+1)); !ok || string(v) != fmt.Sprintf("v%03d", i) {
			t.Fatalf("Get(%s) = (%q,%v), want v%03d", key, v, ok, i)
		}
	}
}
