package btree

import (
	"context"
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// countCells returns the total number of cells (every version of every key, markers
// included) currently stored in the tree, by walking the whole leaf chain.
func countCells(t *testing.T, bt *BTree) int {
	t.Helper()
	entries, err := bt.collectRange(nil, nil)
	if err != nil {
		t.Fatalf("collectRange: %v", err)
	}
	return len(entries)
}

// TestGCReducesCellsPreservesReads applies three versions of many keys across several
// leaves, then runs version GC at the newest version and checks the dead versions are
// physically gone while a read at or above the watermark is unchanged.
func TestGCReducesCellsPreservesReads(t *testing.T) {
	bt := newBTree(t, 512, 16)

	const n = 30
	for _, v := range []uint64{10, 20, 30} {
		b := engine.NewWriteBatch(v)
		for i := 0; i < n; i++ {
			b.Set([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d-%d", i, v)))
		}
		if err := bt.Apply(b, v); err != nil {
			t.Fatalf("apply v%d: %v", v, err)
		}
	}

	if got := countCells(t, bt); got != 3*n {
		t.Fatalf("before GC: %d cells, want %d", got, 3*n)
	}

	rep, err := bt.Maintain(context.Background(), engine.MaintBudget{Watermark: 30})
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if rep.BytesReclaimed <= 0 || rep.PagesCompacted == 0 || rep.More {
		t.Fatalf("maintain report = %+v, want reclaim and a finished pass", rep)
	}

	// Only the newest version of each key survives.
	if got := countCells(t, bt); got != n {
		t.Fatalf("after GC: %d cells, want %d", got, n)
	}

	// A read at the watermark is identical to before GC.
	rd, _ := bt.NewReader(engine.Snapshot{Version: 30})
	defer rd.Close()
	for i := 0; i < n; i++ {
		got, err := rd.Get([]byte(fmt.Sprintf("k%02d", i)))
		if err != nil {
			t.Fatalf("get k%02d: %v", i, err)
		}
		if want := fmt.Sprintf("v%02d-30", i); string(got) != want {
			t.Fatalf("k%02d = %q, want %q", i, got, want)
		}
	}
}

// TestGCDropsRangeMarker runs GC over a committed range delete and checks the marker
// cell and the covered keys' dead versions are reclaimed, the in-memory interval set
// is emptied, and reads at the watermark are unchanged.
func TestGCDropsRangeMarker(t *testing.T) {
	bt := newBTree(t, 512, 16)

	b1 := engine.NewWriteBatch(10)
	for i := 0; i < 20; i++ {
		b1.Set([]byte(fmt.Sprintf("k%02d", i)), []byte("one"))
	}
	if err := bt.Apply(b1, 10); err != nil {
		t.Fatalf("apply sets: %v", err)
	}
	b2 := engine.NewWriteBatch(20)
	b2.DeleteRange([]byte("k05"), []byte("k15")) // covers k05..k14
	if err := bt.Apply(b2, 20); err != nil {
		t.Fatalf("apply range delete: %v", err)
	}
	if len(bt.rangeDels) != 1 {
		t.Fatalf("expected 1 live range delete before GC, got %d", len(bt.rangeDels))
	}

	if _, err := bt.Maintain(context.Background(), engine.MaintBudget{Watermark: 20}); err != nil {
		t.Fatalf("maintain: %v", err)
	}

	if len(bt.rangeDels) != 0 {
		t.Fatalf("range-delete marker should be gone after GC, %d remain", len(bt.rangeDels))
	}
	// 20 keys minus the 10 covered, no marker cell.
	if got := countCells(t, bt); got != 10 {
		t.Fatalf("after GC: %d cells, want 10 (uncovered keys only)", got)
	}

	rd, _ := bt.NewReader(engine.Snapshot{Version: 20})
	defer rd.Close()
	for i := 0; i < 20; i++ {
		_, err := rd.Get([]byte(fmt.Sprintf("k%02d", i)))
		covered := i >= 5 && i <= 14
		if covered && err != engine.ErrNotFound {
			t.Fatalf("covered k%02d: err = %v, want ErrNotFound", i, err)
		}
		if !covered && err != nil {
			t.Fatalf("uncovered k%02d: %v", i, err)
		}
	}
}

// TestGCCleanLeafFastPath checks the clean-leaf fast path: a tree of one plain Set per
// key with no range delete has nothing for version GC to do, so a Maintain pass must
// reclaim nothing, compact no pages, and leave every read intact. This is the common
// steady-state shape the background drain repeatedly revisits, so skipping the per-cell
// rebuild on it is the point of the slice.
func TestGCCleanLeafFastPath(t *testing.T) {
	bt := newBTree(t, 512, 16)

	const n = 80 // enough distinct keys to span several leaves at page size 512
	b := engine.NewWriteBatch(10)
	for i := 0; i < n; i++ {
		b.Set([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i)))
	}
	if err := bt.Apply(b, 10); err != nil {
		t.Fatalf("apply: %v", err)
	}

	before := countCells(t, bt)
	if before != n {
		t.Fatalf("before GC: %d cells, want %d", before, n)
	}

	// Watermark well above the only version: the slow path would fold every group, the
	// fast path skips them. Either way nothing is collectable.
	rep, err := bt.Maintain(context.Background(), engine.MaintBudget{Watermark: 100})
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if rep.BytesReclaimed != 0 || rep.PagesCompacted != 0 || rep.More {
		t.Fatalf("clean tree maintain report = %+v, want zero reclaim and a finished pass", rep)
	}
	if got := countCells(t, bt); got != before {
		t.Fatalf("after GC: %d cells, want %d unchanged", got, before)
	}

	rd, _ := bt.NewReader(engine.Snapshot{Version: 100})
	defer rd.Close()
	for i := 0; i < n; i++ {
		got, err := rd.Get([]byte(fmt.Sprintf("k%03d", i)))
		if err != nil {
			t.Fatalf("get k%03d: %v", i, err)
		}
		if want := fmt.Sprintf("v%03d", i); string(got) != want {
			t.Fatalf("k%03d = %q, want %q", i, got, want)
		}
	}
}

// TestLeafIsCleanSets pins the predicate that gates the fast path: only a leaf of strictly
// ascending distinct user keys, every cell a plain Set, with no range delete in force,
// may skip the collapse. Every other shape (a multi-version group, a tombstone, a TTL set,
// a range marker, or any non-empty range-delete set) must fall through to the general path
// because it could change the leaf.
func TestLeafIsCleanSets(t *testing.T) {
	ik := func(uk string, v uint64, k format.Kind) []byte {
		return format.EncodeInternalKey([]byte(uk), v, k)
	}
	mk := func(keys ...[]byte) *leaf {
		l := &leaf{}
		for _, k := range keys {
			l.keys = append(l.keys, k)
			l.vals = append(l.vals, []byte("v"))
		}
		return l
	}

	clean := mk(ik("a", 1, format.KindSet), ik("b", 1, format.KindSet), ik("c", 2, format.KindSet))
	if !leafIsCleanSets(clean, nil) {
		t.Fatalf("distinct single-version sets should be clean")
	}
	if leafIsCleanSets(clean, []format.RangeDel{{}}) {
		t.Fatalf("a non-empty range-delete set must defeat the fast path")
	}

	// Two versions of the same user key: a real collapse candidate. Cells are newest-first
	// within a group, so the higher version sorts ahead.
	multi := mk(ik("a", 2, format.KindSet), ik("a", 1, format.KindSet), ik("b", 1, format.KindSet))
	if leafIsCleanSets(multi, nil) {
		t.Fatalf("a multi-version group is not clean")
	}

	cases := map[string]*leaf{
		"tombstone":   mk(ik("a", 1, format.KindSet), ik("b", 1, format.KindDelete)),
		"ttl":         mk(ik("a", 1, format.KindSet), ik("b", 1, format.KindSetWithTTL)),
		"range begin": mk(ik("a", 1, format.KindSet), ik("b", 1, format.KindRangeBegin)),
		"range end":   mk(ik("a", 1, format.KindSet), ik("b", 1, format.KindRangeEnd)),
	}
	for name, l := range cases {
		if leafIsCleanSets(l, nil) {
			t.Fatalf("%s leaf must not be clean", name)
		}
	}
}
