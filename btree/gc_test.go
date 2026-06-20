package btree

import (
	"context"
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
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
