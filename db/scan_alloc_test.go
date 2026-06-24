package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// TestScanOpAllocBudget pins the per-op allocation count of a readseq-shaped scan on the B-tree
// fast path: a fresh read txn, a NewScanCursor at a lower bound, a short read, then Close and
// Discard. The reader-free SnapshotForwardCursorer path removed the throwaway reader the
// ForwardCursorer path allocated, taking the op from four allocations to three. This test fails if a
// later change reintroduces a per-op allocation on this hot path (the kvbench readseq op), so the
// win does not silently regress. The view buffer is pooled, so it is not counted here.
func TestScanOpAllocBudget(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "t.kv", Options{PageSize: 4096, Engine: format.EngineBTree, MemtableSize: 64 << 10, Sync: wal.SyncOff})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const keys = 2000
	for i := range keys {
		if _, err := d.Write(func(wb *engine.WriteBatch) {
			wb.Set([]byte(fmt.Sprintf("k%06d", i)), []byte(fmt.Sprintf("v%06d", i)))
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	start := []byte("k000100")
	avg := testing.AllocsPerRun(500, func() {
		txn := d.Begin(false)
		sc, err := txn.NewScanCursor(engine.IterOptions{Lower: start})
		if err != nil {
			panic(err)
		}
		n := 0
		for sc.Next() && n < 50 {
			_ = sc.Key()
			_ = sc.Value()
			n++
		}
		sc.Close()
		txn.Discard()
	})

	// Three is the post-slice budget (txn, B-tree scan cursor, ScanCursor wrapper); the reader is
	// gone. Allow a small slack for runtime accounting noise but fail well below the old count of 4.
	if avg > 3.5 {
		t.Fatalf("scan op allocates %.2f objects/op, want <= 3 (reader-free fast path); a regression reintroduced a per-op allocation", avg)
	}
}
