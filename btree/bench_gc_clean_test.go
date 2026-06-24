package btree

import (
	"context"
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

func newBTreeB(b *testing.B, pageSize, cacheFrames int) *BTree {
	b.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "bench.kv", pager.Options{
		PageSize:    pageSize,
		CacheFrames: cacheFrames,
		Engine:      format.EngineBTree,
	})
	if err != nil {
		b.Fatalf("create pager: %v", err)
	}
	bt := New(p)
	if err := bt.Open(&engine.Env{}); err != nil {
		b.Fatalf("open btree: %v", err)
	}
	return bt
}

// BenchmarkMaintainCleanTree times one version-GC pass over a tree of distinct single-version
// Sets, the steady-state shape the background drain revisits every checkpoint. With no dead
// versions to collect the pass should do no work, so this measures the pure overhead the clean-leaf
// fast path removes: without it every leaf is decoded and rebuilt cell by cell only to be discarded.
func BenchmarkMaintainCleanTree(b *testing.B) {
	const n = 5000
	bt := newBTreeB(b, 4096, 64)
	wb := engine.NewWriteBatch(10)
	for i := 0; i < n; i++ {
		wb.Set([]byte(fmt.Sprintf("k%06d", i)), []byte("value-payload-1234567890"))
	}
	if err := bt.Apply(wb, 10); err != nil {
		b.Fatalf("apply: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := bt.Maintain(context.Background(), engine.MaintBudget{Watermark: 100}); err != nil {
			b.Fatalf("maintain: %v", err)
		}
	}
}
