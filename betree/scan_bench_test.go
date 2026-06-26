package betree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// benchTree builds a flushed tree of n keys for the scan benchmarks. It mirrors newTreeBig but
// takes a testing.TB so a Benchmark can use it.
func benchTree(tb testing.TB, n int) *Tree {
	tb.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "bench.kv", pager.Options{
		PageSize:    4096,
		CacheFrames: 1024,
		Engine:      format.EngineBeta,
	})
	if err != nil {
		tb.Fatalf("create pager: %v", err)
	}
	tr := New(p)
	if err := tr.Open(&engine.Env{}); err != nil {
		tb.Fatalf("open betree: %v", err)
	}
	for base := 0; base < n; base += 500 {
		wb := engine.NewWriteBatch(1)
		for i := base; i < base+500 && i < n; i++ {
			wb.Set([]byte(fmt.Sprintf("k%08d", i)), []byte(fmt.Sprintf("v%08d-%0100d", i, i)))
		}
		if err := tr.Apply(wb, wb.Version()); err != nil {
			tb.Fatalf("apply at %d: %v", base, err)
		}
	}
	if err := tr.Flush(); err != nil {
		tb.Fatalf("flush: %v", err)
	}
	return tr
}

// BenchmarkScanStreaming measures the ycsb-e shape on the streaming cursor: an unbounded forward
// iterator, SeekGE to a key mid-keyspace, then a bounded run of Next. This is the path the
// streaming window exists to make cheap, and it should fold only a window's worth of keys per
// scan, not the whole keyspace.
func BenchmarkScanStreaming(b *testing.B) {
	const n = 20000
	const scanLen = 50
	tr := benchTree(b, n)
	rd, err := tr.NewReader(engine.Snapshot{Version: 1})
	if err != nil {
		b.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seek := []byte(fmt.Sprintf("k%08d", (i*977)%(n-scanLen)))
		it, err := rd.NewIter(engine.IterOptions{})
		if err != nil {
			b.Fatalf("iter: %v", err)
		}
		seen := 0
		for ok := it.SeekGE(seek); ok && seen < scanLen; ok = it.Next() {
			_ = it.Key()
			seen++
		}
		if seen != scanLen {
			b.Fatalf("scan saw %d, want %d", seen, scanLen)
		}
	}
}

// BenchmarkScanEager measures the same shape against the whole-range gather the cursor used before
// the streaming window: gather every key at or after the seek (the old unbounded iterator folded
// the entire keyspace up front) and take the first scanLen. It is the baseline the streaming
// window has to beat on this workload.
func BenchmarkScanEager(b *testing.B) {
	const n = 20000
	const scanLen = 50
	tr := benchTree(b, n)
	g := tr.recl.register()
	defer tr.recl.unregister(g)
	snap := engine.Snapshot{Version: 1}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// The old unbounded iterator gathered the whole keyspace regardless of seek key, then
		// positioned within it, so the honest baseline gathers from nil.
		view, err := tr.snapshotRange(snap, g, nil, nil)
		if err != nil {
			b.Fatalf("gather: %v", err)
		}
		seen := 0
		for j := 0; j < len(view) && seen < scanLen; j++ {
			_ = view[j].uk
			seen++
		}
	}
}
