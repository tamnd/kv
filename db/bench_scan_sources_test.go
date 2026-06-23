package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// BenchmarkForwardScanManySources stresses the streaming scan over a high source fan-in: a
// tiny memtable flushes the seed into many overlapping L0 segments that are left uncompacted,
// so every merge step has many sources to consider. A long scan over this is exactly where a
// per-call rebuild hurts -- the old ScanForward reseeked every source on every step, O(ScanLen
// x sources x log) -- and where the held merge iterator pays off, advancing the same heap once
// per step. The scan length is long so the per-step cost, not the one-time seek, dominates the
// measurement (perf/09 N5).
func BenchmarkForwardScanManySources(b *testing.B) {
	const (
		keys    = 50000
		scanLen = 2000
	)
	fs := vfs.NewMem()
	// A small memtable so the seed lands as many segments, and no Maintain call so they stay
	// as overlapping L0 runs rather than collapsing into a few leveled ones.
	d, err := Open(fs, "bench.kv", Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 32 << 10, Sync: wal.SyncOff})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer d.Close()
	for i := 0; i < keys; i++ {
		if _, err := d.Write(func(wb *engine.WriteBatch) {
			wb.Set([]byte(fmt.Sprintf("k%08d", i)), []byte("value-payload"))
		}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	start := []byte("k00000000")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := d.View(func(txn *Txn) error {
			it, err := txn.NewIterator(engine.IterOptions{Lower: start})
			if err != nil {
				return err
			}
			defer it.Close()
			n := 0
			for ok := it.First(); ok && n < scanLen; ok = it.Next() {
				_ = it.Key()
				n++
			}
			return it.Error()
		}); err != nil {
			b.Fatalf("scan: %v", err)
		}
	}
}
