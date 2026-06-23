package db

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// BenchmarkLargeBatchApplyLSM measures the per-entry cost of applying a large batch into the
// LSM core, the workload the parallel group apply targets: one Write of many entries forms a
// group the leader can spread across cores at apply time (perf/03 W1, perf/07). Durability is
// out of the picture (mem path is not used here, but SyncNormal defers the sync to checkpoint),
// so what it isolates is the memtable insert. b.N counts entries, not batches, so ns/op is the
// per-entry apply cost; batch is the number of entries fused into one Write.
func BenchmarkLargeBatchApplyLSM(b *testing.B) {
	for _, batch := range []int{256, 4096} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			fs := vfs.NewOS()
			path := filepath.Join(b.TempDir(), "bench.kv")
			d, err := Open(fs, path, Options{
				PageSize:     4096,
				Engine:       format.EngineLSM,
				Sync:         wal.SyncNormal,
				MemtableSize: 256 << 20,
			})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer d.Close()

			val := []byte("value-payload-1234567890")
			b.ResetTimer()
			done := 0
			for round := 0; done < b.N; round++ {
				n := batch
				if rem := b.N - done; rem < n {
					n = rem
				}
				base := done
				if _, err := d.Write(func(wb *engine.WriteBatch) {
					for i := 0; i < n; i++ {
						wb.Set([]byte(fmt.Sprintf("r%06d-k%010d", round, base+i)), val)
					}
				}); err != nil {
					b.Fatalf("write: %v", err)
				}
				done += n
			}
		})
	}
}

// BenchmarkConcurrentWriteLSM measures concurrent blind-write throughput into the LSM core
// with fsync removed from the picture (mem VFS, SyncNormal defers the sync to checkpoint), so
// what it isolates is the in-memory commit path: the group-commit ordering lock, the leader's
// serial apply, and the memtable insert at lsm.go:Apply. It is the workload that shows whether
// LSM writers scale across cores once durability is not the gate (perf/03 W1, perf/07).
func BenchmarkConcurrentWriteLSM(b *testing.B) {
	for _, writers := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			fs := vfs.NewOS()
			path := filepath.Join(b.TempDir(), "bench.kv")
			d, err := Open(fs, path, Options{
				PageSize:     4096,
				Engine:       format.EngineLSM,
				Sync:         wal.SyncNormal,
				MemtableSize: 8 << 20,
			})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer d.Close()

			var next atomic.Int64
			b.ResetTimer()
			var wg sync.WaitGroup
			for w := range writers {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for {
						n := next.Add(1) - 1
						if n >= int64(b.N) {
							return
						}
						key := fmt.Sprintf("w%02d-k%010d", w, n)
						if _, err := d.Write(func(wb *engine.WriteBatch) {
							wb.Set([]byte(key), []byte("value-payload-1234567890"))
						}); err != nil {
							b.Errorf("write: %v", err)
							return
						}
					}
				}(w)
			}
			wg.Wait()
		})
	}
}
