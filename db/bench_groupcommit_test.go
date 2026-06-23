package db

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// BenchmarkConcurrentWriteFull measures sustained FULL-durability blind-write throughput
// under a fixed fan-out of concurrent writers. At SyncFull every commit forces a durable
// log, so this is the workload group commit targets: the leader batches the queued commits
// behind one shared fsync. Run with -benchtime to fix the op budget per writer.
func BenchmarkConcurrentWriteFull(b *testing.B) {
	for _, writers := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			fs := vfs.NewOS()
			path := filepath.Join(b.TempDir(), "bench.kv")
			d, err := Open(fs, path, Options{PageSize: 4096, Sync: wal.SyncFull, AutoCheckpoint: -1})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer d.Close()

			var next atomic.Int64
			b.ResetTimer()
			var wg sync.WaitGroup
			for w := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						n := next.Add(1) - 1
						if n >= int64(b.N) {
							return
						}
						key := fmt.Sprintf("k%012d", w*b.N+int(n))
						if _, err := d.Write(func(wb *engine.WriteBatch) {
							wb.Set([]byte(key), []byte("v"))
						}); err != nil {
							b.Errorf("write: %v", err)
							return
						}
					}
				}()
			}
			wg.Wait()
			b.StopTimer()
			b.ReportMetric(float64(d.Stats().Syncs)/float64(b.N), "fsyncs/op")
		})
	}
}

// BenchmarkSyncLevels measures single-writer per-commit durability cost across the three
// levels that actually sync on commit, on the real OS filesystem so the syscall cost is in
// the number. SyncFull pays a full F_FULLFSYNC per commit (the platform's most expensive
// barrier), SyncBarrier pays the cheaper F_BARRIERFSYNC / fdatasync ordering barrier, and
// SyncOff pays nothing. The point of the slice (perf/06 F2) is that SyncBarrier sits far
// closer to SyncOff than to SyncFull while still surviving a process crash; running all
// three side by side shows that gap. Single writer so there is no group batching to mask
// the per-commit cost.
func BenchmarkSyncLevels(b *testing.B) {
	levels := []struct {
		name string
		mode wal.Sync
	}{
		{"full", wal.SyncFull},
		{"barrier", wal.SyncBarrier},
		{"off", wal.SyncOff},
	}
	for _, lvl := range levels {
		b.Run(lvl.name, func(b *testing.B) {
			fs := vfs.NewOS()
			path := filepath.Join(b.TempDir(), "bench.kv")
			d, err := Open(fs, path, Options{PageSize: 4096, Sync: lvl.mode, AutoCheckpoint: -1})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer d.Close()

			b.ResetTimer()
			for i := range b.N {
				key := fmt.Sprintf("k%012d", i)
				if _, err := d.Write(func(wb *engine.WriteBatch) {
					wb.Set([]byte(key), []byte("v"))
				}); err != nil {
					b.Fatalf("write: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(d.Stats().Syncs)/float64(b.N), "fsyncs/op")
		})
	}
}
