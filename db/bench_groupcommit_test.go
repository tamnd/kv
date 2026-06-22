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
