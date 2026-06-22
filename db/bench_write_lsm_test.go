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
