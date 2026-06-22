package db

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// BenchmarkConcurrentGet measures cache-resident point-read throughput under a fixed fan-out
// of concurrent readers. Each Get takes its read snapshot from the oracle; the slice that
// publishes the applied version as an atomic lets that snapshot be a lock-free load, so this
// is the workload that shows whether reads scale across cores or serialize on the oracle.
func BenchmarkConcurrentGet(b *testing.B) {
	const keys = 10000
	fs := vfs.NewMem()
	d, err := Open(fs, "bench.kv", Options{PageSize: 4096, Sync: wal.SyncOff})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer d.Close()
	for i := range keys {
		key := fmt.Sprintf("k%08d", i)
		if _, err := d.Write(func(wb *engine.WriteBatch) {
			wb.Set([]byte(key), []byte("value-payload"))
		}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	for _, readers := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			var next atomic.Int64
			b.ResetTimer()
			var wg sync.WaitGroup
			for range readers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						n := next.Add(1) - 1
						if n >= int64(b.N) {
							return
						}
						key := fmt.Sprintf("k%08d", int(n)%keys)
						if _, err := d.Get([]byte(key)); err != nil {
							b.Errorf("get: %v", err)
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}
