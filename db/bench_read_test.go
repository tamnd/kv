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

// BenchmarkForwardScan measures a bounded forward range scan: seek to a start key and read
// ScanLen entries, the way kvbench drives a scan. The materialized iterator walked from the
// start key to the end of the keyspace before the first entry was read, so its cost grew with
// the database size; the streaming iterator pulls one entry per step and stops after ScanLen,
// so its cost is independent of how many keys sit past the window. Running it at two keyspace
// sizes shows that: the materialized form's time scales with keys, the streaming form's does
// not (spec 04).
func BenchmarkForwardScan(b *testing.B) {
	const scanLen = 100
	for _, keys := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
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
		})
	}
}

// BenchmarkGetLargeValue measures a cache-resident point read of a large value. The read
// path copies the chosen version out of the decoded node before handing it to the caller;
// at large value sizes that memcpy and its allocation dominate, so this is the workload that
// shows the value-copy reductions on the read path (spec 01 Finding 2) where the small-value
// BenchmarkConcurrentGet, dominated by descent and lock costs, cannot.
func BenchmarkGetLargeValue(b *testing.B) {
	for _, size := range []int{256, 4096, 65536} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			fs := vfs.NewMem()
			d, err := Open(fs, "bench.kv", Options{PageSize: 65536, Sync: wal.SyncOff})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer d.Close()
			val := make([]byte, size)
			for i := range val {
				val[i] = byte(i)
			}
			key := []byte("big")
			if _, err := d.Write(func(wb *engine.WriteBatch) {
				wb.Set(key, val)
			}); err != nil {
				b.Fatalf("seed: %v", err)
			}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := d.Get(key); err != nil {
					b.Fatalf("get: %v", err)
				}
			}
		})
	}
}
