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

// BenchmarkPointReadNoTTLClock measures a single-thread cache-resident point read of a key
// that carries no TTL, the overwhelmingly common case. It runs on the real system clock (no
// injected Clock), so the time it does or does not spend reading the wall clock per read is
// part of the number. With the eager clock the read called time.Now() once per Get even
// though no folded cell carried a TTL; the lazy resolver skips that call entirely (perf/01
// F6), so this isolates that one clock read from the descent and lock costs the parallel
// BenchmarkConcurrentGet is dominated by.
// BenchmarkPointReadClean measures the cache-resident point read in isolation: a single
// goroutine, keys pre-built before the timer starts so the timed loop is only Get and its
// returned-value copy, no fmt.Sprintf in the hot path. This is the benchmark for the F2
// descent slices (perf/12), where the lever is a few nanoseconds and a per-iteration Sprintf
// would bury it. ReportAllocs tracks the one allocation Get's contract requires (the
// caller-owned value copy); the descent itself allocates nothing.
func BenchmarkPointReadClean(b *testing.B) {
	const keys = 10000
	fs := vfs.NewMem()
	d, err := Open(fs, "bench.kv", Options{PageSize: 4096, Sync: wal.SyncOff})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer d.Close()
	keyset := make([][]byte, keys)
	for i := range keys {
		keyset[i] = []byte(fmt.Sprintf("k%08d", i))
		if _, err := d.Write(func(wb *engine.WriteBatch) {
			wb.Set(keyset[i], []byte("value-payload"))
		}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := d.Get(keyset[i%keys]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkPointReadNoTTLClock(b *testing.B) {
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
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		key := fmt.Sprintf("k%08d", i%keys)
		if _, err := d.Get([]byte(key)); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}
