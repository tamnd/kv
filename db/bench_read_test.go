package db

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
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

// BenchmarkPointReadLSM measures a point read whose newest version sits in a shallow
// source while older versions of the same key litter the deep levels. A large keyspace is
// settled into a multi-level tree, then a small working set is overwritten several times so
// each working key has a fresh version in the memtable or a recent L0 segment and stale
// versions in every level below. Every level's Bloom filter answers a hit for these keys, so
// before the short-circuit the read pulled a block index and a cell out of every level; after
// it, the read stops at the shallowest level that supplies a base. The cost gap is the deep
// levels the read no longer touches (spec 03 R2).
func BenchmarkPointReadLSM(b *testing.B) {
	const keys = 20000
	const working = 256
	fs := vfs.NewMem()
	d, err := Open(fs, "bench.kv", Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 64 << 10, Sync: wal.SyncOff})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer d.Close()
	for i := range keys {
		key := fmt.Sprintf("k%08d", i)
		if _, err := d.Write(func(wb *engine.WriteBatch) {
			wb.Set([]byte(key), []byte("v00000000"))
		}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
	// Settle the seeded keyspace into the leveled tree so the stale versions sit deep.
	for {
		rep, err := d.Maintain(0)
		if err != nil {
			b.Fatalf("maintain: %v", err)
		}
		if rep.PagesCompacted == 0 {
			break
		}
	}
	// Overwrite the working set repeatedly. The newest version of each key lands in the
	// memtable or a recent L0 segment; the old versions stay in the levels below.
	for round := range 8 {
		for j := range working {
			key := fmt.Sprintf("k%08d", j)
			if _, err := d.Write(func(wb *engine.WriteBatch) {
				wb.Set([]byte(key), []byte(fmt.Sprintf("v%08d", round+1)))
			}); err != nil {
				b.Fatalf("overwrite: %v", err)
			}
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		key := fmt.Sprintf("k%08d", i%working)
		if _, err := d.Get([]byte(key)); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

// BenchmarkPointReadLSMOneVersion measures the dominant point-read shape: a key written once
// and settled into the leveled tree, so a read gathers exactly one version off a single
// on-disk segment. This is the shape the readrandom workload has (a keyspace loaded once and
// read back), as opposed to the multi-version stack BenchmarkPointReadLSM builds. With one
// gathered op that is a visible set under no covering range delete, the GetAt fast path returns
// materializeOp's already-owned value directly instead of folding and copying it a second time,
// so this benchmark isolates that redundant segment-read copy (perf/13).
func BenchmarkPointReadLSMOneVersion(b *testing.B) {
	const keys = 20000
	fs := vfs.NewMem()
	d, err := Open(fs, "bench.kv", Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 64 << 10, Sync: wal.SyncOff})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer d.Close()
	for i := range keys {
		key := fmt.Sprintf("k%08d", i)
		if _, err := d.Write(func(wb *engine.WriteBatch) {
			wb.Set([]byte(key), []byte("v00000000"))
		}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
	// Settle every key into the leveled tree so each lives in exactly one segment, with no
	// shallower version above it: a read gathers a single op and the fast path fires.
	for {
		rep, err := d.Maintain(0)
		if err != nil {
			b.Fatalf("maintain: %v", err)
		}
		if rep.PagesCompacted == 0 {
			break
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
