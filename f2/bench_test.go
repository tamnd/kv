package f2

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// fillF2 returns a store preloaded with n keys, for read benchmarks.
func fillF2(b *testing.B, n int) *Store {
	b.Helper()
	s, err := New(DefaultTunables())
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
	return s
}

// fillF2Durable preloads a single-file store with n keys. budget is the resident
// page cap (0 unbounded); dial is the durability regime. It is the larger-than-
// memory and durable read setup for benchmarks.
func fillF2Durable(b *testing.B, n, pageSize, budget int, dial Durability) *Store {
	b.Helper()
	s, err := New(Tunables{
		Shards:                256,
		PageSize:              pageSize,
		ResidentPagesPerShard: budget,
		Path:                  filepath.Join(b.TempDir(), "f2.db"),
		Durability:            dial,
	})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
	return s
}

const benchKeys = 1 << 20

func BenchmarkF2Set(b *testing.B) {
	s, _ := New(DefaultTunables())
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Set(tkey(i%benchKeys), tval(i))
	}
}

// BenchmarkF2Overwrite isolates the overwrite path: a prefilled store, every Set
// repointing an existing key. This is the path that reads the old record to verify
// the key and account its stranded bytes; folding that to a single read is the
// write-side win measured here, and the ycsb-a/overwrite kvbench gain it drives.
func BenchmarkF2Overwrite(b *testing.B) {
	s := fillF2(b, benchKeys)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Set(tkey(i%benchKeys), tval(i))
	}
}

// BenchmarkF2GrowEvicted isolates the index grow on a budgeted durable shard, the
// path S3 changed. It builds one shard with many live keys whose log pages are
// nearly all evicted, then times a single doubling rehash. Before S3 the replay did
// one pread per live key to recover its home; after it the replay reads the home
// from the slot's fingerprint, touching the log not at all.
func BenchmarkF2GrowEvicted(b *testing.B) {
	const keys = 1 << 16
	s, err := New(Tunables{
		Shards:                1,
		PageSize:              4096,
		ResidentPagesPerShard: 4,
		Path:                  filepath.Join(b.TempDir(), "grow.db"),
		Durability:            DurabilityNone,
	})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer s.Close()
	for i := 0; i < keys; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
	sh := s.shards[0]
	old := sh.index.Load()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sh.grow(old) // old is not mutated, so each iteration rehashes the same set
	}
}

// BenchmarkF2GetNearBoundary measures the load-factor tradeoff at the key count
// where it bites hardest. A single shard is filled to 800,000 keys: that sits above
// 0.7 of a 2^20-slot table but below 0.8 of it, so a 0.7 store has already doubled
// to 2^21 slots (about half full, short probes, ~21 bytes/key) while a 0.8 store
// holds the smaller table (about three-quarters full, longer probes, ~10 bytes/key).
// Running it on the old and new load factor shows both sides at once: the index RAM
// roughly halves at this count, and the read pays only the extra fingerprint-rejected
// probes, never an extra log read. The reported bytes/key metric is the RAM side.
func BenchmarkF2GetNearBoundary(b *testing.B) {
	const keys = 800_000
	s, err := New(Tunables{Shards: 1, PageSize: 1 << 20})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer s.Close()
	for i := 0; i < keys; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = s.Get(tkey(i % keys))
	}
	// After ResetTimer, which clears user metrics: report the RAM side of the
	// tradeoff alongside the read latency.
	b.ReportMetric(s.Stats().BytesPerKey(), "index-bytes/key")
}

// BenchmarkF2SetParallelShards measures concurrent write throughput as the shard
// count rises. Each goroutine writes a disjoint key range so writes spread across
// shards, and more shards mean two goroutines collide on the same shard write lock
// less often. It also exercises counts past 256, which the old single-byte selector
// could not use at all. The win flattens once shards comfortably exceed the core
// count, which is the point: 256 already suffices on a 10-core host, and the value
// of the wider selector is at the billions-of-keys end (smaller per-shard tables,
// cheaper grows), not extra contention headroom here.
func BenchmarkF2SetParallelShards(b *testing.B) {
	for _, shards := range []int{64, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			s, err := New(Tunables{Shards: shards, PageSize: 1 << 16})
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer s.Close()
			var g atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				base := int(g.Add(1)) << 32 // a disjoint key range per goroutine
				i := 0
				for pb.Next() {
					_ = s.Set(tkey(base+i), tval(i))
					i++
				}
			})
		})
	}
}

func BenchmarkF2Get(b *testing.B) {
	s := fillF2(b, benchKeys)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = s.Get(tkey(i % benchKeys))
	}
}

// BenchmarkF2GetParallel exercises the lock-free read path under contention, the
// regime f2 is built for: every goroutine probes with atomic loads only, so the
// aggregate should scale with cores rather than collapse on a shared lock.
func BenchmarkF2GetParallel(b *testing.B) {
	s := fillF2(b, benchKeys)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _, _ = s.Get(tkey(i % benchKeys))
			i++
		}
	})
}

// BenchmarkF2SetDurableNone measures the single-file write path under the None
// dial against the memory ceiling: it pays the CRC and the in-RAM page write but
// never fsyncs, so the gap over BenchmarkF2Set is the cost of the durable record
// format and the page header, not of disk latency.
func BenchmarkF2SetDurableNone(b *testing.B) {
	s := fillF2Durable(b, 0, 1<<20, 0, DurabilityNone)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Set(tkey(i%benchKeys), tval(i))
	}
}

// BenchmarkF2GetDurableResident reads a single-file store whose pages are all
// resident (unbounded budget). A resident read aliases its page exactly as the
// memory core does, so this should land on the in-memory Get ceiling: the file
// backing costs nothing on a read that does not have to touch disk.
func BenchmarkF2GetDurableResident(b *testing.B) {
	s := fillF2Durable(b, benchKeys, 1<<20, 0, DurabilityNone)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = s.Get(tkey(i % benchKeys))
	}
}

// BenchmarkF2GetDurableEvicted reads a larger-than-memory store with a tiny
// resident budget, so almost every read misses RAM and preads its record from the
// file. It is the cold read cost, the price of holding a working set far past RAM;
// the gap over the resident read is one pread plus an owned-buffer copy.
func BenchmarkF2GetDurableEvicted(b *testing.B) {
	s := fillF2Durable(b, benchKeys, 4096, 4, DurabilityNone)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _, _ = s.Get(tkey(i % benchKeys))
			i++
		}
	})
}

// BenchmarkF2Checkpoint measures a checkpoint over a filled store: the per-shard
// capture of the live slot words plus the snapshot chain write and superblock commit.
// The None dial isolates the capture-and-serialize cost from disk latency, so this is
// the CPU and allocation price a checkpoint pays per call, the work that buys recovery
// its delta bound. Each call also frees the prior chain, so it measures steady state,
// not a one-time first write.
func BenchmarkF2Checkpoint(b *testing.B) {
	s := fillF2Durable(b, benchKeys, 4096, 4, DurabilityNone)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Checkpoint(); err != nil {
			b.Fatalf("Checkpoint: %v", err)
		}
	}
}

// prepareRecoveryFile writes a high-overwrite store to disk and crashes it (closes the fd
// without a clean-close checkpoint), leaving a file a later New must recover. With
// checkpoint set, a snapshot is committed after all the writes, so recovery installs the
// index from the cut and replays nothing; without it, no snapshot exists and recovery
// replays every record of the active generation. The returned tunables reopen that file.
func prepareRecoveryFile(b *testing.B, keys, overwrites int, checkpoint bool) Tunables {
	b.Helper()
	tn := Tunables{
		Shards:                64,
		PageSize:              4096,
		ResidentPagesPerShard: 4,
		Path:                  filepath.Join(b.TempDir(), "rec.db"),
		Durability:            DurabilityNone,
	}
	s, err := New(tn)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	for o := 0; o <= overwrites; o++ {
		for i := 0; i < keys; i++ {
			if err := s.Set(tkey(i), tval(i)); err != nil {
				b.Fatalf("Set: %v", err)
			}
		}
	}
	if checkpoint {
		if err := s.Checkpoint(); err != nil {
			b.Fatalf("Checkpoint: %v", err)
		}
	}
	for _, sh := range s.shards { // flush tails so the crash keeps every record on disk
		sh.mu.Lock()
		_ = sh.log.flushTail()
		sh.mu.Unlock()
	}
	_ = s.df.f.Close() // crash: no clean-close checkpoint, file left as-is for the reopen
	return tn
}

// benchmarkRecover times New over a prepared file. Each iteration recovers, then crashes
// the fd without a clean close so the file is byte-identical for the next iteration: every
// reopen does the same recovery work. The history and delta variants share the same record
// count, so the gap is exactly what the index snapshot removes from recovery.
func benchmarkRecover(b *testing.B, checkpoint bool) {
	tn := prepareRecoveryFile(b, 20000, 10, checkpoint) // 220k records over 20k live keys
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := New(tn)
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		b.StopTimer()
		_ = s.df.f.Close()
		b.StartTimer()
	}
}

// BenchmarkF2RecoverHistory recovers a file with no snapshot: every record of the active
// generation is decoded and reinserted, the cost the snapshot exists to remove.
func BenchmarkF2RecoverHistory(b *testing.B) { benchmarkRecover(b, false) }

// BenchmarkF2RecoverDelta recovers the same file with a committed snapshot: the index is
// installed from the cut as slot-word arithmetic and no record is replayed, so recovery is
// bounded by the live key count rather than the operation history.
func BenchmarkF2RecoverDelta(b *testing.B) { benchmarkRecover(b, true) }

// BenchmarkScaleExtrapolate is not a timing benchmark; it is the billions-of-keys
// memory proof printed as a table. It measures the real resident index cost per
// key at a few million keys, where the per-key cost has already converged to its
// asymptote (the table has grown many times), then multiplies that constant out
// to 1B and 10B keys for both f2 and an estimate of hashlog's full-key index.
// Building a literal billion-key store would need terabytes; the point of a flat
// per-key cost is that the small measurement extrapolates exactly.
func BenchmarkScaleExtrapolate(b *testing.B) {
	const measure = 4_000_000
	s := fillF2(b, measure)
	defer s.Close()
	st := s.Stats()
	f2PerKey := st.BytesPerKey()

	// hashlog's resident index holds, per live entry, a 64-bit hash, a value
	// location (addr int64 + vlen uint32), the key bytes, the slice header for the
	// key, and an atomic.Pointer slot, plus its own table load factor. For a
	// 16-byte key that lands near 74 bytes per key; we state the model rather than
	// run hashlog at this size so the table is reproducible.
	const hashlogPerKey = 74.0

	b.ReportMetric(f2PerKey, "f2-bytes/key")
	b.Logf("measured f2 index cost: %.2f bytes/key (key len %d) over %d keys",
		f2PerKey, len(tkey(0)), measure)
	b.Logf("%-12s %14s %14s %10s", "keys", "f2 index", "hashlog index", "ratio")
	for _, n := range []int64{1_000_000, 100_000_000, 1_000_000_000, 10_000_000_000} {
		f2GiB := float64(n) * f2PerKey / (1 << 30)
		hlGiB := float64(n) * hashlogPerKey / (1 << 30)
		b.Logf("%-12s %11.1f GiB %11.1f GiB %9.1fx",
			humanCount(n), f2GiB, hlGiB, hlGiB/f2GiB)
	}
}

// BenchmarkF2Stats measures the operability snapshot so its cost stays low enough to poll
// on a metrics cadence (audit A6, A7). The walk takes every shard's read lock and reads its
// index and byte counters, work that scales with the shard count, not the key count, so it
// stays microsecond-class on a large store.
func BenchmarkF2Stats(b *testing.B) {
	s := fillF2(b, benchKeys)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st := s.Stats()
		if st.Keys != benchKeys {
			b.Fatalf("Keys = %d, want %d", st.Keys, benchKeys)
		}
	}
}

func humanCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%dB", n/1_000_000_000)
	case n >= 1_000_000:
		return fmt.Sprintf("%dM", n/1_000_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}
