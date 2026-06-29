package f2

import (
	"os"
	"sync"
	"testing"
	"time"
)

// TestGroupCommitCoalesces proves the L4 mechanism: when many writers reach the
// device barrier at once under the Full dial, they share a single barrier instead
// of each issuing their own. The barrier is gated so the test is deterministic: the
// first writer to arrive becomes the batch leader and blocks inside the stubbed
// barrier, every other writer piles into the next batch and waits, and once the gate
// opens the whole waiting batch is flushed by one barrier. So a burst of writers
// costs a small constant number of barriers, not one per writer.
func TestGroupCommitCoalesces(t *testing.T) {
	const shards = 64
	s := mustOpenT(t, Tunables{
		Shards:                shards,
		PageSize:              4096,
		ResidentPagesPerShard: 2,
		Path:                  t.TempDir() + "/f2.db",
		Durability:            DurabilityFull,
	})

	// One writer per distinct shard, so no two writers serialize on the same shard
	// write lock (which a writer holds across its sync), and every writer can reach
	// the barrier at once. Keys that collide onto a shard would queue behind the lock
	// and barrier solo after the gate opened, hiding the coalescing.
	keys := make([][]byte, 0, shards)
	seen := make([]bool, shards)
	for i := 0; len(keys) < shards; i++ {
		k := tkey(i)
		si := (hash64(k) >> s.shardShift) & s.mask
		if !seen[si] {
			seen[si] = true
			keys = append(keys, k)
		}
	}
	writers := len(keys)

	// Prime the keys with a no-op barrier so page 0 and every key already exist; the
	// measured phase then overwrites them, which appends one record per write and
	// rarely rolls a page, so almost every barrier in that phase is a record flush.
	s.df.syncHook = func(*os.File) error { return nil }
	for i, k := range keys {
		if err := s.Set(k, tval(i)); err != nil {
			t.Fatalf("prime Set: %v", err)
		}
	}

	// Gate the barrier: the leader signals once on entry, then every barrier blocks
	// until the gate is closed. A closed channel reads immediately, so the second
	// barrier (the one covering the waiting batch) does not block.
	entry := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	s.df.syncHook = func(*os.File) error {
		once.Do(func() { close(entry) })
		<-release
		return nil
	}

	before := s.df.syncCount.Load()
	var wg sync.WaitGroup
	wg.Add(writers)
	for i, k := range keys {
		go func(i int, k []byte) {
			defer wg.Done()
			if err := s.Set(k, tval(i+1_000_000)); err != nil {
				t.Errorf("Set: %v", err)
			}
		}(i, k)
	}

	<-entry                           // the leader is now blocked inside the barrier
	time.Sleep(50 * time.Millisecond) // let every other writer reach the waiting batch
	close(release)                    // flush: the leader's barrier, then one for the batch
	wg.Wait()

	barriers := int(s.df.syncCount.Load() - before)
	if barriers >= writers {
		t.Fatalf("no coalescing: %d barriers for %d concurrent writers", barriers, writers)
	}
	// The leader's barrier plus one for the waiting batch is two; a page roll during
	// the burst can add at most a few. A small constant well under the writer count is
	// the proof that the barrier was shared.
	if barriers > 8 {
		t.Fatalf("weak coalescing: %d barriers for %d concurrent writers, want a small constant", barriers, writers)
	}
	t.Logf("%d concurrent writers flushed by %d barriers", writers, barriers)

	// Every overwrite must be durable and correct: coalescing changes how many
	// barriers run, never which writes they cover.
	s.df.syncHook = func(*os.File) error { return nil }
	for i, k := range keys {
		got, found := get(t, s, k)
		if !found || string(got) != string(tval(i+1_000_000)) {
			t.Fatalf("key %d: found=%v got=%q", i, found, got)
		}
	}
}

// TestGroupCommitSequentialUnchanged confirms a single-threaded Full writer still
// gets one barrier per write: with no concurrency there is nothing to coalesce, each
// sync finds no leader running, leads its own batch, and barriers alone. This pins
// that group commit does not weaken the durability a lone writer is promised.
func TestGroupCommitSequentialUnchanged(t *testing.T) {
	s := mustOpenT(t, durableTunables(t, DurabilityFull))
	s.df.syncHook = func(*os.File) error { return nil }

	const writes = 200
	before := s.df.syncCount.Load()
	for i := 0; i < writes; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	barriers := s.df.syncCount.Load() - before
	// Every write flushes through its own barrier (plus the occasional page-roll
	// header barrier), so the count is at least one per write.
	if barriers < writes {
		t.Fatalf("sequential Full writer issued %d barriers for %d writes, want at least one each", barriers, writes)
	}
}

// BenchmarkF2SetFullParallel measures Full-dial throughput under concurrent writers
// with the barrier stubbed to a fixed latency, the regime group commit targets. The
// reported barriers/op falls below one as concurrent writers share barriers, and the
// fixed per-barrier latency means fewer barriers is directly less time on the device.
func BenchmarkF2SetFullParallel(b *testing.B) {
	s := mustOpenT2(b, Tunables{
		Shards:                64,
		PageSize:              4096,
		ResidentPagesPerShard: 4,
		Path:                  b.TempDir() + "/f2.db",
		Durability:            DurabilityFull,
	})
	// A fixed barrier latency stands in for a device flush, so the benchmark measures
	// the coalescing win rather than the host disk's F_FULLFSYNC time.
	s.df.syncHook = func(*os.File) error { time.Sleep(50 * time.Microsecond); return nil }

	const span = 1 << 16
	before := s.df.syncCount.Load()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := s.Set(tkey(i%span), tval(i)); err != nil {
				b.Fatalf("Set: %v", err)
			}
			i++
		}
	})
	b.StopTimer()
	barriers := s.df.syncCount.Load() - before
	if b.N > 0 {
		b.ReportMetric(float64(barriers)/float64(b.N), "barriers/op")
	}
}

// mustOpenT2 opens a store for a benchmark, mirroring mustOpenT for tests.
func mustOpenT2(b *testing.B, tn Tunables) *Store {
	b.Helper()
	s, err := New(tn)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}
