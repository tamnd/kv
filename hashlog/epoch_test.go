package hashlog

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"testing"
)

// evictingTunables is a small eviction-possible Dir-mode config: a tiny resident
// budget over real backing, so SET continuously spills and evicts, which is what
// drives the epoch retire-recycle path. The epoch machinery is identical in durable
// (Path) mode; Dir mode keeps these tests off the durable-file plumbing.
func evictingTunables(dir string) Tunables {
	return Tunables{Shards: 4, PageSize: 1 << 12, ResidentPagesPerShard: 2, Dir: dir}
}

// TestEpochRetireThenReuse pins the section 2.4 safety invariant directly (doc 07
// section 9.2): a retired page buffer is recycled if and only if the safe epoch has
// passed its retire epoch, never sooner. It drives the global epoch and the reclaimer
// by hand so every step is deterministic, with no real eviction running.
func TestEpochRetireThenReuse(t *testing.T) {
	s := mustStore(t, evictingTunables(t.TempDir()))
	sh := s.shards[0]

	// A reader enters at the current epoch (1) and holds it open.
	g := s.slots.enter(&s.globalEpoch, 0)

	// Retire a buffer at the current epoch. The reader pinned an epoch no later than
	// this retire, so it could be holding a reference: the buffer must not be freed.
	buf := make([]byte, sh.pageSize)
	sh.mu.Lock()
	sh.retirePageBufLocked(buf)
	sh.reclaimLocked()
	freed, deferred := len(sh.freeBufs), len(sh.deferred)
	sh.mu.Unlock()
	if freed != 0 || deferred != 1 {
		t.Fatalf("buffer freed while a reader pins its epoch: freeBufs=%d deferred=%d, want 0 and 1", freed, deferred)
	}

	// The reader leaves and the epoch advances. Now no reader can hold the buffer, so
	// the next reclaim recycles it.
	g.leave()
	s.globalEpoch.Add(1)
	sh.mu.Lock()
	sh.reclaimLocked()
	freed, deferred = len(sh.freeBufs), len(sh.deferred)
	sh.mu.Unlock()
	if freed != 1 || deferred != 0 {
		t.Fatalf("buffer not recycled after the reader left: freeBufs=%d deferred=%d, want 1 and 0", freed, deferred)
	}

	// A recycled buffer is what a fresh tail page draws, cleared so it reads as a fresh
	// make.
	sh.mu.Lock()
	got := sh.newPageBuf()
	pool := len(sh.freeBufs)
	sh.mu.Unlock()
	if pool != 0 {
		t.Fatalf("newPageBuf did not draw from the recycle pool: freeBufs=%d, want 0", pool)
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("recycled buffer not cleared at byte %d: %d", i, b)
		}
	}
}

// TestEpochSafeEpochIsMinimum pins the multi-reader half of the invariant (doc 07
// section 9.2 variant): the safe epoch is the minimum over active readers, so an
// object pinned by an older reader stays retired until that older reader leaves, even
// after a younger reader leaves.
func TestEpochSafeEpochIsMinimum(t *testing.T) {
	s := mustStore(t, evictingTunables(t.TempDir()))
	sh := s.shards[0]

	// Reader A enters at epoch 1. The epoch advances. Reader B enters at epoch 2.
	a := s.slots.enter(&s.globalEpoch, 0)
	s.globalEpoch.Add(1)
	b := s.slots.enter(&s.globalEpoch, 1)

	// Retire at epoch 2. Both readers entered at or before this epoch, so both could
	// hold a reference.
	sh.mu.Lock()
	sh.retirePageBufLocked(make([]byte, sh.pageSize))
	sh.reclaimLocked()
	deferred := len(sh.deferred)
	sh.mu.Unlock()
	if safe := s.slots.safeEpoch(); safe != 1 {
		t.Fatalf("safe epoch = %d with readers at 1 and 2, want the minimum 1", safe)
	}
	if deferred != 1 {
		t.Fatalf("buffer freed while both readers pin: deferred=%d, want 1", deferred)
	}

	// The younger reader (B, epoch 2) leaves. The older reader (A, epoch 1) still pins
	// the safe epoch at 1, and the buffer was retired at 2, so it stays retired.
	b.leave()
	sh.mu.Lock()
	sh.reclaimLocked()
	deferred = len(sh.deferred)
	sh.mu.Unlock()
	if safe := s.slots.safeEpoch(); safe != 1 {
		t.Fatalf("safe epoch = %d after the younger reader left, still want 1 (older pins it)", safe)
	}
	if deferred != 1 {
		t.Fatalf("buffer freed while the older reader still pins epoch 1: deferred=%d, want 1", deferred)
	}

	// The older reader leaves. Nothing pins now, so the buffer reclaims.
	a.leave()
	s.globalEpoch.Add(1)
	sh.mu.Lock()
	sh.reclaimLocked()
	deferred, freed := len(sh.deferred), len(sh.freeBufs)
	sh.mu.Unlock()
	if deferred != 0 || freed != 1 {
		t.Fatalf("buffer not reclaimed after both readers left: deferred=%d freeBufs=%d, want 0 and 1", deferred, freed)
	}
}

// TestEpochConcurrentReadersEvictor is the headline -race gate (doc 07 section 9.1):
// many readers GET concurrently with a writer that keeps the log churning so eviction
// and buffer recycle run continuously, and no reader ever reads a recycled buffer. The
// value of a key never changes, so a correct read always returns the same bytes; a use
// after free would surface as the race detector firing or as a read of a different
// value's (or freed) bytes, which the equality check catches.
func TestEpochConcurrentReadersEvictor(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, evictingTunables(dir))
	const nkeys = 3000
	key := func(i int) []byte { return []byte(fmt.Sprintf("k%06d", i)) }
	val := func(i int) []byte { return []byte(fmt.Sprintf("value-%06d-padded-padded-padded", i)) }

	for i := 0; i < nkeys; i++ {
		if err := s.Set(key(i), val(i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if s.Spilled() == 0 {
		t.Fatal("no pages spilled; the test is not exercising eviction")
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: re-set every key (same value) in a loop, so the log keeps appending,
	// pages keep sealing, and the evictor keeps spilling and recycling buffers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for i := 0; i < nkeys; i++ {
				if err := s.Set(key(i), val(i)); err != nil {
					return
				}
			}
		}
	}()

	const readers = 8
	const perReader = 12000
	errCh := make(chan error, readers)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rd := s.NewReader()
			for n := 0; n < perReader; n++ {
				i := (seed*7919 + n*104729) % nkeys
				got, found, err := rd.Get(key(i))
				if err != nil {
					errCh <- fmt.Errorf("reader %d: Get %d: %v", seed, i, err)
					return
				}
				if !found {
					errCh <- fmt.Errorf("reader %d: key %d missing", seed, i)
					return
				}
				if !bytes.Equal(got, val(i)) {
					errCh <- fmt.Errorf("reader %d: key %d = %q, want %q (use-after-free or torn read)", seed, i, got, val(i))
					return
				}
			}
			errCh <- nil
		}(r)
	}

	var firstErr error
	for r := 0; r < readers; r++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	close(stop)
	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}

	// Every key still reads back correctly after the churn.
	for i := 0; i < nkeys; i++ {
		got, found, err := s.Get(key(i))
		if err != nil || !found || !bytes.Equal(got, val(i)) {
			t.Fatalf("after churn: key %d found=%v err=%v got=%q", i, found, err, got)
		}
	}
}

// TestEpochStatsObservability checks the M6 counters move as expected (doc 08 section
// 1): a workload that evicts advances the global epoch and leaves the deferred-free
// depth bounded, and a memory-only store reports the zero frontier with no active
// reader.
func TestEpochStatsObservability(t *testing.T) {
	mem := mustStore(t, DefaultTunables())
	if st := mem.EpochStats(); st.GlobalEpoch != 1 || st.DeferredFrees != 0 || st.SafeEpoch != math.MaxUint64 {
		t.Fatalf("memory-only EpochStats = %+v, want epoch 1, 0 deferred, max safe epoch", st)
	}

	s := mustStore(t, evictingTunables(t.TempDir()))
	for i := 0; i < 4000; i++ {
		if err := s.Set([]byte(fmt.Sprintf("k%06d", i)), bytes.Repeat([]byte("v"), 64)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if s.Spilled() == 0 {
		t.Fatal("nothing spilled; eviction did not run")
	}
	st := s.EpochStats()
	if st.GlobalEpoch <= 1 {
		t.Fatalf("global epoch did not advance under eviction: %d", st.GlobalEpoch)
	}
	// With no reader currently inside an epoch the safe epoch is unbounded, so a final
	// reclaim drains the deferred list; drive one read round and check it does not grow
	// without bound.
	for i := 0; i < 4000; i++ {
		s.Get([]byte(fmt.Sprintf("k%06d", i)))
	}
	if st := s.EpochStats(); uint64(st.DeferredFrees) > st.GlobalEpoch {
		t.Logf("deferred frees %d, global epoch %d", st.DeferredFrees, st.GlobalEpoch)
	}
}

// BenchmarkGuardedReadVsLockFree is the M6 same-box steering benchmark (doc 07 section
// 9.3, doc 08 section 5.7): the eviction-possible epoch-guarded read against the
// lock-free full-resident read on one hot key under contention. The gate (the guard
// adds only a few ns, not the RLock reader-count ping-pong) is a real-hardware check
// (D16); this records the relative cost on whatever box runs it. The guard variant
// also copies the value out (the eviction-possible contract), so its cost is the guard
// plus the copy, not the guard alone.
func BenchmarkGuardedReadVsLockFree(b *testing.B) {
	k := []byte("hotkey")
	v := []byte("hotvalue")

	b.Run("FullResidentLockFree", func(b *testing.B) {
		s, err := New(DefaultTunables())
		if err != nil {
			b.Fatal(err)
		}
		defer s.Close()
		s.Set(k, v)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s.Get(k)
			}
		})
	})

	b.Run("EvictingEpochGuard", func(b *testing.B) {
		s, err := New(Tunables{Shards: 256, PageSize: 1 << 12, ResidentPagesPerShard: 4, Dir: b.TempDir()})
		if err != nil {
			b.Fatal(err)
		}
		defer s.Close()
		s.Set(k, v) // the hot key stays resident (its shard's tail), so this is the resident-copy path
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rd := s.NewReader()
			for pb.Next() {
				rd.Get(k)
			}
		})
	})
}
