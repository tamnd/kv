package f2

import (
	"path/filepath"
	"sync"
	"testing"
)

// budgetedSingleShard builds a one-shard single-file store with a tiny page and a
// small resident budget, the configuration that rolls and evicts pages fast so the
// page-buffer recycler (audit L5) runs hard at a modest key count. One shard keeps
// the white-box assertions about that shard's log unambiguous.
func budgetedSingleShard(t *testing.T, budget int) (*Store, Tunables) {
	t.Helper()
	tn := Tunables{
		Shards:                1,
		PageSize:              256,
		ResidentPagesPerShard: budget,
		Path:                  filepath.Join(t.TempDir(), "f2.db"),
		Durability:            DurabilityNone,
	}
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, tn
}

// residentPages counts how many of a log's pages still hold a buffer in RAM. The
// caller holds the shard write lock.
func residentPages(l *log) int {
	d := l.dir.Load()
	n := 0
	for pi := 0; pi < l.npages; pi++ {
		if d.refs[pi].Load().mem != nil {
			n++
		}
	}
	return n
}

// TestRecycleEvictedBuffersPooled proves the L5 mechanism: a budgeted log that has
// evicted many pages reclaims their buffers into its free pool rather than dropping
// them to the garbage collector. With no concurrent reader the safe epoch is
// unbounded, so every retired buffer clears on the next eviction, and the pool ends
// up holding the buffers of the pages that left RAM. The test also pins the
// invariants the recycler must keep: the resident page count stays inside the
// budget (plus the tail being filled), and the live buffer set stays a small
// multiple of the budget no matter how many pages were allocated, so nothing leaks.
func TestRecycleEvictedBuffersPooled(t *testing.T) {
	const budget = 4
	s, _ := budgetedSingleShard(t, budget)

	const n = 6000
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	sh := s.shards[0]
	sh.mu.Lock()
	npages := sh.log.npages
	resident := residentPages(sh.log)
	free := len(sh.log.freeBufs)
	sh.mu.Unlock()

	if npages <= budget+1 {
		t.Fatalf("expected many page rolls, got npages=%d (budget=%d)", npages, budget)
	}
	if free == 0 {
		t.Fatalf("expected evicted buffers reclaimed into the pool, freeBufs=0 after %d pages", npages)
	}
	if resident > budget+1 {
		t.Fatalf("resident pages %d exceed budget+1 (%d)", resident, budget+1)
	}
	// Live buffers are the resident pages plus the recycle pool. Recycling caps this
	// near the budget; without it the count would track npages. A generous bound still
	// separates the two by an order of magnitude at this scale.
	liveBufs := resident + free
	if liveBufs > 4*(budget+1) {
		t.Fatalf("live buffer set %d not bounded near budget (resident=%d free=%d npages=%d)",
			liveBufs, resident, free, npages)
	}
	t.Logf("npages=%d resident=%d freeBufs=%d liveBufs=%d", npages, resident, free, liveBufs)

	// Every key still reads back its value: recycling buffers under the reader must
	// not corrupt a single record.
	for i := 0; i < n; i++ {
		got, found := get(t, s, tkey(i))
		if !found || string(got) != string(tval(i)) {
			t.Fatalf("key %d: found=%v got=%q", i, found, got)
		}
	}
}

// TestRecycleRecoveryNoResurrection guards the zeroing in newPageBuf. A recycled
// buffer carries the records of the page it last held, each with a valid CRC.
// Recovery walks a page until a record fails to decode, so if a recycled buffer
// were reused without wiping, its stale tail would decode as live records and
// resurrect dead data. The test drives heavy eviction and recycling with overwrites
// and deletes, takes a clean Close so every page is on disk, reopens, and requires
// the recovered key set to match the oracle exactly, no extra keys and no
// resurrected deletes.
func TestRecycleRecoveryNoResurrection(t *testing.T) {
	tn := Tunables{
		Shards:                1,
		PageSize:              256,
		ResidentPagesPerShard: 2,
		Path:                  filepath.Join(t.TempDir(), "f2.db"),
		Durability:            DurabilityNormal,
	}
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const n = 5000
	oracle := map[string]string{}
	set := func(i, vi int) {
		if err := s.Set(tkey(i), tval(vi)); err != nil {
			t.Fatalf("Set: %v", err)
		}
		oracle[string(tkey(i))] = string(tval(vi))
	}
	for i := 0; i < n; i++ {
		set(i, i)
	}
	for i := 0; i < n; i += 2 { // overwrites: the old record is stranded, often on a buffer that recycles
		set(i, i+1_000_000)
	}
	for i := 0; i < n; i += 5 { // deletes that must not come back after recovery
		if err := s.Delete(tkey(i)); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		delete(oracle, string(tkey(i)))
	}

	sh := s.shards[0]
	sh.mu.Lock()
	free := len(sh.log.freeBufs)
	sh.mu.Unlock()
	if free == 0 {
		t.Fatal("expected buffer recycling before recovery, freeBufs=0")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := New(tn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	seen := 0
	if err := s2.Scan(func(k, v []byte) bool {
		seen++
		want, ok := oracle[string(k)]
		if !ok {
			t.Fatalf("recovered key %q not in oracle: a stale record was resurrected", k)
		}
		if string(v) != want {
			t.Fatalf("recovered key %q: got %q want %q", k, v, want)
		}
		return true
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if seen != len(oracle) {
		t.Fatalf("recovered %d keys, oracle has %d", seen, len(oracle))
	}
}

// TestRecycleConcurrentReadersDuringEviction stresses the read-under-recycle path
// with -race on: many readers copy values out of a budgeted shard while a writer
// evicts and recycles the buffers those reads alias. A buffer must not be wiped and
// reused while a reader is mid-copy of it; the shard read lock is what prevents that,
// since the evictor holds the write lock. The reader count is set well above the
// machine's stripe pool so any latent reliance on the epoch stripes (which alias
// under that many readers) would surface as a torn read. Every read must return
// either the absence of the key or one of its written values, never torn bytes.
func TestRecycleConcurrentReadersDuringEviction(t *testing.T) {
	s, _ := budgetedSingleShard(t, 3)
	const (
		keys    = 4000
		readers = 48
		rounds  = 3
	)
	valid := map[string]bool{}
	for r := 0; r <= rounds; r++ {
		for i := 0; i < keys; i++ {
			valid[string(tval(i+r*1_000_000))] = true
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := 1; r <= rounds; r++ {
			for i := 0; i < keys; i++ {
				if err := s.Set(tkey(i), tval(i+r*1_000_000)); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
			}
		}
	}()
	for i := 0; i < keys; i++ { // seed round 0 so readers find values from the start
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("seed Set: %v", err)
		}
	}
	for rd := 0; rd < readers; rd++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < keys*rounds; n++ {
				v, found, err := s.Get(tkey(n % keys))
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				if found && !valid[string(v)] {
					t.Errorf("torn read: key %d returned %q", n%keys, v)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// BenchmarkBudgetedWriteChurn measures the steady-state write path of a budgeted
// single-file store whose working set far exceeds its resident budget, so every few
// Sets roll a new page and evict an old one. Keys and values are precomputed so the
// reported allocs and bytes per op are the store's own, not the harness's: the
// page-buffer recycler's effect then shows directly, since with recycling a page
// roll draws a wiped buffer from the pool instead of allocating a fresh one, cutting
// the garbage the collector must sweep under sustained writes.
func BenchmarkBudgetedWriteChurn(b *testing.B) {
	tn := Tunables{
		Shards:                1,
		PageSize:              4096,
		ResidentPagesPerShard: 8,
		Path:                  filepath.Join(b.TempDir(), "f2.db"),
		Durability:            DurabilityNone,
	}
	s, err := New(tn)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer s.Close()

	const span = 200000
	keys := make([][]byte, span)
	vals := make([][]byte, span)
	for i := range keys {
		keys[i] = tkey(i)
		vals[i] = tval(i)
	}

	// Prime past the budget so the loop runs in the evict-and-recycle steady state
	// rather than the initial fill.
	for i := 0; i < span; i++ {
		if err := s.Set(keys[i], vals[i]); err != nil {
			b.Fatalf("prime Set: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := i % span
		if err := s.Set(keys[k], vals[k]); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
}
