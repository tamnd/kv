package f2

import (
	"sync"
	"testing"
)

// spillTunables builds a deliberately tiny larger-than-memory store: small pages
// and a two-page resident budget per shard, so all but the newest sliver of each
// shard's log spills to disk. This is the configuration that exercises the spill
// and pread paths hardest at a small key count.
func spillTunables(t *testing.T) Tunables {
	t.Helper()
	return Tunables{
		Shards:                16,
		PageSize:              4096,
		ResidentPagesPerShard: 2,
		Dir:                   t.TempDir(),
	}
}

func mustOpenT(t *testing.T, tn Tunables) *Store {
	t.Helper()
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestSpillOracle is the oracle test under eviction: with most pages spilled to
// disk, every surviving key must still read back its newest value, including keys
// whose records were evicted to the scratch file and must be preadded. It also
// overwrites and deletes so the spilled and resident records mix on the same
// probe chains.
func TestSpillOracle(t *testing.T) {
	s := mustOpenT(t, spillTunables(t))
	const n = 30000
	oracle := map[string]string{}
	put := func(i, vi int) {
		if err := s.Set(tkey(i), tval(vi)); err != nil {
			t.Fatalf("Set: %v", err)
		}
		oracle[string(tkey(i))] = string(tval(vi))
	}
	for i := 0; i < n; i++ {
		put(i, i)
	}
	for i := 0; i < n; i += 3 { // overwrite a third; new record lands resident, old spilled
		put(i, i+1_000_000)
	}
	for i := 0; i < n; i += 7 {
		if err := s.Delete(tkey(i)); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		delete(oracle, string(tkey(i)))
	}
	for i := 0; i < n; i++ {
		got, found := get(t, s, tkey(i))
		want, ok := oracle[string(tkey(i))]
		if ok != found {
			t.Fatalf("key %d: found=%v want %v", i, found, ok)
		}
		if found && string(got) != want {
			t.Fatalf("key %d: got %q want %q", i, got, want)
		}
	}

	st := s.Stats()
	if st.SpilledLog == 0 {
		t.Fatalf("expected some log spilled to disk, got SpilledLog=0")
	}
	t.Logf("keys=%d resident-log=%.1f KiB spilled-log=%.1f KiB",
		st.Keys, float64(st.ResidentLog)/1024, float64(st.SpilledLog)/1024)
}

// TestSpillConcurrent runs the spill store under many readers and writers with
// -race on. Beyond the lock-free index it also stresses the atomic page-ref swap
// the eviction performs while readers are loading the directory, and the pread
// path racing the write path. A reader may pread a page a writer just spilled;
// both must stay correct.
func TestSpillConcurrent(t *testing.T) {
	s := mustOpenT(t, spillTunables(t))
	const (
		writers = 8
		readers = 8
		keys    = 20000
		rounds  = 2
	)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				for i := shard; i < keys; i += writers {
					if err := s.Set(tkey(i), tval(i+r*1_000_000)); err != nil {
						t.Errorf("Set: %v", err)
						return
					}
				}
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < keys*rounds; n++ {
				if _, _, err := s.Get(tkey(n % keys)); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for i := 0; i < keys; i++ {
		if _, found := get(t, s, tkey(i)); !found {
			t.Fatalf("key %d missing after concurrent spill fill", i)
		}
	}
}

// TestSpillBoundsResidentLog is the larger-than-memory proof. The resident log
// footprint must stay capped at the budget no matter how much data is inserted,
// while the spilled total grows without bound. This is what lets the value log
// exceed RAM: a server with a few hundred MiB of resident pages holds a working
// set of terabytes on disk. The index stays compact-resident as in the in-memory
// core, so the two together place a known, bounded ceiling on RAM.
func TestSpillBoundsResidentLog(t *testing.T) {
	tn := spillTunables(t)
	s := mustOpenT(t, tn)

	// The hard ceiling on resident log: every shard may hold up to its budget of
	// full pages plus the partial tail it is filling, so budget+1 pages per shard.
	cap := int64(tn.Shards) * int64(tn.ResidentPagesPerShard+1) * int64(tn.PageSize)

	check := func(after int) {
		st := s.Stats()
		if st.ResidentLog > cap {
			t.Fatalf("after %d keys: resident log %d exceeds cap %d", after, st.ResidentLog, cap)
		}
	}
	const n = 60000
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if i%10000 == 0 {
			check(i)
		}
	}
	check(n)
	st := s.Stats()
	if st.SpilledLog <= st.ResidentLog {
		t.Fatalf("expected spilled log to dominate at scale: spilled=%d resident=%d", st.SpilledLog, st.ResidentLog)
	}
	t.Logf("keys=%d resident-log=%.1f MiB (cap %.1f MiB) spilled-log=%.1f MiB index=%.1f MiB",
		st.Keys, float64(st.ResidentLog)/(1<<20), float64(cap)/(1<<20),
		float64(st.SpilledLog)/(1<<20), float64(st.IndexBytes)/(1<<20))
}

// TestSpillNeedsBudget pins the validation: a spill directory without a resident
// budget is rejected, because a shard must keep at least the tail page in RAM to
// append to it.
func TestSpillNeedsBudget(t *testing.T) {
	_, err := New(Tunables{Shards: 4, PageSize: 4096, Dir: t.TempDir()})
	if err != errBadTunables {
		t.Fatalf("spill without budget: got %v want errBadTunables", err)
	}
}
