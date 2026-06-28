package f2

import (
	"path/filepath"
	"sync"
	"testing"
)

// durableTunables builds a deliberately tiny single-file store: small pages and a
// two-page resident budget per shard, so all but the newest sliver of each
// shard's log is evicted to the file and must be reread by offset. This is the
// configuration that exercises the eviction, pread, and recovery paths hardest at
// a small key count. dial picks the durability regime under test.
func durableTunables(t *testing.T, dial Durability) Tunables {
	t.Helper()
	return Tunables{
		Shards:                16,
		PageSize:              4096,
		ResidentPagesPerShard: 2,
		Path:                  filepath.Join(t.TempDir(), "f2.db"),
		Durability:            dial,
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

// TestDurableWriteErrorSurfaces proves the D2 fix: a failed durable write returns
// an error to the caller instead of acknowledging a write that never reached
// disk. The old path swallowed every WriteAt/Sync error. We close the backing
// file underneath a Full-dial store so the next write-through hits a closed fd,
// and require Set to report it. A Full Set writes through on every call, so the
// first Set after the close must fail.
func TestDurableWriteErrorSurfaces(t *testing.T) {
	s := mustOpenT(t, durableTunables(t, DurabilityFull))
	if err := s.Set(tkey(0), tval(0)); err != nil {
		t.Fatalf("Set before close: %v", err)
	}
	_ = s.df.f.Close() // pull the file out from under the store
	var sawErr bool
	for i := 1; i < 4096; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			sawErr = true
			break
		}
	}
	if !sawErr {
		t.Fatal("Set acknowledged a write after the backing file was closed")
	}
}

// TestDurableOracle is the oracle test under eviction: with most pages evicted to
// the file, every surviving key must still read back its newest value, including
// keys whose records left RAM and must be preadded. It overwrites and deletes so
// the evicted and resident records mix on the same probe chains. It runs the None
// dial: the file is the larger-than-memory backing, no fsync on the hot path.
func TestDurableOracle(t *testing.T) {
	s := mustOpenT(t, durableTunables(t, DurabilityNone))
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
	for i := 0; i < n; i += 3 { // overwrite a third; new record resident, old evicted
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
	if st.EvictedLog == 0 {
		t.Fatalf("expected some log evicted to the file, got EvictedLog=0")
	}
	t.Logf("keys=%d resident-log=%.1f KiB evicted-log=%.1f KiB",
		st.Keys, float64(st.ResidentLog)/1024, float64(st.EvictedLog)/1024)
}

// TestDurableConcurrent runs the single-file store under many readers and writers
// with -race on. Beyond the lock-free index it stresses the atomic page-ref swap
// the eviction performs while readers are loading the directory, and the pread
// path racing the write path. A reader may pread a page a writer just evicted;
// both must stay correct.
func TestDurableConcurrent(t *testing.T) {
	s := mustOpenT(t, durableTunables(t, DurabilityNone))
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
			t.Fatalf("key %d missing after concurrent fill", i)
		}
	}
}

// TestDurableBoundsResidentLog is the larger-than-memory proof. The resident log
// footprint must stay capped at the budget no matter how much data is inserted,
// while the evicted total grows without bound. This is what lets the value log
// exceed RAM: a server with a few hundred MiB of resident pages holds a working
// set of terabytes in the file. The index stays compact-resident as in the
// in-memory core, so the two together place a known, bounded ceiling on RAM.
func TestDurableBoundsResidentLog(t *testing.T) {
	tn := durableTunables(t, DurabilityNone)
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
	if st.EvictedLog <= st.ResidentLog {
		t.Fatalf("expected evicted log to dominate at scale: evicted=%d resident=%d", st.EvictedLog, st.ResidentLog)
	}
	t.Logf("keys=%d resident-log=%.1f MiB (cap %.1f MiB) evicted-log=%.1f MiB index=%.1f MiB",
		st.Keys, float64(st.ResidentLog)/(1<<20), float64(cap)/(1<<20),
		float64(st.EvictedLog)/(1<<20), float64(st.IndexBytes)/(1<<20))
}

// TestDurableRecovery is the crash-recovery proof. It writes under the Full dial
// (every Set fsynced), simulates a crash by dropping the store without a clean
// Close, reopens the same file, and checks every key, overwrite, and delete
// survived. A delete must not resurrect: the logged tombstone has to win over the
// earlier value record during replay.
func TestDurableRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f2.db")
	tn := Tunables{Shards: 8, PageSize: 4096, Path: path, Durability: DurabilityFull}

	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Full fsyncs every Set, so keep the count modest; the large-scale recovery run
	// is TestDurableRecoveryEvicted under the cheaper Normal dial.
	const n = 2000
	oracle := map[string]string{}
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
		oracle[string(tkey(i))] = string(tval(i))
	}
	for i := 0; i < n; i += 2 { // overwrites that must win after recovery
		if err := s.Set(tkey(i), tval(i+1_000_000)); err != nil {
			t.Fatalf("Set: %v", err)
		}
		oracle[string(tkey(i))] = string(tval(i + 1_000_000))
	}
	for i := 0; i < n; i += 5 { // deletes that must not resurrect
		if err := s.Delete(tkey(i)); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		delete(oracle, string(tkey(i)))
	}
	// Simulate a crash: release the file descriptor without the clean-close
	// checkpoint. Under Full every acknowledged write is already fsynced.
	crash(t, s)

	s2, err := New(tn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	for i := 0; i < n; i++ {
		got, found := get(t, s2, tkey(i))
		want, ok := oracle[string(tkey(i))]
		if ok != found {
			t.Fatalf("after recovery key %d: found=%v want %v", i, found, ok)
		}
		if found && string(got) != want {
			t.Fatalf("after recovery key %d: got %q want %q", i, got, want)
		}
	}
	if st := s2.Stats(); st.Keys != int64(len(oracle)) {
		t.Fatalf("after recovery Keys=%d want %d", st.Keys, len(oracle))
	}
	// The recovered store must take new writes and read them back.
	if err := s2.Set(tkey(n), tval(n)); err != nil {
		t.Fatalf("post-recovery Set: %v", err)
	}
	if got, found := get(t, s2, tkey(n)); !found || string(got) != string(tval(n)) {
		t.Fatalf("post-recovery read: found=%v got=%q", found, got)
	}
}

// TestDurableRecoveryEvicted recovers a larger-than-memory store: a tiny budget
// means most pages were evicted before the crash, so recovery must rebuild the
// index from records it rereads out of the file, not from RAM. It uses Normal
// with a clean Close so every sealed page is on disk.
func TestDurableRecoveryEvicted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f2.db")
	tn := Tunables{Shards: 8, PageSize: 4096, ResidentPagesPerShard: 2, Path: path, Durability: DurabilityNormal}

	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const n = 40000
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	st := s.Stats()
	if st.EvictedLog == 0 {
		t.Fatalf("expected eviction before recovery, got EvictedLog=0")
	}
	if err := s.Close(); err != nil { // clean close seals and stamps the superblock
		t.Fatalf("Close: %v", err)
	}

	s2 := mustOpenT(t, tn)
	for i := 0; i < n; i++ {
		got, found := get(t, s2, tkey(i))
		if !found || string(got) != string(tval(i)) {
			t.Fatalf("after recovery key %d: found=%v got=%q", i, found, got)
		}
	}
	if got := s2.Stats(); got.Keys != n {
		t.Fatalf("after recovery Keys=%d want %d", got.Keys, n)
	}
}

// TestDurableValidation pins the open-time contracts for the single-file mode: a
// memory-only store rejects a durability dial and a resident budget (it has
// nowhere to sync or evict), and a record larger than the usable page is refused.
func TestDurableValidation(t *testing.T) {
	if _, err := New(Tunables{Shards: 4, PageSize: 4096, Durability: DurabilityNormal}); err != errBadTunables {
		t.Fatalf("dial without path: got %v want errBadTunables", err)
	}
	if _, err := New(Tunables{Shards: 4, PageSize: 4096, ResidentPagesPerShard: 2}); err != errBadTunables {
		t.Fatalf("budget without path: got %v want errBadTunables", err)
	}
	path := filepath.Join(t.TempDir(), "f2.db")
	s := mustOpenT(t, Tunables{Shards: 4, PageSize: 256, Path: path})
	if err := s.Set([]byte("k"), make([]byte, 256)); err != errValueTooBig {
		t.Fatalf("oversize durable Set: got %v want errValueTooBig", err)
	}
}

// crash releases a durable store's file descriptor without the clean-close
// checkpoint, modeling a process that died mid-run. Anything the dial had already
// fsynced is on disk; the rest is the dial's loss window.
func crash(t *testing.T, s *Store) {
	t.Helper()
	if s.df == nil {
		t.Fatal("crash called on a memory-only store")
	}
	_ = s.df.f.Close()
}
