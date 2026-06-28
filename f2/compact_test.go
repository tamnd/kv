package f2

import (
	"path/filepath"
	"sync"
	"testing"
)

// compactAll force-rewrites every shard, ignoring the dead-fraction trigger, so a
// test gets a deterministic full compaction regardless of how the churn fell
// across shards. It is the white-box hook the trigger-driven Compact wraps.
func compactAll(t *testing.T, s *Store) {
	t.Helper()
	for _, sh := range s.shards {
		if err := s.forceCompact(sh); err != nil {
			t.Fatalf("forceCompact: %v", err)
		}
	}
}

// TestCompactionOracle is the correctness proof: after a churned store is
// compacted through the public Compact, every key still reads back its newest
// value and every deleted key stays gone. It runs the trigger path with a low
// threshold so the overwrite load actually fires a rewrite on each shard.
func TestCompactionOracle(t *testing.T) {
	tn := Tunables{
		Shards: 8, PageSize: 4096,
		Path:                filepath.Join(t.TempDir(), "f2.db"),
		Durability:          DurabilityNone,
		CompactionThreshold: 0.05,
	}
	s := mustOpenT(t, tn)
	const n = 20000
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
	for i := 0; i < n; i += 2 { // overwrite half: each strands its old record
		set(i, i+1_000_000)
	}
	for i := 0; i < n; i += 5 { // delete a fifth
		if err := s.Delete(tkey(i)); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		delete(oracle, string(tkey(i)))
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	for i := 0; i < n; i++ {
		got, found := get(t, s, tkey(i))
		want, ok := oracle[string(tkey(i))]
		if ok != found {
			t.Fatalf("after compact key %d: found=%v want %v", i, found, ok)
		}
		if found && string(got) != want {
			t.Fatalf("after compact key %d: got %q want %q", i, got, want)
		}
	}
	if st := s.Stats(); st.Keys != int64(len(oracle)) {
		t.Fatalf("after compact Keys=%d want %d", st.Keys, len(oracle))
	}
	// The compacted store must keep taking writes and reading them back.
	if err := s.Set(tkey(n), tval(n)); err != nil {
		t.Fatalf("post-compact Set: %v", err)
	}
	if got, found := get(t, s, tkey(n)); !found || string(got) != string(tval(n)) {
		t.Fatalf("post-compact read: found=%v got=%q", found, got)
	}
}

// TestCompactionReclaimsSpace is the space proof: a store overwritten many times
// over carries far more log than live data, and a full compaction drops the log
// back toward the live size and zeros the dead bytes. This is what turns the
// append-only log from an unbounded store into a bounded one.
func TestCompactionReclaimsSpace(t *testing.T) {
	tn := Tunables{
		Shards: 8, PageSize: 4096,
		Path:       filepath.Join(t.TempDir(), "f2.db"),
		Durability: DurabilityNone,
	}
	s := mustOpenT(t, tn)
	const (
		n      = 10000
		rounds = 6
	)
	for r := 0; r < rounds; r++ { // rewrite every key rounds times, stranding rounds-1x
		for i := 0; i < n; i++ {
			if err := s.Set(tkey(i), tval(i+r*1_000_000)); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
	}
	before := s.Stats()
	if before.DeadBytes == 0 {
		t.Fatal("expected stranded bytes before compaction")
	}

	compactAll(t, s)

	after := s.Stats()
	if after.Keys != before.Keys {
		t.Fatalf("compaction changed live key count: %d -> %d", before.Keys, after.Keys)
	}
	if after.DeadBytes != 0 {
		t.Fatalf("after compaction DeadBytes=%d, want 0", after.DeadBytes)
	}
	if after.LogBytes >= before.LogBytes/2 {
		t.Fatalf("compaction did not reclaim: LogBytes %d -> %d", before.LogBytes, after.LogBytes)
	}
	// Every retired block was drained back to the allocator: no reader was active,
	// so the safe epoch passed every retire immediately.
	if es := s.EpochStats(); es.DeferredFrees != 0 {
		t.Fatalf("after compaction DeferredFrees=%d, want 0 (no active reader)", es.DeferredFrees)
	}
	// Newest value of every key survived the rewrite.
	for i := 0; i < n; i++ {
		got, found := get(t, s, tkey(i))
		if !found || string(got) != string(tval(i+(rounds-1)*1_000_000)) {
			t.Fatalf("after compaction key %d: found=%v got=%q", i, found, got)
		}
	}
	t.Logf("LogBytes %d -> %d (%.1fx), keys=%d", before.LogBytes, after.LogBytes,
		float64(before.LogBytes)/float64(after.LogBytes), after.Keys)
}

// TestCompactionConcurrent rewrites shards under live readers and writers with
// -race on. With a tiny resident budget most pages are evicted, so a reader
// preading an old block races the compaction that retires and may reuse it: this
// is exactly what the epoch gate must make safe. Every key written must read back.
func TestCompactionConcurrent(t *testing.T) {
	tn := Tunables{
		Shards: 16, PageSize: 4096, ResidentPagesPerShard: 2,
		Path:                filepath.Join(t.TempDir(), "f2.db"),
		Durability:          DurabilityNone,
		CompactionThreshold: 0.05,
	}
	s := mustOpenT(t, tn)
	const (
		writers = 6
		readers = 6
		keys    = 15000
		rounds  = 3
	)
	for i := 0; i < keys; i++ { // prefill so readers always have something to find
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("prefill Set: %v", err)
		}
	}

	var workers sync.WaitGroup
	for w := 0; w < writers; w++ {
		workers.Add(1)
		go func(base int) {
			defer workers.Done()
			for r := 0; r < rounds; r++ {
				for i := base; i < keys; i += writers {
					if err := s.Set(tkey(i), tval(i+r*1_000_000)); err != nil {
						t.Errorf("Set: %v", err)
						return
					}
				}
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			rd := s.NewReader()
			for n := 0; n < keys*rounds; n++ {
				if _, _, err := rd.Get(tkey(n % keys)); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}()
	}

	// Spin the compactor until the workers finish, then stop it.
	stop := make(chan struct{})
	var comp sync.WaitGroup
	comp.Add(1)
	go func() {
		defer comp.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := s.Compact(); err != nil {
					t.Errorf("Compact: %v", err)
					return
				}
			}
		}
	}()

	workers.Wait()
	close(stop)
	comp.Wait()

	for i := 0; i < keys; i++ {
		if _, found := get(t, s, tkey(i)); !found {
			t.Fatalf("key %d missing after concurrent compaction", i)
		}
	}
}

// TestCompactionRecovery is the durable proof: a churned store is compacted, then
// crashed without a clean close, then reopened. Recovery must read the committed
// new generation, so every live key reads back its newest value and every deleted
// key stays deleted. A tombstone the compaction dropped must not come back, which
// is the compact-then-crash resurrection case.
func TestCompactionRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f2.db")
	tn := Tunables{Shards: 8, PageSize: 4096, Path: path, Durability: DurabilityNormal}
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const n = 6000
	oracle := map[string]string{}
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
		oracle[string(tkey(i))] = string(tval(i))
	}
	for i := 0; i < n; i += 2 { // overwrites whose new value must win after recovery
		if err := s.Set(tkey(i), tval(i+1_000_000)); err != nil {
			t.Fatalf("Set: %v", err)
		}
		oracle[string(tkey(i))] = string(tval(i + 1_000_000))
	}
	for i := 0; i < n; i += 3 { // deletes that must not resurrect after compact+crash
		if err := s.Delete(tkey(i)); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		delete(oracle, string(tkey(i)))
	}

	compactAll(t, s)
	crash(t, s) // no clean close: only the fsynced new generation is on disk

	s2 := mustOpenT(t, tn)
	for i := 0; i < n; i++ {
		got, found := get(t, s2, tkey(i))
		want, ok := oracle[string(tkey(i))]
		if ok != found {
			t.Fatalf("after compact+recovery key %d: found=%v want %v", i, found, ok)
		}
		if found && string(got) != want {
			t.Fatalf("after compact+recovery key %d: got %q want %q", i, got, want)
		}
	}
	if st := s2.Stats(); st.Keys != int64(len(oracle)) {
		t.Fatalf("after compact+recovery Keys=%d want %d", st.Keys, len(oracle))
	}
}

// TestCompactionEmptyShardNoResurrection pins the empty-generation commit marker.
// A single shard is filled, then every key is deleted, then compacted: the new
// generation is an empty page 0. Without that page 0 recovery would fall back to
// the old generation's surviving page 0 and resurrect every deleted key. After a
// crash the store must come back empty.
func TestCompactionEmptyShardNoResurrection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f2.db")
	tn := Tunables{Shards: 1, PageSize: 4096, Path: path, Durability: DurabilityNormal}
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const n = 500
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	for i := 0; i < n; i++ {
		if err := s.Delete(tkey(i)); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}

	compactAll(t, s)
	crash(t, s)

	s2 := mustOpenT(t, tn)
	for i := 0; i < n; i++ {
		if _, found := get(t, s2, tkey(i)); found {
			t.Fatalf("key %d resurrected after compact+crash of an emptied shard", i)
		}
	}
	if st := s2.Stats(); st.Keys != 0 {
		t.Fatalf("after compact+crash of an emptied shard Keys=%d, want 0", st.Keys)
	}
}
