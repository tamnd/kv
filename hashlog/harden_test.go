package hashlog

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// harden_test.go is the M10 hardening campaign (spec 2070 doc 08 section 2.3 M10, section 3
// and 4): the validation harnesses the earlier milestones built, run long and wide over the
// whole durable engine now that every mechanism is present, including the M9 oversize path.
// M10 builds no new engine code; it proves the engine. The three shapes are the long
// single-threaded oracle with crash-recover folded in, the concurrent oracle under -race, and
// the crash-injection sweep across dials with oversize values. The real-hardware durable
// benchmark (D16) is the other half of the gate and runs on the quiet box, never here, so it
// is not in this file; the impl doc records its definition and the kvbench adapter mapping.

// hardenSteps scales the campaign length to the run mode: short for the default fast suite, a
// long sweep when the suite is allowed to run in full. The seed is fixed so a failure
// reproduces exactly, the property the oracle discipline rests on (doc 08 section 3.3).
func hardenSteps(t *testing.T) int {
	if testing.Short() {
		return 4000
	}
	return 40000
}

// hardenValue returns a value whose size is usually small (the inline common case) but
// occasionally spans several extents (the M9 oversize path), keyed by the rng so a replay is
// deterministic. The oversize fraction is small on purpose, matching the design intent that
// the spanning path is reached only by genuinely large values (doc 03 section 7).
func hardenValue(rng *rand.Rand, pageSize int) []byte {
	var n int
	if rng.Intn(8) == 0 {
		// Oversize: larger than one extent, up to several extents, so the cont chain, the
		// descriptor, and the CRC-checked reassembly are all exercised.
		n = pageSize + 1 + rng.Intn(pageSize*5)
	} else {
		n = 8 + rng.Intn(pageSize/2)
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.Intn(256))
	}
	return b
}

// TestM10HardeningOracle is the long single-threaded oracle (doc 08 section 3): a randomized
// set/delete/get stream over a bounded, overwrite-heavy key space, with checkpoints and
// compactions folded in mid-stream and a checkpoint-then-crash-recover cycle that reopens the
// store from a frozen image and re-asserts the whole live set. Values include the oversize
// class, so the campaign exercises the spanning write, read, compaction, and recovery against
// the reference model. Every Get asserts its key; every recovery asserts the whole store.
func TestM10HardeningOracle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harden-oracle.hlog")
	tun := compactTunables(path, DurabilityNormal)
	s := mustStore(t, tun)
	m := newModel()
	rng := rand.New(rand.NewSource(2070))

	const keys = 600
	steps := hardenSteps(t)
	sawOversize := false

	for step := 0; step < steps; step++ {
		k := key(rng.Intn(keys))
		switch rng.Intn(10) {
		case 0, 1: // delete
			if err := s.Delete(k); err != nil {
				t.Fatalf("step %d Delete: %v", step, err)
			}
			m.del(k)
		default: // set, sometimes oversize
			v := hardenValue(rng, tun.PageSize)
			if err := s.Set(k, v); err != nil {
				t.Fatalf("step %d Set (%d bytes): %v", step, len(v), err)
			}
			m.set(k, v)
			if durableRecordLen(k, v) > tun.PageSize {
				sawOversize = true
			}
		}

		// Spot-check the touched key against the model every step (doc 08 section 3.2: one
		// assertion per Get).
		if want, ok := m.get(k); ok {
			v, found, err := s.Get(k)
			if err != nil || !found || string(v) != string(want) {
				t.Fatalf("step %d Get(%q) found=%v err=%v value mismatch", step, k, found, err)
			}
		} else if _, found, _ := s.Get(k); found {
			t.Fatalf("step %d Get(%q) present, model says deleted", step, k)
		}

		// Fold in compaction, checkpoint, and a checkpoint-then-crash-recover cycle at staggered
		// intervals. The crash-recover reopens from a frozen image: a checkpoint immediately
		// before the freeze under Normal syncs the whole store, so the durable prefix is the
		// entire model and the recovered store must equal it exactly.
		switch {
		case step%2000 == 1999:
			if err := s.Compact(); err != nil {
				t.Fatalf("step %d Compact: %v", step, err)
			}
		case step%2000 == 999:
			if err := s.Checkpoint(); err != nil {
				t.Fatalf("step %d Checkpoint: %v", step, err)
			}
		case step%5000 == 2500:
			if err := s.Checkpoint(); err != nil {
				t.Fatalf("step %d pre-crash Checkpoint: %v", step, err)
			}
			frozen := readWholeFile(t, s.df.f)
			cp := filepath.Join(dir, fmt.Sprintf("harden-frozen-%d.hlog", step))
			writeWholeFile(t, cp, frozen)
			if err := s.Close(); err != nil {
				t.Fatalf("step %d close before recover: %v", step, err)
			}
			rt := tun
			rt.Path = cp
			rs, err := New(rt)
			if err != nil {
				t.Fatalf("step %d recover frozen image: %v", step, err)
			}
			assertStoreMatches(t, rs, m)
			checkConservationM8(t, rs)
			// Continue the campaign on the recovered store at its new path.
			s = rs
			tun = rt
		}
	}

	if !sawOversize {
		t.Fatal("campaign generated no oversize values; the spanning path was not exercised")
	}
	if s.CompactionStats().CompactedExtents == 0 {
		t.Fatal("campaign never compacted an extent")
	}

	// Final full sweep, then one more recovery, so the end state (compacted, with live oversize
	// chains) recovers to exactly the model.
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	rs := reopen(t, s, tun)
	defer rs.Close()
	assertStoreMatches(t, rs, m)
	checkConservationM8(t, rs)
}

// TestM10ConcurrentDurableRace is the concurrent oracle under -race (doc 08 section 3.3): many
// writers drive a durable eviction-possible store, each owning a disjoint key range so the
// expected per-key outcome stays computable, while a checkpointer, a compactor, and a reader
// run alongside. Values include the oversize class, so a lock-free-adjacent reader can race a
// spanning write, an evictor, and a compactor. The model is updated under a lock; the final
// state and a reopen must match it. The race detector catches the data-race form of a bug, the
// model diff the logic form.
func TestM10ConcurrentDurableRace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harden-race.hlog")
	tun := compactTunables(path, DurabilityNormal)
	s := mustStore(t, tun)

	const writers = 6
	keysPerWriter := 80
	ops := 1500
	if testing.Short() {
		ops = 400
	}

	var mu sync.Mutex
	live := map[string][]byte{}
	rkey := func(w, i int) []byte { return []byte(fmt.Sprintf("w%02d:k%04d", w, i)) }

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 100))
			for i := 0; i < ops; i++ {
				k := rkey(w, rng.Intn(keysPerWriter))
				if rng.Intn(8) == 0 {
					if err := s.Delete(k); err != nil {
						t.Errorf("writer %d Delete: %v", w, err)
						return
					}
					mu.Lock()
					delete(live, string(k))
					mu.Unlock()
					continue
				}
				v := hardenValue(rng, tun.PageSize)
				if err := s.Set(k, v); err != nil {
					t.Errorf("writer %d Set: %v", w, err)
					return
				}
				mu.Lock()
				live[string(k)] = v
				mu.Unlock()
			}
		}(w)
	}

	// A checkpointer and a compactor churn the durability artifacts while the writers run, so
	// the snapshot capture, the extent free, and the live-copy-forward race the writes.
	stop := make(chan struct{})
	var bg sync.WaitGroup
	bg.Add(2)
	go func() {
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.Checkpoint()
			}
		}
	}()
	go func() {
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.Compact()
			}
		}
	}()
	// A concurrent reader, so a GET races every writer's index publish and the background
	// evictor and compactor.
	bg.Add(1)
	go func() {
		defer bg.Done()
		rng := rand.New(rand.NewSource(999))
		for {
			select {
			case <-stop:
				return
			default:
				if _, _, err := s.Get(rkey(rng.Intn(writers), rng.Intn(keysPerWriter))); err != nil {
					t.Errorf("reader Get: %v", err)
					return
				}
			}
		}
	}()

	wg.Wait()
	close(stop)
	bg.Wait()

	// Final state matches the model: every live key reads its last value, no deleted key
	// reappears. Then a reopen recovers to the same set.
	mu.Lock()
	final := &model{live: live}
	mu.Unlock()
	assertStoreMatches(t, s, final)
	checkConservationM8(t, s)

	rs := reopen(t, s, tun)
	defer rs.Close()
	assertStoreMatches(t, rs, final)
	checkConservationM8(t, rs)
}

// TestM10CrashSweepOversizeFull is the crash-injection sweep (doc 08 section 4) extended to
// the oversize path: under Full every acknowledged write is synced before it returns, so the
// image frozen at sync boundary c holds exactly the first c writes. The workload mixes inline
// and oversize values, so a frozen image cuts across a spanning write's home record and cont
// chain, and the recovered store must equal the durable prefix at every cut, with no torn
// oversize value half-surviving.
func TestM10CrashSweepOversizeFull(t *testing.T) {
	dir := t.TempDir()
	tun := Tunables{Shards: 4, PageSize: 1024, ExtentSize: 1024, ResidentPagesPerShard: 2, Path: filepath.Join(dir, "sweep.hlog"), Durability: DurabilityFull}
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}

	const n = 1200
	freezeAt := map[int64]bool{n / 4: true, n / 2: true, (3 * n) / 4: true, n: true}
	frozen := map[int64][]byte{}
	s.df.syncHook = func(f *os.File) error {
		c := s.df.syncCount.Load()
		if freezeAt[c] {
			fi, err := f.Stat()
			if err != nil {
				return err
			}
			buf := make([]byte, fi.Size())
			if _, err := f.ReadAt(buf, 0); err != nil {
				return err
			}
			frozen[c] = buf
		}
		return platformSyncData(f)
	}

	rng := rand.New(rand.NewSource(7070))
	m := newModel()
	want := map[int64]*model{}
	syncCount := int64(0)
	for i := 1; i <= n; i++ {
		k := []byte(fmt.Sprintf("s%d", i%400))
		v := hardenValue(rng, tun.PageSize)
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		m.set(k, v)
		// Each SET under Full is one home-record sync (an oversize value's cont writes ride the
		// same whole-file barrier, so they do not add a sync). The sync count therefore tracks
		// the write index, and a freeze boundary lands on a known prefix.
		syncCount++
		if freezeAt[syncCount] {
			want[syncCount] = cloneModel(m)
		}
	}
	if s.OversizeValues() == 0 {
		t.Fatal("sweep generated no oversize values")
	}
	if s.Spilled() == 0 {
		t.Fatal("sweep did not spill; the disk path was not exercised")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	cuts := make([]int64, 0, len(frozen))
	for c := range frozen {
		cuts = append(cuts, c)
	}
	sort.Slice(cuts, func(i, j int) bool { return cuts[i] < cuts[j] })
	if len(cuts) != len(freezeAt) {
		t.Fatalf("captured %d frozen images, wanted %d", len(cuts), len(freezeAt))
	}

	for _, c := range cuts {
		p := filepath.Join(dir, fmt.Sprintf("sweep-frozen-%d.hlog", c))
		if err := os.WriteFile(p, frozen[c], 0o644); err != nil {
			t.Fatal(err)
		}
		rt := tun
		rt.Path = p
		rs, err := New(rt)
		if err != nil {
			t.Fatalf("recover frozen image at sync %d: %v", c, err)
		}
		// The recovered store equals the model after exactly c writes: no acknowledged write
		// lost, nothing past the cut applied, and every recovered oversize value reads whole
		// (its read verifies the trailing CRC, so a torn chain would fail the Get, not pass).
		assertStoreMatches(t, rs, want[c])
		checkConservationM8(t, rs)
		if err := rs.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
