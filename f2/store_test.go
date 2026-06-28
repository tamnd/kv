package f2

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func tkey(i int) []byte { return []byte(fmt.Sprintf("key:%08d", i)) }
func tval(i int) []byte { return []byte(fmt.Sprintf("val-%08d-payload", i)) }

func mustOpen(t *testing.T) *Store {
	t.Helper()
	s, err := New(DefaultTunables())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func get(t *testing.T, s *Store, k []byte) ([]byte, bool) {
	t.Helper()
	v, ok, err := s.Get(k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return v, ok
}

// TestScan checks that Scan visits exactly the live keys, with their latest
// values, after a run of inserts, overwrites and deletes that crosses a grow.
// Order is unspecified, so it collects into a map and compares against the oracle.
func TestScan(t *testing.T) {
	s := mustOpen(t)
	const n = 5000
	want := map[string]string{}
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
		want[string(tkey(i))] = string(tval(i))
	}
	for i := 0; i < n; i += 2 { // overwrite the evens
		if err := s.Set(tkey(i), tval(i+n)); err != nil {
			t.Fatalf("Set overwrite: %v", err)
		}
		want[string(tkey(i))] = string(tval(i + n))
	}
	for i := 0; i < n; i += 5 { // delete every fifth
		if err := s.Delete(tkey(i)); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		delete(want, string(tkey(i)))
	}

	got := map[string]string{}
	err := s.Scan(func(k, v []byte) bool {
		got[string(k)] = string(v)
		return true
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("Scan visited %d keys, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("Scan key %q: got %q want %q", k, got[k], v)
		}
	}

	// Early stop: returning false must end the scan promptly.
	count := 0
	_ = s.Scan(func(k, v []byte) bool { count++; return count < 10 })
	if count != 10 {
		t.Fatalf("early-stop Scan visited %d, want 10", count)
	}
}

// TestGetCopy checks that GetCopy returns an owned slice the caller can mutate
// without disturbing the stored value, unlike the aliasing Get.
func TestGetCopy(t *testing.T) {
	s := mustOpen(t)
	if err := s.Set([]byte("k"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	cp, ok, err := s.GetCopy([]byte("k"))
	if err != nil || !ok {
		t.Fatalf("GetCopy: ok=%v err=%v", ok, err)
	}
	for i := range cp {
		cp[i] = 'x' // mutate the copy
	}
	v, _ := get(t, s, []byte("k"))
	if string(v) != "value" {
		t.Fatalf("stored value changed after mutating the copy: %q", v)
	}
	if _, ok, _ := s.GetCopy([]byte("absent")); ok {
		t.Fatal("GetCopy reported a missing key as found")
	}
}

// TestOracle drives the store against a Go map and checks every read back: first
// writes, overwrites that must win, and deletes that must vanish. It pushes past
// the initial table size so a grow runs mid-test, which is where a botched slot
// rehash or a lost overwrite would surface. This is the correctness floor under
// every throughput number; a fast store that loses a write is worthless.
func TestOracle(t *testing.T) {
	s := mustOpen(t)
	const n = 50000
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
	for i := 0; i < n; i += 2 { // overwrite evens, newest wins
		put(i, i+1_000_000)
	}
	for i := 0; i < n; i += 5 { // delete every fifth
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
	if _, found := get(t, s, []byte("never-inserted")); found {
		t.Fatalf("absent key reported present")
	}

	// A deleted key reinserted must come back live (tombstone slot revival).
	if err := s.Set(tkey(0), tval(42)); err != nil {
		t.Fatalf("reinsert: %v", err)
	}
	if got, found := get(t, s, tkey(0)); !found || string(got) != string(tval(42)) {
		t.Fatalf("reinserted key 0: got %q found %v", got, found)
	}
}

// TestConcurrent runs many writers filling and overwriting shards while many
// readers probe the same keyspace, with -race on. It guards the lock-free read,
// the read-copy-update on overwrite, and the index swap a grow performs, none of
// which the single-thread oracle can stress. It asserts liveness, not a point
// value, because a key a reader sees may be one a writer is still rewriting; the
// race detector catches the data race this test exists to catch.
func TestConcurrent(t *testing.T) {
	s := mustOpen(t)
	const (
		writers = 8
		readers = 8
		keys    = 40000
		rounds  = 3
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

// TestHashDistribution guards the probe-table health: a run of realistic keys
// must spread close to uniform across the low bits used to index a table and the
// high byte used to pick a shard. A weak hash that clustered either would turn
// the open-addressing chains into linear scans, slow but still correct, so a
// throughput run would not catch it; this does.
func TestHashDistribution(t *testing.T) {
	const keys = 1 << 16
	low := make([]int, 256)  // first table index width
	high := make([]int, 256) // shard selector width
	for i := 0; i < keys; i++ {
		h := hash64(tkey(i))
		low[h&255]++
		high[(h>>shardShift)&255]++
	}
	mean := float64(keys) / 256
	check := func(name string, c []int) {
		for b, v := range c {
			if float64(v) < mean*0.5 || float64(v) > mean*1.5 {
				t.Fatalf("%s bucket %d holds %d, mean %.0f: bits cluster", name, b, v, mean)
			}
		}
	}
	check("low", low)
	check("high", high)
}

// TestRecordTooBig pins the page-fit contract: a record larger than a page is
// rejected rather than silently corrupting the log.
func TestRecordTooBig(t *testing.T) {
	s, err := New(Tunables{Shards: 4, PageSize: 256})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()
	if err := s.Set([]byte("k"), make([]byte, 512)); err != errValueTooBig {
		t.Fatalf("oversize Set: got %v want errValueTooBig", err)
	}
}

// TestBadTunables checks the open-time validation: a non-power-of-two shard count
// and a durability dial without a path are both rejected.
func TestBadTunables(t *testing.T) {
	if _, err := New(Tunables{Shards: 100, PageSize: 1 << 16}); err != errBadShards {
		t.Fatalf("non-pow2 shards: got %v want errBadShards", err)
	}
	if _, err := New(Tunables{Shards: 4, PageSize: 1 << 16, Durability: DurabilityFull}); err != errDurabilityNoPath {
		t.Fatalf("durability without path: got %v want errDurabilityNoPath", err)
	}
	if _, err := New(Tunables{Shards: 4, PageSize: 8}); err != errBadPageSize {
		t.Fatalf("tiny page: got %v want errBadPageSize", err)
	}
}

// TestScaleMemory is the scalability proof in miniature. It inserts enough keys
// to grow every shard's index several times, then reads the resident index cost
// per key off Stats and asserts it stays inside the compact-index budget. The
// budget is a small constant independent of key length, which is the whole point:
// it is what lets the index for a billion keys fit in memory while hashlog's
// full-key index would not. The same number, multiplied out, is the extrapolation
// the scale benchmark prints for 1B and 10B keys.
func TestScaleMemory(t *testing.T) {
	s := mustOpen(t)
	const n = 2_000_000
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	st := s.Stats()
	if st.Keys != n {
		t.Fatalf("Keys = %d, want %d", st.Keys, n)
	}
	// At the 0.8 load factor the table is between 0.4 and 0.8 full, so 8 bytes per
	// slot is between 10 and 20 bytes per key. Anything above 24 means the index is
	// not the flat-8-bytes-per-slot structure it claims to be.
	bpk := st.BytesPerKey()
	if bpk < 8 || bpk > 24 {
		t.Fatalf("index bytes per key = %.1f, want within [8,24]", bpk)
	}
	t.Logf("keys=%d index=%.1f MiB bytes/key=%.2f log=%.1f MiB",
		st.Keys, float64(st.IndexBytes)/(1<<20), bpk, float64(st.LogBytes)/(1<<20))

	// Confirm a sample reads back, so the compactness is not bought by dropping data.
	for _, i := range []int{0, n / 2, n - 1} {
		if got, found := get(t, s, tkey(i)); !found || string(got) != string(tval(i)) {
			t.Fatalf("sample key %d: found %v got %q", i, found, got)
		}
	}
	runtime.KeepAlive(s)
}
