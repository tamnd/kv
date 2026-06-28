package f2

import (
	"fmt"
	"path/filepath"
	"testing"
)

// findFPCollision returns two distinct keys whose slot fingerprints are equal.
// Equal fingerprints mean equal low mix bits, so in any table up to 2^slotFPBits
// slots the two keys also share a home slot, which is the condition the probe
// chain bugs below depend on. The fingerprint is 24 bits, so a birthday collision
// turns up within a few thousand keys.
func findFPCollision(t *testing.T) (a, b []byte) {
	t.Helper()
	seen := map[uint64][]byte{}
	for i := 0; i < 5_000_000; i++ {
		k := []byte(fmt.Sprintf("collkey-%08d", i))
		fp := fpOf(mixOf(hash64(k)))
		if prev, ok := seen[fp]; ok {
			return prev, k
		}
		seen[fp] = k
	}
	t.Fatal("no fingerprint collision found")
	return nil, nil
}

// TestPutTombstoneReuseNoResurrection guards the put probe against reusing a
// tombstone before confirming the key has no live slot further down the chain.
// Two keys that share a fingerprint share a home, so the second lands one slot past
// the first. Deleting the first then overwriting the second must update the second
// in place, not claim the first's freed slot and leave the original behind. If it
// did, a later delete of the second would resurrect the stale value.
func TestPutTombstoneReuseNoResurrection(t *testing.T) {
	a, b := findFPCollision(t)

	s, err := New(Tunables{Shards: 1, PageSize: 1 << 16})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// a takes the home slot, b probes one past it.
	if err := s.Set(a, []byte("a-v1")); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	if err := s.Set(b, []byte("b-v1")); err != nil {
		t.Fatalf("Set b: %v", err)
	}
	// Free a's slot, then overwrite b. The overwrite must find b's live slot, not
	// stop at a's tombstone.
	if err := s.Delete(a); err != nil {
		t.Fatalf("Delete a: %v", err)
	}
	if err := s.Set(b, []byte("b-v2")); err != nil {
		t.Fatalf("Set b again: %v", err)
	}
	if v, ok, _ := s.Get(b); !ok || string(v) != "b-v2" {
		t.Fatalf("Get b after overwrite = %q,%v, want b-v2,true", v, ok)
	}
	if st := s.Stats(); st.Keys != 1 {
		t.Fatalf("Keys = %d, want 1 (a deleted, b live once)", st.Keys)
	}

	// The discriminating step: delete b. With a duplicate slot left behind, this
	// resurrects the old value; with the probe fixed, b is simply gone.
	if err := s.Delete(b); err != nil {
		t.Fatalf("Delete b: %v", err)
	}
	if v, ok, _ := s.Get(b); ok {
		t.Fatalf("Get b after delete = %q,true, want absent (resurrected duplicate slot)", v)
	}
	if st := s.Stats(); st.Keys != 0 {
		t.Fatalf("Keys = %d, want 0", st.Keys)
	}
}

// TestGrowReadFreeAcrossDoublings drives many doublings on a budgeted durable
// shard, where the old grow did a pread per live key. The slots now carry the home
// hash low bits, so the replay rehashes from the slot alone; this test asserts the
// table stays correct across all those read-free grows, including overwrites and
// deletes that cross the doublings, on shards whose pages are mostly evicted.
func TestGrowReadFreeAcrossDoublings(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Tunables{
		Shards:                4,
		PageSize:              4096,
		Path:                  filepath.Join(dir, "grow.f2"),
		Durability:            DurabilityNormal,
		ResidentPagesPerShard: 2, // force eviction so a naive grow would pread
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	const n = 20000
	want := map[string]string{}
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
		want[string(tkey(i))] = string(tval(i))
	}
	// Overwrite every third key and delete every fifth, crossing more grows.
	for i := 0; i < n; i += 3 {
		nv := []byte(fmt.Sprintf("rewritten-%08d", i))
		if err := s.Set(tkey(i), nv); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
		want[string(tkey(i))] = string(nv)
	}
	for i := 0; i < n; i += 5 {
		if err := s.Delete(tkey(i)); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
		delete(want, string(tkey(i)))
	}

	for i := 0; i < n; i++ {
		v, ok, err := s.Get(tkey(i))
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		exp, live := want[string(tkey(i))]
		if ok != live {
			t.Fatalf("key %d present=%v, want %v", i, ok, live)
		}
		if live && string(v) != exp {
			t.Fatalf("key %d = %q, want %q", i, v, exp)
		}
	}
	if st := s.Stats(); st.Keys != int64(len(want)) {
		t.Fatalf("Keys = %d, want %d", st.Keys, len(want))
	}
}
