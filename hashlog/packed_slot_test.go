package hashlog

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

// TestPackLocRoundTrip pins the slot encoding (L2): packLoc then unpackLoc must recover
// the same valLoc for every field at its limits, including the address ceiling, the full
// inline length, and the oversize marker. A drift here would point an index slot at the
// wrong byte or lose the oversize bit, so the round-trip is the floor the whole packed
// slot rests on.
func TestPackLocRoundTrip(t *testing.T) {
	cases := []valLoc{
		{addr: 0, vlen: 0},
		{addr: 1, vlen: 1},
		{addr: maxLogAddr - 1, vlen: inlineLenMask},
		{addr: maxLogAddr - 1, vlen: 0},
		{addr: 0, vlen: inlineLenMask},
		{addr: 123456789, vlen: 64},
		// Oversize marker set, descriptor-sized length, the shape an oversize home record
		// stores.
		{addr: 4096, vlen: valLocOversizeBit | oversizeDescriptorLen},
		{addr: maxLogAddr - 1, vlen: valLocOversizeBit | inlineLenMask},
	}
	for _, l := range cases {
		got := unpackLoc(packLoc(l))
		if got != l {
			t.Errorf("round-trip of %+v gave %+v", l, got)
		}
	}
}

// TestAddrInRange pins the address ceiling guard (L2): a record that ends at or before the
// ceiling is allowed, one that would pass it returns errLogFull, so the 39-bit slot address
// is never silently truncated into a wrong location.
func TestAddrInRange(t *testing.T) {
	if err := addrInRange(maxLogAddr-1, 1); err != nil {
		t.Fatalf("a record ending exactly at the ceiling must fit, got %v", err)
	}
	if err := addrInRange(maxLogAddr, 1); !errors.Is(err, errLogFull) {
		t.Fatalf("a record past the ceiling must be errLogFull, got %v", err)
	}
	if err := addrInRange(maxLogAddr-4, 8); !errors.Is(err, errLogFull) {
		t.Fatalf("a record straddling the ceiling must be errLogFull, got %v", err)
	}
}

// TestOverwriteZeroAlloc is the L2 win itself: an overwrite of an existing key repoints the
// slot in place with one atomic store, no new *entry and no key copy. A memory-only
// full-resident store never takes the in-place same-size tail path (that is the durable
// eviction profile), so every overwrite here funnels through indexPut's overwrite branch,
// the path the audit flagged for a per-Set allocation. Before the packed slot this measured
// one alloc per op; it must now be zero.
func TestOverwriteZeroAlloc(t *testing.T) {
	s, err := New(Tunables{Shards: 1, PageSize: 1 << 20, ResidentPagesPerShard: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	key := []byte("hot-key")
	val := []byte("0123456789abcdef") // 16 bytes; tiny next to the 1 MiB page so no roll fires
	if err := s.Set(key, val); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(2000, func() {
		if err := s.Set(key, val); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("overwrite allocated %v objects per op, want 0", allocs)
	}
}

// TestOverwriteCorrectnessAcrossSizes checks the packed slot keeps last-writer-wins exact
// when the value size changes, which is the case the in-place tail path cannot take and so
// always lands on the slot repoint. It overwrites each key several times with values of
// different lengths and confirms the final read, then reopens a durable store to confirm the
// repoint survived recovery.
func TestOverwriteCorrectnessAcrossSizes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l2.hlog")
	tun := Tunables{Shards: 4, PageSize: 4096, ExtentSize: 4096, ResidentPagesPerShard: 8, Path: path, Durability: DurabilityNormal}
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}

	oracle := map[string]string{}
	for round := 0; round < 6; round++ {
		for i := 0; i < 500; i++ {
			k := fmt.Sprintf("k%03d", i)
			// Vary the length every round so the overwrite changes size and cannot take the
			// same-size in-place path.
			v := fmt.Sprintf("r%d-%0*d", round, round*7+1, i)
			if err := s.Set([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			oracle[k] = v
		}
	}
	for k, want := range oracle {
		got, ok, err := s.Get([]byte(k))
		if err != nil || !ok || string(got) != want {
			t.Fatalf("live key %q: got %q ok=%v err=%v want %q", k, got, ok, err, want)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := New(tun)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	for k, want := range oracle {
		got, ok, err := s2.Get([]byte(k))
		if err != nil || !ok || string(got) != want {
			t.Fatalf("after reopen key %q: got %q ok=%v err=%v want %q", k, got, ok, err, want)
		}
	}
}
