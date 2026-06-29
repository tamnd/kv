package f2engine

import (
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/f2"
)

// concatMerge is the merge resolver both the engine and the oracle fold with: it appends
// the operand to the existing value, so the fold order is observable and a divergence in
// ordering shows up as a wrong byte string.
func concatMerge(existing, operand []byte) []byte {
	out := make([]byte, 0, len(existing)+len(operand))
	out = append(out, existing...)
	return append(out, operand...)
}

// buildBatches returns a deterministic sequence of committed batches over a small key
// space, mixing sets, deletes, merges, and TTL sets so the fold is exercised across kinds.
// Commit versions increase by one per batch starting at 1.
func buildBatches(seed int64, nBatches, nKeys int) []*engine.WriteBatch {
	rng := rand.New(rand.NewSource(seed))
	batches := make([]*engine.WriteBatch, 0, nBatches)
	for v := 1; v <= nBatches; v++ {
		b := engine.NewWriteBatch(uint64(v))
		ops := 1 + rng.Intn(3)
		// One mutation per user key per commit version: a version stamps a single value
		// for a key, so a batch never carries two ops for the same key. This is what the
		// transaction layer emits; the engine relies on it to key a cell by version.
		used := map[int]bool{}
		for o := 0; o < ops; o++ {
			ki := rng.Intn(nKeys)
			if used[ki] {
				continue
			}
			used[ki] = true
			key := []byte(fmt.Sprintf("k%02d", ki))
			switch rng.Intn(5) {
			case 0, 1:
				b.Set(key, []byte(fmt.Sprintf("v%d-%d", v, o)))
			case 2:
				b.Delete(key)
			case 3:
				b.Merge(key, []byte(fmt.Sprintf("+%d", v)))
			case 4:
				b.SetWithTTL(key, []byte(fmt.Sprintf("t%d", v)), uint64(1_000_000+v))
			}
		}
		batches = append(batches, b)
	}
	return batches
}

// checkPointReads drives the engine and an oracle through the same batches and asserts
// their point reads agree at every commit snapshot. It does not check scans: f2 has no key
// order and does not serve them.
func checkPointReads(t *testing.T, e *Engine, batches []*engine.WriteBatch, keys []string) {
	t.Helper()
	e.SetMergeFunc(concatMerge)
	oracle := engine.NewOracle(concatMerge)

	var maxVer uint64
	snaps := []uint64{0}
	for _, b := range batches {
		if err := e.Apply(b, b.Version()); err != nil {
			t.Fatalf("Apply v%d: %v", b.Version(), err)
		}
		oracle.Apply(b, b.Version())
		maxVer = b.Version()
		snaps = append(snaps, b.Version())
	}

	for _, sv := range snaps {
		snap := engine.Snapshot{Version: sv}
		rd, err := e.NewReader(snap)
		if err != nil {
			t.Fatalf("NewReader %d: %v", sv, err)
		}
		for _, k := range keys {
			want, wantOK := oracle.Get([]byte(k), snap)
			got, gotErr := rd.Get([]byte(k))
			if wantOK {
				if gotErr != nil {
					t.Fatalf("snap %d key %s: oracle %q, engine err %v", sv, k, want, gotErr)
				}
				if string(got) != string(want) {
					t.Fatalf("snap %d key %s: engine %q != oracle %q", sv, k, got, want)
				}
			} else if !errors.Is(gotErr, engine.ErrNotFound) {
				t.Fatalf("snap %d key %s: oracle absent, engine (%q, %v)", sv, k, got, gotErr)
			}
		}
		rd.Close()
	}
	_ = maxVer
}

func keySpace(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%02d", i)
	}
	return keys
}

// TestPointReadConformanceMemory checks that the memory-only engine resolves point reads
// at every snapshot exactly as the shared oracle does, across many seeds.
func TestPointReadConformanceMemory(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		e, err := New(Config{Shards: 8, PageSize: 64 << 10})
		if err != nil {
			t.Fatal(err)
		}
		checkPointReads(t, e, buildBatches(seed, 60, 12), keySpace(12))
		if err := e.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
}

// TestPointReadConformanceDurable runs the same conformance against the single-file,
// self-durable engine: the version groups go through the log, not a map.
func TestPointReadConformanceDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f2.db")
	e, err := New(Config{Shards: 8, PageSize: 64 << 10, Path: path, Durability: f2.DurabilityNormal})
	if err != nil {
		t.Fatal(err)
	}
	checkPointReads(t, e, buildBatches(7, 80, 12), keySpace(12))
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestReopenRecoversReads asserts that a checkpoint then close then reopen recovers the
// store from f2's own durable layout: reads at the final snapshot survive the reopen with
// no host WAL replay involved.
func TestReopenRecoversReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f2.db")
	batches := buildBatches(3, 100, 16)
	keys := keySpace(16)

	e, err := New(Config{Shards: 8, PageSize: 64 << 10, Path: path, Durability: f2.DurabilityNormal})
	if err != nil {
		t.Fatal(err)
	}
	e.SetMergeFunc(concatMerge)
	oracle := engine.NewOracle(concatMerge)
	var maxVer uint64
	for _, b := range batches {
		if err := e.Apply(b, b.Version()); err != nil {
			t.Fatalf("Apply: %v", err)
		}
		oracle.Apply(b, b.Version())
		maxVer = b.Version()
	}
	if err := e.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	e2, err := New(Config{Shards: 8, PageSize: 64 << 10, Path: path, Durability: f2.DurabilityNormal})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()
	e2.SetMergeFunc(concatMerge)

	snap := engine.Snapshot{Version: maxVer}
	for _, k := range keys {
		want, wantOK := oracle.Get([]byte(k), snap)
		got, gotErr := e2.GetAt(snap, []byte(k))
		if wantOK {
			if gotErr != nil || string(got) != string(want) {
				t.Fatalf("after reopen key %s: got (%q, %v), want %q", k, got, gotErr, want)
			}
		} else if !errors.Is(gotErr, engine.ErrNotFound) {
			t.Fatalf("after reopen key %s: want absent, got (%q, %v)", k, got, gotErr)
		}
	}
}

// TestRangeOpsUnsupported asserts that the two ordered operations a hash index cannot serve
// fail cleanly: a scan reports ErrUnsupported and a range delete is rejected by Apply.
func TestRangeOpsUnsupported(t *testing.T) {
	e, err := New(Config{Shards: 4, PageSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	rd, err := e.NewReader(engine.Snapshot{Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	if _, err := rd.NewIter(engine.IterOptions{}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("NewIter err = %v, want ErrUnsupported", err)
	}

	b := engine.NewWriteBatch(1)
	b.DeleteRange([]byte("a"), []byte("z"))
	if err := e.Apply(b, 1); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Apply range delete err = %v, want ErrUnsupported", err)
	}
}
