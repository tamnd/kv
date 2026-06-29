package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/vfs"
)

// putN commits value v for key in its own transaction, advancing the version, and
// returns the new commit version.
func putN(t *testing.T, d *DB, key, v string) uint64 {
	t.Helper()
	if err := d.Update(func(txn *Txn) error {
		return txn.Set([]byte(key), []byte(v))
	}); err != nil {
		t.Fatalf("put %s=%s: %v", key, v, err)
	}
	return d.Version()
}

// seedRange writes keys k00..k{n-1} with values v00..v{n-1} in a single transaction.
func seedRange(t *testing.T, d *DB, n int) {
	t.Helper()
	if err := d.Update(func(txn *Txn) error {
		for i := 0; i < n; i++ {
			txn.Set([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)))
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// TestGCCollapsesOldVersions writes several versions of one key with no live reader,
// runs GC, and checks the live value survives while the dead history is reclaimed and
// a second GC has nothing left to do.
func TestGCCollapsesOldVersions(t *testing.T) {
	d := openMem(t, Options{})

	putN(t, d, "k", "v1")
	putN(t, d, "k", "v2")
	putN(t, d, "k", "v3")

	rep, err := d.Maintain(0)
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if rep.BytesReclaimed <= 0 {
		t.Fatalf("expected reclaimed bytes from collapsing 3 versions, got %d", rep.BytesReclaimed)
	}
	if rep.More {
		t.Fatalf("unbounded GC should finish in one call")
	}

	// The newest value is intact after GC.
	if v, ok := txnGet(t, d, "k"); !ok || v != "v3" {
		t.Fatalf("k after GC = %q,%v, want v3", v, ok)
	}

	// A second GC over the now-collapsed history reclaims nothing.
	rep2, err := d.Maintain(0)
	if err != nil {
		t.Fatalf("maintain 2: %v", err)
	}
	if rep2.BytesReclaimed != 0 || rep2.PagesCompacted != 0 {
		t.Fatalf("second GC should be a no-op, got %+v", rep2)
	}
}

// TestGCPreservesLiveSnapshot checks GC honors the read-mark: a reader holding an old
// snapshot still sees the value it began with, while fresh reads see the newest, and
// once the reader is released GC can collapse the rest.
func TestGCPreservesLiveSnapshot(t *testing.T) {
	d := openMem(t, Options{})
	putN(t, d, "k", "v1")

	// A long-lived reader pins the watermark at the version that sees v1.
	reader := d.Begin(false)
	defer reader.Discard()

	putN(t, d, "k", "v2")
	putN(t, d, "k", "v3")

	if _, err := d.Maintain(0); err != nil {
		t.Fatalf("maintain with live reader: %v", err)
	}

	// The pinned reader still resolves its own snapshot: v1 was not reclaimed.
	if v, err := reader.Get([]byte("k")); err != nil || string(v) != "v1" {
		t.Fatalf("pinned reader k = %q,%v, want v1", v, err)
	}
	// A fresh read sees the newest value.
	if v, ok := txnGet(t, d, "k"); !ok || v != "v3" {
		t.Fatalf("fresh k = %q,%v, want v3", v, ok)
	}

	// Releasing the reader lets the watermark advance, so the next GC collapses the
	// history v1..v2 that the reader had been protecting.
	reader.Discard()
	rep, err := d.Maintain(0)
	if err != nil {
		t.Fatalf("maintain after release: %v", err)
	}
	if rep.BytesReclaimed <= 0 {
		t.Fatalf("expected reclaim once the reader is gone, got %d", rep.BytesReclaimed)
	}
	if v, ok := txnGet(t, d, "k"); !ok || v != "v3" {
		t.Fatalf("k after final GC = %q,%v, want v3", v, ok)
	}
}

// TestGCRangeDeleteMarkerDropped checks GC drops a range-delete marker once its
// covered keys are collapsed: the covered keys stay absent, and a brand new key in the
// old interval is visible (the stale marker is gone, so it cannot re-delete it).
func TestGCRangeDeleteMarkerDropped(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 6) // k00..k05

	if err := d.Update(func(txn *Txn) error {
		return txn.DeleteRange([]byte("k02"), []byte("k05")) // covers k02..k04
	}); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	rep, err := d.Maintain(0)
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if rep.BytesReclaimed <= 0 {
		t.Fatalf("expected reclaim from covered keys + marker, got %d", rep.BytesReclaimed)
	}

	// Covered keys remain absent, surrounding keys remain present.
	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("k%02d", i)
		_, ok := txnGet(t, d, key)
		covered := i >= 2 && i <= 4
		if covered && ok {
			t.Fatalf("covered %s present after GC", key)
		}
		if !covered && !ok {
			t.Fatalf("uncovered %s missing after GC", key)
		}
	}

	// A new write inside the old interval is visible: the marker is gone, so it cannot
	// shadow a version committed above it.
	putN(t, d, "k03", "reborn")
	if v, ok := txnGet(t, d, "k03"); !ok || v != "reborn" {
		t.Fatalf("k03 after re-set = %q,%v, want reborn", v, ok)
	}
}

// TestGCSurvivesReopen checks a collapsed database recovers correctly: after GC and a
// checkpoint, reopening resolves the same values, and the range-delete interval set is
// rebuilt from the surviving markers.
func TestGCSurvivesReopen(t *testing.T) {
	fs := vfs.NewMem()
	const path = "test.kv"
	d, err := Open(fs, path, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := d.Update(func(txn *Txn) error {
		for i := 0; i < 6; i++ {
			txn.Set([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)))
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := d.Update(func(txn *Txn) error { return txn.Set([]byte("k01"), []byte("late")) }); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if err := d.Update(func(txn *Txn) error { return txn.DeleteRange([]byte("k03"), []byte("k05")) }); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	if _, err := d.Maintain(0); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, path, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()

	want := map[string]string{"k00": "v00", "k01": "late", "k02": "v02", "k05": "v05"}
	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("k%02d", i)
		v, ok := txnGet(t, d2, key)
		if exp, present := want[key]; present {
			if !ok || v != exp {
				t.Fatalf("after reopen %s = %q,%v, want %s", key, v, ok, exp)
			}
		} else if ok {
			t.Fatalf("after reopen %s present, want absent (range-deleted)", key)
		}
	}

	// The rebuilt interval set must not re-delete a fresh write in the old range.
	putN(t, d2, "k04", "back")
	if v, ok := txnGet(t, d2, "k04"); !ok || v != "back" {
		t.Fatalf("k04 after reopen+reset = %q,%v, want back", v, ok)
	}
}

// TestGCBudgetResumes checks a page-bounded GC reports More and finishes on a second
// call, leaving every live value intact. A small page size forces several leaves.
func TestGCBudgetResumes(t *testing.T) {
	d := openMem(t, Options{PageSize: 512})

	// Two versions of many keys, across several leaves.
	for round := 0; round < 2; round++ {
		if err := d.Update(func(txn *Txn) error {
			for i := 0; i < 40; i++ {
				txn.Set([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%d-%02d", round, i)))
			}
			return nil
		}); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
	}

	rep, err := d.Maintain(1) // one leaf per call
	if err != nil {
		t.Fatalf("bounded maintain: %v", err)
	}
	if !rep.More {
		t.Fatalf("a one-page budget over a multi-leaf tree should report More")
	}

	// Drain the rest.
	for i := 0; i < 100 && rep.More; i++ {
		rep, err = d.Maintain(1)
		if err != nil {
			t.Fatalf("drain maintain: %v", err)
		}
	}
	if rep.More {
		t.Fatalf("GC never finished draining")
	}

	// Every key resolves to its newest value after the resumed GC.
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("k%03d", i)
		want := fmt.Sprintf("v1-%02d", i)
		if v, ok := txnGet(t, d, key); !ok || v != want {
			t.Fatalf("%s after resumed GC = %q,%v, want %s", key, v, ok, want)
		}
	}
}
