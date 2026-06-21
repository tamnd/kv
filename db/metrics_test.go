package db

import (
	"testing"

	"github.com/tamnd/kv/format"
)

// TestStatsOldestSnapshotAge checks the leaked-reader gauge (spec 19 §1.6): with no live
// reader the age is zero, an open snapshot ages by exactly the clock delta since it was
// taken, and discarding the snapshot drops the age back to zero. The injected clock makes
// the age deterministic rather than racing real time.
func TestStatsOldestSnapshotAge(t *testing.T) {
	clk := &testClock{}
	clk.set(1_000)
	d := openMemClock(t, clk, Options{})

	if age := d.Stats().OldestSnapshotAgeNanos; age != 0 {
		t.Fatalf("idle database reader age = %d, want 0", age)
	}

	// Take a long-lived snapshot at t=1000, then advance the clock to t=4000.
	snap := d.Snapshot()
	clk.set(4_000)
	if age := d.Stats().OldestSnapshotAgeNanos; age != 3_000 {
		t.Errorf("held snapshot age = %d, want 3000 (4000-1000)", age)
	}

	// Commit a write so the applied version advances; the next snapshot then registers at
	// a distinct version cohort with its own stamp, rather than sharing snap's cohort.
	if err := d.Update(func(txn *Txn) error { return txn.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write to advance version: %v", err)
	}

	// A second snapshot taken later is younger, so the oldest age still tracks the first.
	clk.set(5_000)
	snap2 := d.Snapshot()
	clk.set(6_000)
	if age := d.Stats().OldestSnapshotAgeNanos; age != 5_000 {
		t.Errorf("oldest of two snapshots age = %d, want 5000 (6000-1000)", age)
	}

	// Close the older snapshot; its cohort empties, so the age now tracks the younger
	// snapshot (taken at t=5000).
	if err := snap.Close(); err != nil {
		t.Fatalf("close snap: %v", err)
	}
	if age := d.Stats().OldestSnapshotAgeNanos; age != 1_000 {
		t.Errorf("after closing oldest, age = %d, want 1000 (6000-5000)", age)
	}

	// Close the last snapshot; with no reader live the age falls back to zero.
	if err := snap2.Close(); err != nil {
		t.Fatalf("close snap2: %v", err)
	}
	if age := d.Stats().OldestSnapshotAgeNanos; age != 0 {
		t.Errorf("after closing all readers, age = %d, want 0", age)
	}
}

// TestStatsLSMLevels checks the LSM engine fills the per-level shape and a compaction score
// into Stats (spec 19 §1.5): after enough writes to flush at least one on-disk segment, the
// level view is non-empty and reports a positive segment count.
func TestStatsLSMLevels(t *testing.T) {
	// A small memtable forces flushes quickly, so a modest write count produces segments.
	d := openMem(t, Options{Engine: format.EngineLSM, MemtableSize: 4 << 10})

	for i := range 2000 {
		k := []byte{byte(i), byte(i >> 8)}
		if err := d.Update(func(txn *Txn) error { return txn.Set(k, []byte("value")) }); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	s := d.Stats()
	if len(s.Levels) == 0 {
		t.Fatal("LSM Stats reported no levels after 2000 writes")
	}
	var segs int
	for _, lv := range s.Levels {
		segs += lv.Segments
	}
	if segs == 0 {
		t.Errorf("LSM Stats reported zero segments across %d levels", len(s.Levels))
	}
}
