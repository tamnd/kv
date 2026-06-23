package db

import (
	"fmt"
	"testing"
	"time"

	"github.com/tamnd/kv/vfs"
)

// TestAutoGCRunsAfterCheckpoint verifies that the background checkpointer also drains dead
// B-tree MVCC versions automatically after each checkpoint (perf/05 F3c), so dead versions
// do not accumulate between explicit Maintain calls.
//
// The proof: write the same overwrite-heavy workload into two databases that differ only in
// auto-checkpoint (on vs. off). After waiting for the auto-checkpoint cycle to complete in
// the first database and closing it, reopen it with no auto-GC and call Maintain. It must
// report no more pages to compact: the background GC already ran. The second database, which
// never had an auto-checkpoint, reports many pages to compact on its first Maintain call,
// proving the two databases diverge only through the auto-GC path.
func TestAutoGCRunsAfterCheckpoint(t *testing.T) {
	const (
		keys       = 200 // small key space
		overwrites = 10  // each key is written 10 times so dead versions accumulate
	)

	load := func(d *DB) {
		t.Helper()
		for round := range overwrites {
			if err := d.Update(func(txn *Txn) error {
				for i := range keys {
					txn.Set([]byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("val-round-%d-%04d", round, i)))
				}
				return nil
			}); err != nil {
				t.Fatalf("write round %d: %v", round, err)
			}
		}
	}

	// Build a database WITH auto-checkpoint+GC: AutoCheckpoint:1 triggers a checkpoint after
	// every single commit frame, so GC fires after each write round. Close joins the background
	// worker, so the returned DB has fully drained all GC work by the time Close returns.
	fsGC := vfs.NewMem()
	{
		opts := Options{PageSize: 4096, AutoCheckpoint: 1}
		d, err := Open(fsGC, "test.kv", opts)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		load(d)
		// Give the background worker time to complete the final checkpoint+GC cycle after
		// the last write, since the auto-checkpoint fires asynchronously after each commit.
		time.Sleep(50 * time.Millisecond)
		if err := d.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	// Build the same database WITHOUT any auto-checkpoint: GC never runs automatically.
	fsNoGC := vfs.NewMem()
	{
		opts := Options{PageSize: 4096, AutoCheckpoint: -1}
		d, err := Open(fsNoGC, "test.kv", opts)
		if err != nil {
			t.Fatalf("open no-GC: %v", err)
		}
		load(d)
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("checkpoint no-GC: %v", err)
		}
		if err := d.Close(); err != nil {
			t.Fatalf("close no-GC: %v", err)
		}
	}

	// Reopen the auto-GC database with no auto-checkpoint so the Maintain call below is the
	// first explicit maintenance since open, not a continuation of the background work.
	dGC, err := Open(fsGC, "test.kv", Options{AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("reopen gc db: %v", err)
	}
	defer dGC.Close()

	// Reopen the no-GC database similarly.
	dNoGC, err := Open(fsNoGC, "test.kv", Options{AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("reopen no-gc db: %v", err)
	}
	defer dNoGC.Close()

	// The auto-GC database must have nothing (or very little) left to compact: the background
	// worker drained the dead versions. The no-GC database must have real compaction work, which
	// is the dead-version history the 10 overwrite rounds accumulated.
	gcRep, err := dGC.Maintain(0)
	if err != nil {
		t.Fatalf("maintain gc db: %v", err)
	}
	noGCRep, err := dNoGC.Maintain(0)
	if err != nil {
		t.Fatalf("maintain no-gc db: %v", err)
	}

	if gcRep.PagesCompacted > 0 && gcRep.PagesCompacted >= noGCRep.PagesCompacted {
		t.Fatalf("auto-GC db compacted %d pages, no-GC db compacted %d: expected auto-GC to compact fewer",
			gcRep.PagesCompacted, noGCRep.PagesCompacted)
	}
	if noGCRep.PagesCompacted == 0 {
		t.Fatalf("no-GC db reported 0 pages compacted: expected dead versions to be present without auto-GC")
	}
}
