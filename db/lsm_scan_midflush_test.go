package db

import (
	"fmt"
	"sort"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// TestLSMStreamScanSurvivesMidScanFlush pins the held-cursor invariant the stateful
// ScanForward depends on: a streaming scan seeds its merge iterator over a referenced source
// snapshot once and advances it across pulls, so a flush or compaction that retires those
// sources mid-scan must neither corrupt the walk nor leak the snapshot. The test opens a
// read view, pulls a few entries, then from inside the same view drives a second pass of
// overwrites and deletes at higher versions and settles the engine, which seals the memtable
// the cursor captured and compacts away the segments it pinned. The scan then runs to the end
// and must return exactly the state at the view's snapshot, with every later write invisible,
// proving the pinned version kept the retired segments' pages alive and the held memtable skip
// lists stayed valid under the cursor.
func TestLSMStreamScanSurvivesMidScanFlush(t *testing.T) {
	const n = 600
	// A small memtable so the seed already spans several on-disk segments and the mid-scan
	// writes force fresh flushes while the cursor holds an older snapshot.
	d := openMem(t, Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 4096})

	for i := 0; i < n; i++ {
		if err := d.Update(func(txn *Txn) error {
			return txn.Set([]byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("v%04d", i)))
		}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	settleLSM(t, d)

	// The snapshot the scan must return: the seed state, independent of any write the scan
	// interleaves below.
	wantKeys := make([]string, n)
	wantVals := make([]string, n)
	for i := 0; i < n; i++ {
		wantKeys[i] = fmt.Sprintf("key%04d", i)
		wantVals[i] = fmt.Sprintf("v%04d", i)
	}
	sort.Strings(wantKeys) // already ordered, but keep the contract explicit

	var gotKeys, gotVals []string
	disrupted := false
	if err := d.View(func(txn *Txn) error {
		it, err := txn.NewIterator(engine.IterOptions{})
		if err != nil {
			return err
		}
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
			gotKeys = append(gotKeys, string(it.Key()))
			v, err := it.Value()
			if err != nil {
				t.Fatalf("value: %v", err)
			}
			gotVals = append(gotVals, string(v))

			// After the scan is underway but well before it ends, retire the sources it is
			// walking: overwrite and delete at higher versions, then settle so the memtable
			// seals and the pinned segments compact away under the held cursor.
			if !disrupted && len(gotKeys) == 10 {
				disrupted = true
				for i := 0; i < n; i += 2 {
					if err := d.Update(func(txn *Txn) error {
						return txn.Set([]byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("W%04d", i)))
					}); err != nil {
						t.Fatalf("overwrite %d: %v", i, err)
					}
				}
				for i := 1; i < n; i += 5 {
					if err := d.Update(func(txn *Txn) error {
						return txn.Delete([]byte(fmt.Sprintf("key%04d", i)))
					}); err != nil {
						t.Fatalf("delete %d: %v", i, err)
					}
				}
				settleLSM(t, d)
			}
		}
		return it.Error()
	}); err != nil {
		t.Fatalf("scan view: %v", err)
	}

	if !disrupted {
		t.Fatalf("scan ended before the mid-scan disruption fired (%d keys)", len(gotKeys))
	}
	if !eq(gotKeys, wantKeys...) {
		t.Fatalf("scan keys diverged from snapshot under mid-scan flush:\n got %d keys\n want %d keys", len(gotKeys), len(wantKeys))
	}
	if !eq(gotVals, wantVals...) {
		t.Fatalf("scan values diverged from snapshot under mid-scan flush (saw a post-snapshot write)")
	}
}
