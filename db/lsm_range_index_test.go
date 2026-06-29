package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/format"
)

// settleLSM runs Maintain until no compaction remains, draining the L0 segments a small
// memtable produced down into the leveled tree so a scan exercises the merged levels.
func settleLSM(t *testing.T, d *DB) {
	t.Helper()
	for {
		rep, err := d.Maintain(0)
		if err != nil {
			t.Fatalf("maintain: %v", err)
		}
		if rep.PagesCompacted == 0 {
			return
		}
	}
}

// TestLSMRangeIndexTransparent drives the RangeIndex option through the public database:
// the same writes are loaded into one database with the REMIX index on and one with it
// off, both settled into segments, and every key must read back the same. The index is a
// pure performance choice, so turning it on may never change what a read returns.
func TestLSMRangeIndexTransparent(t *testing.T) {
	const n = 300
	load := func(d *DB) {
		// Initial values, then overwrite every third key and delete every seventh at higher
		// versions, so the merged view has real MVCC depth across the segments.
		if err := d.Update(func(txn *Txn) error {
			for i := 0; i < n; i++ {
				txn.Set([]byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("v%04d", i)))
			}
			return nil
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if err := d.Update(func(txn *Txn) error {
			for i := 0; i < n; i += 3 {
				txn.Set([]byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("w%04d", i)))
			}
			for i := 0; i < n; i += 7 {
				txn.Delete([]byte(fmt.Sprintf("key%04d", i)))
			}
			return nil
		}); err != nil {
			t.Fatalf("update: %v", err)
		}
	}

	base := Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 256}
	off := openMem(t, base)
	load(off)
	settleLSM(t, off)

	onOpts := base
	onOpts.RangeIndex = true
	on := openMem(t, onOpts)
	load(on)
	settleLSM(t, on)

	// Point reads of every key, present and absent, must give identical answers with the
	// range index on and off, so turning it on stays invisible to reads. The deleted keys
	// (i%7==0) are the absent population.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%04d", i)
		offVal, offOk := txnGet(t, off, k)
		onVal, onOk := txnGet(t, on, k)
		if offOk != onOk || offVal != onVal {
			t.Fatalf("point read of %s disagrees: off (%q,%v), on (%q,%v)", k, offVal, offOk, onVal, onOk)
		}
	}

	// Spot-check the content against the truth with point reads so a bug shared by both
	// paths cannot pass: a deleted key is gone, an overwritten key carries its new value, an
	// untouched key its original.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%04d", i)
		got, ok := txnGet(t, on, k)
		switch {
		case i%7 == 0:
			if ok {
				t.Fatalf("deleted key %s still present", k)
			}
		case i%3 == 0:
			if !ok || got != fmt.Sprintf("w%04d", i) {
				t.Fatalf("overwritten key %s = %q,%v, want w%04d", k, got, ok, i)
			}
		default:
			if !ok || got != fmt.Sprintf("v%04d", i) {
				t.Fatalf("key %s = %q,%v, want v%04d", k, got, ok, i)
			}
		}
	}
}
