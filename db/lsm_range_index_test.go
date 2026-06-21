package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
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
// off, both settled into segments, and their full and bounded scans must agree key for
// key and value for value. The index is a pure performance choice, so turning it on may
// never change what a scan returns.
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

	scan := func(d *DB, opts engine.IterOptions) ([]string, []string) {
		var keys, vals []string
		if err := d.View(func(txn *Txn) error {
			it, err := txn.NewIterator(opts)
			if err != nil {
				return err
			}
			defer it.Close()
			keys, vals = collect(t, it)
			return nil
		}); err != nil {
			t.Fatalf("scan: %v", err)
		}
		return keys, vals
	}

	cases := []struct {
		name string
		opts engine.IterOptions
	}{
		{"full", engine.IterOptions{}},
		{"bounded", engine.IterOptions{Lower: []byte("key0100"), Upper: []byte("key0200")}},
		{"prefix", engine.IterOptions{Prefix: []byte("key01")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			offKeys, offVals := scan(off, tc.opts)
			onKeys, onVals := scan(on, tc.opts)
			if !eq(offKeys, onKeys...) {
				t.Fatalf("keys differ with the range index on:\n off %v\n on  %v", offKeys, onKeys)
			}
			if !eq(offVals, onVals...) {
				t.Fatalf("values differ with the range index on:\n off %v\n on  %v", offVals, onVals)
			}
		})
	}

	// Spot-check the content against the truth so a bug shared by both paths cannot pass: a
	// deleted key is gone, an overwritten key carries its new value, an untouched key its
	// original.
	keys, vals := scan(on, engine.IterOptions{})
	got := make(map[string]string, len(keys))
	for i, k := range keys {
		got[k] = vals[i]
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%04d", i)
		switch {
		case i%7 == 0:
			if _, ok := got[k]; ok {
				t.Fatalf("deleted key %s still present", k)
			}
		case i%3 == 0:
			if got[k] != fmt.Sprintf("w%04d", i) {
				t.Fatalf("overwritten key %s = %q, want w%04d", k, got[k], i)
			}
		default:
			if got[k] != fmt.Sprintf("v%04d", i) {
				t.Fatalf("key %s = %q, want v%04d", k, got[k], i)
			}
		}
	}
}
