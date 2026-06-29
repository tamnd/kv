package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// TestLSMRibbonFilterTransparent drives the Filter option through the public database:
// the same writes are loaded into one database with the default Bloom filter and one with
// the Ribbon filter, both settled into segments, and every read must agree. A membership
// filter is a pure performance choice, so swapping Bloom for Ribbon may never change what
// a point read or a scan returns, present key or absent. The Ribbon path additionally
// must never report a key missing that is actually there, which a point read of every
// present key checks directly.
func TestLSMRibbonFilterTransparent(t *testing.T) {
	const n = 600
	load := func(d *DB) {
		// Initial values, then overwrite every third key and delete every fifth at higher
		// versions, so the settled tree has real MVCC depth and a population of genuinely
		// absent keys (the deleted ones) for the filters to reject.
		if err := d.Update(func(txn *Txn) error {
			for i := 0; i < n; i++ {
				txn.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("v%05d", i)))
			}
			return nil
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if err := d.Update(func(txn *Txn) error {
			for i := 0; i < n; i += 3 {
				txn.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("w%05d", i)))
			}
			for i := 0; i < n; i += 5 {
				txn.Delete([]byte(fmt.Sprintf("key%05d", i)))
			}
			return nil
		}); err != nil {
			t.Fatalf("update: %v", err)
		}
	}

	// A small memtable forces many flushed segments, so a point read actually fans across
	// segments and consults their filters rather than hitting one resident memtable.
	base := Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 256}
	bloom := openMem(t, base)
	load(bloom)
	settleLSM(t, bloom)

	ribOpts := base
	ribOpts.Filter = engine.FilterRibbon
	ribbon := openMem(t, ribOpts)
	load(ribbon)
	settleLSM(t, ribbon)

	// Point reads of every key, present and absent, must give identical answers under both
	// filters. The deleted keys (i%5==0) are the absent population that exercises a filter
	// reject; a Ribbon false negative on a present key would surface here as a disagreement.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%05d", i)
		bv, bok := txnGet(t, bloom, k)
		rv, rok := txnGet(t, ribbon, k)
		if bok != rok || bv != rv {
			t.Fatalf("point read of %s disagrees: bloom (%q,%v), ribbon (%q,%v)", k, bv, bok, rv, rok)
		}
	}
	// Keys that were never written at all, a second absent population the filters reject.
	for i := n; i < n+100; i++ {
		k := fmt.Sprintf("key%05d", i)
		_, bok := txnGet(t, bloom, k)
		_, rok := txnGet(t, ribbon, k)
		if bok || rok {
			t.Fatalf("never-written key %s reported present: bloom %v, ribbon %v", k, bok, rok)
		}
	}

	// Spot-check the Ribbon result against the truth with point reads so a bug shared by
	// both engines cannot pass: a deleted key gone, an overwritten key carrying its new
	// value, an untouched key its original.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%05d", i)
		got, ok := txnGet(t, ribbon, k)
		switch {
		case i%5 == 0:
			if ok {
				t.Fatalf("deleted key %s still present under Ribbon", k)
			}
		case i%3 == 0:
			if !ok || got != fmt.Sprintf("w%05d", i) {
				t.Fatalf("overwritten key %s = %q,%v, want w%05d", k, got, ok, i)
			}
		default:
			if !ok || got != fmt.Sprintf("v%05d", i) {
				t.Fatalf("key %s = %q,%v, want v%05d", k, got, ok, i)
			}
		}
	}
}
