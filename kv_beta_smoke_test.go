package kv_test

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv"
)

// TestBetaDBStackSmoke proves the Bε-tree core (kv.Beta) round-trips through the full
// public DB stack, not just its own in-package SPI tests: writes through Update, point
// reads and an ordered scan through View, an overwrite, a delete, and a reopen all return
// exactly what a correct store would. It is the correctness gate the directional
// betree-vs-shipped measurement leans on, because the bench harness reports throughput
// without checking the bytes, so a core that returned wrong data would still produce a
// number; this test makes sure the number is over correct reads.
func TestBetaDBStackSmoke(t *testing.T) {
	path := t.TempDir() + "/beta.kv"
	d, err := kv.Open(path, kv.WithEngine(kv.Beta))
	if err != nil {
		t.Fatalf("open beta: %v", err)
	}

	const n = 2000
	key := func(i int) []byte { return []byte(fmt.Sprintf("k%06d", i)) }
	val := func(i int) []byte { return []byte(fmt.Sprintf("v%06d-%0100d", i, i)) }

	// Write the keyspace in batches through the public transaction surface.
	for base := 0; base < n; base += 100 {
		if err := d.Update(func(txn *kv.Txn) error {
			for i := base; i < base+100 && i < n; i++ {
				if err := txn.Set(key(i), val(i)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("update batch at %d: %v", base, err)
		}
	}

	// Point read every key back.
	if err := d.View(func(txn *kv.Txn) error {
		for i := 0; i < n; i++ {
			got, err := txn.Get(key(i))
			if err != nil {
				return fmt.Errorf("get %d: %w", i, err)
			}
			if string(got) != string(val(i)) {
				return fmt.Errorf("get %d = %q, want %q", i, got, val(i))
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("point reads: %v", err)
	}

	// Ordered forward scan must surface every key in order at its value.
	if err := d.View(func(txn *kv.Txn) error {
		it, err := txn.NewIterator(kv.IterOptions{})
		if err != nil {
			return err
		}
		defer it.Close()
		want := 0
		for it.First(); it.Valid(); it.Next() {
			if string(it.Key()) != string(key(want)) {
				return fmt.Errorf("scan pos %d key = %q, want %q", want, it.Key(), key(want))
			}
			gotVal, err := it.Value()
			if err != nil {
				return fmt.Errorf("scan pos %d value: %w", want, err)
			}
			if string(gotVal) != string(val(want)) {
				return fmt.Errorf("scan pos %d val = %q, want %q", want, gotVal, val(want))
			}
			want++
		}
		if want != n {
			return fmt.Errorf("scan saw %d keys, want %d", want, n)
		}
		return nil
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}

	// Overwrite the even keys and delete every fifth key, then verify the new state.
	if err := d.Update(func(txn *kv.Txn) error {
		for i := 0; i < n; i += 2 {
			if err := txn.Set(key(i), []byte(fmt.Sprintf("OW%06d", i))); err != nil {
				return err
			}
		}
		for i := 0; i < n; i += 5 {
			if err := txn.Delete(key(i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("overwrite/delete: %v", err)
	}

	wantVal := func(i int) (string, bool) {
		if i%5 == 0 {
			return "", false // deleted
		}
		if i%2 == 0 {
			return fmt.Sprintf("OW%06d", i), true
		}
		return string(val(i)), true
	}

	check := func(d *kv.DB, label string) {
		if err := d.View(func(txn *kv.Txn) error {
			for i := 0; i < n; i++ {
				got, err := txn.Get(key(i))
				w, present := wantVal(i)
				if !present {
					if err == nil {
						return fmt.Errorf("%s: get %d returned %q, want not-found", label, i, got)
					}
					continue
				}
				if err != nil {
					return fmt.Errorf("%s: get %d: %w", label, i, err)
				}
				if string(got) != w {
					return fmt.Errorf("%s: get %d = %q, want %q", label, i, got, w)
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("%s reads: %v", label, err)
		}
	}

	check(d, "post-mutation")

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and confirm the mutated state survived the round trip to disk.
	d2, err := kv.Open(path, kv.WithEngine(kv.Beta))
	if err != nil {
		t.Fatalf("reopen beta: %v", err)
	}
	defer d2.Close()
	check(d2, "post-reopen")
}
