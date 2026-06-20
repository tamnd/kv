package kv_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv"
)

// TestCompactRoundTrips drives the public Compact surface end to end: it fills a database on
// disk, churns it so it carries dead space, closes it, runs a full vacuum, and reopens the
// swapped-in file to confirm the live data survived and the rebuild is queryable.
func TestCompactRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.kv")
	d, err := kv.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const n = 400
	if err := d.Update(func(txn *kv.Txn) error {
		for i := 0; i < n; i++ {
			if err := txn.Set([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%05d", i))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Delete the even keys, leaving tombstones the rebuild drops.
	if err := d.Update(func(txn *kv.Txn) error {
		for i := 0; i < n; i += 2 {
			if err := txn.Delete([]byte(fmt.Sprintf("k%05d", i))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := kv.Compact(path); err != nil {
		t.Fatalf("compact: %v", err)
	}

	d2, err := kv.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { d2.Close() })

	if err := d2.View(func(txn *kv.Txn) error {
		for i := 0; i < n; i++ {
			k := []byte(fmt.Sprintf("k%05d", i))
			got, err := txn.Get(k)
			if i%2 == 0 {
				if err == nil {
					t.Fatalf("deleted key %q present after compact", k)
				}
				continue
			}
			if err != nil {
				return err
			}
			if want := fmt.Sprintf("v%05d", i); string(got) != want {
				t.Fatalf("get %q = %q, want %q", k, got, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}
