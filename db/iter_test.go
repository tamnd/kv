package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// collect walks an iterator forward (in its direction) and returns the keys, and
// values when not key-only, it visits.
func collect(t *testing.T, it *Iterator) ([]string, []string) {
	t.Helper()
	var keys, vals []string
	for it.First(); it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
		v, err := it.Value()
		if err != nil {
			t.Fatalf("value: %v", err)
		}
		vals = append(vals, string(v))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	return keys, vals
}

// seedRange writes keys k0..k{n-1} with values v0..v{n-1}.
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

func eq(a []string, b ...string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestIterForwardOrder scans the whole space ascending.
func TestIterForwardOrder(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 5)
	d.View(func(txn *Txn) error {
		it, err := txn.NewIterator(engine.IterOptions{})
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		defer it.Close()
		keys, vals := collect(t, it)
		if !eq(keys, "k00", "k01", "k02", "k03", "k04") {
			t.Fatalf("keys = %v", keys)
		}
		if !eq(vals, "v00", "v01", "v02", "v03", "v04") {
			t.Fatalf("vals = %v", vals)
		}
		return nil
	})
}

// TestIterBounds restricts the scan to [k01, k04).
func TestIterBounds(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 5)
	d.View(func(txn *Txn) error {
		it, _ := txn.NewIterator(engine.IterOptions{Lower: []byte("k01"), Upper: []byte("k04")})
		defer it.Close()
		keys, _ := collect(t, it)
		if !eq(keys, "k01", "k02", "k03") {
			t.Fatalf("bounded keys = %v", keys)
		}
		return nil
	})
}

// TestIterPrefix scans only keys under a prefix.
func TestIterPrefix(t *testing.T) {
	d := openMem(t, Options{})
	d.Update(func(txn *Txn) error {
		txn.Set([]byte("user:1"), []byte("a"))
		txn.Set([]byte("user:2"), []byte("b"))
		txn.Set([]byte("post:1"), []byte("c"))
		txn.Set([]byte("zzz"), []byte("d"))
		return nil
	})
	d.View(func(txn *Txn) error {
		it, _ := txn.NewIterator(engine.IterOptions{Prefix: []byte("user:")})
		defer it.Close()
		keys, _ := collect(t, it)
		if !eq(keys, "user:1", "user:2") {
			t.Fatalf("prefix keys = %v", keys)
		}
		return nil
	})
}

// TestIterReverse walks high to low.
func TestIterReverse(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 4)
	d.View(func(txn *Txn) error {
		it, _ := txn.NewIterator(engine.IterOptions{Reverse: true})
		defer it.Close()
		keys, _ := collect(t, it)
		if !eq(keys, "k03", "k02", "k01", "k00") {
			t.Fatalf("reverse keys = %v", keys)
		}
		return nil
	})
}

// TestIterKeysOnly checks values are not materialized.
func TestIterKeysOnly(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 3)
	d.View(func(txn *Txn) error {
		it, _ := txn.NewIterator(engine.IterOptions{KeysOnly: true})
		defer it.Close()
		var keys []string
		for it.First(); it.Valid(); it.Next() {
			keys = append(keys, string(it.Key()))
			if v, _ := it.Value(); v != nil {
				t.Fatalf("key-only value = %q, want nil", v)
			}
		}
		if !eq(keys, "k00", "k01", "k02") {
			t.Fatalf("keys-only = %v", keys)
		}
		return nil
	})
}

// TestIterReadYourWrites checks a scan inside a write transaction sees its own
// buffered insert, overwrite, and delete in order.
func TestIterReadYourWrites(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 4) // k00..k03

	err := d.Update(func(txn *Txn) error {
		txn.Set([]byte("k01"), []byte("OVER")) // overwrite an existing key
		txn.Delete([]byte("k02"))              // hide an existing key
		txn.Set([]byte("k015"), []byte("NEW")) // insert a new key mid-range
		it, err := txn.NewIterator(engine.IterOptions{})
		if err != nil {
			return err
		}
		defer it.Close()
		keys, vals := collect(t, it)
		if !eq(keys, "k00", "k01", "k015", "k03") {
			t.Fatalf("ryw keys = %v", keys)
		}
		if !eq(vals, "v00", "OVER", "NEW", "v03") {
			t.Fatalf("ryw vals = %v", vals)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
}

// TestIterSeek positions with SeekGE and SeekLT.
func TestIterSeek(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 5) // k00..k04
	d.View(func(txn *Txn) error {
		it, _ := txn.NewIterator(engine.IterOptions{})
		defer it.Close()

		if !it.SeekGE([]byte("k02")) || string(it.Key()) != "k02" {
			t.Fatalf("SeekGE k02 -> %q", it.Key())
		}
		if !it.SeekGE([]byte("k025")) || string(it.Key()) != "k03" {
			t.Fatalf("SeekGE k025 -> %q, want k03", it.Key())
		}
		if it.SeekGE([]byte("k99")) {
			t.Fatalf("SeekGE past end should be invalid, got %q", it.Key())
		}
		if !it.SeekLT([]byte("k02")) || string(it.Key()) != "k01" {
			t.Fatalf("SeekLT k02 -> %q, want k01", it.Key())
		}
		return nil
	})
}

// TestIterSnapshotStable checks a long-lived iterator does not see a concurrent
// commit.
func TestIterSnapshotStable(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 3)

	reader := d.Begin(false)
	defer reader.Discard()
	it, _ := reader.NewIterator(engine.IterOptions{})
	defer it.Close()

	// Commit a new key after the iterator was created.
	d.Update(func(txn *Txn) error { return txn.Set([]byte("k09"), []byte("late")) })

	keys, _ := collect(t, it)
	if !eq(keys, "k00", "k01", "k02") {
		t.Fatalf("iterator saw concurrent commit: %v", keys)
	}
}
