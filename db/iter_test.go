package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
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

// TestIterSeekReseat exercises the forward streaming re-seat across a range large enough that a
// mid-range SeekGE skips a real prefix, then drives every backward step that must restore that
// prefix: Prev across the re-seat point, SeekLT below it, a SeekGE to a key behind the current
// position, First, and Last. It runs on both streaming engines (the default B-tree and the Beta
// core) so the re-seat path and its lower-bound fallback are both covered. A regression in the
// re-seat bookkeeping shows up here as a missing earlier key, the exact shape of the bug the
// original front-fill avoided by never skipping the prefix.
func TestIterSeekReseat(t *testing.T) {
	for _, eng := range []struct {
		name string
		kind format.EngineKind
	}{
		{"btree", format.EngineBTree},
		{"beta", format.EngineBeta},
	} {
		t.Run(eng.name, func(t *testing.T) {
			d := openMem(t, Options{Engine: eng.kind})
			seedRange(t, d, 40) // k00..k39
			d.View(func(txn *Txn) error {
				it, _ := txn.NewIterator(engine.IterOptions{})
				defer it.Close()

				// Forward re-seat past the prefix, then step back across the re-seat point.
				if !it.SeekGE([]byte("k20")) || string(it.Key()) != "k20" {
					t.Fatalf("SeekGE k20 -> %q", it.Key())
				}
				if !it.Prev() || string(it.Key()) != "k19" {
					t.Fatalf("Prev after re-seat -> %q, want k19", it.Key())
				}

				// After a forward re-seat, SeekLT below the re-seat point must still find the key.
				if !it.SeekGE([]byte("k30")) || string(it.Key()) != "k30" {
					t.Fatalf("SeekGE k30 -> %q", it.Key())
				}
				if !it.SeekLT([]byte("k05")) || string(it.Key()) != "k04" {
					t.Fatalf("SeekLT k05 after re-seat -> %q, want k04", it.Key())
				}

				// A backward SeekGE (to a key behind the current base) re-opens at the lower bound.
				if !it.SeekGE([]byte("k35")) || string(it.Key()) != "k35" {
					t.Fatalf("SeekGE k35 -> %q", it.Key())
				}
				if !it.SeekGE([]byte("k10")) || string(it.Key()) != "k10" {
					t.Fatalf("SeekGE k10 after re-seat -> %q, want k10", it.Key())
				}

				// First and Last must restore full coverage after a forward re-seat.
				if !it.SeekGE([]byte("k25")) || string(it.Key()) != "k25" {
					t.Fatalf("SeekGE k25 -> %q", it.Key())
				}
				if !it.First() || string(it.Key()) != "k00" {
					t.Fatalf("First after re-seat -> %q, want k00", it.Key())
				}
				if !it.SeekGE([]byte("k25")) || string(it.Key()) != "k25" {
					t.Fatalf("SeekGE k25 (2) -> %q", it.Key())
				}
				if !it.Last() || string(it.Key()) != "k39" {
					t.Fatalf("Last after re-seat -> %q, want k39", it.Key())
				}
				return nil
			})
		})
	}
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
