package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// pairFeed returns a pull function over the given key/value pairs, the shape db.Load
// consumes.
func pairFeed(pairs [][2]string) func() ([]byte, []byte, bool) {
	i := 0
	return func() ([]byte, []byte, bool) {
		if i >= len(pairs) {
			return nil, nil, false
		}
		p := pairs[i]
		i++
		return []byte(p[0]), []byte(p[1]), true
	}
}

// ascendingPairs builds n strictly ascending key/value pairs.
func ascendingPairs(n int) [][2]string {
	pairs := make([][2]string, n)
	for i := 0; i < n; i++ {
		pairs[i] = [2]string{fmt.Sprintf("k%06d", i), fmt.Sprintf("v%06d", i)}
	}
	return pairs
}

// TestLoadFastPathPopulatesEmpty bulk-loads ascending pairs into a fresh database, then
// checks every key reads back at the returned version.
func TestLoadFastPathPopulatesEmpty(t *testing.T) {
	d, err := Open(vfs.NewMem(), "test.kv", Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { d.Close() })

	const n = 400
	v, err := d.Load(pairFeed(ascendingPairs(n)))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if v != 1 {
		t.Fatalf("load version = %d, want 1 (first commit on empty db)", v)
	}

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%06d", i)
		got, ok := txnGet(t, d, k)
		if !ok || got != fmt.Sprintf("v%06d", i) {
			t.Fatalf("get %q = %q,%v", k, got, ok)
		}
	}
}

// TestLoadFastPathDurableAcrossReopen loads a fresh database, closes it, and checks the
// bulk-loaded data survives reopen -- the checkpoint at the end of the fast path made it
// durable in the main file.
func TestLoadFastPathDurableAcrossReopen(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := d.Load(pairFeed(ascendingPairs(200))); err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { d2.Close() })
	if d2.Version() != 1 {
		t.Fatalf("reopened version = %d, want 1", d2.Version())
	}
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("k%06d", i)
		if got, ok := txnGet(t, d2, k); !ok || got != fmt.Sprintf("v%06d", i) {
			t.Fatalf("after reopen get %q = %q,%v", k, got, ok)
		}
	}

	// A normal write after a bulk load gets the next version and commits as usual.
	wv, err := d2.Write(func(b *engine.WriteBatch) { b.Set([]byte("k000000"), []byte("updated")) })
	if err != nil {
		t.Fatalf("write after load: %v", err)
	}
	if wv != 2 {
		t.Fatalf("post-load write version = %d, want 2", wv)
	}
	if got, _ := txnGet(t, d2, "k000000"); got != "updated" {
		t.Fatalf("post-load overwrite = %q, want updated", got)
	}
}

// TestLoadAcceptsUnsorted feeds out-of-order keys into a fresh database. f2 has no key
// order, so its bulk load carries no ascending-order precondition: unsorted input loads
// cleanly and every pair reads back.
func TestLoadAcceptsUnsorted(t *testing.T) {
	d, err := Open(vfs.NewMem(), "test.kv", Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { d.Close() })

	if _, err := d.Load(pairFeed([][2]string{{"b", "1"}, {"a", "2"}})); err != nil {
		t.Fatalf("unsorted load: %v", err)
	}
	for k, want := range map[string]string{"a": "2", "b": "1"} {
		if got, ok := txnGet(t, d, k); !ok || got != want {
			t.Fatalf("get %q = %q,%v, want %q", k, got, ok, want)
		}
	}
}

// TestLoadFallbackOnNonEmpty loads into a database that already holds a commit, so the
// fast path is skipped and the order-agnostic fallback runs. It accepts unsorted keys and
// merges them with the existing data.
func TestLoadFallbackOnNonEmpty(t *testing.T) {
	d, err := Open(vfs.NewMem(), "test.kv", Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { d.Close() })

	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("existing"), []byte("old")) }); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	// Unsorted input is fine on the fallback path.
	if _, err := d.Load(pairFeed([][2]string{{"z", "26"}, {"a", "1"}, {"m", "13"}})); err != nil {
		t.Fatalf("fallback load: %v", err)
	}
	for k, want := range map[string]string{"existing": "old", "z": "26", "a": "1", "m": "13"} {
		if got, ok := txnGet(t, d, k); !ok || got != want {
			t.Fatalf("get %q = %q,%v, want %q", k, got, ok, want)
		}
	}
}
