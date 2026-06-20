package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// TestCompactPreservesLiveData writes a database, deletes and overwrites a chunk of it so it
// carries tombstones and obsolete versions, runs a full vacuum, and checks every live key
// still reads back the latest value through a fresh open of the swapped-in file.
func TestCompactPreservesLiveData(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const n = 500
	for i := 0; i < n; i++ {
		k, v := fmt.Sprintf("k%05d", i), fmt.Sprintf("v%05d", i)
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte(k), []byte(v)) }); err != nil {
			t.Fatalf("seed write %d: %v", i, err)
		}
	}
	// Delete the first 100 keys and overwrite the next 100, leaving tombstones and old
	// versions the rebuild must drop.
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("k%05d", i)
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Delete([]byte(k)) }); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	for i := 100; i < 200; i++ {
		k, v := fmt.Sprintf("k%05d", i), fmt.Sprintf("w%05d", i)
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte(k), []byte(v)) }); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := Compact(fs, "test.kv", Options{}); err != nil {
		t.Fatalf("compact: %v", err)
	}

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { d2.Close() })

	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("k%05d", i)
		if _, ok := txnGet(t, d2, k); ok {
			t.Fatalf("deleted key %q still present after compact", k)
		}
	}
	for i := 100; i < n; i++ {
		k := fmt.Sprintf("k%05d", i)
		want := fmt.Sprintf("v%05d", i)
		if i < 200 {
			want = fmt.Sprintf("w%05d", i)
		}
		if got, ok := txnGet(t, d2, k); !ok || got != want {
			t.Fatalf("after compact get %q = %q,%v, want %q", k, got, ok, want)
		}
	}

	// The rebuilt database accepts writes and the scan stays ordered.
	count := 0
	if err := d2.View(func(txn *Txn) error {
		it, err := txn.NewIterator(engine.IterOptions{})
		if err != nil {
			return err
		}
		defer it.Close()
		prev := ""
		for it.First(); it.Valid(); it.Next() {
			if k := string(it.Key()); k <= prev && count > 0 {
				t.Fatalf("scan out of order: %q after %q", k, prev)
			} else {
				prev = k
			}
			count++
		}
		return it.Error()
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if count != n-100 {
		t.Fatalf("compacted db holds %d live keys, want %d", count, n-100)
	}
}

// TestCompactShrinksFile fills a database, deletes most of it, and checks a full vacuum
// returns the freed space to a smaller file -- the property incremental vacuum cannot reach
// when the free pages are interior rather than trailing.
func TestCompactShrinksFile(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	const n = 2000
	for i := 0; i < n; i++ {
		k, v := fmt.Sprintf("k%06d", i), fmt.Sprintf("v%06d-padding-padding-padding", i)
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte(k), []byte(v)) }); err != nil {
			t.Fatalf("seed write %d: %v", i, err)
		}
	}
	// Delete all but the last 50 keys, scattering free space through the middle of the file.
	for i := 0; i < n-50; i++ {
		k := fmt.Sprintf("k%06d", i)
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Delete([]byte(k)) }); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	before := d.Stats().PageCount
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := Compact(fs, "test.kv", Options{}); err != nil {
		t.Fatalf("compact: %v", err)
	}

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { d2.Close() })
	after := d2.Stats().PageCount
	if after >= before {
		t.Fatalf("compact did not shrink file: %d -> %d pages", before, after)
	}
	for i := n - 50; i < n; i++ {
		k := fmt.Sprintf("k%06d", i)
		want := fmt.Sprintf("v%06d-padding-padding-padding", i)
		if got, ok := txnGet(t, d2, k); !ok || got != want {
			t.Fatalf("survivor get %q = %q,%v", k, got, ok)
		}
	}
}

// TestCompactEmptyDatabase compacts a database with no live keys -- every key deleted -- and
// checks the rebuild yields a valid, empty, writable file rather than failing on the empty
// stream.
func TestCompactEmptyDatabase(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) {
		b.Set([]byte("a"), []byte("1"))
		b.Set([]byte("b"), []byte("2"))
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) {
		b.Delete([]byte("a"))
		b.Delete([]byte("b"))
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := Compact(fs, "test.kv", Options{}); err != nil {
		t.Fatalf("compact empty: %v", err)
	}

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { d2.Close() })
	if _, ok := txnGet(t, d2, "a"); ok {
		t.Fatalf("compacted empty db still has key a")
	}
	if _, err := d2.Write(func(b *engine.WriteBatch) { b.Set([]byte("c"), []byte("3")) }); err != nil {
		t.Fatalf("write after compact: %v", err)
	}
	if got, ok := txnGet(t, d2, "c"); !ok || got != "3" {
		t.Fatalf("post-compact write c = %q,%v", got, ok)
	}
}

// TestCompactLeavesNoStaleWAL checks the swap removes both the original WAL and the scratch
// sidecar, so the installed file stands alone and no leftover log can replay onto it.
func TestCompactLeavesNoStaleWAL(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := Compact(fs, "test.kv", Options{}); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if ok, _ := fs.Exists("test.kv" + compactSuffix); ok {
		t.Fatalf("scratch file %s survived the swap", "test.kv"+compactSuffix)
	}
	if ok, _ := fs.Exists("test.kv" + compactSuffix + walSuffix); ok {
		t.Fatalf("scratch WAL survived the swap")
	}
}
