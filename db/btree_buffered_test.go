package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/vfs"
)

// TestBtreeBufferedInsertsTransparent drives the BufferedInserts option through the
// public database: the same writes load into one default (in-place) B-tree and one with
// the Bε buffered write path, and every read must agree. Buffering is a pure write-path
// performance choice, so swapping it in may never change what a point read or a scan
// returns, present key or absent. It runs at a small page size so the buffered tree
// builds real interior buffers and the flush cascade runs during the load.
func TestBtreeBufferedInsertsTransparent(t *testing.T) {
	const n = 1500
	load := func(d *DB) {
		// Initial values, then overwrite one residue class and delete a disjoint one at a
		// higher version, so the tree carries MVCC depth and a genuinely absent population
		// (the deleted keys) without ever touching a key twice in one batch.
		if err := d.Update(func(txn *Txn) error {
			for i := 0; i < n; i++ {
				txn.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("v%05d", i)))
			}
			return nil
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if err := d.Update(func(txn *Txn) error {
			for i := 0; i < n; i++ {
				switch i % 3 {
				case 0:
					txn.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("w%05d", i)))
				case 1:
					txn.Delete([]byte(fmt.Sprintf("key%05d", i)))
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("update: %v", err)
		}
	}

	base := Options{PageSize: 512, CacheFrames: 256}
	plain := openMem(t, base)
	load(plain)

	bufOpts := base
	bufOpts.BufferedInserts = true
	buffered := openMem(t, bufOpts)
	load(buffered)

	// Point reads of every key, present and absent, must give identical answers. The
	// deleted keys (i%3==1) are the absent population; a buffered write a read failed to
	// pick up off the path would surface here as a disagreement.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%05d", i)
		pv, pok := txnGet(t, plain, k)
		bv, bok := txnGet(t, buffered, k)
		if pok != bok || pv != bv {
			t.Fatalf("point read of %s disagrees: plain (%q,%v), buffered (%q,%v)", k, pv, pok, bv, bok)
		}
	}
	for i := n; i < n+100; i++ {
		k := fmt.Sprintf("key%05d", i)
		_, pok := txnGet(t, plain, k)
		_, bok := txnGet(t, buffered, k)
		if pok || bok {
			t.Fatalf("never-written key %s reported present: plain %v, buffered %v", k, pok, bok)
		}
	}

	// Spot-check the buffered result against the truth with point reads so a bug shared by
	// both engines cannot pass: a deleted key gone, an overwritten key carrying its new
	// value, an untouched key its original.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%05d", i)
		got, ok := txnGet(t, buffered, k)
		switch i % 3 {
		case 0:
			if !ok || got != fmt.Sprintf("w%05d", i) {
				t.Fatalf("overwritten key %s = %q,%v, want w%05d", k, got, ok, i)
			}
		case 1:
			if ok {
				t.Fatalf("deleted key %s still present under BufferedInserts", k)
			}
		default:
			if !ok || got != fmt.Sprintf("v%05d", i) {
				t.Fatalf("key %s = %q,%v, want v%05d", k, got, ok, i)
			}
		}
	}
}

// TestBtreeBufferedInsertsPersist checkpoints a buffered-mode database, whose interior
// buffers hold writes that never reached a leaf, then reopens it and checks every key
// survived. The buffers are ordinary pages, so the checkpoint persists them and the
// reopen reads them back through the same buffer-aware read path; this proves the buffer
// section round-trips through the on-disk interior format and that recovery needs no
// special handling for parked messages.
func TestBtreeBufferedInsertsPersist(t *testing.T) {
	fs := vfs.NewMem()
	opts := Options{PageSize: 512, CacheFrames: 256, BufferedInserts: true}
	d, err := Open(fs, "test.kv", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	const n = 1200
	if err := d.Update(func(txn *Txn) error {
		for i := 0; i < n; i++ {
			txn.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("v%05d", i)))
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Fold the WAL into the main file so the reopen reads the tree from pages, buffers and
	// all, rather than replaying the log.
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%05d", i)
		v, ok := txnGet(t, d2, k)
		if !ok || v != fmt.Sprintf("v%05d", i) {
			t.Fatalf("after reopen key %s = %q,%v, want v%05d", k, v, ok, i)
		}
	}
}
