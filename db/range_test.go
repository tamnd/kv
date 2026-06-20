package db

import (
	"errors"
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// TestDeleteRangeCommit deletes a half-open interval and checks the covered keys
// are gone while the keys on either side survive.
func TestDeleteRangeCommit(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 10) // k00..k09

	if err := d.Update(func(txn *Txn) error {
		return txn.DeleteRange([]byte("k03"), []byte("k07")) // covers k03..k06
	}); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%02d", i)
		_, ok := txnGet(t, d, key)
		covered := i >= 3 && i <= 6
		if covered && ok {
			t.Fatalf("covered key %q still present", key)
		}
		if !covered && !ok {
			t.Fatalf("uncovered key %q went missing", key)
		}
	}
}

// TestDeleteRangeReadYourWrites checks a write transaction sees its own buffered
// range delete: covered keys read absent, keys outside the range remain.
func TestDeleteRangeReadYourWrites(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 6) // k00..k05

	err := d.Update(func(txn *Txn) error {
		txn.DeleteRange([]byte("k02"), []byte("k05")) // covers k02..k04
		if _, err := txn.Get([]byte("k03")); !errors.Is(err, engine.ErrNotFound) {
			t.Fatalf("covered k03 should read absent, got %v", err)
		}
		if v, err := txn.Get([]byte("k05")); err != nil || string(v) != "v05" {
			t.Fatalf("uncovered k05 = %q,%v, want v05", v, err)
		}
		if v, err := txn.Get([]byte("k01")); err != nil || string(v) != "v01" {
			t.Fatalf("uncovered k01 = %q,%v, want v01", v, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
}

// TestDeleteRangeInterleave checks the chronological order of a buffered range
// delete against point writes on a covered key: a set before the range delete is
// erased, a set after it survives.
func TestDeleteRangeInterleave(t *testing.T) {
	d := openMem(t, Options{})

	// Set then delete-range: the key is erased.
	if err := d.Update(func(txn *Txn) error {
		txn.Set([]byte("a"), []byte("v1"))
		txn.DeleteRange([]byte("a"), []byte("b"))
		if _, err := txn.Get([]byte("a")); !errors.Is(err, engine.ErrNotFound) {
			t.Fatalf("a after set-then-rangedelete should be absent, got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("update set-then-delete: %v", err)
	}
	if _, ok := txnGet(t, d, "a"); ok {
		t.Fatalf("committed a should be absent after set-then-rangedelete")
	}

	// Delete-range then set: the set wins.
	if err := d.Update(func(txn *Txn) error {
		txn.DeleteRange([]byte("c"), []byte("d"))
		txn.Set([]byte("c"), []byte("v2"))
		if v, err := txn.Get([]byte("c")); err != nil || string(v) != "v2" {
			t.Fatalf("c after rangedelete-then-set = %q,%v, want v2", v, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("update delete-then-set: %v", err)
	}
	if v, ok := txnGet(t, d, "c"); !ok || v != "v2" {
		t.Fatalf("committed c = %q,%v, want v2", v, ok)
	}
}

// TestDeleteRangeIterator checks a scan inside a write transaction reflects a
// buffered range delete, a buffered insert inside the range, and an overwrite.
func TestDeleteRangeIterator(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 6) // k00..k05

	err := d.Update(func(txn *Txn) error {
		txn.DeleteRange([]byte("k02"), []byte("k05")) // hides k02..k04
		txn.Set([]byte("k03"), []byte("REBORN"))      // re-insert one covered key after the delete
		txn.Set([]byte("k00"), []byte("OVER"))        // overwrite a surviving key
		it, err := txn.NewIterator(engine.IterOptions{})
		if err != nil {
			return err
		}
		defer it.Close()
		keys, vals := collect(t, it)
		if !eq(keys, "k00", "k01", "k03", "k05") {
			t.Fatalf("range-delete scan keys = %v", keys)
		}
		if !eq(vals, "OVER", "v01", "REBORN", "v05") {
			t.Fatalf("range-delete scan vals = %v", vals)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
}

// TestDeleteRangeSnapshotStable checks a reader begun before a committed range
// delete still sees the covered keys (snapshot isolation).
func TestDeleteRangeSnapshotStable(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 5) // k00..k04

	reader := d.Begin(false)
	defer reader.Discard()

	if err := d.Update(func(txn *Txn) error {
		return txn.DeleteRange([]byte("k01"), []byte("k04"))
	}); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	// The old snapshot still has every key; a fresh read sees the deletion.
	if v, err := reader.Get([]byte("k02")); err != nil || string(v) != "v02" {
		t.Fatalf("stable reader k02 = %q,%v, want v02", v, err)
	}
	if _, ok := txnGet(t, d, "k02"); ok {
		t.Fatalf("fresh read of k02 should be absent after range delete")
	}
}

// TestDeleteRangeBlindNoConflict checks a range delete does not conflict with a
// concurrent write to a covered key: both transactions commit.
func TestDeleteRangeBlindNoConflict(t *testing.T) {
	d := openMem(t, Options{})
	seedRange(t, d, 5)

	t1 := d.Begin(true)
	t2 := d.Begin(true)
	t1.DeleteRange([]byte("k01"), []byte("k04"))
	t2.Set([]byte("k02"), []byte("concurrent"))

	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 (range delete) commit: %v", err)
	}
	// The range delete is blind, so t2 is not aborted by it. t2 committing after t1
	// re-establishes k02 at a higher version, which shadows the range delete.
	if err := t2.Commit(); err != nil {
		t.Fatalf("t2 (covered write) commit = %v, want success (range delete is blind)", err)
	}
	if v, ok := txnGet(t, d, "k02"); !ok || v != "concurrent" {
		t.Fatalf("k02 = %q,%v, want concurrent (committed after the range delete)", v, ok)
	}
}
