package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// TestF2PrunedSnapshotStillReads is the correctness obligation of the bounded version group
// (redesign-v2 doc 02): a reader holding a snapshot from before a burst of overwrites must
// still resolve its snapshot's value after the burst has pruned the group. While the snapshot
// is open the readMark cannot advance past it, so the prune the host announces keeps every
// version back to the held snapshot.
func TestF2PrunedSnapshotStillReads(t *testing.T) {
	fs, path := f2TestPath(t)
	d, err := Open(fs, path, Options{PageSize: 4096, Engine: format.EngineF2})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	key := []byte("hot")
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(key, []byte("v-original")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Hold a snapshot at the original value. It pins the readMark so the burst below cannot
	// prune the version this snapshot resolves to.
	snap := d.Snapshot()
	defer snap.Close()

	// A burst of overwrites, each its own commit, the churn pattern that triggers pruning.
	for i := 0; i < 2000; i++ {
		v := []byte(fmt.Sprintf("v-%d", i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(key, v) }); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}

	// The held snapshot must still see the original value.
	if err := snap.View(func(txn *Txn) error {
		got, err := txn.Get(key)
		if err != nil {
			return err
		}
		if string(got) != "v-original" {
			t.Fatalf("held snapshot = %q, want %q", got, "v-original")
		}
		return nil
	}); err != nil {
		t.Fatalf("snapshot view: %v", err)
	}

	// A fresh read sees the latest write, confirming the burst landed and the group is live.
	if got, ok := get(t, d, "hot"); !ok || got != "v-1999" {
		t.Fatalf("latest = %q,%v, want %q", got, ok, "v-1999")
	}
}

// TestF2PrunedDeleteSnapshot checks the delete-tombstone case: a snapshot taken before a delete
// still sees the pre-delete value after later overwrites have pruned the group, since the
// delete is a base the prune cannot drop below while the snapshot pins it.
func TestF2PrunedDeleteSnapshot(t *testing.T) {
	fs, path := f2TestPath(t)
	d, err := Open(fs, path, Options{PageSize: 4096, Engine: format.EngineF2})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	key := []byte("k")
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(key, []byte("before")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	snap := d.Snapshot()
	defer snap.Close()

	if _, err := d.Write(func(b *engine.WriteBatch) { b.Delete(key) }); err != nil {
		t.Fatalf("delete: %v", err)
	}
	for i := 0; i < 500; i++ {
		v := []byte(fmt.Sprintf("after-%d", i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(key, v) }); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}

	if err := snap.View(func(txn *Txn) error {
		got, err := txn.Get(key)
		if err != nil {
			return err
		}
		if string(got) != "before" {
			t.Fatalf("held pre-delete snapshot = %q, want %q", got, "before")
		}
		return nil
	}); err != nil {
		t.Fatalf("snapshot view: %v", err)
	}
}
