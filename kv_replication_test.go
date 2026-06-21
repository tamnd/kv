package kv_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv"
)

// TestShipApplyFacade drives WAL shipping end to end through the public surface: seed a
// primary, restore a base into a follower opened WithReadReplica, ship the post-base
// writes, and read them back on the follower.
func TestShipApplyFacade(t *testing.T) {
	dir := t.TempDir()
	primaryPath := filepath.Join(dir, "primary.kv")
	followerPath := filepath.Join(dir, "follower.kv")

	p, err := kv.Open(primaryPath, kv.WithAutoCheckpoint(-1))
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	if err := p.Update(func(txn *kv.Txn) error { return txn.Set([]byte("seed"), []byte("0")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Seed the follower from a base backup, then open it read-only.
	var base bytes.Buffer
	if _, err := p.Backup(&base); err != nil {
		t.Fatalf("base backup: %v", err)
	}
	if err := kv.RestoreBackup(followerPath, &base); err != nil {
		t.Fatalf("restore base: %v", err)
	}
	f, err := kv.Open(followerPath, kv.WithReadReplica())
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer f.Close()

	for i := 0; i < 50; i++ {
		k := []byte{'k', byte(i)}
		if err := p.Update(func(txn *kv.Txn) error { return txn.Set(k, []byte{'v', byte(i)}) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	var buf bytes.Buffer
	primaryVersion, err := p.ShipWAL(&buf)
	if err != nil {
		t.Fatalf("ship: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close primary: %v", err)
	}
	applied, err := f.ApplyWAL(&buf)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if applied != primaryVersion {
		t.Fatalf("follower applied %d, want %d", applied, primaryVersion)
	}

	if err := f.View(func(txn *kv.Txn) error {
		for i := 0; i < 50; i++ {
			v, err := txn.Get([]byte{'k', byte(i)})
			if err != nil {
				return err
			}
			if !bytes.Equal(v, []byte{'v', byte(i)}) {
				t.Fatalf("follower key %d = %q, want v%d", i, v, i)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("view follower: %v", err)
	}
}

// TestFollowerWriteRefusedFacade confirms a WithReadReplica database rejects user writes
// with kv.ErrReadOnly through the public Update path.
func TestFollowerWriteRefusedFacade(t *testing.T) {
	dir := t.TempDir()
	primaryPath := filepath.Join(dir, "primary.kv")
	followerPath := filepath.Join(dir, "follower.kv")

	p, err := kv.Open(primaryPath)
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	if err := p.Update(func(txn *kv.Txn) error { return txn.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	var base bytes.Buffer
	if _, err := p.Backup(&base); err != nil {
		t.Fatalf("backup: %v", err)
	}
	p.Close()
	if err := kv.RestoreBackup(followerPath, &base); err != nil {
		t.Fatalf("restore: %v", err)
	}
	f, err := kv.Open(followerPath, kv.WithReadReplica())
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer f.Close()

	if err := f.Update(func(txn *kv.Txn) error { return txn.Set([]byte("x"), []byte("y")) }); !errors.Is(err, kv.ErrReadOnly) {
		t.Fatalf("follower Update = %v, want kv.ErrReadOnly", err)
	}
}

// TestApplyGarbageFacade confirms the facade surfaces kv.ErrBackupFormat for a stream that
// is not a ship container.
func TestApplyGarbageFacade(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "follower.kv")
	d, err := kv.Open(path, kv.WithReadReplica())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()
	junk := bytes.NewReader([]byte("definitely not a ship stream"))
	if _, err := d.ApplyWAL(junk); !errors.Is(err, kv.ErrBackupFormat) {
		t.Fatalf("apply garbage = %v, want kv.ErrBackupFormat", err)
	}
}
