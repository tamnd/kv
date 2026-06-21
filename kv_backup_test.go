package kv_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv"
)

// TestBackupRestoreFacade drives a physical backup end to end through the public surface:
// write, back up to a buffer, restore to a new path, reopen, and read the data back. It is
// the contract a caller sees, with no db or pager types in sight.
func TestBackupRestoreFacade(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.kv")
	dstPath := filepath.Join(dir, "dst.kv")

	d, err := kv.Open(srcPath)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error {
		for i := 0; i < 100; i++ {
			if err := txn.Set([]byte{'k', byte(i)}, []byte{'v', byte(i)}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	var buf bytes.Buffer
	version, err := d.Backup(&buf)
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	if version == 0 {
		t.Fatal("backup version should be non-zero after commits")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	if err := kv.RestoreBackup(dstPath, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	r, err := kv.Open(dstPath)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer r.Close()
	if err := r.View(func(txn *kv.Txn) error {
		for i := 0; i < 100; i++ {
			v, err := txn.Get([]byte{'k', byte(i)})
			if err != nil {
				return err
			}
			if !bytes.Equal(v, []byte{'v', byte(i)}) {
				t.Fatalf("key %d = %q, want v%d", i, v, i)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("view restored: %v", err)
	}
}

// TestRestoreFacadeRejectsGarbage confirms the facade surfaces kv.ErrBackupFormat when the
// stream is not a backup container.
func TestRestoreFacadeRejectsGarbage(t *testing.T) {
	dstPath := filepath.Join(t.TempDir(), "dst.kv")
	junk := bytes.NewReader([]byte("definitely not a kv backup"))
	if err := kv.RestoreBackup(dstPath, junk); !errors.Is(err, kv.ErrBackupFormat) {
		t.Fatalf("restore garbage = %v, want kv.ErrBackupFormat", err)
	}
}

// TestBackupRestoreEncryptedFacade confirms an encrypted database backs up and restores
// through the public surface and that the restored database needs the original key.
func TestBackupRestoreEncryptedFacade(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.kv")
	dstPath := filepath.Join(dir, "dst.kv")

	d, err := kv.Open(srcPath, kv.WithEncryptionKey(encKey))
	if err != nil {
		t.Fatalf("open encrypted: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.Set([]byte("secret"), []byte("encrypted-payload"))
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	var buf bytes.Buffer
	if _, err := d.Backup(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := kv.RestoreBackup(dstPath, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if _, err := kv.Open(dstPath); !errors.Is(err, kv.ErrEncryptionKeyRequired) {
		t.Fatalf("open restored without key = %v, want kv.ErrEncryptionKeyRequired", err)
	}
	r, err := kv.Open(dstPath, kv.WithEncryptionKey(encKey))
	if err != nil {
		t.Fatalf("open restored with key: %v", err)
	}
	defer r.Close()
	if err := r.View(func(txn *kv.Txn) error {
		v, err := txn.Get([]byte("secret"))
		if err != nil {
			return err
		}
		if string(v) != "encrypted-payload" {
			t.Fatalf("restored value = %q, want encrypted-payload", v)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}
