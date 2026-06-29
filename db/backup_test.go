package db

import (
	"bytes"
	"errors"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// backupAndRestore runs a database through Backup into a buffer and RestoreBackup into a
// fresh path on the same filesystem, returning the reopened restored database. It is the
// round-trip every backup test leans on.
func backupAndRestore(t *testing.T, src *DB, fs vfs.FS, dstPath string, openOpts Options) *DB {
	t.Helper()
	var buf bytes.Buffer
	if _, err := src.Backup(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := RestoreBackup(fs, dstPath, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	d, err := Open(fs, dstPath, openOpts)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	return d
}

// TestBackupRestoreRoundTrip confirms the default B-tree path: a backup taken while data is
// resident restores to a database that holds every key and passes the structural check.
func TestBackupRestoreRoundTrip(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "src.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 200; i++ {
		k := []byte{'k', byte(i), byte(i >> 8)}
		v := []byte{'v', byte(i)}
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	r := backupAndRestore(t, d, fs, "dst.kv", Options{})
	defer r.Close()
	if err := d.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	for i := 0; i < 200; i++ {
		k := []byte{'k', byte(i), byte(i >> 8)}
		want := []byte{'v', byte(i)}
		got, err := r.Get(k)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("restored key %d = %q,%v, want %q", i, got, err, want)
		}
	}
	rep, err := r.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(rep.Problems) != 0 {
		t.Fatalf("restored database not sound: %v", rep.Problems)
	}
}

// TestBackupContinuesAfterRestore confirms the restored database is a live writable database,
// not a frozen image: it accepts new writes and the source is untouched by the backup.
func TestBackupContinuesAfterRestore(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "src.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("seed"), []byte("1")) }); err != nil {
		t.Fatalf("write: %v", err)
	}

	r := backupAndRestore(t, d, fs, "dst.kv", Options{})
	defer r.Close()
	// The source keeps working after a backup.
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("after"), []byte("2")) }); err != nil {
		t.Fatalf("write src after backup: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}
	// The restore is independent: it has the seed but not the post-backup write.
	if _, err := r.Write(func(b *engine.WriteBatch) { b.Set([]byte("restore-only"), []byte("3")) }); err != nil {
		t.Fatalf("write restore: %v", err)
	}
	if got, err := r.Get([]byte("seed")); err != nil || !bytes.Equal(got, []byte("1")) {
		t.Fatalf("restore seed = %q,%v, want 1", got, err)
	}
	if _, err := r.Get([]byte("after")); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("restore should not hold post-backup key, got err %v", err)
	}
}

// TestRestoreRefusesExistingFile confirms restore never clobbers: a destination that already
// exists is refused before any byte is written.
func TestRestoreRefusesExistingFile(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "src.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	var buf bytes.Buffer
	if _, err := d.Backup(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	d.Close()

	// dst.kv already exists (it is src reopened path). Use src.kv itself as the target.
	if err := RestoreBackup(fs, "src.kv", &buf); err == nil {
		t.Fatal("restore over an existing file should fail")
	}
}

// TestRestoreRejectsGarbage confirms a stream that is not a backup container is refused with
// ErrBackupFormat rather than producing a malformed database.
func TestRestoreRejectsGarbage(t *testing.T) {
	fs := vfs.NewMem()
	junk := bytes.NewReader([]byte("this is not a kv backup stream at all"))
	if err := RestoreBackup(fs, "out.kv", junk); !errors.Is(err, ErrBackupFormat) {
		t.Fatalf("restore garbage = %v, want ErrBackupFormat", err)
	}
	// A truncated container (valid header, missing body) is also a format error.
	var buf bytes.Buffer
	d, _ := Open(fs, "src.kv", Options{PageSize: 4096})
	d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) })
	d.Backup(&buf)
	d.Close()
	truncated := bytes.NewReader(buf.Bytes()[:backupHeaderSize+10])
	if err := RestoreBackup(fs, "trunc.kv", truncated); !errors.Is(err, ErrBackupFormat) {
		t.Fatalf("restore truncated = %v, want ErrBackupFormat", err)
	}
}

// TestBackupEncrypted confirms an encrypted database backs up to ciphertext and restores to
// an encrypted database the same key opens, and that the plaintext is absent from the backup
// stream (spec 18 §7).
func TestBackupEncrypted(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "src.kv", Options{PageSize: 4096, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted: %v", err)
	}
	secret := []byte("BACKUP-PLAINTEXT-NEEDLE-99887766")
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("needle"), secret) }); err != nil {
		t.Fatalf("write: %v", err)
	}

	var buf bytes.Buffer
	if _, err := d.Backup(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if bytes.Contains(buf.Bytes(), secret) {
		t.Fatal("plaintext value found in the encrypted backup stream")
	}
	if err := RestoreBackup(fs, "dst.kv", &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	d.Close()

	// Wrong key is refused; the right key reads the value back.
	wrong := bytes.Repeat([]byte{0x01}, 32)
	if _, err := Open(fs, "dst.kv", Options{EncryptionKey: wrong}); err == nil {
		t.Fatal("restored encrypted database opened under the wrong key")
	}
	r, err := Open(fs, "dst.kv", Options{EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("open restored with key: %v", err)
	}
	defer r.Close()
	if got, err := r.Get([]byte("needle")); err != nil || !bytes.Equal(got, secret) {
		t.Fatalf("restored value = %q,%v, want secret", got, err)
	}
}

// TestBackupSyncFullWALTail confirms a backup taken with durable commits and no checkpoint
// still restores every commit: the checkpoint Backup runs folds the log, so the image is
// whole regardless of the pre-backup checkpoint state.
func TestBackupSyncFullWALTail(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "src.kv", Options{PageSize: 4096, Sync: wal.SyncFull, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 50; i++ {
		k := []byte{'k', byte(i)}
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, []byte{byte(i)}) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	r := backupAndRestore(t, d, fs, "dst.kv", Options{})
	defer r.Close()
	d.Close()
	for i := 0; i < 50; i++ {
		k := []byte{'k', byte(i)}
		got, err := r.Get(k)
		if err != nil || !bytes.Equal(got, []byte{byte(i)}) {
			t.Fatalf("restored key %d = %q,%v", i, got, err)
		}
	}
}
