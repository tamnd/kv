package db

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
)

// TestF2BackupRestoreRoundTrip confirms the self-durable core's backup path: the image
// carries the f2 sidecar, so a restore reconstructs the sidecar plus the WAL tail and the
// reopened database holds every key. The f2 core uses the OS filesystem for its sidecar, so
// these run against vfs.NewOS and temp paths rather than the in-memory filesystem.
func TestF2BackupRestoreRoundTrip(t *testing.T) {
	const n = 300
	fs := vfs.NewOS()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.kv")
	dstPath := filepath.Join(dir, "dst.kv")

	d, err := Open(fs, srcPath, Options{PageSize: 4096, Engine: format.EngineF2})
	if err != nil {
		t.Fatalf("create f2: %v", err)
	}
	for i := 0; i < n; i++ {
		k, v := []byte(f2Key(i)), []byte(f2Val(i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	var buf bytes.Buffer
	if _, err := d.Backup(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := RestoreBackup(fs, dstPath, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	r, err := Open(fs, dstPath, Options{})
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer r.Close()
	if got := r.Stats().Engine; got != format.EngineF2 {
		t.Fatalf("restored engine = %v, want EngineF2", got)
	}
	for i := 0; i < n; i++ {
		if v, ok := get(t, r, f2Key(i)); !ok || v != f2Val(i) {
			t.Fatalf("restored key %d = %q,%v, want %q", i, v, ok, f2Val(i))
		}
	}
}

// TestF2BackupRestoreEncrypted confirms an encrypted f2 backup stays sealed end to end: the
// sidecar is copied as ciphertext, the container never holds the plaintext value, and a
// restore opened with the original key reads every key back.
func TestF2BackupRestoreEncrypted(t *testing.T) {
	const n = 120
	secret := []byte("RESTORE-NEEDLE-VALUE-0987654321")
	fs := vfs.NewOS()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.kv")
	dstPath := filepath.Join(dir, "dst.kv")

	d, err := Open(fs, srcPath, Options{PageSize: 4096, Engine: format.EngineF2, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted f2: %v", err)
	}
	for i := 0; i < n; i++ {
		k, v := []byte(f2Key(i)), []byte(f2Val(i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("needle"), secret) }); err != nil {
		t.Fatalf("write needle: %v", err)
	}

	var buf bytes.Buffer
	if _, err := d.Backup(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if bytes.Contains(buf.Bytes(), secret) {
		t.Fatal("plaintext value found in the encrypted f2 backup container")
	}
	if err := RestoreBackup(fs, dstPath, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	r, err := Open(fs, dstPath, Options{EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("open restored with key: %v", err)
	}
	defer r.Close()
	if v, ok := get(t, r, "needle"); !ok || v != string(secret) {
		t.Fatalf("restored needle = %q,%v, want secret", v, ok)
	}
	for i := 0; i < n; i++ {
		if v, ok := get(t, r, f2Key(i)); !ok || v != f2Val(i) {
			t.Fatalf("restored key %d = %q,%v, want %q", i, v, ok, f2Val(i))
		}
	}
}

// TestF2RestoreRefusesExistingSidecar confirms a restore that would overwrite an existing f2
// sidecar fails loudly and leaves the target untouched, the same create-never-clobber rule
// the main and WAL files follow.
func TestF2RestoreRefusesExistingSidecar(t *testing.T) {
	fs := vfs.NewOS()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.kv")
	dstPath := filepath.Join(dir, "dst.kv")

	d, err := Open(fs, srcPath, Options{PageSize: 4096, Engine: format.EngineF2})
	if err != nil {
		t.Fatalf("create f2: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	var buf bytes.Buffer
	if _, err := d.Backup(&buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Plant a sidecar at the destination, then a restore must refuse rather than clobber it.
	f, err := fs.Open(dstPath+"-f2", vfs.OpenReadWrite|vfs.OpenCreate)
	if err != nil {
		t.Fatalf("plant sidecar: %v", err)
	}
	f.Close()
	if err := RestoreBackup(fs, dstPath, &buf); err == nil {
		t.Fatal("restore over an existing sidecar succeeded, want refusal")
	}
}
