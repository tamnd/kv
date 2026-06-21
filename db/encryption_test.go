package db

import (
	"bytes"
	"errors"
	"testing"

	"github.com/tamnd/kv/crypto"
	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// encKey is a fixed 32-byte raw master key for the encryption tests.
var encKey = bytes.Repeat([]byte{0x42}, 32)

// readWholeFile returns the entire contents of a vfs file, for inspecting on-disk bytes.
func readWholeFile(t *testing.T, fs vfs.FS, path string) []byte {
	t.Helper()
	f, err := fs.Open(path, vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		t.Fatalf("size %s: %v", path, err)
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return buf
}

// TestEncryptionRoundTripAcrossReopen confirms an encrypted database written, checkpointed,
// and closed comes back readable when reopened with the same key.
func TestEncryptionRoundTripAcrossReopen(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted: %v", err)
	}
	for i := 0; i < 40; i++ {
		k := []byte{'k', byte(i)}
		v := []byte{'v', byte(i), byte(i)}
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, "test.kv", Options{EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("reopen with key: %v", err)
	}
	defer d2.Close()
	for i := 0; i < 40; i++ {
		k := []byte{'k', byte(i)}
		want := []byte{'v', byte(i), byte(i)}
		got, err := d2.Get(k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get %d = %q, want %q", i, got, want)
		}
	}
}

// TestEncryptionRoundTripLSM confirms encryption is engine-agnostic: it rides the pager's
// page write and read path, so the LSM core's segment pages encrypt and decrypt across a
// reopen exactly as the B-tree core's do.
func TestEncryptionRoundTripLSM(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Engine: format.EngineLSM, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted lsm: %v", err)
	}
	for i := 0; i < 200; i++ {
		k := []byte{'k', byte(i), byte(i >> 8)}
		v := []byte{'v', byte(i)}
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, "test.kv", Options{Engine: format.EngineLSM, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("reopen encrypted lsm: %v", err)
	}
	defer d2.Close()
	for i := 0; i < 200; i++ {
		k := []byte{'k', byte(i), byte(i >> 8)}
		want := []byte{'v', byte(i)}
		got, err := d2.Get(k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get %d = %q, want %q", i, got, want)
		}
	}
}

// TestEncryptionCiphertextOnDisk confirms a distinctive value written to an encrypted
// database does not appear in the clear in the main file after a checkpoint, while the
// same value in an unencrypted database does.
func TestEncryptionCiphertextOnDisk(t *testing.T) {
	secret := []byte("TOP-SECRET-NEEDLE-VALUE-1234567890")

	// Encrypted: the needle must not be present in the main file.
	fs := vfs.NewMem()
	d, err := Open(fs, "enc.kv", Options{PageSize: 4096, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("needle"), secret) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if raw := readWholeFile(t, fs, "enc.kv"); bytes.Contains(raw, secret) {
		t.Fatal("plaintext value found in the encrypted main file")
	}

	// Unencrypted control: the same value is plainly present.
	fs2 := vfs.NewMem()
	p, err := Open(fs2, "plain.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("create plain: %v", err)
	}
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("needle"), secret) }); err != nil {
		t.Fatalf("write plain: %v", err)
	}
	if err := p.Checkpoint(); err != nil {
		t.Fatalf("checkpoint plain: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close plain: %v", err)
	}
	if raw := readWholeFile(t, fs2, "plain.kv"); !bytes.Contains(raw, secret) {
		t.Fatal("sanity check failed: value not found in the unencrypted file")
	}
}

// TestEncryptionReservedTrailer confirms a fresh encrypted file widens its per-page
// reserved trailer to cover the AEAD envelope on top of the checksum, and sets the
// encryption header flag.
func TestEncryptionReservedTrailer(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted: %v", err)
	}
	defer d.Close()
	h := d.pgr.Header()
	if h.Flags&format.FlagEncryption == 0 {
		t.Errorf("encrypted file missing FlagEncryption")
	}
	wantReserved := h.Checksum.ChecksumSize() + crypto.Overhead
	if int(h.ReservedPerPage) != wantReserved {
		t.Errorf("ReservedPerPage = %d, want %d (checksum %d + envelope %d)",
			h.ReservedPerPage, wantReserved, h.Checksum.ChecksumSize(), crypto.Overhead)
	}
}

// TestEncryptionWrongKeyRejected confirms reopening an encrypted database with the wrong
// key fails cleanly with crypto.ErrWrongKey rather than returning garbage.
func TestEncryptionWrongKeyRejected(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	wrong := bytes.Repeat([]byte{0x99}, 32)
	_, err = Open(fs, "test.kv", Options{EncryptionKey: wrong})
	if !errors.Is(err, crypto.ErrWrongKey) {
		t.Fatalf("reopen with wrong key = %v, want crypto.ErrWrongKey", err)
	}
}

// TestEncryptionMissingKeyRejected confirms reopening an encrypted database without a key
// fails with pager.ErrKeyRequired rather than serving ciphertext as data.
func TestEncryptionMissingKeyRejected(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := Open(fs, "test.kv", Options{}); !errors.Is(err, pager.ErrKeyRequired) {
		t.Fatalf("reopen without key = %v, want pager.ErrKeyRequired", err)
	}
}

// TestEncryptionKeyOnPlaintextRejected confirms supplying a key to open a database that
// was created unencrypted is refused, so a mismatched key is never silently ignored.
func TestEncryptionKeyOnPlaintextRejected(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("create plain: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := Open(fs, "test.kv", Options{EncryptionKey: encKey}); !errors.Is(err, ErrKeyOnPlaintext) {
		t.Fatalf("open plaintext with key = %v, want ErrKeyOnPlaintext", err)
	}
}

// TestEncryptionWALRecovery confirms a committed-but-uncheckpointed encrypted write survives
// a crash: the data lives only in the WAL when the process dies, so recovery has to decrypt
// the log frames to redo it. It writes every key as its own SyncFull commit with checkpointing
// disabled, abandons the handle, reverts the filesystem to its durable image, then reopens with
// the key and reads every value back.
func TestEncryptionWALRecovery(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Sync: wal.SyncFull, AutoCheckpoint: -1, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted: %v", err)
	}
	const n = 40
	for i := 0; i < n; i++ {
		k := []byte{'k', byte(i)}
		v := []byte{'v', byte(i), byte(i)}
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// No checkpoint and no Close: the committed data lives entirely in the encrypted WAL.
	fs.Crash()

	d2, err := Open(fs, "test.kv", Options{Sync: wal.SyncFull, AutoCheckpoint: -1, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("reopen after crash with key: %v", err)
	}
	defer d2.Close()
	for i := 0; i < n; i++ {
		k := []byte{'k', byte(i)}
		want := []byte{'v', byte(i), byte(i)}
		got, err := d2.Get(k)
		if err != nil {
			t.Fatalf("get %d after recovery: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get %d after recovery = %q, want %q", i, got, want)
		}
	}
}

// TestEncryptionWALCiphertextOnDisk confirms a distinctive value committed to an encrypted
// database but not yet checkpointed does not appear in the clear in the WAL sidecar, while the
// same value does in an unencrypted control. This pins the slice-2 guarantee that the log, not
// just the main file, is sealed.
func TestEncryptionWALCiphertextOnDisk(t *testing.T) {
	secret := []byte("TOP-SECRET-WAL-NEEDLE-0987654321")

	fs := vfs.NewMem()
	d, err := Open(fs, "enc.kv", Options{PageSize: 4096, Sync: wal.SyncFull, AutoCheckpoint: -1, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("needle"), secret) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Deliberately no checkpoint: the value is in the WAL, not the main file.
	if raw := readWholeFile(t, fs, "enc.kv-wal"); bytes.Contains(raw, secret) {
		t.Fatal("plaintext value found in the encrypted WAL")
	}
	_ = d.Close()

	fs2 := vfs.NewMem()
	p, err := Open(fs2, "plain.kv", Options{PageSize: 4096, Sync: wal.SyncFull, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("create plain: %v", err)
	}
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("needle"), secret) }); err != nil {
		t.Fatalf("write plain: %v", err)
	}
	if raw := readWholeFile(t, fs2, "plain.kv-wal"); !bytes.Contains(raw, secret) {
		t.Fatal("sanity check failed: value not found in the unencrypted WAL")
	}
	_ = p.Close()
}

// TestRotateEncryptionKeyLazyCoexistence confirms a lazy rotation: data written and
// checkpointed before the rotation is sealed under the old epoch and stays on disk under it,
// data written after is sealed under the new epoch, and a reopen reads both. This is the
// coexistence property spec 14 §5 promises, where a rotation does not re-encrypt the file.
func TestRotateEncryptionKeyLazyCoexistence(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted: %v", err)
	}
	// Pre-rotation data, folded to the main file under epoch 0.
	for i := 0; i < 30; i++ {
		k := []byte{'o', byte(i)}
		v := []byte{'a', byte(i), byte(i)}
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("pre-write %d: %v", i, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint before rotate: %v", err)
	}

	if err := d.RotateEncryptionKey(); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	// Post-rotation data, sealed under the new epoch.
	for i := 0; i < 30; i++ {
		k := []byte{'n', byte(i)}
		v := []byte{'b', byte(i), byte(i)}
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("post-write %d: %v", i, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after rotate: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The on-disk descriptor now records the new epoch.
	desc, err := pager.ReadDescriptor(fs, "test.kv")
	if err != nil {
		t.Fatalf("read descriptor: %v", err)
	}
	if desc.Epoch != 1 {
		t.Fatalf("descriptor epoch = %d, want 1 after one rotation", desc.Epoch)
	}

	// Reopen with the same key and read both generations: old-epoch and new-epoch pages
	// coexist and both decrypt.
	d2, err := Open(fs, "test.kv", Options{EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("reopen after rotate: %v", err)
	}
	defer d2.Close()
	for i := 0; i < 30; i++ {
		ok, owant := []byte{'o', byte(i)}, []byte{'a', byte(i), byte(i)}
		got, err := d2.Get(ok)
		if err != nil || !bytes.Equal(got, owant) {
			t.Fatalf("old key %d = %q,%v, want %q", i, got, err, owant)
		}
		nk, nwant := []byte{'n', byte(i)}, []byte{'b', byte(i), byte(i)}
		got, err = d2.Get(nk)
		if err != nil || !bytes.Equal(got, nwant) {
			t.Fatalf("new key %d = %q,%v, want %q", i, got, err, nwant)
		}
	}
}

// TestRotateEncryptionKeySurvivesCrash confirms data written under the new epoch but only
// logged to the WAL (not checkpointed) recovers across a crash, so the rotation reaches the
// log as well as the main file.
func TestRotateEncryptionKeySurvivesCrash(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Sync: wal.SyncFull, AutoCheckpoint: -1, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("before"), []byte("0")) }); err != nil {
		t.Fatalf("pre-write: %v", err)
	}
	if err := d.RotateEncryptionKey(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	// Written after the rotation, lives only in the WAL under the new epoch.
	for i := 0; i < 20; i++ {
		k := []byte{'p', byte(i)}
		v := []byte{'q', byte(i)}
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("post-write %d: %v", i, err)
		}
	}
	fs.Crash()

	d2, err := Open(fs, "test.kv", Options{Sync: wal.SyncFull, AutoCheckpoint: -1, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer d2.Close()
	if got, err := d2.Get([]byte("before")); err != nil || !bytes.Equal(got, []byte("0")) {
		t.Fatalf("pre-rotation key = %q,%v, want 0", got, err)
	}
	for i := 0; i < 20; i++ {
		k := []byte{'p', byte(i)}
		want := []byte{'q', byte(i)}
		got, err := d2.Get(k)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("post-rotation key %d = %q,%v, want %q", i, got, err, want)
		}
	}
}

// TestRotateEncryptionKeyRejectsPlaintext confirms a rotation on an unencrypted database is
// refused with ErrNotEncrypted rather than silently doing nothing.
func TestRotateEncryptionKeyRejectsPlaintext(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("create plain: %v", err)
	}
	defer d.Close()
	if err := d.RotateEncryptionKey(); !errors.Is(err, ErrNotEncrypted) {
		t.Fatalf("rotate on plaintext = %v, want ErrNotEncrypted", err)
	}
}

// TestRotateEncryptionKeyWrongKeyAfterReopen confirms a rotated database still rejects the
// wrong key cleanly: the verification tag is re-sealed under the new epoch, so a wrong key
// fails the descriptor check on reopen.
func TestRotateEncryptionKeyWrongKeyAfterReopen(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := d.RotateEncryptionKey(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	wrong := bytes.Repeat([]byte{0x11}, 32)
	if _, err := Open(fs, "test.kv", Options{EncryptionKey: wrong}); !errors.Is(err, crypto.ErrWrongKey) {
		t.Fatalf("reopen rotated with wrong key = %v, want crypto.ErrWrongKey", err)
	}
}

// TestEncryptionShortKeyRejected confirms a key that is not 32 bytes is refused at create.
func TestEncryptionShortKeyRejected(t *testing.T) {
	fs := vfs.NewMem()
	if _, err := Open(fs, "test.kv", Options{PageSize: 4096, EncryptionKey: []byte("short")}); !errors.Is(err, crypto.ErrKeySize) {
		t.Fatalf("create with short key = %v, want crypto.ErrKeySize", err)
	}
}
