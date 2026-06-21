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

// TestEncryptionShortKeyRejected confirms a key that is not 32 bytes is refused at create.
func TestEncryptionShortKeyRejected(t *testing.T) {
	fs := vfs.NewMem()
	if _, err := Open(fs, "test.kv", Options{PageSize: 4096, EncryptionKey: []byte("short")}); !errors.Is(err, crypto.ErrKeySize) {
		t.Fatalf("create with short key = %v, want crypto.ErrKeySize", err)
	}
}
