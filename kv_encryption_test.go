package kv_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv"
)

// encKey is a fixed 32-byte master key for the facade-level encryption tests.
var encKey = bytes.Repeat([]byte{0x37}, 32)

// TestWithEncryptionKeyRoundTrip drives encryption end to end through the public surface:
// create with a key, write through Update, close, reopen with the same key, and read the
// value back. It is the contract a caller sees, with no db or pager types in sight.
func TestWithEncryptionKeyRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.kv")

	d, err := kv.Open(path, kv.WithEncryptionKey(encKey))
	if err != nil {
		t.Fatalf("open encrypted: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.Set([]byte("hello"), []byte("ciphered-world"))
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := kv.Open(path, kv.WithEncryptionKey(encKey))
	if err != nil {
		t.Fatalf("reopen with key: %v", err)
	}
	defer d2.Close()
	if err := d2.View(func(txn *kv.Txn) error {
		v, err := txn.Get([]byte("hello"))
		if err != nil {
			return err
		}
		if string(v) != "ciphered-world" {
			t.Fatalf("get = %q, want ciphered-world", v)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

// TestWithEncryptionKeyWrongKey checks the facade surfaces a clean kv.ErrWrongKey when a
// database is reopened under the wrong key, rather than returning garbage or a generic error.
func TestWithEncryptionKeyWrongKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.kv")

	d, err := kv.Open(path, kv.WithEncryptionKey(encKey))
	if err != nil {
		t.Fatalf("open encrypted: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.Set([]byte("k"), []byte("v"))
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	wrong := bytes.Repeat([]byte{0x99}, 32)
	if _, err := kv.Open(path, kv.WithEncryptionKey(wrong)); !errors.Is(err, kv.ErrWrongKey) {
		t.Fatalf("reopen with wrong key = %v, want kv.ErrWrongKey", err)
	}
}

// TestWithEncryptionKeyMissingKey checks reopening an encrypted database without a key fails
// with kv.ErrEncryptionKeyRequired, the loud refusal that keeps ciphertext from being served.
func TestWithEncryptionKeyMissingKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.kv")

	d, err := kv.Open(path, kv.WithEncryptionKey(encKey))
	if err != nil {
		t.Fatalf("open encrypted: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := kv.Open(path); !errors.Is(err, kv.ErrEncryptionKeyRequired) {
		t.Fatalf("reopen without key = %v, want kv.ErrEncryptionKeyRequired", err)
	}
}

// TestWithEncryptionKeyOnPlaintext checks offering a key to a database that was created
// unencrypted is refused with kv.ErrKeyOnPlaintext, so a mismatched key is never ignored.
func TestWithEncryptionKeyOnPlaintext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.kv")

	d, err := kv.Open(path)
	if err != nil {
		t.Fatalf("open plain: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := kv.Open(path, kv.WithEncryptionKey(encKey)); !errors.Is(err, kv.ErrKeyOnPlaintext) {
		t.Fatalf("open plaintext with key = %v, want kv.ErrKeyOnPlaintext", err)
	}
}

// TestRotateEncryptionKeyRoundTrip drives a key rotation through the public surface: write,
// rotate, write again, reopen with the same key, and read both generations. The key the
// caller holds does not change; the rotation bumps the internal epoch off the same master.
func TestRotateEncryptionKeyRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.kv")

	d, err := kv.Open(path, kv.WithEncryptionKey(encKey))
	if err != nil {
		t.Fatalf("open encrypted: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.Set([]byte("old"), []byte("before-rotate"))
	}); err != nil {
		t.Fatalf("pre-rotate update: %v", err)
	}
	if err := d.RotateEncryptionKey(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.Set([]byte("new"), []byte("after-rotate"))
	}); err != nil {
		t.Fatalf("post-rotate update: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := kv.Open(path, kv.WithEncryptionKey(encKey))
	if err != nil {
		t.Fatalf("reopen with key: %v", err)
	}
	defer d2.Close()
	if err := d2.View(func(txn *kv.Txn) error {
		old, err := txn.Get([]byte("old"))
		if err != nil || string(old) != "before-rotate" {
			t.Fatalf("old = %q,%v, want before-rotate", old, err)
		}
		nw, err := txn.Get([]byte("new"))
		if err != nil || string(nw) != "after-rotate" {
			t.Fatalf("new = %q,%v, want after-rotate", nw, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

// TestRotateEncryptionKeyOnPlaintext checks a rotation on a database created without a key is
// refused with kv.ErrNotEncrypted, so the call fails loudly rather than appearing to succeed.
func TestRotateEncryptionKeyOnPlaintext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.kv")

	d, err := kv.Open(path)
	if err != nil {
		t.Fatalf("open plain: %v", err)
	}
	defer d.Close()
	if err := d.RotateEncryptionKey(); !errors.Is(err, kv.ErrNotEncrypted) {
		t.Fatalf("rotate on plaintext = %v, want kv.ErrNotEncrypted", err)
	}
}

// TestWithEncryptionKeyCiphertextOnDisk checks a value written through the public surface to
// an encrypted database is not present in the clear in the on-disk file after a checkpoint.
func TestWithEncryptionKeyCiphertextOnDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.kv")
	secret := []byte("FACADE-SECRET-NEEDLE-5544332211")

	d, err := kv.Open(path, kv.WithEncryptionKey(encKey))
	if err != nil {
		t.Fatalf("open encrypted: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.Set([]byte("needle"), secret)
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if bytes.Contains(raw, secret) {
		t.Fatal("plaintext value found in the encrypted file")
	}
}
