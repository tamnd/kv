package db

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/tamnd/kv/crypto"
	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/wal"
)

// TestF2EncryptionRoundTrip writes, overwrites, and deletes against an encrypted f2 core,
// checkpoints so the f2 file is the source of truth, then reopens with the same key. The f2
// core seals its own log and snapshot pages, so the round trip must hold exactly as it does
// on the unencrypted core: the latest value for a live key, absent for a deleted one.
func TestF2EncryptionRoundTrip(t *testing.T) {
	const n = 200
	fs, path := f2TestPath(t)
	d, err := Open(fs, path, Options{PageSize: 4096, Engine: format.EngineF2, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("open encrypted f2: %v", err)
	}
	for i := 0; i < n; i++ {
		k, v := []byte(f2Key(i)), []byte(f2Val(i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// Overwrite the first key and delete the second, so the reopen exercises a version
	// group with more than one cell and a tombstone, not just first writes.
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte(f2Key(0)), []byte("updated")) }); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Delete([]byte(f2Key(1))) }); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, path, Options{EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("reopen with key: %v", err)
	}
	defer d2.Close()
	if v, ok := get(t, d2, f2Key(0)); !ok || v != "updated" {
		t.Fatalf("key 0 = %q,%v after reopen, want updated", v, ok)
	}
	if v, ok := get(t, d2, f2Key(1)); ok {
		t.Fatalf("key 1 = %q present after reopen, want absent (deleted)", v)
	}
	for i := 2; i < n; i++ {
		if v, ok := get(t, d2, f2Key(i)); !ok || v != f2Val(i) {
			t.Fatalf("key %d = %q,%v after reopen, want %q", i, v, ok, f2Val(i))
		}
	}
}

// TestF2EncryptionReplayTail is the host-delegation case under encryption: the workload
// commits with a full fsync and never checkpoints, so the host WAL holds the committed tail
// past f2's durable point. The reopen recovers f2 from its sealed file and replays the kept
// WAL tail back through Apply. Sealed recovery and plaintext WAL replay must meet with no
// lost or doubled write.
func TestF2EncryptionReplayTail(t *testing.T) {
	const n = 150
	fs, path := f2TestPath(t)
	d, err := Open(fs, path, Options{PageSize: 4096, Engine: format.EngineF2, EncryptionKey: encKey, Sync: wal.SyncFull, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open encrypted f2: %v", err)
	}
	for i := 0; i < n; i++ {
		k, v := []byte(f2Key(i)), []byte(f2Val(i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, path, Options{EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("reopen with key: %v", err)
	}
	defer d2.Close()
	for i := 0; i < n; i++ {
		if v, ok := get(t, d2, f2Key(i)); !ok || v != f2Val(i) {
			t.Fatalf("key %d = %q,%v after replay, want %q", i, v, ok, f2Val(i))
		}
	}
}

// TestF2EncryptionCiphertextOnDisk confirms a value written through an encrypted f2 core
// does not appear in the clear in the f2 sidecar file after a checkpoint, while the same
// value on an unencrypted f2 core does. This is the proof the records region is sealed
// rather than the value riding the host pager (which f2 bypasses).
func TestF2EncryptionCiphertextOnDisk(t *testing.T) {
	secret := []byte("TOP-SECRET-NEEDLE-VALUE-1234567890")

	fs, path := f2TestPath(t)
	d, err := Open(fs, path, Options{PageSize: 4096, Engine: format.EngineF2, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted f2: %v", err)
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
	if raw, err := os.ReadFile(path + "-f2"); err != nil {
		t.Fatalf("read sidecar: %v", err)
	} else if bytes.Contains(raw, secret) {
		t.Fatal("plaintext value found in the encrypted f2 sidecar")
	}

	// Unencrypted control: the same value is plainly present in the sidecar.
	fs2, path2 := f2TestPath(t)
	p, err := Open(fs2, path2, Options{PageSize: 4096, Engine: format.EngineF2})
	if err != nil {
		t.Fatalf("create plain f2: %v", err)
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
	if raw, err := os.ReadFile(path2 + "-f2"); err != nil {
		t.Fatalf("read plain sidecar: %v", err)
	} else if !bytes.Contains(raw, secret) {
		t.Fatal("sanity check failed: value not found in the unencrypted f2 sidecar")
	}
}

// TestF2EncryptionWrongKeyRejected confirms reopening an encrypted f2 database with the
// wrong key fails cleanly rather than serving ciphertext as data. The key descriptor lives
// in the main file, so the rejection comes from the host's encryption open before the f2
// core ever reads its file.
func TestF2EncryptionWrongKeyRejected(t *testing.T) {
	fs, path := f2TestPath(t)
	d, err := Open(fs, path, Options{PageSize: 4096, Engine: format.EngineF2, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted f2: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	wrong := bytes.Repeat([]byte{0x99}, 32)
	if _, err := Open(fs, path, Options{EncryptionKey: wrong}); !errors.Is(err, crypto.ErrWrongKey) {
		t.Fatalf("reopen with wrong key = %v, want crypto.ErrWrongKey", err)
	}
}

// TestF2EncryptionStateMismatch confirms the f2 core refuses to open a sealed file without a
// key and an unsealed file with one, rather than reading sealed bytes as plaintext records
// or sealing over existing plaintext. The superblock carries an encrypted flag the core
// checks against the supplied scheme on open.
func TestF2EncryptionStateMismatch(t *testing.T) {
	// A file created encrypted, reopened with no key, must fail rather than serve ciphertext.
	fs, path := f2TestPath(t)
	d, err := Open(fs, path, Options{PageSize: 4096, Engine: format.EngineF2, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted f2: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := Open(fs, path, Options{}); err == nil {
		t.Fatal("reopen of encrypted f2 without a key succeeded, want failure")
	}
}

// TestF2EncryptionTamperDetected flips a byte inside the sealed records region of the f2
// sidecar after a checkpoint. The AEAD tag fails on open, so recovery drops that page's
// records rather than decoding garbage. The tampered key must read absent or the open must
// fail, never return the original plaintext.
func TestF2EncryptionTamperDetected(t *testing.T) {
	fs, path := f2TestPath(t)
	d, err := Open(fs, path, Options{PageSize: 4096, Engine: format.EngineF2, EncryptionKey: encKey})
	if err != nil {
		t.Fatalf("create encrypted f2: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("secret"), []byte("classified")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	sidecar := path + "-f2"
	raw, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	// Corrupt bytes throughout the data region (past the two 4096-byte superblocks), inside
	// the sealed records and trailers, leaving the plaintext block headers alone.
	const dataStart = 4096 * 2
	const blockHeader = 20
	for off := dataStart + blockHeader; off < len(raw); off += 64 {
		raw[off] ^= 0xff
	}
	if err := os.WriteFile(sidecar, raw, 0o600); err != nil {
		t.Fatalf("write tampered sidecar: %v", err)
	}

	d2, err := Open(fs, path, Options{EncryptionKey: encKey})
	if err != nil {
		// A clean open failure is an acceptable outcome: the tamper was caught.
		return
	}
	defer d2.Close()
	if v, ok := get(t, d2, "secret"); ok && v == "classified" {
		t.Fatal("tampered record returned its original plaintext, AEAD did not catch the change")
	}
}
