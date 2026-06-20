package db

import (
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// TestLSMFlushReopenFromManifest drives the full durability split through the public
// database. A small memtable size makes most batches flush into on-disk segments, so a
// checkpoint folds those segments and the MANIFEST and advances the durable mark past
// them; the WAL keeps only the unflushed tail. After a clean reopen the bulk of the
// keys can only return through the MANIFEST, since redo replays just the tail past the
// checkpoint boundary, not the flushed prefix.
func TestLSMFlushReopenFromManifest(t *testing.T) {
	const n = 200
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 256})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < n; i++ {
		k, v := []byte(lsmKey(i)), []byte(lsmVal(i))
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

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	for i := 0; i < n; i++ {
		if v, ok := get(t, d2, lsmKey(i)); !ok || v != lsmVal(i) {
			t.Fatalf("key %d = %q,%v after reopen, want %q", i, v, ok, lsmVal(i))
		}
	}
}

// TestLSMManifestVersionedReopen flushes overwrites and deletes into segments, then
// reopens and confirms the restored set resolves each key to its newest visible
// version, so MVCC folding survives a flush-and-reload exactly as it survives a WAL
// replay.
func TestLSMManifestVersionedReopen(t *testing.T) {
	const n = 120
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 256})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < n; i++ {
		k := []byte(lsmKey(i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, []byte("first")) }); err != nil {
			t.Fatalf("write first %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		k := []byte(lsmKey(i))
		if _, err := d.Write(func(b *engine.WriteBatch) {
			if i%4 == 0 {
				b.Delete(k)
			} else {
				b.Set(k, []byte("second"))
			}
		}); err != nil {
			t.Fatalf("write second %d: %v", i, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	for i := 0; i < n; i++ {
		v, ok := get(t, d2, lsmKey(i))
		if i%4 == 0 {
			if ok {
				t.Fatalf("key %d = %q after reopen, want deleted", i, v)
			}
			continue
		}
		if !ok || v != "second" {
			t.Fatalf("key %d = %q,%v after reopen, want second", i, v, ok)
		}
	}
}

// TestLSMFlushCrashRecoversFromWAL models a crash mid-run while flushes are happening:
// a small memtable flushes segments as the workload writes, but nothing is ever
// checkpointed, so the segment and MANIFEST pages never reach the file and the engine
// root is never advanced. The reopen finds an empty MANIFEST and replays the whole WAL
// into a fresh memtable, restoring every key exactly once with no double-applied flush.
func TestLSMFlushCrashRecoversFromWAL(t *testing.T) {
	const n = 150
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 256, Sync: wal.SyncFull, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < n; i++ {
		k, v := []byte(lsmKey(i)), []byte(lsmVal(i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// Crash: no checkpoint, no Close. The flushed segments are dirty pages that never
	// reached the file; the WAL holds every batch.

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer d2.Close()
	for i := 0; i < n; i++ {
		if v, ok := get(t, d2, lsmKey(i)); !ok || v != lsmVal(i) {
			t.Fatalf("key %d = %q,%v after crash recovery, want %q", i, v, ok, lsmVal(i))
		}
	}
}
