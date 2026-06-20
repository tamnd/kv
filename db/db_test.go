package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// concatMerge appends the operand to the existing value, a deterministic resolver
// shared by the tests that exercise merges.
func concatMerge(existing, operand []byte) []byte {
	return append(append([]byte(nil), existing...), operand...)
}

func get(t *testing.T, d *DB, key string) (string, bool) {
	t.Helper()
	v, err := d.Get([]byte(key))
	if err == engine.ErrNotFound {
		return "", false
	}
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	return string(v), true
}

// TestWriteReadReopenClean writes, checkpoints, closes cleanly, reopens, and reads
// the data back -- the no-crash round trip through a folded main file and an empty
// WAL.
func TestWriteReadReopenClean(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) {
		b.Set([]byte("a"), []byte("1"))
		b.Set([]byte("b"), []byte("2"))
	}); err != nil {
		t.Fatalf("write: %v", err)
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
	if v, ok := get(t, d2, "a"); !ok || v != "1" {
		t.Fatalf("a = %q,%v after clean reopen", v, ok)
	}
	if v, ok := get(t, d2, "b"); !ok || v != "2" {
		t.Fatalf("b = %q,%v after clean reopen", v, ok)
	}
}

// TestRecoverAfterCrashNoCheckpoint is the M1 exit criterion: committed batches at
// SyncFull survive a crash with no checkpoint, recovered by replaying the WAL. The
// engine's dirty pages were never folded into the main file, so this proves redo
// from the log reconstructs the state.
func TestRecoverAfterCrashNoCheckpoint(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Sync: wal.SyncFull})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v1")) })
	d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v2")) })
	d.Write(func(b *engine.WriteBatch) { b.Set([]byte("other"), []byte("x")) })

	// Power loss: drop everything not fsynced. No checkpoint ran, so the main file
	// holds none of the engine pages -- only the WAL is durable.
	fs.Crash()

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer d2.Close()
	if v, ok := get(t, d2, "k"); !ok || v != "v2" {
		t.Fatalf("k = %q,%v after crash, want v2 (newest version wins)", v, ok)
	}
	if v, ok := get(t, d2, "other"); !ok || v != "x" {
		t.Fatalf("other = %q,%v after crash", v, ok)
	}
	// The version counter must resume past the redone versions, so a fresh write
	// sorts as newer than the recovered data.
	if _, err := d2.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v3")) }); err != nil {
		t.Fatalf("post-recovery write: %v", err)
	}
	if v, ok := get(t, d2, "k"); !ok || v != "v3" {
		t.Fatalf("k = %q,%v after post-recovery write, want v3", v, ok)
	}
}

// TestCrashAfterCheckpoint checkpoints some writes (folding them into the main file
// and resetting the WAL), then writes more and crashes. Recovery must combine the
// folded data with the redone post-checkpoint tail.
func TestCrashAfterCheckpoint(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	d.Write(func(b *engine.WriteBatch) { b.Set([]byte("folded"), []byte("A")) })
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	// These land in the new WAL generation, past the checkpoint boundary.
	d.Write(func(b *engine.WriteBatch) { b.Set([]byte("tail"), []byte("B")) })
	d.Write(func(b *engine.WriteBatch) { b.Set([]byte("folded"), []byte("A2")) })

	fs.Crash()

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if v, ok := get(t, d2, "folded"); !ok || v != "A2" {
		t.Fatalf("folded = %q,%v, want A2 (checkpoint base + redone overwrite)", v, ok)
	}
	if v, ok := get(t, d2, "tail"); !ok || v != "B" {
		t.Fatalf("tail = %q,%v, want B (redone past checkpoint)", v, ok)
	}
}

// TestCheckpointFoldsRedoneVersions is the regression for the durable-version bug: a
// checkpoint that runs in a process which rebuilt its state from the WAL (rather than
// from live commits) must persist the recovered commit version, not a stale zero. A
// live commit updates the header's version through applyCommitted, but redo applies
// batches straight through eng.Apply and never touches the header, so the checkpoint
// must take the version from the oracle. Without that, the folded pages persist under
// version 0 and the next open reads a snapshot below every commit, finding the data
// invisible even though it is physically present (spec 08 §5, spec 10 §1).
func TestCheckpointFoldsRedoneVersions(t *testing.T) {
	fs := vfs.NewMem()
	// Several commits across separate opens, none checkpointed: the data lives only in
	// the WAL and is replayed on each open, exactly as repeated CLI invocations do.
	for i := 0; i < 5; i++ {
		d, err := Open(fs, "test.kv", Options{PageSize: 4096})
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if _, err := d.Write(func(b *engine.WriteBatch) {
			b.Set([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if err := d.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}

	// A fresh process rebuilds from the WAL, then checkpoints: this is the path the
	// header-version bug lived on.
	d, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen for checkpoint: %v", err)
	}
	if got := d.Version(); got != 5 {
		t.Fatalf("recovered version = %d, want 5", got)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen against the folded main file with an empty WAL: every key must be visible
	// and the version must resume at 5.
	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen after checkpoint: %v", err)
	}
	defer d2.Close()
	if got := d2.Version(); got != 5 {
		t.Fatalf("post-checkpoint version = %d, want 5", got)
	}
	for i := 0; i < 5; i++ {
		if v, ok := get(t, d2, fmt.Sprintf("k%d", i)); !ok || v != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%d = %q,%v after checkpoint+reopen, want v%d", i, v, ok, i)
		}
	}
}

// TestSyncNormalLosesUncheckpointed verifies that at SyncNormal a crash before any
// checkpoint loses the commits (they were never fsynced), with no corruption.
func TestSyncNormalLosesUncheckpointed(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Sync: wal.SyncNormal})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) })
	fs.Crash()

	d2, err := Open(fs, "test.kv", Options{Sync: wal.SyncNormal})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if v, ok := get(t, d2, "k"); ok {
		t.Fatalf("k = %q present after SyncNormal crash, want lost", v)
	}
}

// TestRecoverLargeBatchWithSplits redoes a batch large enough to split the B-tree
// across many pages, proving recovery drives the same split path normal writes do.
func TestRecoverLargeBatchWithSplits(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 512, CacheFrames: 16})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	const n = 400
	if _, err := d.Write(func(b *engine.WriteBatch) {
		for i := 0; i < n; i++ {
			b.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i)))
		}
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	fs.Crash()

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%05d", i)
		if v, ok := get(t, d2, k); !ok || v != fmt.Sprintf("val%05d", i) {
			t.Fatalf("after recovery %s = %q,%v", k, v, ok)
		}
	}
}

// TestRepeatedCrashRecoveryIsIdempotent recovers, then crashes again before any
// checkpoint, and recovers a second time. Redo must be safe to run twice.
func TestRepeatedCrashRecoveryIsIdempotent(t *testing.T) {
	fs := vfs.NewMem()
	d, _ := Open(fs, "test.kv", Options{PageSize: 4096})
	d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) })
	d.Write(func(b *engine.WriteBatch) { b.Merge([]byte("m"), []byte("a")) })
	d.Write(func(b *engine.WriteBatch) { b.Merge([]byte("m"), []byte("b")) })
	fs.Crash()

	for round := 0; round < 3; round++ {
		dr, err := Open(fs, "test.kv", Options{Merge: concatMerge})
		if err != nil {
			t.Fatalf("round %d open: %v", round, err)
		}
		if v, ok := get(t, dr, "k"); !ok || v != "v" {
			t.Fatalf("round %d k = %q,%v", round, v, ok)
		}
		if v, ok := get(t, dr, "m"); !ok || v != "ab" {
			t.Fatalf("round %d m = %q,%v, want ab (folded merges)", round, v, ok)
		}
		dr.Close()
		// Crash again without checkpointing: the WAL is unchanged, so the next round
		// redoes the same frames.
		fs.Crash()
	}
}

// TestMergeAndDeleteResolution drives merges and a delete through the live engine
// (no crash) to confirm the DB read path resolves versions like the oracle.
func TestMergeAndDeleteResolution(t *testing.T) {
	fs := vfs.NewMem()
	d, _ := Open(fs, "test.kv", Options{PageSize: 4096, Merge: concatMerge})
	defer d.Close()
	d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("base")) })
	d.Write(func(b *engine.WriteBatch) { b.Merge([]byte("k"), []byte("+1")) })
	d.Write(func(b *engine.WriteBatch) { b.Merge([]byte("k"), []byte("+2")) })
	if v, ok := get(t, d, "k"); !ok || v != "base+1+2" {
		t.Fatalf("k = %q,%v, want base+1+2", v, ok)
	}
	d.Write(func(b *engine.WriteBatch) { b.Delete([]byte("k")) })
	if v, ok := get(t, d, "k"); ok {
		t.Fatalf("k = %q present after delete", v)
	}
}
