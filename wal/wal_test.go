package wal

import (
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// makeBatch builds and serializes a one-version batch of sets.
func makeBatch(version uint64, kv map[string]string) []byte {
	b := engine.NewWriteBatch(version)
	for k, v := range kv {
		b.Set([]byte(k), []byte(v))
	}
	return b.Encode()
}

// recoverWAL runs the durable-tail scan against an open memfs WAL file.
func recoverWAL(t *testing.T, fs *vfs.Mem, path string) RecoverResult {
	t.Helper()
	f, err := fs.Open(path, vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer f.Close()
	size, _ := f.Size()
	res, err := Recover(f.ReadAt, size)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	return res
}

func TestCommitThenRecover(t *testing.T) {
	fs := vfs.NewMem()
	w, err := Create(fs, "db.kv-wal", Options{PageSize: 4096, Sync: SyncFull, Salt: 0xABCD})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Two committed batches.
	w.LogBatch(10, makeBatch(10, map[string]string{"a": "1"}))
	if _, err := w.Commit(10); err != nil {
		t.Fatalf("commit 10: %v", err)
	}
	w.LogBatch(20, makeBatch(20, map[string]string{"b": "2"}))
	commitLSN, err := w.Commit(20)
	if err != nil {
		t.Fatalf("commit 20: %v", err)
	}
	if commitLSN == 0 {
		t.Fatalf("commit LSN should be non-zero")
	}

	res := recoverWAL(t, fs, "db.kv-wal")
	if len(res.Batches) != 2 {
		t.Fatalf("recovered %d batches, want 2", len(res.Batches))
	}
	if res.Batches[0].Version != 10 || res.Batches[1].Version != 20 {
		t.Fatalf("versions = %d, %d; want 10, 20", res.Batches[0].Version, res.Batches[1].Version)
	}
	if res.TornTail {
		t.Fatalf("clean WAL should not report a torn tail")
	}
	// The decoded batch must round-trip.
	b, err := engine.DecodeBatch(res.Batches[0].Encoded)
	if err != nil {
		t.Fatalf("decode batch: %v", err)
	}
	if b.Version() != 10 || b.Len() != 1 {
		t.Fatalf("decoded batch version %d len %d", b.Version(), b.Len())
	}
}

// TestUncommittedTailDropped logs a batch with no commit frame; recovery must
// discard it.
func TestUncommittedTailDropped(t *testing.T) {
	fs := vfs.NewMem()
	w, _ := Create(fs, "db.kv-wal", Options{PageSize: 4096, Sync: SyncFull})
	w.LogBatch(1, makeBatch(1, map[string]string{"x": "1"}))
	w.Commit(1)
	// A second batch logged but never committed (crash before commit).
	w.LogBatch(2, makeBatch(2, map[string]string{"y": "2"}))

	res := recoverWAL(t, fs, "db.kv-wal")
	if len(res.Batches) != 1 || res.Batches[0].Version != 1 {
		t.Fatalf("expected only the committed batch, got %d", len(res.Batches))
	}
}

// TestTornTailDetected corrupts a byte in the middle of the log; recovery must
// stop at the corruption and keep only what chained before it.
func TestTornTailDetected(t *testing.T) {
	fs := vfs.NewMem()
	w, _ := Create(fs, "db.kv-wal", Options{PageSize: 4096, Sync: SyncFull, Salt: 7})
	w.LogBatch(1, makeBatch(1, map[string]string{"a": "1"}))
	w.Commit(1)
	w.LogBatch(2, makeBatch(2, map[string]string{"b": "2"}))
	w.Commit(2)

	// Flip a byte inside the second batch's payload region (well past the header
	// and the first frame), simulating a torn write.
	f, _ := fs.Open("db.kv-wal", vfs.OpenReadWrite)
	size, _ := f.Size()
	corruptOff := size - 4
	one := make([]byte, 1)
	f.ReadAt(one, corruptOff)
	one[0] ^= 0xFF
	f.WriteAt(one, corruptOff)
	f.Sync(vfs.SyncFull)
	f.Close()

	res := recoverWAL(t, fs, "db.kv-wal")
	if !res.TornTail {
		t.Fatalf("expected torn tail to be detected")
	}
	// The first committed batch survives; the corrupted second does not.
	if len(res.Batches) != 1 || res.Batches[0].Version != 1 {
		t.Fatalf("after torn tail got %d batches, want 1 (version 1)", len(res.Batches))
	}
}

// TestCrashLosesUnsyncedAtNormal verifies that at SyncNormal a crash before any
// flush loses commits (they were never made durable), while SyncFull keeps them.
func TestCrashDurabilityBySyncLevel(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sync      Sync
		wantAfter int
	}{
		{"full survives crash", SyncFull, 1},
		{"normal loses unsynced", SyncNormal, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := vfs.NewMem()
			w, _ := Create(fs, "db.kv-wal", Options{PageSize: 4096, Sync: tc.sync})
			w.LogBatch(1, makeBatch(1, map[string]string{"a": "1"}))
			w.Commit(1)
			// Power loss: drop everything not yet fsynced.
			fs.Crash()
			res := recoverWAL(t, fs, "db.kv-wal")
			if len(res.Batches) != tc.wantAfter {
				t.Fatalf("%s: recovered %d batches, want %d", tc.name, len(res.Batches), tc.wantAfter)
			}
		})
	}
}

// TestCheckpointRotatesSalt verifies a checkpoint frame records the folded LSN and
// that frames from the previous generation no longer chain under the new salt.
func TestCheckpointRotatesSalt(t *testing.T) {
	fs := vfs.NewMem()
	w, _ := Create(fs, "db.kv-wal", Options{PageSize: 4096, Sync: SyncFull, Salt: 1})
	w.LogBatch(1, makeBatch(1, map[string]string{"a": "1"}))
	folded, _ := w.Commit(1)
	saltBefore := w.Salt()
	if err := w.Checkpointed(folded); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if w.Salt() == saltBefore {
		t.Fatalf("salt did not rotate at checkpoint")
	}
	// Log and commit a fresh batch in the new generation.
	w.LogBatch(2, makeBatch(2, map[string]string{"b": "2"}))
	w.Commit(2)

	res := recoverWAL(t, fs, "db.kv-wal")
	// The WAL was reset to the new generation; only the post-checkpoint batch is
	// present in the live log.
	if len(res.Batches) != 1 || res.Batches[0].Version != 2 {
		t.Fatalf("after checkpoint got %d batches, want 1 (version 2)", len(res.Batches))
	}
	if res.Salt != w.Salt() {
		t.Fatalf("recovered salt %d, want %d", res.Salt, w.Salt())
	}
}

func TestSyncCounts(t *testing.T) {
	fs := vfs.NewMem()
	full, _ := Create(fs, "full.kv-wal", Options{Sync: SyncFull})
	full.LogBatch(1, makeBatch(1, map[string]string{"a": "1"}))
	full.Commit(1)
	if full.Syncs() == 0 {
		t.Fatalf("SyncFull should fsync on commit")
	}

	norm, _ := Create(fs, "norm.kv-wal", Options{Sync: SyncNormal})
	before := norm.Syncs()
	norm.LogBatch(1, makeBatch(1, map[string]string{"a": "1"}))
	norm.Commit(1)
	if norm.Syncs() != before {
		t.Fatalf("SyncNormal should defer the per-commit fsync")
	}
}
