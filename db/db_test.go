package db

import (
	"errors"
	"fmt"
	"testing"
	"time"

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

// TestSyncBarrierSurvivesCrashWithoutCheckpoint pins the intermediate durability
// level (perf/06 F2): at SyncBarrier every commit issues a per-commit barrier sync,
// so committed data survives a crash even with no checkpoint, unlike SyncNormal
// which defers the sync and loses the uncheckpointed tail. Each serial commit must
// advance the fsync counter, and the data must replay from the WAL after the crash.
func TestSyncBarrierSurvivesCrashWithoutCheckpoint(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Sync: wal.SyncBarrier, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	const n = 8
	var prev uint64
	for i := range n {
		key := fmt.Sprintf("k%02d", i)
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte(key), []byte("v")) }); err != nil {
			t.Fatalf("write %q: %v", key, err)
		}
		// A serial committer is its own group, so each commit forces one barrier sync.
		if got := d.Stats().Syncs; got <= prev {
			t.Fatalf("commit %d did not sync: syncs stayed at %d", i, got)
		} else {
			prev = got
		}
	}

	// No checkpoint ran, so the main file holds none of the engine pages; only the
	// per-commit barrier synced WAL is durable across the crash.
	fs.Crash()

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer d2.Close()
	for i := range n {
		key := fmt.Sprintf("k%02d", i)
		if v, ok := get(t, d2, key); !ok || v != "v" {
			t.Fatalf("%s = %q,%v after SyncBarrier crash, want v (replayed from WAL)", key, v, ok)
		}
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

// TestFatalSyncFencesAndRecovers is the fsyncgate proof (spec 07 §6, spec 23 §4). A
// commit whose WAL fsync fails must not be acknowledged, must fence the database against
// further writes until reopen, and must be absent after a crash-and-recover, while every
// earlier committed write survives. Reads keep working throughout.
func TestFatalSyncFencesAndRecovers(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Sync: wal.SyncFull, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// A clean commit that must survive everything below.
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v1")) }); err != nil {
		t.Fatalf("first write: %v", err)
	}

	// Arm the next fsync to fail, then attempt a commit whose durability cannot be
	// confirmed. The commit sync is the first sync after arming, so it is the one that
	// faults.
	fs.SetSyncFault(1)
	_, err = d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v2")) })
	if !errors.Is(err, ErrFatalSync) {
		t.Fatalf("write under sync fault = %v, want ErrFatalSync", err)
	}

	// The failed commit must not be acknowledged: a live read still sees v1, because
	// Apply never ran for the un-synced batch.
	if v, ok := get(t, d, "k"); !ok || v != "v1" {
		t.Fatalf("k = %q,%v after failed commit, want v1 (commit not acknowledged)", v, ok)
	}

	// The database is now fenced: a later write is refused even once the underlying fault
	// is cleared, because the fence, not the fault, is what bites.
	fs.SetSyncFault(0)
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("z"), []byte("zz")) }); !errors.Is(err, ErrFatalSync) {
		t.Fatalf("write after fence = %v, want ErrFatalSync", err)
	}

	// Power loss after the failed sync drops the un-synced bytes.
	fs.Crash()

	// Recovery runs against the durable log: v1 survives, the failed v2 is gone, and the
	// reopened database accepts writes again.
	d2, err := Open(fs, "test.kv", Options{AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if v, ok := get(t, d2, "k"); !ok || v != "v1" {
		t.Fatalf("k = %q,%v after recovery, want v1 (only the acked commit survives)", v, ok)
	}
	if v, ok := get(t, d2, "z"); ok {
		t.Fatalf("z = %q present after recovery, want absent (it was written behind the fence)", v)
	}
	if _, err := d2.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v3")) }); err != nil {
		t.Fatalf("post-recovery write: %v", err)
	}
	if v, ok := get(t, d2, "k"); !ok || v != "v3" {
		t.Fatalf("k = %q,%v after post-recovery write, want v3", v, ok)
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

// TestAutoCheckpointBoundsWAL drives sustained commits against a small auto-checkpoint
// threshold and asserts the background worker folds the log so the backlog stays
// bounded instead of growing with every write (spec 09 §1.3). Without the worker the
// backlog would climb to one frame per commit; with it the backlog settles below the
// threshold once writing stops and the last signaled checkpoint catches up.
func TestAutoCheckpointBoundsWAL(t *testing.T) {
	fs := vfs.NewMem()
	const threshold = 8
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, AutoCheckpoint: threshold})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const writes = 300
	for i := 0; i < writes; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, []byte("v")) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// The checkpointer runs asynchronously, so poll until the backlog drains below the
	// threshold. A failure to converge means the worker never kept up -- a real bug, not
	// flakiness -- so the deadline is generous.
	deadline := time.Now().Add(5 * time.Second)
	var last uint64
	for time.Now().Before(deadline) {
		last = d.Stats().WALBacklog
		if last < threshold {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if last >= threshold {
		t.Fatalf("WAL backlog %d never fell below threshold %d after %d writes", last, threshold, writes)
	}

	// The folded data must still read back correctly: auto-checkpoint is durability, not
	// data loss.
	for _, i := range []int{0, writes / 2, writes - 1} {
		k := fmt.Sprintf("k%04d", i)
		if v, ok := get(t, d, k); !ok || v != "v" {
			t.Fatalf("%s = %q,%v after auto-checkpoint, want v,true", k, v, ok)
		}
	}
}

// TestAutoCheckpointDisabledLetsWALGrow is the negative control: with auto-checkpointing
// disabled the backlog grows with the writes and no background worker folds it, so the
// feature's effect in the test above is attributable to the worker and not to some other
// path checkpointing on its own.
func TestAutoCheckpointDisabledLetsWALGrow(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const writes = 50
	for i := 0; i < writes; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, []byte("v")) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if backlog := d.Stats().WALBacklog; backlog < uint64(writes) {
		t.Fatalf("WAL backlog %d with auto-checkpoint disabled, want >= %d", backlog, writes)
	}
}
