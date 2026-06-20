package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// crashWorkloadKeys is the number of single-key commits the durable-prefix harness drives.
// Each commit writes one more key in ascending order, so the recovered key set after a crash
// must be a contiguous prefix of these.
const crashWorkloadKeys = 40

// crashKey and crashVal name the i-th commit's key and value. Keys ascend with i, which is
// what lets the harness assert the recovered set is a hole-free prefix.
func crashKey(i int) string { return fmt.Sprintf("key%04d", i) }
func crashVal(i int) string { return fmt.Sprintf("val%04d", i) }

// runCrashWorkload opens a database on fs and commits crashWorkloadKeys keys, each as its own
// SyncFull transaction so every commit forces an fsync. It deliberately leaves the database
// open and never checkpoints: the data lives entirely in the WAL, so recovery has to redo it
// from the log, and the auto-checkpointer is disabled so no background goroutine writes after
// a simulated crash. The caller arms the crash on fs before calling.
func runCrashWorkload(t *testing.T, fs *vfs.Mem) {
	t.Helper()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Sync: wal.SyncFull, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < crashWorkloadKeys; i++ {
		k, v := []byte(crashKey(i)), []byte(crashVal(i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	// Intentionally no Close: closing would checkpoint and sync a clean shutdown, which is
	// the opposite of the crash this harness models.
}

// writeKeysOpen opens a database on fs, commits keys 0..k-1 each as its own SyncFull
// transaction, and returns the still-open handle so the caller can drive a checkpoint
// before crashing. Auto-checkpointing is disabled so no background goroutine writes after
// a simulated crash, and the handle is intentionally not closed.
func writeKeysOpen(t *testing.T, fs *vfs.Mem, k int) *DB {
	t.Helper()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Sync: wal.SyncFull, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < k; i++ {
		key, val := []byte(crashKey(i)), []byte(crashVal(i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(key, val) }); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	return d
}

// TestInterruptedCheckpointRecovers crashes in the window the strict checkpoint ordering
// exists to protect (spec 08 §5, spec 23 §4): after the main file has been folded and
// fsynced with the new checkpoint boundary, but before the WAL has been reset. On reopen
// the main file already holds the folded pages while the WAL still holds every frame, and
// recovery must produce exactly the committed state with no loss and no double application.
//
// The crash is pinned to that window deterministically: a probe run measures how many syncs
// the writes perform, so the checkpoint's main-file fsync is the very next sync, and the
// freeze fires there -- after the fold is durable, before Checkpointed resets the log.
func TestInterruptedCheckpointRecovers(t *testing.T) {
	const k = 20

	probe := vfs.NewMem()
	writeKeysOpen(t, probe, k)
	mainFsync := probe.SyncCount() + 1 // the checkpoint's main-file fsync is the next sync

	fs := vfs.NewMem()
	fs.CrashAfterSync(mainFsync)
	d := writeKeysOpen(t, fs, k)
	// Checkpoint folds the WAL and fsyncs the main file (the freeze captures state here),
	// then resets the WAL -- but the frozen image is the pre-reset one, so the crash lands
	// in the interrupted-checkpoint window.
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	fs.Crash()

	d2, err := Open(fs, "test.kv", Options{AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("reopen after interrupted checkpoint: %v", err)
	}
	defer d2.Close()

	rep, err := d2.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("tree unsound after interrupted checkpoint: %+v", rep.Problems)
	}
	if rep.Keys != k {
		t.Fatalf("recovered %d keys after interrupted checkpoint, want %d", rep.Keys, k)
	}
	for i := 0; i < k; i++ {
		if v, ok := get(t, d2, crashKey(i)); !ok || v != crashVal(i) {
			t.Fatalf("key %d = %q,%v after interrupted checkpoint, want %q", i, v, ok, crashVal(i))
		}
	}
	// The recovered database must accept new writes and resume its version counter past the
	// folded commits, so a fresh write is the newest version.
	if _, err := d2.Write(func(b *engine.WriteBatch) { b.Set([]byte(crashKey(0)), []byte("rewritten")) }); err != nil {
		t.Fatalf("post-recovery write: %v", err)
	}
	if v, ok := get(t, d2, crashKey(0)); !ok || v != "rewritten" {
		t.Fatalf("key 0 = %q,%v after post-recovery rewrite, want rewritten", v, ok)
	}
}

// recoveredPrefixLen reopens fs after a crash, verifies the recovered tree is structurally
// sound, and returns the length j of the committed prefix it holds. It proves the
// durable-prefix property directly: the recovered keys must be exactly key0000..key{j-1},
// each with its correct value, and nothing at or beyond key{j} -- no committed transaction
// lost from the middle, no uncommitted transaction resurrected.
func recoveredPrefixLen(t *testing.T, fs *vfs.Mem) int {
	t.Helper()
	d, err := Open(fs, "test.kv", Options{AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer d.Close()

	// A recovered database must always be structurally sound, however the crash fell.
	rep, err := d.Verify()
	if err != nil {
		t.Fatalf("verify after recovery: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("recovered tree is not sound: %+v", rep.Problems)
	}

	// Find the first absent key: that is the prefix boundary j.
	j := 0
	for j < crashWorkloadKeys {
		if _, ok := get(t, d, crashKey(j)); !ok {
			break
		}
		j++
	}
	// Everything below j must be present with the right value (no torn or stale value).
	for i := 0; i < j; i++ {
		if v, ok := get(t, d, crashKey(i)); !ok || v != crashVal(i) {
			t.Fatalf("key %d = %q,%v inside the recovered prefix, want %q", i, v, ok, crashVal(i))
		}
	}
	// Everything at or above j must be absent: a present key past the boundary is a hole in
	// the prefix or a resurrected uncommitted write.
	for i := j; i < crashWorkloadKeys; i++ {
		if v, ok := get(t, d, crashKey(i)); ok {
			t.Fatalf("key %d = %q present past the prefix boundary %d, want absent", i, v, j)
		}
	}
	return j
}

// TestDurablePrefixAtEverySyncPoint is the durability proof (spec 23 §4, spec 08 §8). It runs
// the same single-key-per-commit workload many times, each time crashing at a different fsync
// boundary, and asserts the recovered state is always exactly the committed-batch prefix of
// the WAL. It first measures how many syncs a full clean run performs, then sweeps a crash
// across every one of those boundaries.
//
// Two properties are asserted across the sweep: each recovery yields a hole-free prefix that
// is structurally sound (checked per-run by recoveredPrefixLen), and the prefix length is
// monotonic non-decreasing as the crash moves later -- a strictly later fsync can only make
// more data durable, never less. The final boundary must recover the whole workload.
func TestDurablePrefixAtEverySyncPoint(t *testing.T) {
	// Measure the workload's sync boundaries on a clean run that is never crashed.
	probe := vfs.NewMem()
	runCrashWorkload(t, probe)
	totalSyncs := probe.SyncCount()
	if totalSyncs < crashWorkloadKeys {
		t.Fatalf("workload performed %d syncs, want at least one per commit (%d)", totalSyncs, crashWorkloadKeys)
	}

	prevPrefix := 0
	for n := 1; n <= totalSyncs; n++ {
		fs := vfs.NewMem()
		fs.CrashAfterSync(n)
		runCrashWorkload(t, fs)
		fs.Crash()

		prefix := recoveredPrefixLen(t, fs)
		if prefix < prevPrefix {
			t.Fatalf("crash after sync %d recovered prefix %d, shorter than the %d recovered at an earlier boundary", n, prefix, prevPrefix)
		}
		prevPrefix = prefix
	}

	if prevPrefix != crashWorkloadKeys {
		t.Fatalf("crash at the final sync boundary recovered prefix %d, want the whole workload (%d)", prevPrefix, crashWorkloadKeys)
	}
}
