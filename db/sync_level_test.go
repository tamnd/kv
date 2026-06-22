package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/wal"
)

// TestSyncOffSkipsEveryFsync pins the perf/06 Finding 1 fix: WithSynchronous(SyncOff)
// must actually turn fsync off. Before the fix SyncOff was the iota-zero value of the
// durability enum, indistinguishable from an unconfigured Options, so Options.sync()
// substituted SyncFull and the database fsynced on every commit even when the caller
// asked for none. The enum now reserves zero for an unexported "unset" sentinel, so OFF
// is a real, non-zero choice that reaches the WAL.
//
// The proof is the WAL fsync counter (Stats().Syncs). Auto-checkpointing is disabled so
// the only thing that could sync during the run is a commit; under SyncOff a commit must
// not. The contrast cases hold the counter to the level's contract: SyncFull syncs once
// per commit, and an unset Options behaves as SyncFull, preserving the safe default.
func TestSyncOffSkipsEveryFsync(t *testing.T) {
	const commits = 200

	run := func(t *testing.T, opts Options) uint64 {
		opts.AutoCheckpoint = -1 // no background checkpoint, so commits are the only sync source
		d := openMem(t, opts)
		for i := 0; i < commits; i++ {
			err := d.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("k%04d", i)), []byte("v"))
			})
			if err != nil {
				t.Fatalf("update %d: %v", i, err)
			}
		}
		return d.Stats().Syncs
	}

	t.Run("off skips every fsync", func(t *testing.T) {
		if got := run(t, Options{Sync: wal.SyncOff}); got != 0 {
			t.Fatalf("SyncOff fsynced %d times over %d commits, want 0", got, commits)
		}
	})

	t.Run("normal defers the per-commit fsync", func(t *testing.T) {
		// NORMAL finalizes durability at checkpoint, not per commit, so with checkpointing
		// disabled it too never syncs from a commit. This is the level OFF used to be
		// confused with; both must read zero here for the right reason.
		if got := run(t, Options{Sync: wal.SyncNormal}); got != 0 {
			t.Fatalf("SyncNormal fsynced %d times over %d commits, want 0", got, commits)
		}
	})

	t.Run("full syncs every commit", func(t *testing.T) {
		if got := run(t, Options{Sync: wal.SyncFull}); got < commits {
			t.Fatalf("SyncFull fsynced %d times over %d commits, want at least %d", got, commits, commits)
		}
	})

	t.Run("unset Options keeps the safe SyncFull default", func(t *testing.T) {
		// The whole point of reserving the zero value is that an unconfigured database is
		// still fully durable. A regression that made zero mean OFF would drop this to 0.
		if got := run(t, Options{}); got < commits {
			t.Fatalf("unset Sync fsynced %d times over %d commits, want at least %d (SyncFull default)", got, commits, commits)
		}
	})
}
