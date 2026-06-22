package kv_test

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv"
)

// TestWithSynchronousOffSkipsFsync is the public-surface guard for the perf/06 Finding 1
// fix. The finding was reported against WithSynchronous(SyncOff): it set the durability
// field to the enum's zero value, which the open path could not tell from "unset", so the
// database silently fsynced on every commit. This test drives the real public option and
// reads the public fsync counter (Stats().Syncs) to prove OFF now skips the syscall while
// FULL still pays it.
func TestWithSynchronousOffSkipsFsync(t *testing.T) {
	const commits = 200

	syncsAfterCommits := func(t *testing.T, opts ...kv.Option) uint64 {
		// Disable auto-checkpointing so a commit is the only thing that can fsync.
		d := open(t, append(opts, kv.WithAutoCheckpoint(-1))...)
		for i := 0; i < commits; i++ {
			err := d.Update(func(txn *kv.Txn) error {
				return txn.Set([]byte(fmt.Sprintf("k%04d", i)), []byte("v"))
			})
			if err != nil {
				t.Fatalf("update %d: %v", i, err)
			}
		}
		return d.Stats().Syncs
	}

	if got := syncsAfterCommits(t, kv.WithSynchronous(kv.SyncOff)); got != 0 {
		t.Fatalf("WithSynchronous(SyncOff) fsynced %d times over %d commits, want 0", got, commits)
	}
	if got := syncsAfterCommits(t, kv.WithSynchronous(kv.SyncFull)); got < commits {
		t.Fatalf("WithSynchronous(SyncFull) fsynced %d times over %d commits, want at least %d", got, commits, commits)
	}
}
