package wal

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/vfs"
)

// fileSize returns the on-disk size of a path through the filesystem.
func fileSize(t *testing.T, fs *vfs.Mem, path string) int64 {
	t.Helper()
	f, err := fs.Open(path, vfs.OpenRead)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	sz, err := f.Size()
	if err != nil {
		t.Fatalf("size %s: %v", path, err)
	}
	return sz
}

// TestTruncateFileShrinksAfterCheckpoint logs and commits a run of batches to grow the WAL,
// resets it with Checkpointed, and checks TruncateFile shrinks the file to its header while
// leaving a log the next writer can still append to and recover from.
func TestTruncateFileShrinksAfterCheckpoint(t *testing.T) {
	fs := vfs.NewMem()
	w, err := Create(fs, "db.kv-wal", Options{PageSize: 4096, Sync: SyncFull, Salt: 9})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	for i := 0; i < 50; i++ {
		v := uint64(i + 1)
		w.LogBatch(v, makeBatch(v, map[string]string{fmt.Sprintf("k%02d", i): "x"}))
		if _, err := w.Commit(v); err != nil {
			t.Fatalf("commit %d: %v", v, err)
		}
	}
	grown := fileSize(t, fs, "db.kv-wal")

	// Reset the log as a checkpoint would, then truncate.
	if err := w.Checkpointed(w.LSN() - 1); err != nil {
		t.Fatalf("checkpointed: %v", err)
	}
	if err := w.TruncateFile(); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	shrunk := fileSize(t, fs, "db.kv-wal")
	if shrunk >= grown {
		t.Fatalf("TruncateFile did not shrink: %d -> %d", grown, shrunk)
	}
	if shrunk != headerSize {
		t.Fatalf("truncated size = %d, want the header %d", shrunk, headerSize)
	}

	// A second truncate with nothing to drop is a no-op, not an error.
	if err := w.TruncateFile(); err != nil {
		t.Fatalf("second truncate: %v", err)
	}

	// The reset, truncated log still accepts a new batch and recovers it.
	w.LogBatch(100, makeBatch(100, map[string]string{"after": "y"}))
	if _, err := w.Commit(100); err != nil {
		t.Fatalf("commit after truncate: %v", err)
	}
	res := recoverWAL(t, fs, "db.kv-wal")
	if len(res.Batches) != 1 || res.Batches[0].Version != 100 {
		t.Fatalf("post-truncate recovery = %+v, want one batch at version 100", res.Batches)
	}
}
