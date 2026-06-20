package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// walSize returns the current size of the database's -wal sidecar through the filesystem,
// so a test can watch the TRUNCATE mode actually shrink the file on disk.
func walSize(t *testing.T, fs vfs.FS, path string) int64 {
	t.Helper()
	f, err := fs.Open(path+walSuffix, vfs.OpenRead)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer f.Close()
	sz, err := f.Size()
	if err != nil {
		t.Fatalf("wal size: %v", err)
	}
	return sz
}

// TestCheckpointTruncateShrinksWAL writes enough to grow the WAL, checkpoints in TRUNCATE
// mode, and checks the -wal file shrank to a small header while the data still reads back.
// A PASSIVE checkpoint on the same workload folds the data but leaves the file grown, which
// is the behavioral line between the two modes in this architecture.
func TestCheckpointTruncateShrinksWAL(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const n = 200
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, []byte("value-with-some-length")) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	grown := walSize(t, fs, "test.kv")
	if grown == 0 {
		t.Fatal("WAL did not grow under writes")
	}

	// A passive checkpoint folds the data and resets the log in place, so it never shrinks
	// the file on disk (it may append a checkpoint frame, leaving it the same size or a touch
	// larger).
	if err := d.CheckpointMode(CheckpointPassive); err != nil {
		t.Fatalf("passive checkpoint: %v", err)
	}
	if got := walSize(t, fs, "test.kv"); got < grown {
		t.Fatalf("passive checkpoint shrank WAL file %d -> %d, want it left grown", grown, got)
	}

	// A truncate checkpoint returns the frame space to the OS.
	if err := d.CheckpointMode(CheckpointTruncate); err != nil {
		t.Fatalf("truncate checkpoint: %v", err)
	}
	shrunk := walSize(t, fs, "test.kv")
	if shrunk >= grown {
		t.Fatalf("truncate checkpoint did not shrink WAL: %d -> %d", grown, shrunk)
	}
	if shrunk > 1024 {
		t.Fatalf("truncate left %d bytes, want it shrunk to roughly the header", shrunk)
	}

	// The folded data survives every mode, and a write after a truncate still commits.
	for _, i := range []int{0, n / 2, n - 1} {
		k := fmt.Sprintf("k%04d", i)
		if v, ok := get(t, d, k); !ok || v != "value-with-some-length" {
			t.Fatalf("%s = %q,%v after truncate checkpoint", k, v, ok)
		}
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("after"), []byte("x")) }); err != nil {
		t.Fatalf("write after truncate: %v", err)
	}
	if v, ok := get(t, d, "after"); !ok || v != "x" {
		t.Fatalf("post-truncate write = %q,%v", v, ok)
	}
}

// TestCheckpointTruncateDurableAcrossReopen checkpoints in TRUNCATE mode and reopens, to
// prove truncating the WAL after a fold loses nothing: the data lives in the main file and
// the shrunk log still opens cleanly.
func TestCheckpointTruncateDurableAcrossReopen(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, []byte("v")) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := d.CheckpointMode(CheckpointTruncate); err != nil {
		t.Fatalf("truncate checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen after truncate: %v", err)
	}
	defer d2.Close()
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("k%03d", i)
		if v, ok := get(t, d2, k); !ok || v != "v" {
			t.Fatalf("after reopen %s = %q,%v", k, v, ok)
		}
	}
}
