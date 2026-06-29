package db

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// f2Key and f2Val name the i-th key and value in the f2 round-trip workloads.
func f2Key(i int) string { return fmt.Sprintf("f2%04d", i) }
func f2Val(i int) string { return fmt.Sprintf("val%04d", i) }

// f2TestPath returns a real on-disk path. The f2 core opens its own file with the OS
// directly and takes an exclusive lock on it, so its tests run against vfs.NewOS and a
// temp directory rather than the in-memory filesystem the pager-backed cores use.
func f2TestPath(t *testing.T) (vfs.FS, string) {
	t.Helper()
	return vfs.NewOS(), filepath.Join(t.TempDir(), "test.kv")
}

// TestF2EngineSelected confirms a file created with the f2 option reports the f2 core, and
// that reopening with a zero Options reads the engine back from the header rather than
// defaulting to the B-tree core.
func TestF2EngineSelected(t *testing.T) {
	fs, path := f2TestPath(t)
	d, err := Open(fs, path, Options{PageSize: 4096, Engine: format.EngineF2})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := d.Stats().Engine; got != format.EngineF2 {
		t.Fatalf("Stats().Engine = %v, want EngineF2", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, path, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if got := d2.Stats().Engine; got != format.EngineF2 {
		t.Fatalf("reopened Stats().Engine = %v, want EngineF2", got)
	}
}

// TestF2ReopenClean writes a workload, checkpoints, closes, and reopens. The checkpoint
// folds the WAL no further than f2's durable point and persists the f2 file; the clean
// close persists the rest. The reopen must read every key back.
func TestF2ReopenClean(t *testing.T) {
	const n = 200
	fs, path := f2TestPath(t)
	d, err := Open(fs, path, Options{PageSize: 4096, Engine: format.EngineF2})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < n; i++ {
		k, v := []byte(f2Key(i)), []byte(f2Val(i))
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

	d2, err := Open(fs, path, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	for i := 0; i < n; i++ {
		if v, ok := get(t, d2, f2Key(i)); !ok || v != f2Val(i) {
			t.Fatalf("key %d = %q,%v after clean reopen, want %q", i, v, ok, f2Val(i))
		}
	}
}

// TestF2ReopenNoCheckpoint is the host-delegation case: the workload commits with a full
// fsync and never checkpoints, so the host WAL holds the whole committed tail past f2's
// durable point. The reopen recovers f2 from its own file and replays the kept WAL tail
// back through Apply, and the two meet with no lost or doubled write.
func TestF2ReopenNoCheckpoint(t *testing.T) {
	const n = 150
	fs, path := f2TestPath(t)
	d, err := Open(fs, path, Options{PageSize: 4096, Engine: format.EngineF2, Sync: wal.SyncFull, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
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

	d2, err := Open(fs, path, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	for i := 0; i < n; i++ {
		if v, ok := get(t, d2, f2Key(i)); !ok || v != f2Val(i) {
			t.Fatalf("key %d = %q,%v after no-checkpoint reopen, want %q", i, v, ok, f2Val(i))
		}
	}
}

// TestF2OverwriteAndDelete checks the version group survives the round trip under updates
// and deletes, not just first writes: a key set twice reads its latest value, and a deleted
// key reads absent after a checkpoint, close, and reopen.
func TestF2OverwriteAndDelete(t *testing.T) {
	fs, path := f2TestPath(t)
	d, err := Open(fs, path, Options{PageSize: 4096, Engine: format.EngineF2})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("a"), []byte("1")) }); err != nil {
		t.Fatalf("set a=1: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("a"), []byte("2")) }); err != nil {
		t.Fatalf("set a=2: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("b"), []byte("keep")) }); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Delete([]byte("b")) }); err != nil {
		t.Fatalf("delete b: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, path, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if v, ok := get(t, d2, "a"); !ok || v != "2" {
		t.Fatalf("a = %q,%v after reopen, want 2", v, ok)
	}
	if v, ok := get(t, d2, "b"); ok {
		t.Fatalf("b = %q present after reopen, want absent", v)
	}
}
