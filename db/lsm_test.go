package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// lsmKey and lsmVal name the i-th key and value in the LSM round-trip workloads.
func lsmKey(i int) string { return fmt.Sprintf("lsm%04d", i) }
func lsmVal(i int) string { return fmt.Sprintf("val%04d", i) }

// TestLSMEngineSelected confirms a file created with the LSM option reports the LSM
// core, and that reopening with a zero Options reads the engine back from the header
// rather than defaulting to the B-tree core.
func TestLSMEngineSelected(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Engine: format.EngineLSM})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := d.Stats().Engine; got != format.EngineLSM {
		t.Fatalf("Stats().Engine = %v, want EngineLSM", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if got := d2.Stats().Engine; got != format.EngineLSM {
		t.Fatalf("reopened Stats().Engine = %v, want EngineLSM", got)
	}
}

// TestLSMReopenClean is the durability case the DurableLSN seam exists for: an LSM
// memtable holds applied-but-unflushed data, so a clean checkpoint must fold only to
// what the engine has persisted (nothing, in this slice) and keep the WAL frames it
// has not. A checkpoint that reset the log here would drop the memtable, and the
// reopen below would read an empty database. It must instead replay the kept WAL.
func TestLSMReopenClean(t *testing.T) {
	const n = 50
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Engine: format.EngineLSM})
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
			t.Fatalf("key %d = %q,%v after clean reopen, want %q", i, v, ok, lsmVal(i))
		}
	}
}

// TestLSMReopenTruncate drives the same clean round trip through the TRUNCATE
// checkpoint mode. With an unflushed memtable the engine lags the log, so the mode
// must skip the -wal truncation that would discard the live frames; the data still
// survives the reopen.
func TestLSMReopenTruncate(t *testing.T) {
	const n = 30
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Engine: format.EngineLSM})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < n; i++ {
		k, v := []byte(lsmKey(i)), []byte(lsmVal(i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := d.CheckpointMode(CheckpointTruncate); err != nil {
		t.Fatalf("checkpoint truncate: %v", err)
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
			t.Fatalf("key %d = %q,%v after truncate reopen, want %q", i, v, ok, lsmVal(i))
		}
	}
}

// TestLSMRecoverFromWAL models a crash: the workload commits each key with a full
// fsync and never checkpoints or closes, so the data lives entirely in the WAL.
// Reopening the same filesystem replays the log back into a fresh memtable, the
// memtable-only core's whole durability story.
func TestLSMRecoverFromWAL(t *testing.T) {
	const n = 40
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Engine: format.EngineLSM, Sync: wal.SyncFull, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < n; i++ {
		k, v := []byte(lsmKey(i)), []byte(lsmVal(i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// Intentionally no checkpoint and no Close: a crash leaves the data in the WAL.

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer d2.Close()
	for i := 0; i < n; i++ {
		if v, ok := get(t, d2, lsmKey(i)); !ok || v != lsmVal(i) {
			t.Fatalf("key %d = %q,%v after WAL recovery, want %q", i, v, ok, lsmVal(i))
		}
	}
}

// TestLSMVersionedReopen overwrites and deletes keys across versions before a clean
// checkpoint, then confirms the reopened database resolves the newest version of
// each key, so MVCC folding survives the WAL replay unchanged.
func TestLSMVersionedReopen(t *testing.T) {
	const n = 30
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Engine: format.EngineLSM})
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
			if i%3 == 0 {
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
		if i%3 == 0 {
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
