package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// TestVacuumReclaimsFreedTailPages proves the db-level vacuum plumbing end to end: with
// trailing pages on the freelist, Vacuum folds the WAL and truncates them off, so the
// page count and the on-disk file both shrink and the smaller size survives a reopen
// (spec 09 §3.1). It reaches under the public API to seed the freelist directly, because
// the B-tree core does not yet return emptied pages to it (lazy node merge is a later
// milestone); the seam Vacuum truncates against is exercised here regardless.
func TestVacuumReclaimsFreedTailPages(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Commit some real data so the tree occupies a stable page range below the pages we
	// will free at the tail.
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, []byte("v")) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Grow the file past the tree with pages the tree does not reference, then free them
	// so they sit at the tail of the freelist.
	var freed []uint32
	for i := 0; i < 4; i++ {
		pgno, fr, err := d.pgr.Allocate()
		if err != nil {
			t.Fatalf("allocate %d: %v", i, err)
		}
		d.pgr.Unpin(fr, false)
		freed = append(freed, pgno)
	}
	grown := d.pgr.DBSize()
	for _, pgno := range freed {
		d.pgr.Free(pgno)
	}

	n, err := d.Vacuum(0)
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if n != len(freed) {
		t.Fatalf("vacuum freed %d pages, want %d", n, len(freed))
	}
	wantPages := grown - uint32(len(freed))
	if got := d.pgr.DBSize(); got != wantPages {
		t.Fatalf("page count = %d after vacuum, want %d", got, wantPages)
	}
	wantBytes := int64(wantPages) * int64(d.pgr.PageSize())
	if got := fileSize(t, fs, "test.kv"); got != wantBytes {
		t.Fatalf("file size = %d after vacuum, want %d", got, wantBytes)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if got := d2.pgr.DBSize(); got != wantPages {
		t.Fatalf("page count after reopen = %d, want %d", got, wantPages)
	}
	// The data below the truncated tail is intact.
	if v, ok := get(t, d2, "k025"); !ok || v != "v" {
		t.Fatalf("k025 = %q present=%v after vacuum+reopen, want \"v\" present", v, ok)
	}
}

// fileSize returns the on-disk byte size of a path in the test filesystem.
func fileSize(t *testing.T, fs vfs.FS, path string) int64 {
	t.Helper()
	f, err := fs.Open(path, vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("open %s for size: %v", path, err)
	}
	defer f.Close()
	sz, err := f.Size()
	if err != nil {
		t.Fatalf("size %s: %v", path, err)
	}
	return sz
}
