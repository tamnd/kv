package db

import (
	"errors"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
)

// hasClass reports whether a verify report carries a problem of the given class.
func hasClass(rep *engine.VerifyReport, class string) bool {
	for _, p := range rep.Problems {
		if p.Class == class {
			return true
		}
	}
	return false
}

// corruptDataPage flips one content byte of a page directly in the file while the
// database keeps it open, modelling bit rot under a live process. The page stays clean
// in the buffer pool, so the structural walk reads good bytes from cache while the
// checksum sweep reads the torn bytes from disk: the report then isolates the checksum
// class.
func corruptDataPage(t *testing.T, fs vfs.FS, path string, pgno uint32, pageSize int) {
	t.Helper()
	f, err := fs.Open(path, vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer f.Close()
	buf := make([]byte, pageSize)
	off := int64(pgno-1) * int64(pageSize)
	if _, err := f.ReadAt(buf, off); err != nil {
		t.Fatalf("read page %d: %v", pgno, err)
	}
	buf[pageSize/2] ^= 0xFF
	if _, err := f.WriteAt(buf, off); err != nil {
		t.Fatalf("write page %d: %v", pgno, err)
	}
	if err := f.Sync(vfs.SyncFull); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

// TestCheckDetectsBitRot is the end-to-end exit-gate test (spec 24 M3): a checksummed
// database whose on-disk page bit-rots is caught by check as a checksum-class problem,
// through the real DB.Verify -> engine.Verify path. It proves the integrity checker
// detects the torn-write/bit-rot class on the live stack, not just in the verifier unit.
func TestCheckDetectsBitRot(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	for i := 0; i < 20; i++ {
		k := []byte{'k', byte('0' + i)}
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, []byte("value")) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// Default checksum is CRC32C, so a created file reserves a trailer.
	if d.pgr.Header().Checksum != format.ChecksumCRC32C {
		t.Fatalf("fresh db checksum = %d, want CRC32C", d.pgr.Header().Checksum)
	}
	// Fold the writes into the main file so the data pages carry stamped checksums.
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// A sound file checks clean.
	rep, err := d.Verify()
	if err != nil {
		t.Fatalf("verify clean: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("clean checksummed db reported problems: %+v", rep.Problems)
	}

	// Corrupt the first data page (the root leaf for this small tree) on disk.
	corruptDataPage(t, fs, "test.kv", 2, 4096)

	rep, err = d.Verify()
	if err != nil {
		t.Fatalf("verify after corruption: %v", err)
	}
	if rep.OK() {
		t.Fatal("check passed a database with a bit-rotted page")
	}
	if !hasClass(rep, "checksum") {
		t.Fatalf("want a checksum problem, got %+v", rep.Problems)
	}
}

// TestReadRejectsBitRotAcrossReopen confirms that a database whose data page bit-rotted
// before reopen still opens (so the checker and recovery tools can run), check reports
// the checksum class, and a read that touches the corrupt page surfaces ErrCorrupt
// rather than returning torn bytes: the pager verifies every page it reads from disk
// (spec 02 §3.2, spec 16 §4).
func TestReadRejectsBitRotAcrossReopen(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 20; i++ {
		k := []byte{'k', byte('0' + i)}
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, []byte("value")) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	corruptDataPage(t, fs, "test.kv", 2, 4096)

	// The database opens despite the corruption so check can diagnose it.
	d2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen of a bit-rotted database: %v", err)
	}
	defer d2.Close()

	rep, err := d2.Verify()
	if err != nil {
		t.Fatalf("verify after reopen: %v", err)
	}
	if !hasClass(rep, "checksum") {
		t.Fatalf("check on a reopened bit-rotted db missed the checksum class: %+v", rep.Problems)
	}

	// A read that descends into the corrupt page fails fast rather than serving torn bytes.
	if _, err := d2.Get([]byte("k0")); !errors.Is(err, format.ErrCorrupt) {
		t.Fatalf("read of a key on a torn page = %v, want ErrCorrupt", err)
	}
}
