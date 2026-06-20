package pager

import (
	"errors"
	"testing"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
)

// allocAndWrite allocates one page, fills its body with a recognizable pattern, and
// checkpoints so the page reaches disk with a stamped checksum. It returns the page
// number written.
func allocAndWrite(t *testing.T, p *Pager) uint32 {
	t.Helper()
	pgno, fr, err := p.Allocate()
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	data := fr.Data()
	data[0] = byte(format.PageBTreeLeaf) // a non-zero type so the page is not an all-zero hole
	for i := 1; i < len(data); i++ {
		data[i] = byte(i)
	}
	p.Unpin(fr, true)
	if err := p.Checkpoint(0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	return pgno
}

// corruptOnDisk flips one content byte of a page directly in the file, behind the
// pager, so a later read sees a torn page.
func corruptOnDisk(t *testing.T, fs vfs.FS, path string, pgno uint32, pageSize int) {
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

// TestGetVerifiesChecksum corrupts a checksummed page on disk and confirms a fresh Get
// (after reopen, so the read hits disk rather than the cache) returns format.ErrCorrupt
// rather than handing back the torn bytes (spec 02 §3.2).
func TestGetVerifiesChecksum(t *testing.T) {
	fs := vfs.NewMem()
	p, err := Create(fs, "test.kv", Options{PageSize: 4096, Engine: format.EngineBTree, Checksum: format.ChecksumCRC32C})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	pgno := allocAndWrite(t, p)
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	corruptOnDisk(t, fs, "test.kv", pgno, 4096)

	p2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()
	_, err = p2.Get(pgno, Read)
	if !errors.Is(err, format.ErrCorrupt) {
		t.Fatalf("Get on a torn page = %v, want ErrCorrupt", err)
	}
}

// TestGetAcceptsCleanChecksum confirms a reopened checksummed page that was not
// corrupted reads back without error, so verification adds no false positives.
func TestGetAcceptsCleanChecksum(t *testing.T) {
	fs := vfs.NewMem()
	p, err := Create(fs, "test.kv", Options{PageSize: 4096, Engine: format.EngineBTree, Checksum: format.ChecksumCRC32C})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	pgno := allocAndWrite(t, p)
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()
	fr, err := p2.Get(pgno, Read)
	if err != nil {
		t.Fatalf("Get on a clean page: %v", err)
	}
	p2.Unpin(fr, false)
}

// TestOpenRejectsCorruptHeaderPage corrupts page 1 on disk and confirms reopen fails
// with ErrCorrupt: a torn header must be caught at open, not silently trusted.
func TestOpenRejectsCorruptHeaderPage(t *testing.T) {
	fs := vfs.NewMem()
	p, err := Create(fs, "test.kv", Options{PageSize: 4096, Engine: format.EngineBTree, Checksum: format.ChecksumCRC32C})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := p.Checkpoint(0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Flip a byte in page 1's engine-root region (well past the magic, before the trailer).
	corruptOnDisk(t, fs, "test.kv", 1, 4096)

	if _, err := Open(fs, "test.kv", Options{}); !errors.Is(err, format.ErrCorrupt) {
		t.Fatalf("Open on a torn header page = %v, want ErrCorrupt", err)
	}
}
