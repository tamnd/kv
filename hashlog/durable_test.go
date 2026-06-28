package hashlog

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestDurableFileFreshOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.hlog")
	d, err := openDurableFile(path, 16, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.sb.generation != 0 {
		t.Fatalf("fresh file generation %d, want 0", d.sb.generation)
	}
	if d.alloc.count != 0 {
		t.Fatalf("fresh allocator count %d, want 0", d.alloc.count)
	}
	// Both slots must be valid on a fresh file, so a crash right after creation still
	// recovers.
	buf := make([]byte, d.sbSize)
	if _, err := d.f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeSuperblock(buf[:d.slotSize]); err != nil {
		t.Fatalf("slot A invalid on fresh file: %v", err)
	}
	if _, err := decodeSuperblock(buf[d.slotSize:d.sbSize]); err != nil {
		t.Fatalf("slot B invalid on fresh file: %v", err)
	}
}

func TestDurableFileCommitAlternatesAndReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.hlog")
	d, err := openDurableFile(path, 16, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	// Allocate a few extents, commit, and confirm the commit alternated slots and
	// bumped the generation.
	for i := 0; i < 5; i++ {
		d.alloc.alloc()
	}
	d.alloc.freeExtent(2)
	firstSlot := d.newerSlot
	if err := d.commit(); err != nil {
		t.Fatal(err)
	}
	if d.newerSlot == firstSlot {
		t.Fatal("commit did not alternate the slot")
	}
	if d.sb.generation != 1 {
		t.Fatalf("generation after commit %d, want 1", d.sb.generation)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and confirm the committed state recovered: generation, extent count, and
	// the free stack.
	d2, err := openDurableFile(path, 16, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if d2.sb.generation != 1 {
		t.Fatalf("reopened generation %d, want 1", d2.sb.generation)
	}
	if d2.alloc.count != 5 {
		t.Fatalf("reopened extent count %d, want 5", d2.alloc.count)
	}
	count, free := d2.alloc.counts()
	if count != 5 || len(free) != 1 || free[0] != 2 {
		t.Fatalf("reopened allocator count=%d free=%v, want 5 [2]", count, free)
	}
}

// TestDurableFileTornSlotFallback overwrites the newer slot with garbage after a
// commit and confirms reopen falls back to the older valid slot (I5).
func TestDurableFileTornSlotFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.hlog")
	d, err := openDurableFile(path, 16, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	d.alloc.alloc()
	if err := d.commit(); err != nil { // generation 1 in one slot, generation 0 in the other
		t.Fatal(err)
	}
	tornSlot := d.newerSlot
	olderGen := d.sb.generation - 1
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// Tear the newer slot: overwrite it with zeros so its CRC fails.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	garbage := make([]byte, superblockSlotSize(16))
	if _, err := f.WriteAt(garbage, int64(tornSlot*superblockSlotSize(16))); err != nil {
		t.Fatal(err)
	}
	f.Close()

	d2, err := openDurableFile(path, 16, 1<<20)
	if err != nil {
		t.Fatalf("reopen with a torn newer slot should fall back, got: %v", err)
	}
	defer d2.Close()
	if d2.sb.generation != olderGen {
		t.Fatalf("fell back to generation %d, want the older %d", d2.sb.generation, olderGen)
	}
}

func TestDurableFileShardMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.hlog")
	d, err := openDurableFile(path, 16, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	d.Close()
	if _, err := openDurableFile(path, 32, 1<<20); err == nil {
		t.Fatal("opening a 16-shard file with 32 shards should fail")
	}
}

// TestExtentByteOffsetAligned proves I1: every extent begins at an aligned,
// in-bounds offset past the superblock.
func TestExtentByteOffsetAligned(t *testing.T) {
	const extentSize = 1 << 20
	sbSize := superblockSize(256)
	if sbSize%sbBlockSize != 0 {
		t.Fatalf("superblock size %d not block-aligned", sbSize)
	}
	for id := int64(0); id < 1000; id++ {
		off := extentByteOffset(sbSize, extentSize, id)
		if off < sbSize {
			t.Fatalf("extent %d offset %d overlaps the superblock", id, off)
		}
		if (off-sbSize)%extentSize != 0 {
			t.Fatalf("extent %d offset %d not extent-aligned", id, off)
		}
	}
}

func TestGrowExtent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.hlog")
	d, err := openDurableFile(path, 16, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	id, _ := d.alloc.alloc()
	if err := d.growExtent(id); err != nil {
		t.Fatal(err)
	}
	fi, err := d.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	want := extentByteOffset(d.sbSize, d.extentSize, id) + d.extentSize
	if fi.Size() != want {
		t.Fatalf("file size after grow %d, want %d", fi.Size(), want)
	}
	// Growing a lower extent after a higher one must not shrink the file.
	id2, _ := d.alloc.alloc()
	if err := d.growExtent(id2); err != nil {
		t.Fatal(err)
	}
	if err := d.growExtent(id); err != nil { // re-grow the lower id
		t.Fatal(err)
	}
	fi, _ = d.f.Stat()
	want2 := extentByteOffset(d.sbSize, d.extentSize, id2) + d.extentSize
	if fi.Size() != want2 {
		t.Fatalf("file shrank: size %d, want %d", fi.Size(), want2)
	}
}

func TestValidateDurableTunables(t *testing.T) {
	base := DefaultTunables()
	base.Path = "/tmp/x"

	got, err := validateDurableTunables(base)
	if err != nil {
		t.Fatalf("valid tunables rejected: %v", err)
	}
	if got.ExtentSize != base.PageSize {
		t.Fatalf("ExtentSize defaulted to %d, want PageSize %d", got.ExtentSize, base.PageSize)
	}

	bad := base
	bad.Dir = "/tmp/y"
	if _, err := validateDurableTunables(bad); err == nil {
		t.Fatal("Path and Dir together should be rejected")
	}

	bad = base
	bad.ExtentSize = base.PageSize / 2
	if _, err := validateDurableTunables(bad); err == nil {
		t.Fatal("ExtentSize != PageSize should be rejected")
	}

	bad = base
	bad.Path = ""
	if _, err := validateDurableTunables(bad); err == nil {
		t.Fatal("empty Path should be rejected")
	}
}

// TestDurableFileManyCommits exercises a longer alternation: many commits, reopen,
// confirm the latest state. It also fuzzes the free stack across commits within the
// inline capacity.
func TestDurableFileManyCommits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.hlog")
	d, err := openDurableFile(path, 64, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(7))
	live := map[int64]bool{}
	for c := 0; c < 50; c++ {
		ops := rng.Intn(8)
		for i := 0; i < ops; i++ {
			id, _ := d.alloc.alloc()
			live[id] = true
		}
		// Free a few, staying well inside the inline capacity.
		for id := range live {
			if rng.Intn(3) == 0 {
				delete(live, id)
				d.alloc.freeExtent(id)
			}
		}
		if err := d.commit(); err != nil {
			t.Fatalf("commit %d: %v", c, err)
		}
	}
	wantGen := d.sb.generation
	wantCount := d.alloc.count
	d.Close()

	d2, err := openDurableFile(path, 64, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if d2.sb.generation != wantGen {
		t.Fatalf("reopened generation %d, want %d", d2.sb.generation, wantGen)
	}
	if d2.alloc.count != wantCount {
		t.Fatalf("reopened count %d, want %d", d2.alloc.count, wantCount)
	}
}
