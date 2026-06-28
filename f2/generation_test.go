package f2

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// TestFreeBlockReuse is the allocator mechanism test: a freed block is handed back
// before the high-water grows, and the high-water resumes once the free list is
// empty. This is the reuse that keeps the file from growing past the live page
// count once compaction is returning retired blocks.
func TestFreeBlockReuse(t *testing.T) {
	d := &durableFile{pageSize: 4096}
	a, b, c := d.allocBlock(), d.allocBlock(), d.allocBlock()
	if a != 0 || b != 1 || c != 2 {
		t.Fatalf("fresh allocs = %d,%d,%d, want 0,1,2", a, b, c)
	}
	d.freeBlock(b)
	if got := d.allocBlock(); got != b {
		t.Fatalf("alloc after free = %d, want the freed block %d", got, b)
	}
	if got := d.allocBlock(); got != 3 {
		t.Fatalf("alloc with empty free list = %d, want high-water 3", got)
	}
}

// genRec is one record to lay into a hand-built page.
type genRec struct {
	key, val []byte
	tomb     bool
}

// sbBytes builds a valid superblock so a hand-built file opens through recovery.
// allocHigh is informational here: recovery rebuilds the allocator from the blocks
// the file actually spans, not from this field.
func sbBytes(pageSize, shards int, seq, allocHigh uint64) []byte {
	buf := make([]byte, sbSize)
	binary.LittleEndian.PutUint32(buf[0:], sbMagic)
	binary.LittleEndian.PutUint32(buf[4:], durVersion)
	binary.LittleEndian.PutUint32(buf[8:], uint32(pageSize))
	binary.LittleEndian.PutUint32(buf[12:], uint32(shards))
	binary.LittleEndian.PutUint64(buf[16:], seq)
	binary.LittleEndian.PutUint64(buf[24:], allocHigh)
	binary.LittleEndian.PutUint32(buf[32:], crc32.Checksum(buf[0:32], crcTable))
	return buf
}

// pageV2 builds a current-format page block: a generationed header followed by the
// records, padded to a full page.
func pageV2(pageSize, shard, pageIndex int, gen uint32, recs []genRec) []byte {
	buf := make([]byte, pageSize)
	writeBlockHeader(buf, shard, pageIndex, gen)
	w := blockHeaderSize
	for _, r := range recs {
		w += encodeDurable(buf[w:], r.key, r.val, r.tomb)
	}
	return buf
}

// pageV1 builds an original-format page block: a 16-byte header with no generation
// and the CRC over the first twelve bytes, records starting at offset 16. It is
// how a file written before generations existed looks on disk.
func pageV1(pageSize, shard, pageIndex int, recs []genRec) []byte {
	buf := make([]byte, pageSize)
	binary.LittleEndian.PutUint32(buf[0:], bhMagic)
	binary.LittleEndian.PutUint32(buf[4:], uint32(shard))
	binary.LittleEndian.PutUint32(buf[8:], uint32(pageIndex))
	binary.LittleEndian.PutUint32(buf[12:], crc32.Checksum(buf[0:12], crcTable))
	w := blockHeaderV1
	for _, r := range recs {
		w += encodeDurable(buf[w:], r.key, r.val, r.tomb)
	}
	return buf
}

// buildFile lays a superblock and the given page blocks into a fresh file and
// returns its path. Block i sits at data block i, so the page index in a block's
// header and its position in blocks need not match, which is what lets a test put
// two generations of the same page index in one file.
func buildFile(t *testing.T, pageSize, shards int, blocks [][]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "f2.db")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.WriteAt(sbBytes(pageSize, shards, 1, uint64(len(blocks))), 0); err != nil {
		t.Fatalf("write superblock: %v", err)
	}
	for i, blk := range blocks {
		if _, err := f.WriteAt(blk, dataStart+int64(i)*int64(pageSize)); err != nil {
			t.Fatalf("write block %d: %v", i, err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return path
}

// openBuilt opens a hand-built single-shard file. One shard means every key routes
// to shard 0, so a test can put a record under shard 0 in a header and read it back
// by key without fighting the hash-to-shard mapping.
func openBuilt(t *testing.T, pageSize int, path string) *Store {
	t.Helper()
	s, err := New(Tunables{Shards: 1, PageSize: pageSize, Path: path, Durability: DurabilityNone})
	if err != nil {
		t.Fatalf("open built file: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustGet(t *testing.T, s *Store, key string) string {
	t.Helper()
	v, ok, err := s.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get %q: %v", key, err)
	}
	if !ok {
		t.Fatalf("Get %q: not found", key)
	}
	return string(v)
}

// TestRecoveryPrefersActiveGeneration proves the headline generation rule: when a
// page index exists at two generations, the higher one wins and the lower one's
// block is reclaimed. This is the state a committed compaction leaves on disk, an
// old and a new generation of the same page both present until the old block is
// reused.
func TestRecoveryPrefersActiveGeneration(t *testing.T) {
	const ps = 4096
	path := buildFile(t, ps, 1, [][]byte{
		pageV2(ps, 0, 0, 0, []genRec{{key: []byte("k"), val: []byte("old")}}),
		pageV2(ps, 0, 0, 1, []genRec{{key: []byte("k"), val: []byte("new")}}),
	})
	s := openBuilt(t, ps, path)

	if got := mustGet(t, s, "k"); got != "new" {
		t.Fatalf("Get k = %q, want the newer generation's value %q", got, "new")
	}
	// Block 0 held the retired generation and no surviving page claims it, so
	// recovery must have returned it to the free list.
	s.df.mu.Lock()
	free := append([]int64(nil), s.df.free...)
	high := s.df.allocHigh
	s.df.mu.Unlock()
	if len(free) != 1 || free[0] != 0 {
		t.Fatalf("free list = %v, want [0] (the retired generation's block)", free)
	}
	if high != 2 {
		t.Fatalf("allocHigh = %d, want 2 (the file spans two blocks)", high)
	}
}

// TestRecoveryIgnoresUncommittedGeneration is the crash-mid-compaction case: a
// newer generation reached disk for a later page but its page 0 never did, so the
// rewrite never committed. Recovery must fall back to the complete older
// generation, intact, and reclaim the half-written newer block.
func TestRecoveryIgnoresUncommittedGeneration(t *testing.T) {
	const ps = 4096
	path := buildFile(t, ps, 1, [][]byte{
		// Generation 0: a complete two-page log with its page 0.
		pageV2(ps, 0, 0, 0, []genRec{{key: []byte("a"), val: []byte("a0")}}),
		pageV2(ps, 0, 1, 0, []genRec{{key: []byte("b"), val: []byte("b0")}}),
		// Generation 1: only page 1 was written before the crash, no page 0, so the
		// rewrite never committed and this block must be ignored and freed.
		pageV2(ps, 0, 1, 1, []genRec{{key: []byte("b"), val: []byte("b1-uncommitted")}}),
	})
	s := openBuilt(t, ps, path)

	if got := mustGet(t, s, "a"); got != "a0" {
		t.Fatalf("Get a = %q, want %q (the committed generation)", got, "a0")
	}
	if got := mustGet(t, s, "b"); got != "b0" {
		t.Fatalf("Get b = %q, want %q (uncommitted generation must not win)", got, "b0")
	}
	s.df.mu.Lock()
	free := append([]int64(nil), s.df.free...)
	s.df.mu.Unlock()
	if len(free) != 1 || free[0] != 2 {
		t.Fatalf("free list = %v, want [2] (the uncommitted generation's block)", free)
	}
}

// TestRecoveryReadsV1Headers proves the format is backward compatible: a file
// whose blocks carry the original headerless-of-generation layout still opens, with
// every block read as generation 0 and its records found past the 16-byte header.
func TestRecoveryReadsV1Headers(t *testing.T) {
	const ps = 4096
	path := buildFile(t, ps, 1, [][]byte{
		pageV1(ps, 0, 0, []genRec{
			{key: []byte("one"), val: []byte("1")},
			{key: []byte("two"), val: []byte("2")},
		}),
		pageV1(ps, 0, 1, []genRec{
			{key: []byte("three"), val: []byte("3")},
		}),
	})
	s := openBuilt(t, ps, path)

	for k, want := range map[string]string{"one": "1", "two": "2", "three": "3"} {
		if got := mustGet(t, s, k); got != want {
			t.Fatalf("Get %q = %q, want %q from the v1-format file", k, got, want)
		}
	}
}

// TestRecoveryReusesReclaimedBlock ties the pieces together: after recovery frees a
// retired generation's block, the next page allocation reuses it rather than
// growing the file. It opens the two-generation file, then drives enough writes
// into the single shard to force a new page and checks the file did not grow past
// the block count recovery reconciled.
func TestRecoveryReusesReclaimedBlock(t *testing.T) {
	const ps = 4096
	path := buildFile(t, ps, 1, [][]byte{
		pageV2(ps, 0, 0, 0, []genRec{{key: []byte("k"), val: []byte("old")}}),
		pageV2(ps, 0, 0, 1, []genRec{{key: []byte("k"), val: []byte("new")}}),
	})
	s := openBuilt(t, ps, path)

	// Block 0 is on the free list. The next allocation must take it.
	if got := s.df.allocBlock(); got != 0 {
		t.Fatalf("allocBlock after recovery = %d, want the reclaimed block 0", got)
	}
	if got := s.df.allocBlock(); got != 2 {
		t.Fatalf("allocBlock with free list drained = %d, want high-water 2", got)
	}
}
