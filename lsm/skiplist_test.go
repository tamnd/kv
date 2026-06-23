package lsm

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/tamnd/kv/format"
)

// ik builds an internal key for a user key at a version, kind Set.
func ik(user string, version uint64) []byte {
	return format.EncodeInternalKey([]byte(user), version, format.KindSet)
}

// TestSkiplistOrdersByInternalKey inserts keys out of order and confirms a forward
// walk yields them in CompareInternal order: user ascending, version descending.
func TestSkiplistOrdersByInternalKey(t *testing.T) {
	sl := newSkiplist(256)
	// Insert in deliberately scrambled order, including two versions of one key.
	sl.insert(ik("banana", 5), []byte("b5"))
	sl.insert(ik("apple", 3), []byte("a3"))
	sl.insert(ik("apple", 7), []byte("a7"))
	sl.insert(ik("cherry", 1), []byte("c1"))

	var got []string
	for off := sl.first(); off != 0; off = sl.next(off) {
		got = append(got, fmt.Sprintf("%s@%d=%s",
			format.UserKey(sl.nodeKey(off)), format.Version(sl.nodeKey(off)), sl.nodeValue(off)))
	}
	want := []string{"apple@7=a7", "apple@3=a3", "banana@5=b5", "cherry@1=c1"}
	if len(got) != len(want) {
		t.Fatalf("walk produced %d entries, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d = %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}

// TestSkiplistInsertIsIdempotent re-inserts the same internal key and confirms the
// list keeps exactly one node, the property recovery relies on when it redoes an
// already-applied committed batch.
func TestSkiplistInsertIsIdempotent(t *testing.T) {
	sl := newSkiplist(256)
	sl.insert(ik("k", 1), []byte("v"))
	sl.insert(ik("k", 1), []byte("v"))
	if int(sl.count.Load()) != 1 {
		t.Fatalf("count = %d after duplicate insert, want 1", sl.count.Load())
	}
	n := 0
	for off := sl.first(); off != 0; off = sl.next(off) {
		n++
	}
	if n != 1 {
		t.Fatalf("walk found %d nodes, want 1", n)
	}
}

// TestSkiplistManyKeysStayOrdered stresses the tower logic across an arena growth
// boundary: insert a few thousand keys in shuffled order and confirm the walk is
// strictly ascending and complete.
func TestSkiplistManyKeysStayOrdered(t *testing.T) {
	sl := newSkiplist(64) // tiny, so the arena must grow many times
	const n = 4000
	// A simple multiplicative shuffle visits every residue mod n exactly once.
	for i := 0; i < n; i++ {
		k := (i * 2654435761) % n
		sl.insert(ik(fmt.Sprintf("key%06d", k), 1), []byte("v"))
	}
	if int(sl.count.Load()) != n {
		t.Fatalf("count = %d, want %d", sl.count.Load(), n)
	}
	var prev []byte
	seen := 0
	for off := sl.first(); off != 0; off = sl.next(off) {
		key := sl.nodeKey(off)
		if prev != nil && format.CompareInternal(prev, key) >= 0 {
			t.Fatalf("walk not strictly ascending at %q after %q", key, prev)
		}
		prev = append([]byte(nil), key...)
		seen++
	}
	if seen != n {
		t.Fatalf("walk visited %d nodes, want %d", seen, n)
	}
}

// TestArenaGrowthPreservesOffsets writes a recognizable pattern across enough
// allocations to span several fixed blocks, mixes in an oversized allocation larger
// than a block, and confirms every earlier allocation still reads back unchanged: an
// offset names (block, within) and must stay valid as later blocks are appended,
// because the blocks never move.
func TestArenaGrowthPreservesOffsets(t *testing.T) {
	a := newArena(8)
	type rec struct {
		off  uint32
		want uint32
		n    int
	}
	var recs []rec
	// 4 KiB chunks past several block boundaries, then a chunk larger than a block, then
	// more normal chunks so the oversized block is not the last thing allocated.
	sizes := make([]int, 0, 700)
	for i := 0; i < 600; i++ {
		sizes = append(sizes, 4096)
	}
	sizes = append(sizes, blockSize+4096) // oversized: its own right-sized block
	for i := 0; i < 100; i++ {
		sizes = append(sizes, 4096)
	}
	for i, n := range sizes {
		off := a.alloc(n)
		marker := uint32(i*7 + 1)
		// Write a head and tail marker through the allocation's own slice, the way the
		// skip list reaches a node's bytes: bytesAt(off, n) returns the whole contiguous
		// allocation, including the spill of an oversized block past blockSize.
		s := a.bytesAt(off, n)
		binary.LittleEndian.PutUint32(s[:4], marker)
		binary.LittleEndian.PutUint32(s[n-4:], marker^0x5a5a5a5a)
		recs = append(recs, rec{off: off, want: marker, n: n})
	}
	for i, r := range recs {
		s := a.bytesAt(r.off, r.n)
		if got := binary.LittleEndian.Uint32(s[:4]); got != r.want {
			t.Fatalf("alloc %d at off %d = %d, want %d", i, r.off, got, r.want)
		}
		if got := binary.LittleEndian.Uint32(s[r.n-4:]); got != r.want^0x5a5a5a5a {
			t.Fatalf("alloc %d tail at off %d = %d, want %d", i, r.off, got, r.want^0x5a5a5a5a)
		}
	}
	if a.bytesAt(0, 1)[0] != 0 {
		t.Fatal("offset 0 sentinel was overwritten")
	}
}

// TestUint32ArenaExactBlockBoundary covers the boundary case a packed-cursor allocator gets
// wrong: a tower that fills the last block to its final slot. The cursor then sits exactly on
// the block boundary, and a packed cursor decoded by shift-and-mask reads a within-block offset
// of zero and hands the next allocation a base in a block that was never allocated, which a
// later atomic access faults on. The explicit within counter must instead reach u32Size and
// open a fresh block on the following allocation, so every base decodes into a block that
// exists.
func TestUint32ArenaExactBlockBoundary(t *testing.T) {
	u := newUint32Arena()
	// Slot 0 is burned, so within starts at 1; u32Size-1 more slots bring it to exactly
	// u32Size, landing the cursor on the block boundary without opening a new block.
	base0 := u.alloc(u32Size - 1)
	if base0 != 1 {
		t.Fatalf("first alloc base = %d, want 1", base0)
	}
	// The next allocation must open block 1 and return a base inside it.
	base1 := u.alloc(4)
	if bi := base1 >> u32Shift; bi != 1 {
		t.Fatalf("post-boundary base = %d decodes to block %d, want block 1", base1, bi)
	}
	// Both ends must be addressable through the atomic accessors: the last slot of block 0
	// and the first slot of block 1. A base in a never-allocated block would panic here.
	u.store(u32Size-1, 0xdeadbeef)
	u.store(base1, 0x12345678)
	if got := u.load(u32Size - 1); got != 0xdeadbeef {
		t.Fatalf("last slot of block 0 = %#x, want 0xdeadbeef", got)
	}
	if got := u.load(base1); got != 0x12345678 {
		t.Fatalf("first slot of block 1 = %#x, want 0x12345678", got)
	}
}

// TestSkiplistConcurrentInsert hammers the lock-free insert from many goroutines, each
// owning a disjoint key range the way the parallel-apply path does (versions differ across
// batches, so no two goroutines insert the same internal key), and confirms every key is
// present exactly once, in order, and reachable by seek. Readers run alongside the writers
// so the atomic forward-pointer loads are exercised against live inserts. It is the
// load-bearing check for this slice and must pass under -race, which is what would catch a
// torn link or a lost update in the CAS retry loop.
func TestSkiplistConcurrentInsert(t *testing.T) {
	sl := newSkiplist(1024)
	const writers = 8
	const perWriter = 4000
	total := writers * perWriter

	var wg sync.WaitGroup
	// Writers: goroutine g inserts keys g, g+writers, g+2*writers, ... so the ranges are
	// disjoint and interleave across the keyspace, stressing splices at every level.
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := g + i*writers
				sl.insert(ik(fmt.Sprintf("key%08d", k), 1), []byte(fmt.Sprintf("v%d", k)))
			}
		}(g)
	}
	// A reader that walks and seeks while the writers run, just to drive the atomic read
	// path concurrently with inserts; it asserts nothing about completeness mid-flight, only
	// that the structure never hands back an out-of-order or torn key.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := 0; r < 200; r++ {
			var prev []byte
			for off := sl.first(); off != 0; off = sl.next(off) {
				key := sl.nodeKey(off)
				if prev != nil && format.CompareInternal(prev, key) >= 0 {
					t.Errorf("concurrent walk not ascending at %q after %q", key, prev)
					return
				}
				prev = append([]byte(nil), key...)
			}
		}
	}()
	wg.Wait()

	if got := int(sl.count.Load()); got != total {
		t.Fatalf("count = %d, want %d", got, total)
	}

	// Every key present exactly once, in strict order.
	seen := 0
	var prev []byte
	for off := sl.first(); off != 0; off = sl.next(off) {
		key := sl.nodeKey(off)
		if prev != nil && format.CompareInternal(prev, key) >= 0 {
			t.Fatalf("final walk not strictly ascending at %q after %q", key, prev)
		}
		prev = append([]byte(nil), key...)
		seen++
	}
	if seen != total {
		t.Fatalf("final walk visited %d nodes, want %d", seen, total)
	}

	// Every key reachable by seek, landing on its own node.
	for k := 0; k < total; k++ {
		want := ik(fmt.Sprintf("key%08d", k), 1)
		off := sl.seek(want)
		if off == 0 || format.CompareInternal(sl.nodeKey(off), want) != 0 {
			t.Fatalf("seek did not find key %d", k)
		}
	}
}
