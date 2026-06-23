package lsm

import (
	"encoding/binary"
	"fmt"
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
	if sl.count != 1 {
		t.Fatalf("count = %d after duplicate insert, want 1", sl.count)
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
	if sl.count != n {
		t.Fatalf("count = %d, want %d", sl.count, n)
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
