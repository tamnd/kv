package lsm

import "github.com/tamnd/kv/format"

// maxHeight bounds the skip list's tower. 12 levels with a 1-in-4 promotion gives a
// well-balanced index up to a few million entries, which is the scale a single
// memtable reaches before it is sealed (spec 06 §2).
const maxHeight = 12

// skiplist is an arena-backed ordered index over internal keys (spec 06 §2). Keys
// are ordered by the shared format.CompareInternal, so the memtable iterates in
// exactly the order a flush needs to emit sorted data blocks and the order the
// read-path merge expects. A node's key and value bytes are copied into the arena
// at insert and never move; only the forward-pointer tower mutates, in place.
//
// This slice writes the list under the engine's write lock and reads it under a
// read lock (the memtable wrapper holds the mutex), so the structure itself is not
// yet lock-free; the arena and tower layout are the same a later lock-light
// concurrent version will use, so that refinement does not disturb the format.
type skiplist struct {
	a      *arena
	head   uint32 // offset of the sentinel head node, tower-only
	height int    // current highest occupied level, 1..maxHeight
	count  int    // number of entries inserted
	rng    uint64 // xorshift state for height generation, deterministic and seeded
}

// node header field offsets within an allocation.
const (
	nodeHeightOff = 0 // 1 byte: tower height
	nodeKeyLenOff = 1 // 4 bytes
	nodeValLenOff = 5 // 4 bytes
	nodeTowerOff  = 9 // height*4 bytes, then key bytes, then value bytes
)

// newSkiplist returns an empty list with a full-height head sentinel.
func newSkiplist(arenaCap int) *skiplist {
	a := newArena(arenaCap)
	sl := &skiplist{a: a, height: 1, rng: 0x9E3779B97F4A7C15}
	// The head is a tower-only node at full height with no key or value.
	sl.head = sl.allocNode(maxHeight, nil, nil)
	return sl
}

// allocNode lays out a node and returns its offset. The tower pointers are left
// zero (nil) for the caller to splice.
func (sl *skiplist) allocNode(height int, key, val []byte) uint32 {
	size := nodeTowerOff + height*4 + len(key) + len(val)
	off := sl.a.alloc(size)
	sl.a.buf[off+nodeHeightOff] = byte(height)
	sl.a.putU32(off+nodeKeyLenOff, uint32(len(key)))
	sl.a.putU32(off+nodeValLenOff, uint32(len(val)))
	// Zero the tower (alloc may hand back reused-looking bytes only on a fresh
	// slice, but be explicit so a grown buffer is always clean here).
	for i := 0; i < height; i++ {
		sl.a.putU32(off+nodeTowerOff+uint32(i*4), 0)
	}
	keyStart := off + nodeTowerOff + uint32(height*4)
	copy(sl.a.buf[keyStart:], key)
	copy(sl.a.buf[keyStart+uint32(len(key)):], val)
	return off
}

func (sl *skiplist) nodeHeight(off uint32) int { return int(sl.a.buf[off+nodeHeightOff]) }

func (sl *skiplist) nodeNext(off uint32, level int) uint32 {
	return sl.a.getU32(off + nodeTowerOff + uint32(level*4))
}

func (sl *skiplist) setNodeNext(off uint32, level int, next uint32) {
	sl.a.putU32(off+nodeTowerOff+uint32(level*4), next)
}

// nodeKey returns the internal key bytes of a node as a sub-slice of the arena.
// The bytes are stable for the life of the memtable, so the slice may be held.
func (sl *skiplist) nodeKey(off uint32) []byte {
	h := sl.nodeHeight(off)
	klen := sl.a.getU32(off + nodeKeyLenOff)
	start := off + nodeTowerOff + uint32(h*4)
	return sl.a.buf[start : start+klen]
}

// nodeValue returns the value bytes of a node as a sub-slice of the arena.
func (sl *skiplist) nodeValue(off uint32) []byte {
	h := sl.nodeHeight(off)
	klen := sl.a.getU32(off + nodeKeyLenOff)
	vlen := sl.a.getU32(off + nodeValLenOff)
	start := off + nodeTowerOff + uint32(h*4) + klen
	return sl.a.buf[start : start+vlen]
}

// randomHeight draws a tower height with a 1-in-4 promotion per level, using a
// self-contained xorshift so the list needs no external RNG and is deterministic
// for a given insertion order (reproducible tests, no math/rand dependency).
func (sl *skiplist) randomHeight() int {
	sl.rng ^= sl.rng << 13
	sl.rng ^= sl.rng >> 7
	sl.rng ^= sl.rng << 17
	h := 1
	for h < maxHeight && sl.rng&3 == 0 {
		h++
		sl.rng ^= sl.rng << 13
		sl.rng ^= sl.rng >> 7
		sl.rng ^= sl.rng << 17
	}
	return h
}

// insert adds key->value, ordered by CompareInternal. Re-inserting an internal key
// already present is a no-op: an internal key carries its version and kind, so an
// equal key is the same committed mutation (the idempotent redo recovery performs),
// and its value is identical. Keeping the first keeps the list free of duplicates.
func (sl *skiplist) insert(key, value []byte) {
	var prev [maxHeight]uint32
	x := sl.head
	for level := sl.height - 1; level >= 0; level-- {
		for {
			next := sl.nodeNext(x, level)
			if next == 0 {
				break
			}
			cmp := format.CompareInternal(sl.nodeKey(next), key)
			if cmp < 0 {
				x = next
				continue
			}
			if cmp == 0 {
				return // already present: idempotent
			}
			break
		}
		prev[level] = x
	}

	h := sl.randomHeight()
	if h > sl.height {
		for level := sl.height; level < h; level++ {
			prev[level] = sl.head
		}
		sl.height = h
	}
	n := sl.allocNode(h, key, value)
	for level := 0; level < h; level++ {
		sl.setNodeNext(n, level, sl.nodeNext(prev[level], level))
		sl.setNodeNext(prev[level], level, n)
	}
	sl.count++
}

// first returns the offset of the lowest-keyed node, or 0 if the list is empty.
func (sl *skiplist) first() uint32 { return sl.nodeNext(sl.head, 0) }

// next returns the offset of the node after off in key order, or 0 at the end.
func (sl *skiplist) next(off uint32) uint32 { return sl.nodeNext(off, 0) }
