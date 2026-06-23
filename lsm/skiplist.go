package lsm

import "github.com/tamnd/kv/format"

// maxHeight bounds the skip list's tower. 12 levels with a 1-in-4 promotion gives a
// well-balanced index up to a few million entries, which is the scale a single
// memtable reaches before it is sealed (spec 06 §2).
const maxHeight = 12

// skiplist is an arena-backed ordered index over internal keys (spec 06 §2). Keys
// are ordered by the shared format.CompareInternal, so the memtable iterates in
// exactly the order a flush needs to emit sorted data blocks and the order the
// read-path merge expects. A node's key and value bytes are copied into the byte
// arena at insert and never move; its forward-pointer tower lives in the parallel
// uint32 arena, also never moving, so a reader may slice into either without a lock.
//
// This slice still writes the list under the engine's write lock and reads it under
// a read lock (the memtable wrapper holds the mutex), so the structure is not yet
// lock-free; what changed is the layout. Splitting the tower into a separate uint32
// arena puts every forward pointer on a 4-byte boundary, which is what the lock-light
// concurrent version needs for atomic load and compare-and-swap, and the never-moving
// blocks remove the reallocation a concurrent reader could not tolerate. The
// concurrent insert path lands in the slice that follows, on exactly this layout.
type skiplist struct {
	a      *arena
	tw     *uint32Arena
	head   uint32 // offset of the sentinel head node, tower-only
	height int    // current highest occupied level, 1..maxHeight
	count  int    // number of entries inserted
	rng    uint64 // xorshift state for height generation, deterministic and seeded
}

// node header field offsets within a byte-arena allocation. The tower no longer lives
// inline; nodeTowerOff now holds the base slot index of the node's tower in the uint32
// arena, and the key and value follow the fixed-size header at nodeDataOff.
const (
	nodeHeightOff = 0  // 1 byte: tower height
	nodeKeyLenOff = 1  // 4 bytes
	nodeValLenOff = 5  // 4 bytes
	nodeTowerOff  = 9  // 4 bytes: tower base index into the uint32 arena
	nodeDataOff   = 13 // key bytes, then value bytes
)

// newSkiplist returns an empty list with a full-height head sentinel.
func newSkiplist(arenaCap int) *skiplist {
	sl := &skiplist{a: newArena(arenaCap), tw: newUint32Arena(), height: 1, rng: 0x9E3779B97F4A7C15}
	// The head is a tower-only node at full height with no key or value.
	sl.head = sl.allocNode(maxHeight, nil, nil)
	return sl
}

// allocNode lays out a node across the two arenas and returns its byte-arena offset. The
// header, key, and value go to the byte arena; the height tower of forward pointers goes
// to the uint32 arena, and its base index is stored in the header. The tower slots are
// left zero (nil) for the caller to splice; a freshly allocated tower reads as all-nil
// because the uint32 blocks are zero-filled and never reused.
func (sl *skiplist) allocNode(height int, key, val []byte) uint32 {
	off := sl.a.alloc(nodeDataOff + len(key) + len(val))
	towerBase := sl.tw.alloc(height)
	sl.a.bytesAt(off, 1)[0] = byte(height)
	sl.a.putU32(off+nodeKeyLenOff, uint32(len(key)))
	sl.a.putU32(off+nodeValLenOff, uint32(len(val)))
	sl.a.putU32(off+nodeTowerOff, towerBase)
	dst := sl.a.bytesAt(off+nodeDataOff, len(key)+len(val))
	copy(dst, key)
	copy(dst[len(key):], val)
	return off
}

func (sl *skiplist) nodeHeight(off uint32) int { return int(sl.a.bytesAt(off, 1)[0]) }

func (sl *skiplist) towerBase(off uint32) uint32 { return sl.a.getU32(off + nodeTowerOff) }

func (sl *skiplist) nodeNext(off uint32, level int) uint32 {
	return *sl.tw.slot(sl.towerBase(off) + uint32(level))
}

func (sl *skiplist) setNodeNext(off uint32, level int, next uint32) {
	*sl.tw.slot(sl.towerBase(off) + uint32(level)) = next
}

// nodeKey returns the internal key bytes of a node as a sub-slice of the byte arena.
// The bytes are stable for the life of the memtable, so the slice may be held.
func (sl *skiplist) nodeKey(off uint32) []byte {
	klen := sl.a.getU32(off + nodeKeyLenOff)
	return sl.a.bytesAt(off+nodeDataOff, int(klen))
}

// nodeValue returns the value bytes of a node as a sub-slice of the byte arena.
func (sl *skiplist) nodeValue(off uint32) []byte {
	klen := sl.a.getU32(off + nodeKeyLenOff)
	vlen := sl.a.getU32(off + nodeValLenOff)
	return sl.a.bytesAt(off+nodeDataOff+klen, int(vlen))
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

// seek returns the offset of the first node whose key is >= the target by
// CompareInternal, or 0 if every key is smaller. It descends the tower the same way
// insert does, so a point read lands on a user key's group in logarithmic time
// instead of scanning from the head.
func (sl *skiplist) seek(key []byte) uint32 {
	x := sl.head
	for level := sl.height - 1; level >= 0; level-- {
		for {
			next := sl.nodeNext(x, level)
			if next == 0 {
				break
			}
			if format.CompareInternal(sl.nodeKey(next), key) < 0 {
				x = next
				continue
			}
			break
		}
	}
	return sl.nodeNext(x, 0)
}

// first returns the offset of the lowest-keyed node, or 0 if the list is empty.
func (sl *skiplist) first() uint32 { return sl.nodeNext(sl.head, 0) }

// next returns the offset of the node after off in key order, or 0 at the end.
func (sl *skiplist) next(off uint32) uint32 { return sl.nodeNext(off, 0) }
