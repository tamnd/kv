package lsm

import (
	"sync"
	"sync/atomic"

	"github.com/tamnd/kv/format"
)

// maxHeight bounds the skip list's tower. 12 levels with a 1-in-4 promotion gives a
// well-balanced index up to a few million entries, which is the scale a single
// memtable reaches before it is sealed (spec 06 §2).
const maxHeight = 12

// skiplist is an arena-backed ordered index over internal keys (spec 06 §2). Keys
// are ordered by the shared format.CompareInternal, so the memtable iterates in
// exactly the order a flush needs to emit sorted data blocks and the order the
// read-path merge expects. A node's key and value bytes are copied into the never-
// moving byte arena at insert and never move; its forward-pointer tower lives in the
// parallel uint32 arena, also never moving, on 4-byte boundaries.
//
// The list is lock-free for insert and read: any number of goroutines may insert and
// read at once with no mutex (perf/03 W1, perf/07). An insert links its new node into
// each level with a compare-and-swap on the predecessor's forward pointer, re-finding
// the splice and retrying when a concurrent insert wins the race; readers load every
// forward pointer atomically, so a reader sees either the old link or the fully
// initialized new node, never a torn one. The structure is insert-only: a node is never
// removed (a delete is just an insert of a tombstone cell), and the whole list is
// dropped at once when the memtable is sealed, which is what keeps the lock-free path
// simple, no deletion marking and no reclamation.
type skiplist struct {
	a      *arena
	tw     *uint32Arena
	head   uint32        // offset of the sentinel head node, tower-only
	height atomic.Uint32 // current highest occupied level, 1..maxHeight
	count  atomic.Int64  // number of entries inserted
}

// node header field offsets within a byte-arena allocation. nodeTowerOff holds the base
// slot index of the node's tower in the uint32 arena, and the key and value follow the
// fixed-size header at nodeDataOff. The header is written once at allocation, before the
// node is published by the linking CAS, so a reader that reaches the node through an
// atomic forward pointer sees a fully written header.
const (
	nodeHeightOff = 0  // 1 byte: tower height
	nodeKeyLenOff = 1  // 4 bytes
	nodeValLenOff = 5  // 4 bytes
	nodeTowerOff  = 9  // 4 bytes: tower base index into the uint32 arena
	nodeDataOff   = 13 // key bytes, then value bytes
)

// newSkiplist returns an empty list with a full-height head sentinel.
func newSkiplist(arenaCap int) *skiplist {
	sl := &skiplist{a: newArena(arenaCap), tw: newUint32Arena()}
	sl.height.Store(1)
	// The head is a tower-only node at full height with no key or value.
	sl.head = sl.allocNode(maxHeight, nil, nil)
	return sl
}

// allocNode lays out a node across the two arenas and returns its byte-arena offset. The
// header, key, and value go to the byte arena; the height tower of forward pointers goes
// to the uint32 arena, and its base index is stored in the header. The tower slots are
// left zero (nil); a freshly allocated tower reads as all-nil because the uint32 blocks
// are zero-filled and never reused.
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

// getNext atomically loads the forward pointer of off at level. storeNext and casNext are
// its write counterparts: storeNext sets a not-yet-published node's own pointer, casNext
// publishes the node by swapping a predecessor's pointer.
func (sl *skiplist) getNext(off uint32, level int) uint32 {
	return sl.tw.load(sl.towerBase(off) + uint32(level))
}

func (sl *skiplist) storeNext(off uint32, level int, next uint32) {
	sl.tw.store(sl.towerBase(off)+uint32(level), next)
}

func (sl *skiplist) casNext(off uint32, level int, old, next uint32) bool {
	return sl.tw.cas(sl.towerBase(off)+uint32(level), old, next)
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

// heightRNGPool hands each inserting goroutine a private xorshift word so randomHeight
// needs no lock and two concurrent inserts never share mutable RNG state. The list no
// longer carries a single rng field, because that would be a write race the moment two
// goroutines insert at once. Heights are no longer reproducible across a run, which no
// test depends on (tests assert order and count, not tower shape).
var heightRNGPool = sync.Pool{New: func() any { return &rngState{s: nextRNGSeed()} }}

type rngState struct{ s uint64 }

var rngSeedCounter atomic.Uint64

// nextRNGSeed returns a distinct nonzero seed for a fresh pooled RNG. xorshift needs a
// nonzero word, and distinct seeds keep two goroutines' height streams independent.
func nextRNGSeed() uint64 {
	return (rngSeedCounter.Add(1) * 0x9E3779B97F4A7C15) | 1
}

// randomHeight draws a tower height with a 1-in-4 promotion per level, using a private
// pooled xorshift so the draw needs no external RNG, no lock, and no shared state.
func randomHeight() int {
	rs := heightRNGPool.Get().(*rngState)
	rs.s ^= rs.s << 13
	rs.s ^= rs.s >> 7
	rs.s ^= rs.s << 17
	h := 1
	for h < maxHeight && rs.s&3 == 0 {
		h++
		rs.s ^= rs.s << 13
		rs.s ^= rs.s >> 7
		rs.s ^= rs.s << 17
	}
	heightRNGPool.Put(rs)
	return h
}

// findSplice returns, at one level, the node before key and the node at-or-after it,
// starting the walk from before. When key is already present it returns that node as both
// prev and next so the caller can detect the duplicate (prev == next). It is the shared
// inner search of insert and of the descent that precedes it.
func (sl *skiplist) findSplice(key []byte, before uint32, level int) (prev, next uint32) {
	for {
		nx := sl.getNext(before, level)
		if nx == 0 {
			return before, 0
		}
		cmp := format.CompareInternal(key, sl.nodeKey(nx))
		if cmp == 0 {
			return nx, nx // already present
		}
		if cmp < 0 {
			return before, nx
		}
		before = nx
	}
}

// insert adds key->value, ordered by CompareInternal, lock-free. Re-inserting an internal
// key already present is a no-op: an internal key carries its version and kind, so an equal
// key is the same committed mutation (the idempotent redo recovery performs) with an
// identical value, and keeping the first leaves the list duplicate-free. Concurrent inserts
// in the parallel-apply path always carry distinct internal keys (versions differ across
// batches, and a batch holds one cell per key), so the equal-key return is reached only by
// the single-threaded redo path; under that invariant a CAS loss is always a genuine
// neighbour insert, never a duplicate of our own key.
func (sl *skiplist) insert(key, value []byte) {
	listHeight := int(sl.height.Load())
	var prev, next [maxHeight + 1]uint32
	prev[listHeight] = sl.head
	for i := listHeight - 1; i >= 0; i-- {
		prev[i], next[i] = sl.findSplice(key, prev[i+1], i)
		if prev[i] == next[i] {
			return // already present: idempotent
		}
	}

	h := randomHeight()
	x := sl.allocNode(h, key, value)

	// Raise the list height to cover the new node, retrying the CAS against concurrent
	// growers until the published height is at least h.
	for {
		lh := sl.height.Load()
		if uint32(h) <= lh {
			break
		}
		if sl.height.CompareAndSwap(lh, uint32(h)) {
			break
		}
	}

	// Levels at or above the height we searched were never spliced; splice them now,
	// against the head, before linking.
	for i := listHeight; i < h; i++ {
		prev[i], next[i] = sl.findSplice(key, sl.head, i)
		if prev[i] == next[i] {
			return
		}
	}

	// Link bottom-up. At each level, point the new node at the found successor and CAS the
	// predecessor onto it; on a lost CAS a neighbour was inserted between prev[i] and key,
	// so re-find from prev[i] and retry that level.
	for i := 0; i < h; i++ {
		for {
			sl.storeNext(x, i, next[i])
			if sl.casNext(prev[i], i, next[i], x) {
				break
			}
			prev[i], next[i] = sl.findSplice(key, prev[i], i)
			if prev[i] == next[i] {
				return
			}
		}
	}
	sl.count.Add(1)
}

// seek returns the offset of the first node whose key is >= the target by
// CompareInternal, or 0 if every key is smaller. It descends the tower the same way
// insert does, loading every forward pointer atomically so it is safe to run beside
// concurrent inserts, and lands a point read on a user key's group in logarithmic time.
func (sl *skiplist) seek(key []byte) uint32 {
	x := sl.head
	for level := int(sl.height.Load()) - 1; level >= 0; level-- {
		for {
			nx := sl.getNext(x, level)
			if nx == 0 {
				break
			}
			if format.CompareInternal(sl.nodeKey(nx), key) < 0 {
				x = nx
				continue
			}
			break
		}
	}
	return sl.getNext(x, 0)
}

// first returns the offset of the lowest-keyed node, or 0 if the list is empty.
func (sl *skiplist) first() uint32 { return sl.getNext(sl.head, 0) }

// next returns the offset of the node after off in key order, or 0 at the end.
func (sl *skiplist) next(off uint32) uint32 { return sl.getNext(off, 0) }
