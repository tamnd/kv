package btree

import (
	"bytes"
	"sort"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// NewReader implements engine.Engine: a consistent read view at a snapshot
// version. M1 materializes the MVCC-resolved view the reader needs from the live
// tree -- a leaf-by-leaf walk over the B-link chain, folded the same way the model
// oracle folds it (spec 10 §3). The streaming cursor that resolves versions lazily
// while it scans, and the parent-stack reverse walk, are M2; the SPI this returns
// does not change when they land.
func (t *BTree) NewReader(snap engine.Snapshot) (engine.Reader, error) {
	return &reader{t: t, snap: snap}, nil
}

type reader struct {
	t    *BTree
	snap engine.Snapshot
}

// Get returns the value for userKey visible at the reader's snapshot. A user key's
// whole version group lives in one leaf (splitPoint never cuts a group), so Get
// descends to that single leaf and resolves the group there. In Bε mode it also
// collects any buffered messages for the key parked in the interiors along the
// descent: a pending write that has not yet reached its leaf is just a newer version
// of the group, which resolveStream folds in the same fold (spec 05 §4).
func (r *reader) Get(userKey []byte) ([]byte, error) {
	// Back the version group with a small array in this frame so the overwhelmingly
	// common case (a key with a handful of versions and buffered messages) gathers
	// without touching the heap. gatherPoint appends into it and only spills to a heap
	// grow for a pathologically deep version chain. The array stays on Get's stack: the
	// group flows through resolveStream, which copies what it keeps and never retains the
	// slice, so it does not escape (spec 01 Finding 2, perf/09 N3).
	var scratch [8]entry
	group, err := r.t.gatherPoint(userKey, scratch[:0])
	if err != nil {
		return nil, err
	}
	res := resolveStream(group, r.snap, r.t.merge, r.t.rangeDels)
	if len(res) == 0 {
		return nil, engine.ErrNotFound
	}
	// resolveStream already returns a freshly allocated, caller-owned value (it copies
	// the folded version out of the shared decoded node), and res is local here, so
	// res[0].val is not aliased anywhere else. Hand it back directly rather than copying
	// it a second time: the value was copied three to four times on the read path, and
	// this drops the redundant final copy so the fold-step copy is the only one (spec 01
	// Finding 2). The shared leaf bytes are never exposed, so the no-alias contract holds.
	return res[0].val, nil
}

// GetZeroCopy implements engine.ZeroCopyReader: the same resolution as Get, returning the
// value aliased to the decoded leaf rather than copied out. It is sound here because a
// decoded node is immutable and separately allocated: unmarshalLeaf copies every key and
// value out of the page into the node's own byte slices, and a writer that changes a page
// replaces the cached decoded node wholesale (clearDecoded then decode a private copy)
// rather than editing it, so a value this resolves keeps the read bytes alive and never
// changes under the caller. The single difference from Get is that resolvePoint hands back
// Fold's value with no defensive copy, which for the common single-version Set is the
// decoded leaf's own slice and for a merge is Fold's freshly built buffer; both satisfy the
// read-only ZeroCopyReader contract. Multi-version, tombstone, merge, and range-delete
// groups resolve identically to Get, just without the trailing copy.
func (r *reader) GetZeroCopy(userKey []byte) ([]byte, error) {
	// Same stack-backed group gather as Get (#136): the array stays on this frame and the
	// group does not escape, since resolvePoint reads it and retains nothing.
	var scratch [8]entry
	group, err := r.t.gatherPoint(userKey, scratch[:0])
	if err != nil {
		return nil, err
	}
	val, ok := resolvePoint(group, r.snap, r.t.merge, r.t.rangeDels)
	if !ok {
		return nil, engine.ErrNotFound
	}
	return val, nil
}

// GetAt implements engine.PointReader: the same point resolution as Get, but without
// allocating a reader. db.Get's hot path is NewReader -> Get -> Close per call; NewReader
// heap-allocates the &reader because it escapes through engine.Reader, and Get folds the group
// through resolveStream, which allocates an ops slice and a one-element result slice for what is
// a single key. GetAt resolves straight off the tree with the same stack-backed gather Get and
// GetZeroCopy use, folds through resolvePoint (no result slice), and copies the value out once
// for the caller-owned contract. The descent reads the shared, immutable decoded nodes and
// retains nothing, so it needs no per-call reader object; the only allocation left is the one
// value copy Get's contract requires. The result is identical to NewReader(snap).Get(userKey).
func (t *BTree) GetAt(snap engine.Snapshot, userKey []byte) ([]byte, error) {
	var scratch [8]entry
	group, err := t.gatherPoint(userKey, scratch[:0])
	if err != nil {
		return nil, err
	}
	val, ok := resolvePoint(group, snap, t.merge, t.rangeDels)
	if !ok {
		return nil, engine.ErrNotFound
	}
	// resolvePoint hands back a value aliased to the immutable decoded leaf (or Fold's buffer);
	// Get's contract is a caller-owned copy, so copy once here, the same single copy the
	// reader's Get makes. This is the only heap allocation on the path. make+copy goes straight
	// to one mallocgc plus a memmove; append([]byte(nil), val...) would route the same allocation
	// through runtime.growslice first, which recomputes the rounded capacity on every read for no
	// gain when the exact length is already known (perf/12 F2).
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// resolvePoint folds one user key's version group to its MVCC-visible value at snap,
// returning the value with no caller-owned copy. The group gathered for a point read holds
// only entries for userKey (gatherPoint stops at the user-key boundary), so unlike
// resolveStream it does not loop over distinct user keys: it collects the ops once and folds
// them once. The returned value aliases whatever Fold returns -- the decoded leaf's slice for
// a Set base, Fold's built buffer for a merge -- so the caller must treat it as read-only.
// ok is false when the group folds to absent (tombstoned, range-deleted, or nothing visible
// at the snapshot), the same not-found resolveStream would skip.
func resolvePoint(entries []entry, snap engine.Snapshot, merge func(existing, operand []byte) []byte, rangeDels []format.RangeDel) ([]byte, bool) {
	if len(entries) == 0 {
		return nil, false
	}
	tc := snap.TTLClock()
	uk := format.UserKey(entries[0].ik)
	// A point group is almost always a handful of versions, so collect the ops in a small
	// stack array and only spill to the heap for a key with an unusually deep version chain.
	// Fold reads the slice and never retains it, so the backing array does not escape.
	var obuf [8]format.Op
	ops := obuf[:0]
	for k := range entries {
		op, ok := format.OpFromCell(entries[k].ik, entries[k].val, tc.For(format.KindOf(entries[k].ik)))
		if !ok {
			continue // range markers resolve through rangeDels, not as ops
		}
		ops = append(ops, op)
	}
	rd := format.NewestCoveringRangeDel(rangeDels, uk, snap.Version)
	return format.Fold(ops, snap.Version, rd, merge)
}

// gatherPoint descends from the root to the leaf covering userKey and returns the
// key's whole version group in ascending internal-key order (newest first). On the way
// down, in Bε mode, it picks up any buffered messages for the key from the interior
// nodes on the path, then sorts the combined group so resolveStream sees one ordered
// version list. With buffering off the interiors carry no messages and this collapses
// to the single-leaf scan it replaces. group is a caller-supplied zero-length scratch
// slice the versions are appended into, so the common shallow group is gathered without
// a heap allocation; it grows on its own only for an unusually deep chain.
func (t *BTree) gatherPoint(userKey []byte, group []entry) ([]entry, error) {
	// Resolve the root. The interior swizzle caches every child edge on its parent, but the
	// root has no parent, so cache its decoded box on the tree handle instead and skip the
	// pager shard latch + map for it too (impl 149). The cache is valid only while the root
	// page number still matches the header (a grow or collapse moves the root) and the box is
	// still Live (an in-place rewrite or eviction deads it); either miss re-resolves and
	// refreshes the cache.
	rp := t.root()
	var (
		typ format.PageType
		l   *leaf
		in  *interior
		err error
	)
	resolved := false
	if re := t.rootRef.Load(); re != nil && re.pgno == rp && re.box.Live() {
		switch n := re.box.Value().(type) {
		case *leaf:
			typ, l, resolved = format.PageBTreeLeaf, n, true
		case *interior:
			typ, in, resolved = format.PageBTreeInterior, n, true
		}
	}
	if !resolved {
		var box *pager.DecodedNode
		typ, l, in, box, err = t.viewNodeRef(rp)
		if err != nil {
			return nil, err
		}
		if box != nil {
			t.rootRef.Store(&rootEdge{pgno: rp, box: box})
		}
	}
	for {
		if typ == format.PageBTreeLeaf {
			// A user key's versions are contiguous in the leaf (cells sort by user
			// key first), so binary-search to the lower bound of the group and walk
			// forward while the user key matches, instead of scanning the whole leaf
			// (spec 01 Finding 3). The probe compares user keys directly so it needs
			// no allocated search key.
			lo := sort.Search(len(l.keys), func(i int) bool {
				return format.CompareUser(format.UserKey(l.keys[i]), userKey) >= 0
			})
			for i := lo; i < len(l.keys); i++ {
				if format.CompareUser(format.UserKey(l.keys[i]), userKey) != 0 {
					break
				}
				group = append(group, entry{ik: l.keys[i], val: l.vals[i]})
			}
			break
		}
		for i := range in.msgKeys {
			if format.CompareUser(format.UserKey(in.msgKeys[i]), userKey) == 0 {
				group = append(group, entry{ik: in.msgKeys[i], val: in.msgVals[i]})
			}
		}
		idx := in.childFor(userKey)
		// Swizzle: follow the child edge the parent cached on its last descent, skipping
		// the pager shard RLatch and the decode-cache map for warm interiors (perf/12 F2,
		// ~19% of the read). A point read never overlaps a structural write (db.rl is held
		// exclusive by every mutator and shared across the whole descent), and frame eviction
		// preserves page content, so any box still Live is the correct, current decode of the
		// child. A nil (never resolved) or dead (page changed or frame rebound) box falls back
		// to viewNodeRef, which re-resolves the child and refreshes the cached edge.
		if box := in.childRefs[idx].Load(); box != nil && box.Live() {
			switch n := box.Value().(type) {
			case *leaf:
				typ, l, in = format.PageBTreeLeaf, n, nil
				continue
			case *interior:
				typ, l, in = format.PageBTreeInterior, nil, n
				continue
			}
		}
		parent := in
		var box *pager.DecodedNode
		typ, l, in, box, err = t.viewNodeRef(parent.children[idx])
		if err != nil {
			return nil, err
		}
		if box != nil {
			parent.childRefs[idx].Store(box)
		}
	}
	// Sort the combined group by internal key. A version group is small (a few versions
	// plus any buffered messages on the path), so an in-place insertion sort beats
	// sort.Slice here and, unlike sort.Slice, captures nothing in an escaping closure, so
	// the caller's scratch backing array stays on the stack (perf/09 N3).
	for i := 1; i < len(group); i++ {
		e := group[i]
		j := i - 1
		for j >= 0 && format.CompareInternal(group[j].ik, e.ik) > 0 {
			group[j+1] = group[j]
			j--
		}
		group[j+1] = e
	}
	return group, nil
}

// NewIter returns a cursor over a user-key range at the snapshot.
func (r *reader) NewIter(opts engine.IterOptions) (engine.Cursor, error) {
	lower, upper := opts.Lower, opts.Upper
	if len(opts.Prefix) > 0 {
		lower = opts.Prefix
		upper = format.PrefixSuccessor(opts.Prefix)
	}
	entries, err := r.t.collectRange(lower, upper)
	if err != nil {
		return nil, err
	}
	// Clean-range fast path: when the materialized range is all distinct single-version Sets
	// with no range delete in force, every group folds to its own value, so the view is each
	// cell that is visible at the snapshot with no per-key OpFromCell or Fold. This is the
	// NewIterator twin of the forward-cursor clean-leaf fast path (slice 159); it is gated to
	// this materialized-range path and never touches the point-read Get fold above it.
	var view []resolvedKV
	if entriesCleanSets(entries, r.t.rangeDels) {
		view = resolveCleanStream(entries, r.snap)
	} else {
		view = resolveStream(entries, r.snap, r.t.merge, r.t.rangeDels)
	}
	return &cursor{view: view, pos: -1, reverse: opts.Reverse}, nil
}

// entriesCleanSets reports whether a materialized entry stream is the clean shape that resolves
// without folding: no range delete in force, every cell a plain Set, and strictly ascending user
// keys so every version group has size one. It is the []entry form of leafIsCleanSets and fails
// fast on the first non-Set or non-ascending cell, so a stream that needs the fold pays only a
// short prefix scan.
func entriesCleanSets(entries []entry, rangeDels []format.RangeDel) bool {
	if len(rangeDels) != 0 {
		return false
	}
	var prev []byte
	for i := range entries {
		ik := entries[i].ik
		if format.KindOf(ik) != format.KindSet {
			return false
		}
		uk := format.UserKey(ik)
		if i > 0 && format.CompareUser(prev, uk) >= 0 {
			return false
		}
		prev = uk
	}
	return true
}

// resolveCleanStream builds the snapshot view of a clean entry stream (one validated by
// entriesCleanSets) with no fold: each cell visible at the snapshot becomes one resolved entry,
// and a cell newer than the snapshot is skipped exactly as Fold would skip it. The returned keys
// and values are freshly allocated and caller-owned, the same ownership resolveStream gives.
func resolveCleanStream(entries []entry, snap engine.Snapshot) []resolvedKV {
	out := make([]resolvedKV, 0, len(entries))
	for i := range entries {
		if format.Version(entries[i].ik) > snap.Version {
			continue // newer than the snapshot: not visible
		}
		uk := format.UserKey(entries[i].ik)
		out = append(out, resolvedKV{
			uk:  append([]byte(nil), uk...),
			val: append([]byte(nil), entries[i].val...),
		})
	}
	return out
}

func (r *reader) Close() error { return nil }

// StreamForward reports whether this reader can serve a forward streaming scan
// (spec 04). A buffered (Bε) tree parks messages in the interior nodes that the
// single-leaf group gather in ScanForward would miss, so a buffered tree cannot
// stream and the db layer materializes the range instead. An unbuffered tree keeps
// every version in the leaf chain, so the leaf-chain walk sees everything.
func (r *reader) StreamForward() bool { return !r.t.buffered }

// ScanForward returns the next version-resolved, visible user key strictly greater
// than after (or the first key >= lower when after is nil) within [lower, upper), at
// the reader's snapshot, ascending. ok is false when the range is exhausted. keysOnly
// drops the value. The returned key and value are freshly allocated and caller-owned.
//
// It holds no leaf pin or scan position across calls: it re-descends from the root
// every call and walks the leaf-link chain only as far as the next visible group.
// That is what lets the caller release and reacquire the engine lock between calls,
// the same consistency a point Get gives: each call reads a stable tree under the
// caller's read lock, and the fixed snapshot version makes the sequence of calls
// consistent even though writers may run in the gaps. Tombstoned or range-deleted
// groups fold to absent and are skipped, so a call may cross several groups and leaf
// boundaries before it returns a visible one. Only valid when StreamForward is true.
func (r *reader) ScanForward(after, lower, upper []byte, keysOnly bool) (uk, val []byte, ok bool, err error) {
	t := r.t
	// Descend to the leaf to start from: the one covering `after` (we want the first
	// key strictly past it), or on the first call the leaf covering lower, or the
	// leftmost leaf when the range is left-open.
	var pgno format.PageNo
	switch {
	case after != nil:
		pgno, err = t.leafCovering(after)
	case lower != nil:
		pgno, err = t.leafCovering(lower)
	default:
		pgno, err = t.leftmostLeaf()
	}
	if err != nil {
		return nil, nil, false, err
	}

	for pgno != format.NoPage {
		l, err := t.viewLeaf(pgno)
		if err != nil {
			return nil, nil, false, err
		}
		// Skip the already-consumed prefix of the leaf with a binary search: the first
		// cell strictly past `after`, or the first cell >= lower on the first call.
		i := 0
		switch {
		case after != nil:
			i = sort.Search(len(l.keys), func(j int) bool {
				return format.CompareUser(format.UserKey(l.keys[j]), after) > 0
			})
		case lower != nil:
			i = sort.Search(len(l.keys), func(j int) bool {
				return format.CompareUser(format.UserKey(l.keys[j]), lower) >= 0
			})
		}
		for i < len(l.keys) {
			guk := format.UserKey(l.keys[i])
			if upper != nil && format.CompareUser(guk, upper) >= 0 {
				return nil, nil, false, nil // crossed the upper bound: range exhausted
			}
			// A user key's whole version group is contiguous in this one leaf (splitPoint
			// never cuts a group), so gather it here and fold it in isolation.
			j := i
			var group []entry
			for j < len(l.keys) && format.CompareUser(format.UserKey(l.keys[j]), guk) == 0 {
				group = append(group, entry{ik: l.keys[j], val: l.vals[j]})
				j++
			}
			res := resolveStream(group, r.snap, t.merge, t.rangeDels)
			if len(res) > 0 {
				v := res[0].val
				if keysOnly {
					v = nil
				}
				return res[0].uk, v, true, nil
			}
			// Folded to absent (tombstone or covering range delete): skip and keep going.
			i = j
		}
		pgno = l.next
	}
	return nil, nil, false, nil
}

// entry is one raw cell (full internal key + value) read from a leaf.
type entry struct {
	ik  []byte
	val []byte
}

// resolved is one user key's resolved value at a snapshot.
type resolvedKV struct {
	uk  []byte
	val []byte
}

// resolveStream folds an ascending (by CompareInternal) stream of cells into the
// MVCC-visible view at snap: for each user key the newest version <= snap, with
// tombstones removed, merge operands folded over the base, and covering range
// deletes applied. It delegates the per-key fold to format.Fold, the one resolver
// shared with the model and the oracle (spec 10 §3, spec 11 §4) -- intentionally
// identical so the conformance check passes by construction. rangeDels is the
// engine's live interval set, since a covering marker may live in a leaf this scan
// never reads.
func resolveStream(entries []entry, snap engine.Snapshot, merge func(existing, operand []byte) []byte, rangeDels []format.RangeDel) []resolvedKV {
	var out []resolvedKV
	tc := snap.TTLClock()
	i := 0
	for i < len(entries) {
		uk := format.UserKey(entries[i].ik)
		// Gather this user key's group (newest-first), dropping range markers, which
		// resolve through rangeDels rather than as ops.
		var ops []format.Op
		j := i
		for j < len(entries) && format.CompareUser(format.UserKey(entries[j].ik), uk) == 0 {
			ik := entries[j].ik
			val := entries[j].val
			j++
			op, ok := format.OpFromCell(ik, val, tc.For(format.KindOf(ik)))
			if !ok {
				continue // range markers resolve through rangeDels, not as ops
			}
			ops = append(ops, op)
		}
		i = j

		rd := format.NewestCoveringRangeDel(rangeDels, uk, snap.Version)
		val, ok := format.Fold(ops, snap.Version, rd, merge)
		if !ok {
			continue
		}
		out = append(out, resolvedKV{uk: append([]byte(nil), uk...), val: append([]byte(nil), val...)})
	}
	return out
}

// resolveOne folds one already-gathered version group -- newest-first by CompareInternal,
// every cell the same user key -- to its snapshot-visible value. It is the single-group
// form of resolveStream the stateful cursor uses, which gathers exactly one group per step:
// resolveStream allocates a fresh ops list and a one-element result slice on every call,
// and resolveOne removes both. It writes the op list through opsScratch (the cursor's reused
// []format.Op, regrown in place and handed back so the next step reuses the same backing
// array), and returns the resolved key and value directly instead of in a slice. The fold
// itself is format.Fold, the one resolver resolveStream and the oracle share, so the result
// is identical to resolveStream(group, ...)[0]; only the per-step allocation differs.
//
// The returned key, and value when keysOnly is false, are freshly allocated and caller-owned,
// the same ownership resolveStream gives, so the host may retain and mutate them. found is
// false when the group folds to absent (a tombstone or a covering range delete).
func resolveOne(group []entry, snap engine.Snapshot, merge func(existing, operand []byte) []byte, rangeDels []format.RangeDel, opsScratch *[]format.Op, keysOnly bool) (uk, val []byte, found bool) {
	tc := snap.TTLClock()
	user := format.UserKey(group[0].ik)
	ops := (*opsScratch)[:0]
	for _, e := range group {
		op, ok := format.OpFromCell(e.ik, e.val, tc.For(format.KindOf(e.ik)))
		if !ok {
			continue // range markers resolve through rangeDels, not as ops
		}
		ops = append(ops, op)
	}
	*opsScratch = ops // keep the regrown backing array for the next step
	rd := format.NewestCoveringRangeDel(rangeDels, user, snap.Version)
	v, ok := format.Fold(ops, snap.Version, rd, merge)
	if !ok {
		return nil, nil, false
	}
	uk = append([]byte(nil), user...)
	if !keysOnly {
		val = append([]byte(nil), v...)
	}
	return uk, val, true
}

// resolveOneZeroCopy folds a version group exactly as resolveOne does but returns the key and value
// aliased to the immutable decoded leaf (or Fold's buffer) rather than copying them onto the heap.
// user is a slice into the decoded leaf's key bytes and v is whatever Fold returns: the decoded
// leaf's own value slice for a single-version Set base, Fold's freshly built buffer for a merge.
// A decoded leaf is immutable and a writer replaces it wholesale, so both stay valid read-only
// bytes for as long as the caller's slice references them, which is the zero-copy BatchCursor
// contract (valid and unmodified until the next NextBatch call recycles the buffer). This is the
// scan counterpart of resolvePoint, which gives GetZeroCopy the same aliasing on the point path.
func resolveOneZeroCopy(group []entry, snap engine.Snapshot, merge func(existing, operand []byte) []byte, rangeDels []format.RangeDel, opsScratch *[]format.Op, keysOnly bool) (uk, val []byte, found bool) {
	tc := snap.TTLClock()
	user := format.UserKey(group[0].ik)
	ops := (*opsScratch)[:0]
	for _, e := range group {
		op, ok := format.OpFromCell(e.ik, e.val, tc.For(format.KindOf(e.ik)))
		if !ok {
			continue // range markers resolve through rangeDels, not as ops
		}
		ops = append(ops, op)
	}
	*opsScratch = ops // keep the regrown backing array for the next step
	rd := format.NewestCoveringRangeDel(rangeDels, user, snap.Version)
	v, ok := format.Fold(ops, snap.Version, rd, merge)
	if !ok {
		return nil, nil, false
	}
	if keysOnly {
		return user, nil, true
	}
	return user, v, true
}

// NewForwardCursor returns a stateful forward scan cursor over [lower, upper) at the
// reader's snapshot (engine.ForwardCursorer, impl 150). It is the O(1)-per-step counterpart
// to ScanForward: the host prefers it on a streamable (unbuffered) tree, where every version
// lives in the leaf chain so a leaf-chain walk sees everything. A buffered tree reports
// StreamForward false and never reaches this, so the cursor does not handle interior messages.
//
// The range-delete set is snapshotted into the cursor once, here, under the read lock the host
// holds while it builds the cursor. A covering marker visible at this snapshot has version <=
// snap and cannot be dropped while this read snapshot's mark is held, and any marker a writer
// adds afterward has version > snap and is invisible, so the captured set is the stable, correct
// one for the whole scan and the per-step fold needs no further synchronization on it.
func (r *reader) NewForwardCursor(lower, upper []byte) (engine.StreamCursor, error) {
	return &scanCursor{
		t:         r.t,
		snap:      r.snap,
		lower:     lower,
		upper:     upper,
		rangeDels: append([]format.RangeDel(nil), r.t.rangeDels...),
	}, nil
}

// scanCursor is the stateful forward scan cursor NewForwardCursor returns. It holds the
// immutable decoded leaf it is positioned in and the index of the next cell to consider, so a
// step within a leaf is a slice advance with no pager work, and only a leaf boundary resolves a
// new page by following the B-link right pointer. Correctness under concurrent writers rests on
// three facts established in spec 04 / impl 150: the host keeps the read snapshot open for the
// cursor's life, so the snapshot's read mark holds every page the cursor may still reach against
// free and pgno reuse; a decoded leaf is immutable and separately allocated, so the held leaf
// stays valid bytes even after its frame is evicted or its page rewritten; and a split only
// re-homes the upper half of a leaf into a new right page, so the leaf copy the cursor already
// holds still carries every key it had and its recorded next still reaches the correct sibling
// (the split's new page holds only keys the cursor has already passed through its own copy).
type scanCursor struct {
	t         *BTree
	snap      engine.Snapshot
	lower     []byte
	upper     []byte
	rangeDels []format.RangeDel
	started   bool
	leaf      *leaf       // the leaf currently positioned in (immutable); nil once exhausted
	leafClean bool        // the current leaf is all distinct single-version Sets with no range delete in force, so a cell resolves to itself with no fold (set whenever leaf is)
	idx       int         // index in leaf.keys of the next cell to consider
	group     []entry     // reused scratch for one user key's version group
	ops       []format.Op // reused scratch for the group's ops the fold consumes
}

var (
	_ engine.StreamCursor = (*scanCursor)(nil)
	_ engine.BatchCursor  = (*scanCursor)(nil)
)

// loadLeafAt views the leaf at pgno into the cursor and records whether it is a clean
// single-version leaf. The cleanliness flag (computed once per leaf, the same shape slice 157
// uses to skip a GC rebuild) lets step emit each cell without folding its version group when the
// whole leaf is distinct single-version Sets and no range delete is in force at the snapshot. The
// predicate fails fast on the first non-Set or non-ascending cell, so a leaf that needs the fold
// pays at most a short prefix scan before falling through to the general path.
func (c *scanCursor) loadLeafAt(pgno format.PageNo) error {
	l, err := c.t.viewLeaf(pgno)
	if err != nil {
		return err
	}
	c.leaf = l
	c.leafClean = leafIsCleanSets(l, c.rangeDels)
	return nil
}

// ensureStarted performs the one-time descent that positions the cursor at the first cell of its
// range. It is split out of the step loop so both NextEntry and NextBatch pay the descent once, on
// the first call, and every later call walks the held leaf chain.
func (c *scanCursor) ensureStarted() error {
	if c.started {
		return nil
	}
	c.started = true
	t := c.t
	var pgno format.PageNo
	var err error
	if c.lower != nil {
		pgno, err = t.leafCovering(c.lower)
	} else {
		pgno, err = t.leftmostLeaf()
	}
	if err != nil {
		return err
	}
	if err := c.loadLeafAt(pgno); err != nil {
		return err
	}
	// Start at the first cell whose user key is >= lower; that cell is always a group
	// boundary (a group is one user key and the search lands on its newest version), so
	// later steps stay group-aligned by always advancing past the whole group.
	if c.lower != nil {
		c.idx = sort.Search(len(c.leaf.keys), func(j int) bool {
			return format.CompareUser(format.UserKey(c.leaf.keys[j]), c.lower) >= 0
		})
	}
	return nil
}

// step advances to and returns the next version-resolved, snapshot-visible entry, or ok=false when
// the range is exhausted. It assumes ensureStarted has run. zeroCopy chooses how the value group
// is resolved: false copies the key and value onto the heap for NextEntry's caller-owned contract,
// true returns views aliased to the immutable decoded leaf for NextBatch's zero-copy contract. A
// step within a leaf is a slice advance over the held copy; only a leaf boundary follows the B-link.
func (c *scanCursor) step(keysOnly, zeroCopy bool) (uk, val []byte, ok bool, err error) {
	t := c.t
	for c.leaf != nil {
		for c.idx < len(c.leaf.keys) {
			guk := format.UserKey(c.leaf.keys[c.idx])
			if c.upper != nil && format.CompareUser(guk, c.upper) >= 0 {
				c.leaf = nil // crossed the upper bound: range exhausted
				return nil, nil, false, nil
			}
			// Clean-leaf fast path: when the whole leaf is distinct single-version Sets with no
			// range delete in force (leafClean), every group has size one and folds to itself, so
			// resolving a cell is just the snapshot-version visibility check. Emit the cell with no
			// group gather, OpFromCell, or Fold. This is the scan twin of the single-version
			// point-read fast path and of slice 157's GC clean-leaf skip; it removes the per-key
			// fold that profiling named as the readseq overhead bbolt does not pay. The result is
			// identical to resolveOne on a clean leaf: a Set with version <= snap folds to its own
			// value, and one with a newer version is invisible and skipped.
			if c.leafClean {
				ik := c.leaf.keys[c.idx]
				v := c.leaf.vals[c.idx]
				c.idx++
				if format.Version(ik) > c.snap.Version {
					continue // newer than the snapshot: not visible, skip
				}
				if zeroCopy {
					// Views aliased to the immutable decoded leaf, the NextBatch contract.
					if keysOnly {
						return guk, nil, true, nil
					}
					return guk, v, true, nil
				}
				// NextEntry's caller-owned contract: copy the key and value out.
				uk = append([]byte(nil), guk...)
				if !keysOnly {
					val = append([]byte(nil), v...)
				}
				return uk, val, true, nil
			}
			// A user key's whole version group is contiguous in this one leaf (splitPoint
			// never cuts a group), so gather it here and fold it in isolation.
			c.group = c.group[:0]
			j := c.idx
			for j < len(c.leaf.keys) && format.CompareUser(format.UserKey(c.leaf.keys[j]), guk) == 0 {
				c.group = append(c.group, entry{ik: c.leaf.keys[j], val: c.leaf.vals[j]})
				j++
			}
			c.idx = j
			var found bool
			if zeroCopy {
				uk, val, found = resolveOneZeroCopy(c.group, c.snap, t.merge, c.rangeDels, &c.ops, keysOnly)
			} else {
				uk, val, found = resolveOne(c.group, c.snap, t.merge, c.rangeDels, &c.ops, keysOnly)
			}
			if found {
				return uk, val, true, nil
			}
			// Folded to absent (tombstone or covering range delete): skip and keep going.
		}
		// Leaf exhausted: follow the B-link right pointer to the next leaf, or finish.
		next := c.leaf.next
		if next == format.NoPage {
			c.leaf = nil
			return nil, nil, false, nil
		}
		if err := c.loadLeafAt(next); err != nil {
			return nil, nil, false, err
		}
		c.idx = 0
	}
	return nil, nil, false, nil
}

// NextEntry implements engine.StreamCursor. The host calls it under the engine read lock, one entry
// per call, so the lock spans a single step and is released between steps exactly as the stateless
// ScanForward contract requires. Each returned key and value is a fresh caller-owned copy.
func (c *scanCursor) NextEntry(keysOnly bool) (uk, val []byte, ok bool, err error) {
	if err = c.ensureStarted(); err != nil {
		return nil, nil, false, err
	}
	return c.step(keysOnly, false)
}

// NextBatch implements engine.BatchCursor. It fills dst with up to len(dst) resolved entries in one
// call, each key and value a zero-copy view into the immutable decoded leaf rather than a heap copy,
// valid until the next call. The fill crosses leaf boundaries freely up to the batch cap. A short
// fill (n < len(dst)) means the range was exhausted, the only way step returns ok=false, so the
// host marks the scan drained on a short fill with no probe call. The views into a leaf the fill
// crossed off stay alive because each dst slot references that leaf's decoded byte arrays directly,
// so they outlive the cursor's own leaf pointer until dst is recycled by the next call.
func (c *scanCursor) NextBatch(dst []engine.KV, keysOnly bool) (n int, err error) {
	if err = c.ensureStarted(); err != nil {
		return 0, err
	}
	for n < len(dst) {
		uk, v, ok, err := c.step(keysOnly, true)
		if err != nil {
			return n, err
		}
		if !ok {
			return n, nil
		}
		dst[n] = engine.KV{Key: uk, Value: v}
		n++
	}
	return n, nil
}

// leafCovering descends from the root to the leaf whose range covers userKey.
func (t *BTree) leafCovering(userKey []byte) (format.PageNo, error) {
	pgno := t.root()
	for {
		typ, _, in, err := t.viewNode(pgno)
		if err != nil {
			return 0, err
		}
		if typ == format.PageBTreeLeaf {
			return pgno, nil
		}
		pgno = in.children[in.childFor(userKey)]
	}
}

// leftmostLeaf descends from the root following child[0] to the first leaf.
func (t *BTree) leftmostLeaf() (format.PageNo, error) {
	pgno := t.root()
	for {
		typ, _, in, err := t.viewNode(pgno)
		if err != nil {
			return 0, err
		}
		if typ == format.PageBTreeLeaf {
			return pgno, nil
		}
		pgno = in.children[0]
	}
}

// collectRange walks the B-link leaf chain and returns every cell whose user key
// falls in [lower, upper) (nil bounds are unbounded), in ascending internal-key
// order. It starts at the leaf covering lower so a bounded scan does not read the
// whole tree, and stops as soon as it crosses upper.
func (t *BTree) collectRange(lower, upper []byte) ([]entry, error) {
	var start format.PageNo
	var err error
	if lower != nil {
		start, err = t.leafCovering(lower)
	} else {
		start, err = t.leftmostLeaf()
	}
	if err != nil {
		return nil, err
	}

	var out []entry
	pgno := start
	for pgno != format.NoPage {
		l, err := t.viewLeaf(pgno)
		if err != nil {
			return nil, err
		}
		stop := false
		for i := range l.keys {
			uk := format.UserKey(l.keys[i])
			if lower != nil && format.CompareUser(uk, lower) < 0 {
				continue
			}
			if upper != nil && format.CompareUser(uk, upper) >= 0 {
				stop = true
				break
			}
			out = append(out, entry{ik: l.keys[i], val: l.vals[i]})
		}
		if stop {
			break
		}
		pgno = l.next
	}

	// In Bε mode, messages still parked in interior buffers along this range have not
	// reached the leaf chain, so the walk above missed them. Gather the in-range ones
	// from the overlapping interior subtrees and merge them in, then sort the whole
	// stream by internal key so resolveStream sees the same ascending, group-clustered
	// order it gets from a single source.
	if t.buffered {
		buffered, err := t.collectBufferedRange(lower, upper)
		if err != nil {
			return nil, err
		}
		if len(buffered) > 0 {
			out = append(out, buffered...)
			sort.Slice(out, func(i, j int) bool {
				return format.CompareInternal(out[i].ik, out[j].ik) < 0
			})
		}
	}
	return out, nil
}

// collectBufferedRange returns every buffered message whose user key falls in
// [lower, upper) (nil bounds unbounded), gathered from the interior nodes whose subtree
// overlaps the range. It descends only interiors, never leaves, and prunes children
// whose key span is disjoint from the range, so a bounded scan reads a bounded slice of
// the interior level rather than the whole tree.
func (t *BTree) collectBufferedRange(lower, upper []byte) ([]entry, error) {
	var out []entry
	var walk func(pgno format.PageNo) error
	walk = func(pgno format.PageNo) error {
		typ, _, in, err := t.viewNode(pgno)
		if err != nil {
			return err
		}
		if typ == format.PageBTreeLeaf {
			return nil
		}
		for i := range in.msgKeys {
			uk := format.UserKey(in.msgKeys[i])
			if lower != nil && format.CompareUser(uk, lower) < 0 {
				continue
			}
			if upper != nil && format.CompareUser(uk, upper) >= 0 {
				continue
			}
			out = append(out, entry{ik: in.msgKeys[i], val: in.msgVals[i]})
		}
		for ci := 0; ci < len(in.children); ci++ {
			// child ci covers [loBound, hiBound): loBound = seps[ci-1] (-inf at ci 0),
			// hiBound = seps[ci] (+inf past the last separator). Skip a child whose span
			// cannot intersect [lower, upper).
			var loBound, hiBound []byte
			if ci > 0 {
				loBound = in.seps[ci-1]
			}
			if ci < len(in.seps) {
				hiBound = in.seps[ci]
			}
			if upper != nil && loBound != nil && format.CompareUser(loBound, upper) >= 0 {
				continue
			}
			if lower != nil && hiBound != nil && format.CompareUser(hiBound, lower) <= 0 {
				continue
			}
			if err := walk(in.children[ci]); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(t.root()); err != nil {
		return nil, err
	}
	return out, nil
}

// cursor walks a pre-resolved snapshot view. Bounds and prefix were applied while
// collecting; reverse flips the direction of First/Last/Next/Prev. It mirrors the
// model engine's cursor so both satisfy the spec 11 protocol identically.
type cursor struct {
	view    []resolvedKV
	pos     int
	reverse bool
}

func (c *cursor) First() bool {
	if c.reverse {
		c.pos = len(c.view) - 1
	} else {
		c.pos = 0
	}
	return c.Valid()
}

func (c *cursor) Last() bool {
	if c.reverse {
		c.pos = 0
	} else {
		c.pos = len(c.view) - 1
	}
	return c.Valid()
}

func (c *cursor) Next() bool {
	if c.reverse {
		c.pos--
	} else {
		c.pos++
	}
	return c.Valid()
}

func (c *cursor) Prev() bool {
	if c.reverse {
		c.pos++
	} else {
		c.pos--
	}
	return c.Valid()
}

func (c *cursor) SeekGE(userKey []byte) bool {
	idx := sort.Search(len(c.view), func(i int) bool {
		return bytes.Compare(c.view[i].uk, userKey) >= 0
	})
	c.pos = idx
	return c.Valid()
}

func (c *cursor) SeekLT(userKey []byte) bool {
	idx := sort.Search(len(c.view), func(i int) bool {
		return bytes.Compare(c.view[i].uk, userKey) >= 0
	})
	c.pos = idx - 1
	return c.Valid()
}

func (c *cursor) Valid() bool { return c.pos >= 0 && c.pos < len(c.view) }

func (c *cursor) Key() []byte {
	if !c.Valid() {
		return nil
	}
	return c.view[c.pos].uk
}

func (c *cursor) InternalKey() []byte {
	if !c.Valid() {
		return nil
	}
	return format.EncodeInternalKey(c.view[c.pos].uk, format.MaxVersion, format.KindSet)
}

func (c *cursor) Value() (engine.LazyValue, error) {
	if !c.Valid() {
		return engine.LazyValue{}, nil
	}
	return engine.InlineValue(c.view[c.pos].val), nil
}

func (c *cursor) Error() error { return nil }
func (c *cursor) Close() error { return nil }
