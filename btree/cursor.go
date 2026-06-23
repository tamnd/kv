package btree

import (
	"bytes"
	"sort"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
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
	pgno := t.root()
	for {
		typ, l, in, err := t.viewNode(pgno)
		if err != nil {
			return nil, err
		}
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
		pgno = in.children[in.childFor(userKey)]
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
	view := resolveStream(entries, r.snap, r.t.merge, r.t.rangeDels)
	return &cursor{view: view, pos: -1, reverse: opts.Reverse}, nil
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
