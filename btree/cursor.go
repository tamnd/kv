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
// descends to that single leaf and resolves the group there.
func (r *reader) Get(userKey []byte) ([]byte, error) {
	pgno, err := r.t.leafCovering(userKey)
	if err != nil {
		return nil, err
	}
	l, err := r.t.loadLeaf(pgno)
	if err != nil {
		return nil, err
	}
	// Gather this user key's cells (ascending internal order == newest version
	// first within the group).
	var group []entry
	for i := range l.keys {
		if format.CompareUser(format.UserKey(l.keys[i]), userKey) == 0 {
			group = append(group, entry{ik: l.keys[i], val: l.vals[i]})
		}
	}
	res := resolveStream(group, r.snap, r.t.merge, r.t.rangeDels)
	if len(res) == 0 {
		return nil, engine.ErrNotFound
	}
	return append([]byte(nil), res[0].val...), nil
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
			k := format.KindOf(ik)
			if k == format.KindRangeBegin || k == format.KindRangeEnd {
				continue
			}
			ops = append(ops, format.Op{Version: format.Version(ik), Kind: k, Value: val})
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
		typ, err := t.typeOf(pgno)
		if err != nil {
			return 0, err
		}
		if typ == format.PageBTreeLeaf {
			return pgno, nil
		}
		in, err := t.loadInterior(pgno)
		if err != nil {
			return 0, err
		}
		pgno = in.children[in.childFor(userKey)]
	}
}

// leftmostLeaf descends from the root following child[0] to the first leaf.
func (t *BTree) leftmostLeaf() (format.PageNo, error) {
	pgno := t.root()
	for {
		typ, err := t.typeOf(pgno)
		if err != nil {
			return 0, err
		}
		if typ == format.PageBTreeLeaf {
			return pgno, nil
		}
		in, err := t.loadInterior(pgno)
		if err != nil {
			return 0, err
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
		l, err := t.loadLeaf(pgno)
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
