package betree

import (
	"bytes"
	"fmt"
	"runtime"
	"sort"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// This file is M0's paged storage: it moves the core's cells off the in-memory map
// the skeleton started with and onto a real chain of generation-2 leaf pages on the
// pager, using the node codec from node.go. It is the PR that puts the new core on
// disk. The store is now a sorted run of leaf pages linked by right-sibling
// pointers, rooted at the pager header's EngineRoot field, exactly the substrate
// contract the shipped btree keeps.
//
// What this lands, and what it leaves. The write path here is deliberately the
// simplest thing that is obviously correct: every Apply reads the whole run, merges
// the batch by internal key, and rewrites the run as a fresh sequence of full
// leaves. That is O(n) per batch, the slow base M0 is allowed to be. The
// interior-routed logarithmic descent and the in-place leaf insert that make writes
// and reads scale are the next M0 slice; they replace the rewrite under this same
// SPI without changing what is on disk. The read path resolves a snapshot by
// decoding the run and folding with the shared format helpers, so it answers the
// conformance oracle identically to the skeleton it replaces.

// loadRun walks the leaf run from its head along right-sibling links, decoding each
// leaf, and returns every cell in run order together with the page numbers the run
// occupies. The head is the leftmost leaf: the root may now be an interior node, so
// the walk descends the leftmost spine to the first leaf before following siblings.
// The caller holds at least a read latch, so a writer cannot rewrite the run
// mid-walk. The cycle guard turns a corrupt sibling loop into an error rather than a
// hang.
func (t *Tree) loadRun() (cells []record, pages []format.PageNo, err error) {
	pgno, err := t.leftmostLeaf()
	if err != nil {
		return nil, nil, err
	}
	seen := map[format.PageNo]bool{}
	for pgno != format.NoPage {
		if seen[pgno] {
			return nil, nil, fmt.Errorf("betree: leaf run cycles at page %d", pgno)
		}
		seen[pgno] = true
		lf, derr := t.viewLeaf(pgno)
		if derr != nil {
			return nil, nil, derr
		}
		cells = append(cells, lf.records...)
		pages = append(pages, pgno)
		pgno = lf.right
	}
	return cells, pages, nil
}

// collectBufferedMessages walks the interior nodes of the tree depth-first from the
// root and returns every pending buffer message as a record, so a snapshot folds a
// buffered write the same way it folds one that has already reached a leaf. This is
// the read half of M1's correctness lever (buffered.go): a message resolves by the
// commit version in its internal key, not by where it physically sits, so a message
// in an interior buffer and the leaf record it will become produce the identical op.
// The walk visits interior nodes only; leaves carry no buffer and loadRun covers
// them. The caller holds the read latch, so the tree shape is fixed across the walk.
// The cycle guard turns a corrupt child pointer into an error rather than a hang.
func (t *Tree) collectBufferedMessages() ([]record, error) {
	var out []record
	seen := map[format.PageNo]bool{}
	var visit func(pgno format.PageNo) error
	visit = func(pgno format.PageNo) error {
		if pgno == format.NoPage {
			return nil
		}
		if seen[pgno] {
			return fmt.Errorf("betree: interior buffer walk cycles at page %d", pgno)
		}
		seen[pgno] = true
		typ, err := t.pageType(pgno)
		if err != nil {
			return err
		}
		if typ == format.PageBTreeLeaf {
			return nil
		}
		in, err := t.viewInterior(pgno)
		if err != nil {
			return err
		}
		for _, m := range in.buffer {
			out = append(out, record{
				key: append([]byte(nil), m.key...),
				val: append([]byte(nil), m.val...),
			})
		}
		if err := visit(in.leftmost); err != nil {
			return err
		}
		for _, p := range in.pivots {
			if err := visit(p.child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(t.root()); err != nil {
		return nil, err
	}
	return out, nil
}

// viewLeaf returns the decoded leaf at pgno for a read-only caller, reusing the
// immutable decode cached on the frame when the page is resident and unchanged and
// decoding only on a miss (spec 01 Finding 1: stop re-decoding a node already in the
// buffer pool). It is the read path's counterpart to readLeaf: the returned *leaf is
// shared and immutable, so a caller only reads its records and copies out, while the
// write path keeps readLeaf, which decodes a private copy it is free to mutate. The
// pager drops the cached view before any write-intent pin or frame rebind (Get with
// Write intent calls clearDecoded), so a hit always describes the page's current bytes,
// and the shared instance is exactly the immutable-content channel M2.3 needs to remove
// the read latch: a reader pulls a published object, never raw frame bytes a writer is
// mutating.
func (t *Tree) viewLeaf(pgno format.PageNo) (*leaf, error) {
	box, fr, err := t.pgr.ViewDecodedRef(pgno)
	if err != nil {
		return nil, err
	}
	if box != nil {
		if l, ok := box.Value().(*leaf); ok {
			return l, nil
		}
		// The cached node is not a leaf: a corrupt pointer led the walk to a page that is
		// not the leaf it expected, the same type confusion decodeLeaf fails closed on.
		return nil, ErrCorruptNode
	}
	// Cold miss: decode the bytes and publish the result for the next reader, both under
	// fillGate so the raw read and the SetDecoded are serialized against a writer's memcpy
	// and its second clear (tree.go writePage). Publishing inside the gate is what keeps a
	// stale view from reaching the box: a writer serialized against this either ran fully
	// before, so this decodes the new bytes, or fully after, so its clear drops whatever
	// this published and the next reader cold-decodes the new bytes.
	t.fillGate.RLock()
	l, derr := decodeLeaf(fr.Data()[:t.pgr.UsablePageSize()])
	if derr == nil {
		fr.SetDecoded(l)
	}
	t.fillGate.RUnlock()
	t.pgr.Unpin(fr, false)
	if derr != nil {
		return nil, derr
	}
	return l, nil
}

// viewInterior is viewLeaf for an interior node: the read path's shared-immutable decode
// of an interior page, cached on the frame and reused across reads, with the write path
// keeping loadInterior for its private mutable copy. The returned *interior is read-only.
func (t *Tree) viewInterior(pgno format.PageNo) (*interior, error) {
	box, fr, err := t.pgr.ViewDecodedRef(pgno)
	if err != nil {
		return nil, err
	}
	if box != nil {
		if in, ok := box.Value().(*interior); ok {
			return in, nil
		}
		return nil, ErrCorruptNode
	}
	// The same cold-fill gate as viewLeaf: decode and publish under fillGate so neither the
	// raw read nor the SetDecoded races a writer's in-place rewrite of this frame.
	t.fillGate.RLock()
	in, derr := decodeInterior(fr.Data()[:t.pgr.UsablePageSize()])
	if derr == nil {
		fr.SetDecoded(in)
	}
	t.fillGate.RUnlock()
	t.pgr.Unpin(fr, false)
	if derr != nil {
		return nil, derr
	}
	return in, nil
}

// readLeaf pins a leaf page for reading, decodes it with the generation-2 leaf
// codec over the usable area, and unpins. decodeLeaf copies every key and value, so
// the returned leaf owns its bytes and stays valid after the unpin and after the
// frame is later evicted or rebound. The write path uses it for a private mutable
// decode; the read path uses viewLeaf, which shares one immutable decode.
func (t *Tree) readLeaf(pgno format.PageNo) (*leaf, error) {
	fr, err := t.pgr.Get(pgno, pager.Read)
	if err != nil {
		return nil, err
	}
	lf, derr := decodeLeaf(fr.Data()[:t.pgr.UsablePageSize()])
	t.pgr.Unpin(fr, false)
	if derr != nil {
		return nil, derr
	}
	return lf, nil
}

// snapshot returns the sorted, MVCC-resolved view at snap under the optimistic read
// protocol (doc 05 section 1): no whole-operation latch, with the gen seqlock validating
// that no writer crossed the gather. A writer holding wmu bumps gen to odd before its
// change and back to even after (betree.go beginWrite/endWrite), so a reader that sees
// the same even gen before it starts and after it finishes knows its combined tail and
// tree view is a consistent point in time. An odd gen, or a gen that moved, means a
// writer was mid-change or crossed the read, so the reader retries. The retries are
// bounded: after a few optimistic attempts the reader falls back to taking wmu, which
// excludes every writer and gathers once cleanly, so a reader never starves under
// sustained write load and is never worse than the M0 read latch it replaces. The
// epoch guard g is pinned for the span of each gather so a page a writer retires
// mid-read is not freed under the reader; the betree frees no page yet, so today it
// holds nothing alive, but the protocol is in place for the milestones that free.
func (t *Tree) snapshot(snap engine.Snapshot, g *guard) ([]resolved, error) {
	const maxOptimistic = 4
	for attempt := 0; attempt < maxOptimistic; attempt++ {
		g0 := t.gen.Load()
		if g0&1 != 0 {
			// A writer is mid-change; the view it is building is not yet consistent. Yield and
			// reread rather than gather a half-applied change the post-check would reject.
			runtime.Gosched()
			continue
		}
		g.pin()
		view, err := t.gather(snap)
		g.unpin()
		if t.gen.Load() != g0 {
			// A writer crossed this gather. Whatever it read may mix pre- and post-change
			// state, so discard it (error and all, since the error may be a transient
			// torn-structure decode) and retry from a fresh generation.
			runtime.Gosched()
			continue
		}
		return view, err
	}
	// Optimism exhausted under contention: take the writer lock so no writer runs, pin the
	// guard, and gather once. This is the pessimistic floor, identical in cost to the M0
	// read latch, and it always terminates.
	t.wmu.Lock()
	defer t.wmu.Unlock()
	g.pin()
	defer g.unpin()
	return t.gather(snap)
}

// gather builds the resolved view once, with no latch of its own: it reads the tree's
// hot nodes through the pager's immutable decoded boxes and the tail under tailMu, so
// every shared access it makes is individually safe, and its caller (snapshot) wraps it
// in the gen-validation that makes the combined view consistent as a whole. It decodes
// the run and folds each user key's version group with the shared format helpers: the
// same fold the shipped cores and the oracle use, so a divergence is always a real bug
// rather than a difference in resolution policy. It rebuilds the range-delete set from
// the run's range-begin markers, the way the shipped btree does, so a read folds a range
// delete whose marker cell a point read never lands on. M0 resolves the whole run on
// every read; the paged descent and zero-copy cursor that make reads scale are later
// slices.
func (t *Tree) gather(snap engine.Snapshot) ([]resolved, error) {
	cells, _, err := t.loadRun()
	if err != nil {
		return nil, err
	}
	// M1: a write may rest in the mutable hot tail or in an interior node's buffer
	// instead of having reached its leaf, so the read must consult both. Resolution is
	// by the commit version baked into the internal key, so a tail slot and a buffered
	// message each fold exactly like the leaf record they will become; gathering all
	// three into one run keeps the fold below bit-for-bit identical to M0's. The tail
	// gather and the buffer walk are bounded by the same latch the leaf walk holds, so a
	// writer cannot move a message between the gathers and have it counted twice or zero
	// times. The tail is newest, but order does not matter to the fold: it sorts by
	// internal key and resolves by version, and an exact-internal-key write present in
	// both the tail and the tree (a replay window) carries the identical value, so a
	// duplicate folds idempotently.
	cells = append(cells, t.collectTailMessages()...)
	buffered, err := t.collectBufferedMessages()
	if err != nil {
		return nil, err
	}
	cells = append(cells, buffered...)
	sort.Slice(cells, func(i, j int) bool {
		return format.CompareInternal(cells[i].key, cells[j].key) < 0
	})

	var rangeDels []format.RangeDel
	for _, c := range cells {
		if format.KindOf(c.key) == format.KindRangeBegin {
			rangeDels = append(rangeDels, format.RangeDel{
				Lo:      append([]byte(nil), format.UserKey(c.key)...),
				Hi:      append([]byte(nil), c.val...),
				Version: format.Version(c.key),
			})
		}
	}

	var out []resolved
	tc := snap.TTLClock()
	var i int
	for i < len(cells) {
		uk := format.UserKey(cells[i].key)
		// The sort puts a user key's versions contiguous and newest-first under the
		// inverted-version internal key, which is the order Fold expects.
		var ops []format.Op
		j := i
		for j < len(cells) && bytes.Equal(format.UserKey(cells[j].key), uk) {
			op, ok := format.OpFromCell(cells[j].key, cells[j].val, tc.For(format.KindOf(cells[j].key)))
			j++
			if !ok {
				continue
			}
			ops = append(ops, op)
		}
		i = j

		rd := format.NewestCoveringRangeDel(rangeDels, uk, snap.Version)
		val, ok := format.Fold(ops, snap.Version, rd, t.merge)
		if !ok {
			continue
		}
		out = append(out, resolved{uk: append([]byte(nil), uk...), val: append([]byte(nil), val...)})
	}
	return out, nil
}
