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
func (t *Tree) snapshotRange(snap engine.Snapshot, g *guard, lower, upper []byte) ([]resolved, error) {
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
		view, err := t.gatherRange(snap, lower, upper)
		g.unpin()
		// g.unpin is the read-side barrier this post-check depends on: it is a full-barrier
		// atomic read-modify-write (epoch.go), so the gather's page reads above are ordered
		// before the generation re-read below. A plain atomic load is only an acquire and
		// would let a gather read sink past it, so the re-read could see an unchanged even
		// generation while a gather read still raced a writer's in-place rewrite. Keep the
		// unpin between the gather and this load.
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
	return t.gatherRange(snap, lower, upper)
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
	return t.foldResolved(cells, snap, rangeDels), nil
}

// gatherRange is the bounded read of M3: it resolves only the user keys in the half-open
// range [lower, upper) instead of the whole keyspace, so a point read folds one key's
// version group and a short scan folds only the leaves its range overlaps, the readseq and
// ycsb-scan lever from doc 04. A nil lower means unbounded below, a nil upper unbounded
// above, so gatherRange(snap, nil, nil) is the full read.
//
// The fast bounded path is correct only where no range delete is in play, because a range
// delete's coverage is not local to its marker: a marker below lower can cover keys inside
// the range, and the bounded leaf walk would skip the leaf that marker sits in. So whenever
// the tree may carry a range delete (the sticky hasRangeDel flag, set at Open and on any
// range-begin write), gatherRange falls back to the full gather and clips its result to the
// range. The workloads M3 targets issue no range deletes, so they take the bounded path; the
// fallback keeps the conformance oracle, which does exercise range deletes, bit-for-bit
// correct.
func (t *Tree) gatherRange(snap engine.Snapshot, lower, upper []byte) ([]resolved, error) {
	if t.hasRangeDel.Load() {
		view, err := t.gather(snap)
		if err != nil {
			return nil, err
		}
		return clipRange(view, lower, upper), nil
	}
	cells, err := t.collectRange(lower, upper)
	if err != nil {
		return nil, err
	}
	sort.Slice(cells, func(i, j int) bool {
		return format.CompareInternal(cells[i].key, cells[j].key) < 0
	})
	// Clean fold-skip (doc 04, decision D5): when the collected range is all distinct
	// single-version plain Sets, every version group has size one and folds to its own
	// value, so the snapshot view is just the cells visible at snap with no per-key
	// OpFromCell and no Fold. This is the readseq lever: a dense ordered scan over freshly
	// written keys never overwritten skips the MVCC machinery entirely. The bounded path
	// has already excluded range deletes, which is the other thing the fold is needed for,
	// so the predicate only checks the cells themselves. A range with any overwrite,
	// delete, merge, or TTL set falls to the general fold below.
	if cellsCleanSets(cells) {
		return foldCleanResolved(cells, snap), nil
	}
	// No range delete is in play on this path, so the fold needs no interval set.
	return t.foldResolved(cells, snap, nil), nil
}

// cellsCleanSets reports whether a run of cells, already sorted by internal key, is the
// clean shape that resolves without folding: every cell a plain Set and strictly ascending
// user keys, so every version group has size one. It is the betree twin of the btree's
// entriesCleanSets and fails fast on the first non-Set or non-ascending cell, so a run that
// needs the fold pays only a short prefix scan. A TTL set (its own kind) is not a plain Set,
// so a range holding one folds, which is where the expiry check lives.
func cellsCleanSets(cells []record) bool {
	var prev []byte
	for i := range cells {
		if format.KindOf(cells[i].key) != format.KindSet {
			return false
		}
		uk := format.UserKey(cells[i].key)
		if i > 0 && format.CompareUser(prev, uk) >= 0 {
			return false
		}
		prev = uk
	}
	return true
}

// foldCleanResolved builds the snapshot view of a clean cell run (one validated by
// cellsCleanSets) with no fold: each cell visible at the snapshot becomes one resolved pair,
// and a cell newer than the snapshot is skipped exactly as Fold would skip it. The keys and
// values are copied out, the same caller-owned ownership the general fold gives, so a
// returned pair never aliases a mutable tail slot or a page a later writer rewrites. (True
// zero-copy hand-back of slices that point into the page for the transaction lifetime waits
// for M6's off-heap arena, where a page lives in stable memory a reader can borrow without a
// decode copy; on this substrate the decode already copies, so the saving here is the
// removed fold, not the removed copy.)
func foldCleanResolved(cells []record, snap engine.Snapshot) []resolved {
	out := make([]resolved, 0, len(cells))
	for i := range cells {
		if format.Version(cells[i].key) > snap.Version {
			continue // newer than the snapshot: not visible
		}
		out = append(out, resolved{
			uk:  append([]byte(nil), format.UserKey(cells[i].key)...),
			val: append([]byte(nil), cells[i].val...),
		})
	}
	return out
}

// foldResolved turns a run of cells, already sorted by internal key, into the MVCC-resolved
// (userKey, value) pairs visible at snap, with rangeDels applied. It is the shared fold
// behind both the full gather and the bounded gatherRange: the same per-version-group fold
// the shipped cores and the oracle use, so a divergence is always a real bug rather than a
// resolution-policy difference. The output is ascending by user key, one pair per visible
// key, which is the order the cursor and the point search expect.
func (t *Tree) foldResolved(cells []record, snap engine.Snapshot, rangeDels []format.RangeDel) []resolved {
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
	return out
}

// clipRange drops the resolved pairs outside [lower, upper) from an already-ascending view.
// It is the range-delete fallback's clip: the full gather resolves the whole keyspace, and
// this trims it to the bounded reader's window so the caller sees the same shape it would
// from the bounded path. A nil bound is unbounded on that side.
func clipRange(view []resolved, lower, upper []byte) []resolved {
	var out []resolved
	for _, e := range view {
		if lower != nil && bytes.Compare(e.uk, lower) < 0 {
			continue
		}
		if upper != nil && bytes.Compare(e.uk, upper) >= 0 {
			continue
		}
		out = append(out, e)
	}
	return out
}

// collectRange gathers every cell whose user key falls in [lower, upper) from the three
// places a write may rest: the leaf run, the mutable tail, and the interior buffers. The
// leaf walk is the bounded part that makes this cheap: it routes straight to the leaf that
// would hold lower and follows right siblings only until a leaf's keys reach upper, so the
// leaves entirely below or above the range are never touched. The run is globally sorted by
// internal key, so once a record's user key reaches upper the walk is done. The tail and the
// buffers are small and bounded by the rollover budget and the per-node page size, so they
// are collected whole and filtered to the range rather than indexed. The caller holds the
// epoch guard and the gen validation wraps the whole gather, so the three reads compose into
// one consistent point in time. This path runs only when no range delete is in play, so a
// missed marker below lower cannot change a result inside the range.
func (t *Tree) collectRange(lower, upper []byte) ([]record, error) {
	var start format.PageNo
	var err error
	if lower == nil {
		start, err = t.leftmostLeaf()
	} else {
		// Route to the leaf holding lower's newest version, using the resident learned index to
		// skip the interior descent when it can. (lower, MaxVersion) is the smallest internal
		// key for user key lower, so no record at or above lower sits in an earlier leaf, and
		// the right-sibling walk from here covers the rest of the range.
		start, err = t.startLeafFor(lower)
	}
	if err != nil {
		return nil, err
	}

	var cells []record
	seen := map[format.PageNo]bool{}
	for pgno := start; pgno != format.NoPage; {
		if seen[pgno] {
			return nil, fmt.Errorf("betree: leaf run cycles at page %d", pgno)
		}
		seen[pgno] = true
		lf, derr := t.viewLeaf(pgno)
		if derr != nil {
			return nil, derr
		}
		stop := false
		for i := range lf.records {
			uk := format.UserKey(lf.records[i].key)
			if lower != nil && bytes.Compare(uk, lower) < 0 {
				continue
			}
			if upper != nil && bytes.Compare(uk, upper) >= 0 {
				// Sorted ascending by user key, so this leaf and every leaf to its right hold
				// only keys at or above upper. The range is complete.
				stop = true
				break
			}
			cells = append(cells, lf.records[i])
		}
		if stop {
			break
		}
		pgno = lf.right
	}

	for _, r := range t.collectTailMessages() {
		if inHalfOpen(format.UserKey(r.key), lower, upper) {
			cells = append(cells, r)
		}
	}
	buffered, err := t.collectBufferedRange(lower, upper)
	if err != nil {
		return nil, err
	}
	cells = append(cells, buffered...)
	return cells, nil
}

// collectBufferedRange is the bounded twin of collectBufferedMessages: it walks the interior
// tree but descends only into the children whose key span overlaps [lower, upper), so a short
// scan never decodes the buffers of the subtrees its range cannot touch. At each interior node
// it keeps the messages resting in that node's own buffer that fall in the range, then recurses
// into the contiguous band of child slots from the one that owns lower to the one that owns
// upper. A buffered message bound for a key in the range rests either in a node on that band
// (its key routes it there) or higher up where this collects it directly, so pruning the
// off-range subtrees drops no message the range needs. A nil lower or upper is unbounded on
// that side, matching the leaf walk. The cycle guard turns a corrupt child pointer into an
// error rather than a hang.
func (t *Tree) collectBufferedRange(lower, upper []byte) ([]record, error) {
	var lik, uik []byte
	if lower != nil {
		// (lower, MaxVersion) is the smallest internal key for user key lower, so the child that
		// owns it is the leftmost child that can hold any record at or above lower.
		lik = format.EncodeInternalKey(lower, format.MaxVersion, format.KindSet)
	}
	if upper != nil {
		uik = format.EncodeInternalKey(upper, format.MaxVersion, format.KindSet)
	}
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
			if inHalfOpen(format.UserKey(m.key), lower, upper) {
				out = append(out, record{
					key: append([]byte(nil), m.key...),
					val: append([]byte(nil), m.val...),
				})
			}
		}
		// Descend only the contiguous child band whose spans overlap the range: from the child
		// owning lower (slot 0 when lower is unbounded) to the child owning upper (the last slot
		// when upper is unbounded). childIndex(uik) still owns keys below upper, since upper is
		// exclusive and the child to its right starts at an internal key whose user key is upper,
		// so the band includes it. Slots left of the lower slot hold only keys below the range.
		loSlot := 0
		if lik != nil {
			loSlot = in.childIndex(lik)
		}
		hiSlot := len(in.pivots)
		if uik != nil {
			hiSlot = in.childIndex(uik)
		}
		for s := loSlot; s <= hiSlot; s++ {
			if err := visit(in.childPage(s)); err != nil {
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

// inHalfOpen reports whether uk lies in [lower, upper), with a nil bound meaning unbounded
// on that side.
func inHalfOpen(uk, lower, upper []byte) bool {
	if lower != nil && bytes.Compare(uk, lower) < 0 {
		return false
	}
	if upper != nil && bytes.Compare(uk, upper) >= 0 {
		return false
	}
	return true
}

// startLeafFor returns the leaf to begin the bounded right-sibling walk at for user key lower,
// using the resident learned locator (learned.go) to skip the interior descent when it can and
// falling back to the leafForKey descent otherwise. The locator predicts a leaf whose smallest
// key is at or before lower; a right-sibling walk from any such live leaf is correct, because
// the run only grows (a split keeps the left piece's page and smallest key and chains right, no
// leaf is freed or merged), so the walk filters keys below lower and stops past upper and a
// too-far-left start only adds a few leaf decodes. The prediction is verified against the live
// leaf before it is trusted: if the predicted page is no longer a leaf, or its smallest key is
// not at or before lower, the read takes leafForKey, the proven descent. So the model can never
// make a read wrong, only faster, and a miss is descent speed. The verifying viewLeaf decodes
// the same leaf the walk decodes next, which is a cached-box hit, so the check is not an extra
// page fetch.
func (t *Tree) startLeafFor(lower []byte) (format.PageNo, error) {
	// (lower, MaxVersion) is the smallest internal key for user key lower, so it sorts at or
	// before every version of lower and before any larger user key. The locator and the verify
	// both compare against this internal key, not the bare user key, because a user key whose
	// versions straddle a leaf boundary must start at the leaf holding its newest version (the
	// one the descent reaches), and a user-key comparison would accept the next leaf and walk
	// past that newest version.
	lik := format.EncodeInternalKey(lower, format.MaxVersion, format.KindSet)
	if loc := t.locator.Load(); loc != nil {
		if pg := loc.locate(lik); pg != format.NoPage {
			lf, err := t.viewLeaf(pg)
			if err == nil && len(lf.records) > 0 &&
				format.CompareInternal(lf.records[0].key, lik) <= 0 {
				return pg, nil
			}
		}
	}
	return t.leafForKey(lik)
}

// leafForKey descends from the root to the leaf whose key range covers the internal key ik,
// routing at each interior the same way the write path's descent does but through the
// read-path immutable views. It is the bounded gather's entry point: a scan starts its
// right-sibling walk at the leaf this returns rather than at the head of the run. The cycle
// guard turns a corrupt child pointer into an error rather than a hang.
func (t *Tree) leafForKey(ik []byte) (format.PageNo, error) {
	pgno := t.root()
	seen := map[format.PageNo]bool{}
	for {
		if seen[pgno] {
			return 0, fmt.Errorf("betree: interior spine cycles at page %d", pgno)
		}
		seen[pgno] = true
		typ, err := t.pageType(pgno)
		if err != nil {
			return 0, err
		}
		if typ == format.PageBTreeLeaf {
			return pgno, nil
		}
		in, err := t.viewInterior(pgno)
		if err != nil {
			return 0, err
		}
		pgno = in.route(ik)
	}
}

// initRangeDelFlag sets the sticky hasRangeDel flag when the on-disk run or the interior
// buffers already hold a range-begin marker, so a reopened database that carries a range
// delete takes the correct full-gather path from its first read. It runs once at Open before
// any concurrent use, so it needs no latch. Markers written after Open set the flag on the
// write path (tail.go) instead, so the two together cover every range delete the tree holds.
func (t *Tree) initRangeDelFlag() error {
	cells, _, err := t.loadRun()
	if err != nil {
		return err
	}
	for _, c := range cells {
		if format.KindOf(c.key) == format.KindRangeBegin {
			t.hasRangeDel.Store(true)
			return nil
		}
	}
	buffered, err := t.collectBufferedMessages()
	if err != nil {
		return err
	}
	for _, c := range buffered {
		if format.KindOf(c.key) == format.KindRangeBegin {
			t.hasRangeDel.Store(true)
			return nil
		}
	}
	return nil
}
