package betree

import (
	"bytes"
	"fmt"
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

// loadRun walks the leaf run from the engine root along right-sibling links,
// decoding each leaf, and returns every cell in run order together with the page
// numbers the run occupies. The caller holds at least a read latch, so a writer
// cannot rewrite the run mid-walk. The cycle guard turns a corrupt sibling loop
// into an error rather than a hang.
func (t *Tree) loadRun() (cells []record, pages []format.PageNo, err error) {
	pgno := t.pgr.Header().EngineRoot
	seen := map[format.PageNo]bool{}
	for pgno != format.NoPage {
		if seen[pgno] {
			return nil, nil, fmt.Errorf("betree: leaf run cycles at page %d", pgno)
		}
		seen[pgno] = true
		lf, derr := t.readLeaf(pgno)
		if derr != nil {
			return nil, nil, derr
		}
		cells = append(cells, lf.records...)
		pages = append(pages, pgno)
		pgno = lf.right
	}
	return cells, pages, nil
}

// readLeaf pins a leaf page for reading, decodes it with the generation-2 leaf
// codec over the usable area, and unpins. decodeLeaf copies every key and value, so
// the returned leaf owns its bytes and stays valid after the unpin and after the
// frame is later evicted or rebound.
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

// rewriteRun packs cells into a fresh chain of generation-2 leaf pages, links the
// siblings, installs the first page as the new engine root, and frees the pages the
// old run occupied. cells must be in ascending internal-key order. Every new page
// is allocated before any old page is freed, so a reused page number can never
// alias a page still live in the new run.
func (t *Tree) rewriteRun(cells []record, oldPages []format.PageNo) error {
	usable := t.pgr.UsablePageSize()

	// Greedily group cells into leaf-sized runs. Growing the trial slice one cell at
	// a time costs O(n^2) encode calls; M0 accepts that because the base is correct
	// and the run is small, and the incremental-insert slice removes the rewrite.
	var groups [][]record
	i := 0
	for i < len(cells) {
		fit := i
		for j := i + 1; j <= len(cells); j++ {
			_, err := encodeLeaf(make([]byte, usable), &leaf{records: cells[i:j], bucketSize: defaultBucketSize})
			if err == ErrPageFull {
				break
			}
			if err != nil {
				return err
			}
			fit = j
		}
		if fit == i {
			return fmt.Errorf("betree: cell does not fit in a page (key %x, value %d bytes)", cells[i].key, len(cells[i].val))
		}
		groups = append(groups, cells[i:fit])
		i = fit
	}
	if len(groups) == 0 {
		groups = [][]record{nil} // an empty run is still one empty leaf, so the root is always valid
	}

	// Reserve every new page number first so the sibling links can point forward to
	// pages not yet written.
	pgnos := make([]format.PageNo, len(groups))
	for k := range groups {
		pgnos[k] = t.pgr.AllocateNumber()
	}
	for k, g := range groups {
		lf := &leaf{records: g, bucketSize: defaultBucketSize}
		if k > 0 {
			lf.left = pgnos[k-1]
		}
		if k+1 < len(groups) {
			lf.right = pgnos[k+1]
		}
		dst := make([]byte, usable)
		if _, err := encodeLeaf(dst, lf); err != nil {
			return err
		}
		fr, err := t.pgr.GetAllocated(pgnos[k])
		if err != nil {
			return err
		}
		copy(fr.Data(), dst)
		t.pgr.Unpin(fr, true)
	}

	t.pgr.Header().EngineRoot = pgnos[0]
	for _, old := range oldPages {
		t.pgr.Free(old)
	}
	return nil
}

// snapshot returns the sorted, MVCC-resolved view at snap by decoding the run and
// folding each user key's version group with the shared format helpers: the same
// fold the shipped cores and the oracle use, so a divergence is always a real bug
// rather than a difference in resolution policy. It rebuilds the range-delete set
// from the run's range-begin markers, the way the shipped btree does, so a read
// folds a range delete whose marker cell a point read never lands on. M0 resolves
// the whole run on every read; the paged descent and zero-copy cursor that make
// reads scale are later slices.
func (t *Tree) snapshot(snap engine.Snapshot) ([]resolved, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	cells, _, err := t.loadRun()
	if err != nil {
		return nil, err
	}
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
