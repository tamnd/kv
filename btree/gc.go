package btree

import (
	"bytes"
	"context"
	"sort"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// Maintain implements engine.Engine. Its job for M2 is version GC: it reclaims the
// space held by the dead version history at or below the watermark (spec 05 §6,
// spec 10 §6). The watermark is the oldest version any live or future reader can
// still observe, so every version at or below it can be collapsed to the single
// value a snapshot at the watermark resolves -- no reader will ever see the
// intermediate versions again. Lazy node merge, which would reclaim the leaves GC
// empties, is a later milestone; the SPI does not change when it lands.
func (t *BTree) Maintain(ctx context.Context, budget engine.MaintBudget) (engine.MaintReport, error) {
	if budget.Watermark == 0 {
		// Nothing sits below version 0, so there is no dead history to reclaim.
		return engine.MaintReport{}, nil
	}
	return t.gcVersions(budget.Watermark, budget.MaxPages)
}

// gcVersions collapses dead version history at or below w in two phases over the
// B-link leaf chain. First it collapses each user key's versions at or below w into
// the single value a snapshot at w resolves, keeping every version above w verbatim.
// Then, only if the whole chain was collapsed within the page budget, it drops the
// range-delete markers at or below w and rebuilds the in-memory interval set.
//
// The two-phase split is what makes GC crash-safe. A covered key's collapse already
// folds in its covering range delete, so a marker cell becomes redundant only once
// every key it covers has been collapsed; coverage runs rightward without bound, so
// that is guaranteed solely after a complete pass. Dropping a marker any earlier
// could, after a checkpoint, leave a covered key further right resurrected. Phase one
// only ever removes read-redundant cells, so it is safe to stop at any leaf and finish
// the pass on a later call.
func (t *BTree) gcVersions(w uint64, maxPages int) (engine.MaintReport, error) {
	var report engine.MaintReport

	// Resume a mid-chain pass only at the same watermark it adopted; otherwise start
	// over from the leftmost leaf so the whole chain is collapsed against one watermark
	// before any marker is dropped.
	var pgno format.PageNo
	var err error
	if t.gcActive && t.gcResumeW == w {
		pgno = t.gcResume
	} else {
		pgno, err = t.leftmostLeaf()
		if err != nil {
			return report, err
		}
	}

	completed := true
	pages := 0
	for pgno != format.NoPage {
		if maxPages > 0 && pages >= maxPages {
			completed = false
			break
		}
		l, err := t.loadLeaf(pgno)
		if err != nil {
			return report, err
		}
		pages++
		next := l.next
		nl, changed := gcCollapseLeaf(l, w, t.rangeDels, t.merge)
		if changed {
			report.BytesReclaimed += int64(len(marshalLeaf(l)) - len(marshalLeaf(nl)))
			if err := t.storeLeaf(pgno, nl); err != nil {
				return report, err
			}
			report.PagesCompacted++
		}
		pgno = next
	}

	if !completed {
		// The page budget ran out before the chain end. The collapse done so far is
		// read-equivalent, but the markers cannot be dropped until every leaf is
		// collapsed, so record where to resume and ask to be called again.
		t.gcActive = true
		t.gcResume = pgno
		t.gcResumeW = w
		report.More = true
		return report, nil
	}
	t.gcActive = false
	t.gcResume = format.NoPage
	t.gcResumeW = 0

	// The whole chain is collapsed, so every range-delete marker at or below w is now
	// folded into the keys it covered and can be dropped.
	reclaimed, err := t.dropDeadMarkers(w)
	if err != nil {
		return report, err
	}
	report.BytesReclaimed += reclaimed
	return report, nil
}

// gcCollapseLeaf rewrites one leaf, collapsing every user key's versions at or below
// w into the single value a snapshot at w resolves. Versions above w are kept
// verbatim, as is every range-delete marker (markers are dropped, when dead, only by
// dropDeadMarkers after the whole chain is collapsed). It returns the original leaf
// and false when nothing changed, or a fresh leaf and true otherwise; the returned
// leaf's cells stay in ascending internal-key order and its B-link next is preserved.
func gcCollapseLeaf(l *leaf, w uint64, rangeDels []format.RangeDel, merge func(existing, operand []byte) []byte) (*leaf, bool) {
	out := &leaf{next: l.next}
	changed := false

	i := 0
	for i < len(l.keys) {
		uk := format.UserKey(l.keys[i])

		// Gather the group for this user key (cells are contiguous, ascending internal
		// order == newest version first). Cells kept verbatim are every marker and every
		// value version above w; the value versions at or below w collapse.
		var leOps []format.Op
		j := i
		for j < len(l.keys) && format.CompareUser(format.UserKey(l.keys[j]), uk) == 0 {
			ik, val := l.keys[j], l.vals[j]
			j++
			k := format.KindOf(ik)
			if k == format.KindRangeBegin || k == format.KindRangeEnd {
				out.keys = append(out.keys, ik)
				out.vals = append(out.vals, val)
				continue
			}
			if format.Version(ik) > w {
				out.keys = append(out.keys, ik)
				out.vals = append(out.vals, val)
				continue
			}
			leOps = append(leOps, format.Op{Version: format.Version(ik), Kind: k, Value: val})
		}
		i = j

		if len(leOps) == 0 {
			continue
		}

		rd := format.NewestCoveringRangeDel(rangeDels, uk, w)
		val, ok := format.Fold(leOps, w, rd, merge)
		if !ok {
			// The key resolves absent at the watermark: drop its whole history at or
			// below w. A version above w, if any, was already kept and stands alone.
			changed = true
			continue
		}

		// Replace the history at or below w with a single Set at its newest such version
		// carrying the resolved value. A lone, already-canonical set is left untouched so
		// a steady-state leaf reports no change.
		if !(len(leOps) == 1 && leOps[0].Kind == format.KindSet && bytes.Equal(leOps[0].Value, val)) {
			changed = true
		}
		out.keys = append(out.keys, format.EncodeInternalKey(uk, leOps[0].Version, format.KindSet))
		out.vals = append(out.vals, append([]byte(nil), val...))
	}

	if !changed {
		return l, false
	}
	sortLeafCells(out)
	return out, true
}

// dropDeadMarkers removes the range-delete marker cells at or below w from every leaf
// and rebuilds the in-memory interval set from the survivors. It runs only after the
// whole chain has been version-collapsed, so every key a dropped marker covered now
// resolves without it. It returns the bytes reclaimed.
func (t *BTree) dropDeadMarkers(w uint64) (int64, error) {
	var reclaimed int64
	pgno, err := t.leftmostLeaf()
	if err != nil {
		return 0, err
	}
	for pgno != format.NoPage {
		l, err := t.loadLeaf(pgno)
		if err != nil {
			return 0, err
		}
		next := l.next
		nl := &leaf{next: l.next}
		dropped := false
		for idx := range l.keys {
			k := format.KindOf(l.keys[idx])
			if (k == format.KindRangeBegin || k == format.KindRangeEnd) && format.Version(l.keys[idx]) <= w {
				dropped = true
				continue
			}
			nl.keys = append(nl.keys, l.keys[idx])
			nl.vals = append(nl.vals, l.vals[idx])
		}
		if dropped {
			reclaimed += int64(len(marshalLeaf(l)) - len(marshalLeaf(nl)))
			if err := t.storeLeaf(pgno, nl); err != nil {
				return 0, err
			}
		}
		pgno = next
	}
	return reclaimed, t.rebuildRangeDels()
}

// sortLeafCells restores ascending internal-key order after a collapse may have left a
// group's surviving cells out of order (a marker can sort between a kept version and
// the collapsed cell). Keys and values move together.
func sortLeafCells(l *leaf) {
	idx := make([]int, len(l.keys))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		return format.CompareInternal(l.keys[idx[a]], l.keys[idx[b]]) < 0
	})
	keys := make([][]byte, len(l.keys))
	vals := make([][]byte, len(l.vals))
	for i, src := range idx {
		keys[i] = l.keys[src]
		vals[i] = l.vals[src]
	}
	l.keys, l.vals = keys, vals
}
