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
	if budget.Watermark == 0 && budget.Now == 0 {
		// Nothing sits below version 0 to collapse, and with no clock there is no expiry
		// to sweep, so there is no work to do.
		return engine.MaintReport{}, nil
	}
	return t.gcVersions(budget.Watermark, budget.Now, budget.MaxPages)
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
func (t *BTree) gcVersions(w, sweepNow uint64, maxPages int) (engine.MaintReport, error) {
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
		nl, changed, swept := gcCollapseLeaf(l, w, sweepNow, t.rangeDels, t.merge)
		if changed {
			report.BytesReclaimed += int64(leafEncodedSize(l) - leafEncodedSize(nl))
			report.ExpiredSwept += swept
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
// w into the single value a snapshot at w resolves, and sweeping any expired TTL set
// into a tombstone. Versions above w are kept verbatim, as is every range-delete marker
// (markers are dropped, when dead, only by dropDeadMarkers after the whole chain is
// collapsed). It returns the original leaf and false when nothing changed, or a fresh
// leaf and true otherwise, plus the count of TTL sets swept; the returned leaf's cells
// stay in ascending internal-key order and its B-link next is preserved.
//
// The sweep is version-independent: a TTL set whose expiry is at or before sweepNow is
// invisible to every reader (read resolution already folds it to absent at the current
// clock, and reads are never pinned in time), so rewriting it to a tombstone at its own
// version changes nothing observable while reclaiming the framed value bytes. A swept
// cell above w is kept as a verbatim tombstone; one at or below w folds with the rest of
// the history. A sweepNow of zero disables the sweep, which is what recovery passes.
func gcCollapseLeaf(l *leaf, w, sweepNow uint64, rangeDels []format.RangeDel, merge func(existing, operand []byte) []byte) (*leaf, bool, int64) {
	out := &leaf{next: l.next}
	changed := false
	var swept int64

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
				// Range markers resolve out of band, never as ops.
				out.keys = append(out.keys, ik)
				out.vals = append(out.vals, val)
				continue
			}
			if k == format.KindSetWithTTL {
				expiry, _ := format.DecodeTTLValue(val)
				if sweepNow != 0 && expiry != 0 && expiry <= sweepNow {
					// Sweep: the deadline has passed, so reclaim the framed value and leave a
					// tombstone at the same version. It then either stands verbatim (above w)
					// or folds with the rest of the history (at or below w) below.
					ik = format.EncodeInternalKey(uk, format.Version(ik), format.KindDelete)
					val = nil
					k = format.KindDelete
					changed = true
					swept++
				} else {
					// A live or non-expiring TTL set is kept verbatim so a later read still
					// resolves it; version GC never re-encodes its framed value as a plain set
					// and silently strips the expiry.
					out.keys = append(out.keys, ik)
					out.vals = append(out.vals, val)
					continue
				}
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
		return l, false, 0
	}
	sortLeafCells(out)
	return out, true, swept
}

// dropDeadMarkers removes the range-delete marker cells at or below w from every leaf
// and rebuilds the in-memory interval set from the survivors. It runs only after the
// whole chain has been version-collapsed, so every key a dropped marker covered now
// resolves without it. It returns the bytes reclaimed.
func (t *BTree) dropDeadMarkers(w uint64) (int64, error) {
	// A marker cell can be dead only if a live range delete sits at or below the watermark.
	// The in-memory set mirrors every marker's version (begin and end share a version and are
	// dropped together), so when none is at or below w there is nothing to drop, the set
	// already matches the leaves, and the whole-chain scan plus the full-tree rebuildRangeDels
	// it ends with would only reproduce the set byte for byte. Skip both. A pure-overwrite
	// workload has no range deletes at all, so this turns the GC tail into a single comparison
	// instead of a full leaf-chain decode every pass (perf/11 W3).
	dead := false
	for i := range t.rangeDels {
		if t.rangeDels[i].Version <= w {
			dead = true
			break
		}
	}
	if !dead {
		return 0, nil
	}

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
			reclaimed += int64(leafEncodedSize(l) - leafEncodedSize(nl))
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
