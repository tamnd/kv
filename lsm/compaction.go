package lsm

import (
	"bytes"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// Compaction is where the LSM pays down the debt a flush runs up (spec 06 §6). Each
// flush adds a segment, so the segment set grows without bound: a point read probes
// every segment's filter and a range scan seeks every segment, and every superseded
// version lingers on disk as space the live data does not need. Compaction merges
// segments back into one, dropping the versions no reader can still see, and frees the
// inputs' pages to the freelist.
//
// This slice runs one shape of compaction: a major merge of the whole segment set into
// a single segment. It bounds read fan-in to one segment and collapses each key's
// history down to the versions a live reader could observe. The incremental, budgeted,
// level-aware Fluid policy (leveled at the small levels, tiered at the largest) that
// picks a partial input set per Maintain call is a later slice; the merge primitive and
// the version-drop rule it builds on are what this slice puts in place.
//
// Version GC is anchored on the watermark the host supplies (spec 10 §6): the oldest
// version any current or future reader can observe. Every version at or below it
// collapses to the single value a snapshot at the watermark resolves, because no
// snapshot below the watermark will ever be taken again. A watermark of zero disables
// the drop, so a compaction with no GC horizon rewrites every version verbatim.

// compactionTrigger is the segment count at or above which Maintain runs a compaction.
// It is the read fan-in the core tolerates before paying to merge it back down; the
// level-aware policy that replaces this flat threshold tunes it per level.
const compactionTrigger = 4

// chainPages walks an LSM page chain from head following the common header's overflow
// slot and returns every page number in it. It is how compaction enumerates the index,
// range-delete, and filter chains of a segment it is about to free.
func (l *LSM) chainPages(head format.PageNo) ([]format.PageNo, error) {
	var pages []format.PageNo
	for pgno := head; pgno != format.NoPage; {
		fr, err := l.pgr.Get(pgno, pager.Read)
		if err != nil {
			return nil, err
		}
		next := format.DecodeCommonHeader(fr.Data()).Overflow
		l.pgr.Unpin(fr, false)
		pages = append(pages, pgno)
		pgno = next
	}
	return pages, nil
}

// freeSegmentPages returns every page a segment owns to the freelist: its data pages
// (named directly by the block index), its index, range-delete, and filter chains, and
// its footer. The caller holds l.mu and has already removed the segment from the live
// set and recorded the removal in the MANIFEST, so no reader can still reach these
// pages. The freed pages are reused only after the next checkpoint, the same discipline
// the B-tree core frees pages under, so a crash before the checkpoint leaves the old
// segment intact.
func (l *LSM) freeSegmentPages(seg *segment) error {
	for _, e := range seg.index {
		l.pgr.Free(e.page)
	}
	for _, head := range []format.PageNo{seg.indexHead, seg.rdHead, seg.filterHead} {
		pages, err := l.chainPages(head)
		if err != nil {
			return err
		}
		for _, pgno := range pages {
			l.pgr.Free(pgno)
		}
	}
	l.pgr.Free(seg.footer)
	return nil
}

// compactLocked merges the whole segment set into one segment, dropping dead versions
// at or below the watermark, installs the result through the MANIFEST, and frees the
// inputs. The caller holds l.mu. It returns the number of input segments merged, zero
// when there was nothing to do.
//
// The merge is the same k-way streaming merge a range scan uses, so the output cells
// arrive in internal-key order, exactly what writeSegment consumes. A per-group filter
// applies the version-drop rule as the cells stream through, so the output is built in
// one pass with no intermediate buffering of the keyspace.
func (l *LSM) compactLocked(watermark uint64) (int, error) {
	if len(l.segments) < 2 {
		return 0, nil
	}
	inputs := l.segments

	sources := make([]mergeSource, len(inputs))
	for i, seg := range inputs {
		sources[i] = &segSource{pgr: l.pgr, seg: seg}
	}
	mi, err := newMergeIter(sources, nil)
	if err != nil {
		return 0, err
	}

	// The version-drop rule, applied newest-first within each user key's group:
	//   - keep every version newer than the watermark;
	//   - among versions at or below the watermark, keep them up to and including the
	//     first set or delete (the base a snapshot at the watermark resolves to), then
	//     drop the rest, since nothing below that base is visible to any live reader;
	//   - always keep range-delete markers: a marker covers a span of other keys, so it
	//     is not shadowed by a newer version of its own key and dropping it would
	//     resurrect keys the delete still covers.
	var curUser []byte
	keptBase := false
	var mergeErr error
	out, err := writeSegment(l.pgr, func(emit func(ik, val []byte) bool) {
		for mi.valid() {
			ik := mi.key()
			val := mi.value()
			uk := format.UserKey(ik)
			if curUser == nil || !bytes.Equal(uk, curUser) {
				curUser = append(curUser[:0], uk...)
				keptBase = false
			}
			keep := true
			if format.KindOf(ik) != format.KindRangeBegin && format.Version(ik) <= watermark {
				if keptBase {
					keep = false
				} else if k := format.KindOf(ik); k == format.KindSet || k == format.KindDelete {
					keptBase = true
				}
			}
			if keep {
				if !emit(ik, val) {
					return
				}
			}
			if err := mi.next(); err != nil {
				mergeErr = err
				return
			}
		}
	})
	if mergeErr != nil {
		return 0, mergeErr
	}
	if err != nil {
		return 0, err
	}

	// Install the new view: remove every input from the MANIFEST, then add the output,
	// so a replay that sees these edits drops the inputs and keeps only the merge. The
	// output is published to the live set only after its edit is recorded, the LSM's
	// atomic version install.
	for _, seg := range inputs {
		if err := l.appendEditLocked(manifestRemove, seg.footer); err != nil {
			return 0, err
		}
	}
	if out.numCells > 0 {
		if err := l.appendEditLocked(manifestAdd, out.footer); err != nil {
			return 0, err
		}
		l.segments = []*segment{out}
	} else {
		l.segments = nil
	}

	for _, seg := range inputs {
		if err := l.freeSegmentPages(seg); err != nil {
			return 0, err
		}
	}
	return len(inputs), nil
}
