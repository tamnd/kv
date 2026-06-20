package lsm

import (
	"bytes"
	"sort"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// Compaction is where the LSM pays down the debt a flush runs up (spec 06 §6). Each
// flush adds an L0 segment, so without compaction the segment set grows without bound:
// a point read probes every segment's filter, a range scan seeks every segment, and
// every superseded version lingers on disk as space the live data does not need.
// Compaction merges segments down through the levels, dropping the versions no reader
// can still see and the tombstones nothing lives under, and frees the inputs' pages.
//
// The on-disk tree is a stack of levels. L0 receives flushed memtables and its segments
// may overlap in key range. L1 and below hold key-range-disjoint segments sorted by
// their first key, the classic leveled invariant, so each such level is one sorted run
// split into several size-bounded pieces. Each level targets a size a fixed ratio T
// larger than the one above it; a level over its target is a compaction candidate.
//
// One Maintain call runs one compaction unit: pick the level most over target, take a
// bounded input set from it (all of L0, or one segment of a deeper level) plus the
// segments it overlaps in the level below, merge them with the shared streaming merge,
// and write the survivors back as one or more size-bounded segments in the lower level.
// Cutting the output at segTargetBytes is what makes the policy incremental: a level
// holds several runs, so the next compaction touches only the overlapping subset rather
// than rewriting the whole level. Version GC rides the merge, anchored on the host's
// watermark (spec 10 §6); at the deepest level, where nothing lives below, point
// tombstones at or below the watermark are dropped outright.
//
// This slice runs leveled compaction at every level. The lazy-leveling refinement
// (tiered at the largest level) and the Monkey per-level filter tuning are later slices
// built on this structure; the level machinery and the budgeted picker are what this
// slice puts in place.

const (
	// defaultL0Trigger is the L0 segment count at or above which L0 is a compaction
	// candidate, the read fan-in L0 tolerates before it is merged into L1.
	defaultL0Trigger = 4
	// defaultLevelRatio is T, the size multiple between adjacent levels: each level
	// targets T times the bytes of the one above it.
	defaultLevelRatio = 10
	// defaultL1TargetBytes is L1's size target; deeper levels grow from it by T.
	defaultL1TargetBytes int64 = 8 << 20 // 8 MiB
	// defaultSegTargetBytes is the size a compaction output segment is cut at, so a
	// level is several runs and a compaction touches only the overlapping ones.
	defaultSegTargetBytes = 2 << 20 // 2 MiB
)

// allSegmentsLocked returns every segment across every level, the flat view the read
// paths fold (resolution depends on the version in each key, not the level a segment
// sits at, so a read need not care about levels). The caller holds l.mu.
func (l *LSM) allSegmentsLocked() []*segment {
	var segs []*segment
	for _, lvl := range l.levels {
		segs = append(segs, lvl...)
	}
	return segs
}

// addSegmentLocked inserts seg into the given level, growing the level slice as needed
// and keeping every level below L0 sorted by first key so the non-overlapping run reads
// in order. The caller holds l.mu.
func (l *LSM) addSegmentLocked(level int, seg *segment) {
	for len(l.levels) <= level {
		l.levels = append(l.levels, nil)
	}
	l.levels[level] = append(l.levels[level], seg)
	if level >= 1 {
		sort.Slice(l.levels[level], func(i, j int) bool {
			return format.CompareUser(l.levels[level][i].minKey, l.levels[level][j].minKey) < 0
		})
	}
}

// removeSegmentsLocked drops every segment in set from the given level. The caller holds
// l.mu.
func (l *LSM) removeSegmentsLocked(level int, set map[*segment]bool) {
	if level >= len(l.levels) || len(set) == 0 {
		return
	}
	kept := l.levels[level][:0]
	for _, s := range l.levels[level] {
		if !set[s] {
			kept = append(kept, s)
		}
	}
	l.levels[level] = kept
}

// setOf collects segments into a membership set for removeSegmentsLocked.
func setOf(segs []*segment) map[*segment]bool {
	m := make(map[*segment]bool, len(segs))
	for _, s := range segs {
		m[s] = true
	}
	return m
}

// levelBytesLocked reports the on-disk byte footprint of a level. The caller holds l.mu.
func (l *LSM) levelBytesLocked(i int) int64 {
	if i < 0 || i >= len(l.levels) {
		return 0
	}
	pageSize := int64(l.pgr.PageSize())
	var b int64
	for _, s := range l.levels[i] {
		b += int64(s.pages) * pageSize
	}
	return b
}

// levelTargetBytesLocked is level i's size target: L1's base scaled by the level ratio
// for each level below L1. L0 has no byte target (it triggers on segment count), so this
// returns zero for it.
func (l *LSM) levelTargetBytesLocked(i int) int64 {
	if i < 1 {
		return 0
	}
	t := l.l1TargetBytes
	for j := 1; j < i; j++ {
		t *= int64(l.levelRatio)
	}
	return t
}

// hasSegmentsBelowLocked reports whether any level deeper than the given one holds a
// segment, the test for whether a compaction output is at the bottom of the tree and may
// therefore drop tombstones. The caller holds l.mu.
func (l *LSM) hasSegmentsBelowLocked(level int) bool {
	for i := level + 1; i < len(l.levels); i++ {
		if len(l.levels[i]) > 0 {
			return true
		}
	}
	return false
}

// overlappingLocked returns the segments in a level whose key range intersects the
// half-closed user-key span [lo, hi]. The caller holds l.mu.
func (l *LSM) overlappingLocked(level int, lo, hi []byte) []*segment {
	if level < 0 || level >= len(l.levels) {
		return nil
	}
	var out []*segment
	for _, s := range l.levels[level] {
		if s.minKey == nil {
			continue
		}
		if format.CompareUser(s.maxKey, lo) >= 0 && format.CompareUser(s.minKey, hi) <= 0 {
			out = append(out, s)
		}
	}
	return out
}

// pickSegmentLocked chooses one segment from a level below L0 to compact, rotating a
// per-level cursor across the level so successive compactions spread over the key space
// instead of repeatedly rewriting the same low range. The caller holds l.mu.
func (l *LSM) pickSegmentLocked(level int) *segment {
	segs := l.levels[level]
	if len(segs) == 0 {
		return nil
	}
	// segs is sorted by first key; pick the first whose first key is at or past the
	// cursor, wrapping to the start when the cursor has passed the level's last segment.
	cur := l.compactCursor[level]
	pick := segs[0]
	for _, s := range segs {
		if cur == nil || format.CompareUser(s.minKey, cur) >= 0 {
			pick = s
			break
		}
	}
	l.compactCursor[level] = append([]byte(nil), pick.maxKey...)
	return pick
}

// pickCompactionLocked scores every level against its target and returns the level most
// over it, or ok=false when no level needs compaction. L0 scores on segment count
// against its trigger; deeper levels score on bytes against their size target. Both
// scores are normalized so 1.0 means at target, so the larger score is the more urgent
// level regardless of which metric it uses. The caller holds l.mu.
func (l *LSM) pickCompactionLocked() (int, bool) {
	best := -1
	var bestScore float64
	if len(l.levels) > 0 && len(l.levels[0]) >= l.l0Trigger {
		best = 0
		bestScore = float64(len(l.levels[0])) / float64(l.l0Trigger)
	}
	for i := 1; i < len(l.levels); i++ {
		if len(l.levels[i]) == 0 {
			continue
		}
		target := l.levelTargetBytesLocked(i)
		if target <= 0 {
			continue
		}
		score := float64(l.levelBytesLocked(i)) / float64(target)
		if score > 1 && score > bestScore {
			best = i
			bestScore = score
		}
	}
	return best, best >= 0
}

// keyRangeOf returns the smallest and largest user key across a set of segments, the
// span a compaction's inputs cover.
func keyRangeOf(segs []*segment) (lo, hi []byte) {
	for _, s := range segs {
		if s.minKey == nil {
			continue
		}
		if lo == nil || format.CompareUser(s.minKey, lo) < 0 {
			lo = s.minKey
		}
		if hi == nil || format.CompareUser(s.maxKey, hi) > 0 {
			hi = s.maxKey
		}
	}
	return lo, hi
}

// runCompactionLocked merges a bounded input set from the source level into the level
// below it, installs the result through the MANIFEST, and frees the inputs. The caller
// holds l.mu. It returns a report of the work done, or a zero report when there was
// nothing to merge.
//
// The inputs are all of L0 (its segments overlap, so they compact as a unit) or one
// segment of a deeper level, plus every segment they overlap in the level below. The
// merge is the shared streaming k-way merge, so the output arrives in internal-key
// order; the splitter applies version GC and tombstone dropping as the cells stream and
// cuts the output into size-bounded segments. Every overlapping lower-level segment is
// an input, so the outputs and the lower-level segments left behind stay
// non-overlapping, preserving the leveled invariant.
func (l *LSM) runCompactionLocked(src int, watermark uint64) (engine.MaintReport, error) {
	if src < 0 || src >= len(l.levels) || len(l.levels[src]) == 0 {
		return engine.MaintReport{}, nil
	}

	var in0 []*segment
	if src == 0 {
		in0 = append(in0, l.levels[0]...)
	} else {
		seg := l.pickSegmentLocked(src)
		if seg == nil {
			return engine.MaintReport{}, nil
		}
		in0 = append(in0, seg)
	}

	lo, hi := keyRangeOf(in0)
	out := src + 1
	in1 := l.overlappingLocked(out, lo, hi)

	inputs := make([]*segment, 0, len(in0)+len(in1))
	inputs = append(inputs, in0...)
	inputs = append(inputs, in1...)

	pageSize := int64(l.pgr.PageSize())
	var before int64
	for _, seg := range inputs {
		before += int64(seg.pages) * pageSize
	}

	// The output is the deepest level reached when nothing lives below it, the case
	// where a point tombstone shadows nothing and can be dropped outright.
	dropTomb := !l.hasSegmentsBelowLocked(out)

	sources := make([]mergeSource, len(inputs))
	for i, seg := range inputs {
		sources[i] = &segSource{pgr: l.pgr, seg: seg}
	}
	mi, err := newMergeIter(sources, nil)
	if err != nil {
		return engine.MaintReport{}, err
	}

	outputs, err := l.writeSplitLocked(mi, watermark, dropTomb, bloomBitsForLevel(out, l.levelRatio))
	if err != nil {
		return engine.MaintReport{}, err
	}

	// Install the new view through the MANIFEST: remove every input at its level, then
	// add every output at the lower level, so a replay drops the inputs and keeps the
	// outputs. The edits are recorded before the live set is swapped, the LSM's atomic
	// version install.
	for _, seg := range in0 {
		if err := l.appendEditLocked(manifestRemove, uint8(src), seg.footer); err != nil {
			return engine.MaintReport{}, err
		}
	}
	for _, seg := range in1 {
		if err := l.appendEditLocked(manifestRemove, uint8(out), seg.footer); err != nil {
			return engine.MaintReport{}, err
		}
	}
	for _, seg := range outputs {
		if err := l.appendEditLocked(manifestAdd, uint8(out), seg.footer); err != nil {
			return engine.MaintReport{}, err
		}
	}

	l.removeSegmentsLocked(src, setOf(in0))
	l.removeSegmentsLocked(out, setOf(in1))
	for _, seg := range outputs {
		l.addSegmentLocked(out, seg)
	}

	for _, seg := range inputs {
		if err := l.freeSegmentPages(seg); err != nil {
			return engine.MaintReport{}, err
		}
	}

	var after int64
	for _, seg := range outputs {
		after += int64(seg.pages) * pageSize
	}
	reclaimed := before - after
	if reclaimed < 0 {
		reclaimed = 0
	}
	return engine.MaintReport{
		PagesCompacted: len(inputs),
		BytesWritten:   after,
		BytesReclaimed: reclaimed,
	}, nil
}

// writeSplitLocked drives the merge through a splitter, writing the kept cells into one
// or more size-bounded output segments, each filter sized for the output level by the
// Monkey budget bitsPerKey. The caller holds l.mu. An empty trailing segment (when the
// final pull dropped every cell) is reclaimed rather than published, so a compaction
// whose tail is all dead versions leaks no pages.
func (l *LSM) writeSplitLocked(mi *mergeIter, watermark uint64, dropTomb bool, bitsPerKey int) ([]*segment, error) {
	target := l.segTargetBytes
	if target < 1 {
		target = 1
	}
	sp := &splitter{mi: mi, watermark: watermark, dropTomb: dropTomb, target: target}
	var outs []*segment
	for !sp.exhausted {
		seg, err := writeSegment(l.pgr, bitsPerKey, sp.fill)
		if err != nil {
			return nil, err
		}
		if sp.err != nil {
			return nil, sp.err
		}
		if seg.numCells > 0 {
			outs = append(outs, seg)
			continue
		}
		if err := l.freeSegmentPages(seg); err != nil {
			return nil, err
		}
	}
	return outs, nil
}

// splitter feeds the merge into writeSegment, applying the version-drop rule and cutting
// the stream into size-bounded segments at version-group boundaries (a group never
// straddles two segments, the segment analog of the never-split-a-group page rule).
type splitter struct {
	mi        *mergeIter
	watermark uint64
	dropTomb  bool
	target    int

	curUser   []byte
	keptBase  bool
	exhausted bool
	err       error
}

// fill emits cells for one output segment, returning when the segment reaches its target
// at a group boundary or the merge runs dry. Every call that finds a live merge advances
// it at least once, so the outer loop always makes progress.
func (s *splitter) fill(emit func(ik, val []byte) bool) {
	bytesThis := 0
	for s.mi.valid() {
		ik := s.mi.key()
		val := s.mi.value()
		uk := format.UserKey(ik)
		if s.curUser == nil || !bytes.Equal(uk, s.curUser) {
			// A new group: cut here if the segment is already at target, so the group
			// starts the next segment whole.
			if bytesThis >= s.target {
				return
			}
			s.curUser = append(s.curUser[:0], uk...)
			s.keptBase = false
		}
		if s.keep(ik) {
			if !emit(ik, val) {
				return
			}
			bytesThis += len(ik) + len(val) + 2
		}
		if err := s.mi.next(); err != nil {
			s.err = err
			return
		}
	}
	s.exhausted = true
}

// keep applies the version-drop rule to one cell, newest-first within its group:
//   - keep every version newer than the watermark;
//   - among versions at or below the watermark, keep up to and including the first set or
//     delete (the base a watermark snapshot resolves to), then drop the rest;
//   - always keep range-delete markers, which cover other keys and are not shadowed by a
//     newer version of their own key;
//   - at the deepest level, drop a point delete that becomes the base, since nothing
//     lives below it for the tombstone to shadow.
func (s *splitter) keep(ik []byte) bool {
	kind := format.KindOf(ik)
	if kind == format.KindRangeBegin {
		return true
	}
	if format.Version(ik) <= s.watermark {
		if s.keptBase {
			return false
		}
		if kind == format.KindSet || kind == format.KindDelete {
			s.keptBase = true
			if s.dropTomb && kind == format.KindDelete {
				return false
			}
		}
	}
	return true
}

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
