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
// may overlap in key range. The middle levels are leveled: each holds key-range-disjoint
// segments sorted by their first key, so the level is one sorted run split into several
// size-bounded pieces. Each level targets a size a fixed ratio T larger than the one
// above it; a level over its target is a compaction candidate.
//
// The largest (deepest) populated level is tiered, not leveled, the Dostoevsky
// lazy-leveling shape (spec 06 §6). Most of the data lives at the bottom, and keeping it
// as one perfectly sorted run is what costs the most write amplification: every
// compaction into it would rewrite a slice of the whole keyspace. So a compaction into
// the bottom adds its output as another run beside the runs already there rather than
// merging into them, and the bottom self-merges only when it has stacked up tierFanout
// runs over some region. That trades a little read and space cost at the cold bottom for
// a factor of tierFanout fewer rewrites of it.
//
// One Maintain call runs one compaction unit. The picker scores every level and runs the
// most urgent of three actions: push one run from a leveled level (all of L0, or one
// segment deeper) down into the level below, merging with the segments it overlaps there;
// self-merge the tiered bottom when its runs have stacked too deep, folding them back to
// one disjoint run; or descend the tiered bottom when it has outgrown its size target,
// moving the whole level down a step so a new tiered bottom forms beneath the leveled
// levels above. Cutting output at segTargetBytes keeps a leveled level several runs, so a
// push-down touches only the overlapping subset. Version GC rides every merge, anchored
// on the host's watermark (spec 10 §6); a point tombstone is dropped outright only in a
// self-merge of the bottom, where every run is an input and nothing lives below for it to
// shadow.

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
	// defaultTierFanout is the run overlap the tiered bottom level accumulates over a
	// region before it self-merges, the bottom-level analog of the level ratio.
	defaultTierFanout = 10
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

// isTieredLocked reports whether a level is the tiered bottom: the deepest level that
// still holds a segment. That level accumulates runs instead of merging into one, while
// every level above it stays leveled. The caller holds l.mu.
func (l *LSM) isTieredLocked(level int) bool {
	if level < 1 || level >= len(l.levels) || len(l.levels[level]) == 0 {
		return false
	}
	return !l.hasSegmentsBelowLocked(level)
}

// maxOverlapLocked returns the largest number of segments in a level that cover a single
// user key, the run depth of a tiered level. A single disjoint run reports 1 however many
// pieces it is cut into, and K runs stacked over the same region report K, so it is the
// metric the tiered bottom self-merges on. It is computed by a sweep over the segments'
// closed key ranges, which already live in their footers, so it needs no extra state and
// recomputes correctly after a reopen. The caller holds l.mu.
func (l *LSM) maxOverlapLocked(level int) int {
	if level < 0 || level >= len(l.levels) {
		return 0
	}
	segs := l.levels[level]
	if len(segs) < 2 {
		return len(segs)
	}
	type event struct {
		key  []byte
		open bool
	}
	evs := make([]event, 0, len(segs)*2)
	for _, s := range segs {
		if s.minKey == nil {
			continue
		}
		evs = append(evs, event{s.minKey, true}, event{s.maxKey, false})
	}
	sort.Slice(evs, func(i, j int) bool {
		if c := format.CompareUser(evs[i].key, evs[j].key); c != 0 {
			return c < 0
		}
		// At an equal key an open sorts before a close, so two runs that touch at a
		// boundary count as overlapping there (the ranges are closed).
		return evs[i].open && !evs[j].open
	})
	cur, mx := 0, 0
	for _, e := range evs {
		if e.open {
			cur++
			if cur > mx {
				mx = cur
			}
		} else {
			cur--
		}
	}
	return mx
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

// compactionKind names the action the picker chose for a Maintain call.
type compactionKind int

const (
	// compactNone means no level needs work.
	compactNone compactionKind = iota
	// compactPushDown merges one run from a level down into the level below it.
	compactPushDown
	// compactSelfMerge folds the tiered bottom's stacked runs back into one disjoint run.
	compactSelfMerge
)

// compaction is the picker's decision for one Maintain call: which action to run and at
// which source level. wholeLevel says the input is the whole level (all of L0, or the
// tiered bottom descending) rather than one picked segment.
type compaction struct {
	kind       compactionKind
	level      int
	wholeLevel bool
}

// pickCompactionLocked scores every level and returns the most urgent action, or a zero
// compaction when nothing needs work. L0 scores on segment count against its trigger and
// pushes down. A leveled middle level scores on bytes against its size target and pushes
// one run down. The tiered bottom self-merges when its run overlap reaches tierFanout,
// and otherwise descends when it has outgrown its size target. Scores are normalized so
// 1.0 means at threshold, so the larger score is the more urgent action whatever metric
// it came from. The caller holds l.mu.
func (l *LSM) pickCompactionLocked() compaction {
	best := compaction{kind: compactNone}
	var bestScore float64
	consider := func(c compaction, score float64) {
		if score > bestScore {
			best, bestScore = c, score
		}
	}
	if len(l.levels) > 0 && len(l.levels[0]) >= l.l0Trigger {
		consider(compaction{kind: compactPushDown, level: 0, wholeLevel: true},
			float64(len(l.levels[0]))/float64(l.l0Trigger))
	}
	for i := 1; i < len(l.levels); i++ {
		if len(l.levels[i]) == 0 {
			continue
		}
		if l.isTieredLocked(i) {
			// The tiered bottom: fold the runs back together once they stack too deep,
			// otherwise grow the tree a level once the bottom outgrows its target.
			if ov := l.maxOverlapLocked(i); ov >= l.tierFanout {
				consider(compaction{kind: compactSelfMerge, level: i}, float64(ov)/float64(l.tierFanout))
				continue
			}
			if target := l.levelTargetBytesLocked(i); target > 0 {
				if score := float64(l.levelBytesLocked(i)) / float64(target); score > 1 {
					consider(compaction{kind: compactPushDown, level: i, wholeLevel: true}, score)
				}
			}
			continue
		}
		target := l.levelTargetBytesLocked(i)
		if target <= 0 {
			continue
		}
		if score := float64(l.levelBytesLocked(i)) / float64(target); score > 1 {
			consider(compaction{kind: compactPushDown, level: i, wholeLevel: false}, score)
		}
	}
	return best
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

// runCompactionLocked merges a run from the source level into the level below it,
// installs the result through the MANIFEST, and frees the inputs. The caller holds l.mu.
// It returns a report of the work done, or a zero report when there was nothing to merge.
//
// The input is all of src when wholeLevel is set (L0, whose segments overlap and compact
// as a unit, or the tiered bottom descending a level), otherwise one picked segment of a
// leveled level. When the output level is the deepest, the merge is a tiered add: the
// output joins the bottom's other runs and no lower-level segment is rewritten, which is
// the lazy-leveling saving. When the output level is a middle leveled level, every
// segment the input overlaps there is also an input, so the output and the segments left
// behind stay disjoint, preserving the leveled invariant. The merge is the shared
// streaming k-way merge, so the output arrives in internal-key order; the splitter
// applies version GC and cuts the output into size-bounded segments. A point tombstone is
// never dropped here, since the bottom's other runs (a tiered add) or a deeper level (a
// leveled push-down) may hold an older version it still shadows; only a self-merge of the
// whole bottom drops one.
func (l *LSM) runCompactionLocked(src int, watermark uint64, wholeLevel bool) (engine.MaintReport, error) {
	if src < 0 || src >= len(l.levels) || len(l.levels[src]) == 0 {
		return engine.MaintReport{}, nil
	}

	var in0 []*segment
	if wholeLevel {
		in0 = append(in0, l.levels[src]...)
	} else {
		seg := l.pickSegmentLocked(src)
		if seg == nil {
			return engine.MaintReport{}, nil
		}
		in0 = append(in0, seg)
	}

	lo, hi := keyRangeOf(in0)
	out := src + 1
	// A tiered add when the output is the deepest level: the run joins the bottom beside
	// the runs already there, so nothing there is rewritten. Otherwise a leveled merge,
	// which pulls in the overlapping segments of the lower level.
	tieredOut := !l.hasSegmentsBelowLocked(out)
	var in1 []*segment
	if !tieredOut {
		in1 = l.overlappingLocked(out, lo, hi)
	}

	inputs := make([]*segment, 0, len(in0)+len(in1))
	inputs = append(inputs, in0...)
	inputs = append(inputs, in1...)

	pageSize := int64(l.pgr.PageSize())
	var before int64
	for _, seg := range inputs {
		before += int64(seg.pages) * pageSize
	}

	const dropTomb = false

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

// runSelfMergeLocked folds every run at a tiered level back into one disjoint run, cut at
// segTargetBytes, and installs it in place through the MANIFEST. It is what the bottom
// pays for accumulating runs instead of merging on every push: it runs once per tierFanout
// pushes rather than on each one. Because every run at the level is an input and nothing
// lives below, this is the one merge that may drop a point tombstone that becomes the
// base. The caller holds l.mu.
func (l *LSM) runSelfMergeLocked(level int, watermark uint64) (engine.MaintReport, error) {
	if level < 1 || level >= len(l.levels) || len(l.levels[level]) < 2 {
		return engine.MaintReport{}, nil
	}
	inputs := append([]*segment(nil), l.levels[level]...)

	pageSize := int64(l.pgr.PageSize())
	var before int64
	for _, seg := range inputs {
		before += int64(seg.pages) * pageSize
	}

	dropTomb := !l.hasSegmentsBelowLocked(level)

	sources := make([]mergeSource, len(inputs))
	for i, seg := range inputs {
		sources[i] = &segSource{pgr: l.pgr, seg: seg}
	}
	mi, err := newMergeIter(sources, nil)
	if err != nil {
		return engine.MaintReport{}, err
	}

	outputs, err := l.writeSplitLocked(mi, watermark, dropTomb, bloomBitsForLevel(level, l.levelRatio))
	if err != nil {
		return engine.MaintReport{}, err
	}

	for _, seg := range inputs {
		if err := l.appendEditLocked(manifestRemove, uint8(level), seg.footer); err != nil {
			return engine.MaintReport{}, err
		}
	}
	for _, seg := range outputs {
		if err := l.appendEditLocked(manifestAdd, uint8(level), seg.footer); err != nil {
			return engine.MaintReport{}, err
		}
	}

	l.removeSegmentsLocked(level, setOf(inputs))
	for _, seg := range outputs {
		l.addSegmentLocked(level, seg)
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
