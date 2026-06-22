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
// It returns the chosen action together with its normalized urgency score, so a caller
// that only wants the backlog signal (the Stats path) reads the score without acting on
// it, and the maintenance path ignores the score and runs the action.
func (l *LSM) pickCompactionLocked() (compaction, float64) {
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
	return best, bestScore
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

// A compaction runs in three phases so the expensive middle one need not hold l.mu, the
// same split the background flush uses (flush.go, perf/03 W3/W4). planCompactionLocked picks
// the most urgent action and snapshots its inputs under l.mu; buildCompaction merges those
// immutable inputs into new output segments with l.mu released, so foreground writers keep
// inserting while the merge serializes; installCompactionLocked swaps the live set under
// l.mu in one step. The host's Maintain path runs all three under l.mu (it already holds it
// and wants the simplest contract); the background compactor (compactOnce in flush.go) holds
// l.mu only for the plan and the install. Both take flushMu around the whole thing so only
// one compaction, flush build, or vLog GC writes the value-log and segment pages at a time.

// compactionPlan is one compaction unit's inputs and shape, chosen under l.mu so the merge
// that turns the inputs into outputs can then run with l.mu released. in0 is the source-level
// run (all of a wholeLevel push or one picked segment); in1 the lower-level segments a
// leveled push-down overlaps, empty for a tiered add or a self-merge. For a self-merge
// srcLevel == outLevel and in0 holds every run at the level. before is the inputs' on-disk
// byte size, captured at plan time for the work report.
type compactionPlan struct {
	srcLevel int
	outLevel int
	in0      []*segment
	in1      []*segment
	dropTomb bool
	before   int64
}

// inputBytes is the on-disk byte footprint of a segment set. The caller holds l.mu (it reads
// each segment's page count and the pager's page size).
func (l *LSM) inputBytes(segs []*segment) int64 {
	pageSize := int64(l.pgr.PageSize())
	var b int64
	for _, seg := range segs {
		b += int64(seg.pages) * pageSize
	}
	return b
}

// planCompactionLocked picks the most urgent compaction and snapshots its inputs, or reports
// false when nothing is due. The caller holds l.mu.
func (l *LSM) planCompactionLocked() (*compactionPlan, bool) {
	c, _ := l.pickCompactionLocked()
	switch c.kind {
	case compactPushDown:
		return l.planPushDownLocked(c)
	case compactSelfMerge:
		return l.planSelfMergeLocked(c)
	default:
		return nil, false
	}
}

// planPushDownLocked snapshots a push-down's inputs: the source run plus, for a leveled
// output, the lower-level segments it overlaps. The input is all of src when wholeLevel is
// set (L0, whose segments overlap and compact as a unit, or the tiered bottom descending a
// level), otherwise one picked segment of a leveled level. When the output is the deepest
// level the merge is a tiered add, so no lower-level segment is rewritten (the lazy-leveling
// saving); when it is a middle leveled level every segment the input overlaps there joins the
// merge, so the output and the segments left behind stay disjoint. A point tombstone is never
// dropped here, since the bottom's other runs or a deeper level may still hold an older
// version under it. The caller holds l.mu.
func (l *LSM) planPushDownLocked(c compaction) (*compactionPlan, bool) {
	src := c.level
	if src < 0 || src >= len(l.levels) || len(l.levels[src]) == 0 {
		return nil, false
	}
	var in0 []*segment
	if c.wholeLevel {
		in0 = append(in0, l.levels[src]...)
	} else {
		seg := l.pickSegmentLocked(src)
		if seg == nil {
			return nil, false
		}
		in0 = append(in0, seg)
	}
	lo, hi := keyRangeOf(in0)
	out := src + 1
	var in1 []*segment
	if !l.hasSegmentsBelowLocked(out) {
		// A tiered add: the run joins the deepest level beside the runs already there.
	} else {
		in1 = l.overlappingLocked(out, lo, hi)
	}
	p := &compactionPlan{srcLevel: src, outLevel: out, in0: in0, in1: in1, dropTomb: false}
	p.before = l.inputBytes(in0) + l.inputBytes(in1)
	return p, true
}

// planSelfMergeLocked snapshots a self-merge's inputs: every run at the tiered level, folded
// back into one disjoint run in place. Because every run is an input and (when nothing lives
// below) nothing shadows them, this is the one compaction that may drop a point tombstone
// that becomes the base. The caller holds l.mu.
func (l *LSM) planSelfMergeLocked(c compaction) (*compactionPlan, bool) {
	level := c.level
	if level < 1 || level >= len(l.levels) || len(l.levels[level]) < 2 {
		return nil, false
	}
	inputs := append([]*segment(nil), l.levels[level]...)
	p := &compactionPlan{
		srcLevel: level,
		outLevel: level,
		in0:      inputs,
		dropTomb: !l.hasSegmentsBelowLocked(level),
	}
	p.before = l.inputBytes(inputs)
	return p, true
}

// buildCompaction runs the plan's inputs through the shared streaming k-way merge and the
// splitter, producing the size-bounded output segments (in internal-key order, version GC
// applied at the watermark). It reads only the input segments, which are immutable once
// published, and the open-time-immutable split knobs, so it is safe with l.mu released:
// background compaction runs it that way so foreground writers keep inserting while the
// merge serializes. The caller holds flushMu, so no flush build or other compaction writes
// the same pages at once.
func (l *LSM) buildCompaction(p *compactionPlan, watermark uint64) ([]*segment, error) {
	sources := make([]mergeSource, 0, len(p.in0)+len(p.in1))
	for _, seg := range p.in0 {
		sources = append(sources, &segSource{pgr: l.pgr, seg: seg})
	}
	for _, seg := range p.in1 {
		sources = append(sources, &segSource{pgr: l.pgr, seg: seg})
	}
	mi, err := newMergeIter(sources, nil)
	if err != nil {
		return nil, err
	}
	return l.writeSplit(mi, watermark, p.dropTomb, bloomBitsForLevel(p.outLevel, l.levelRatio), l.codecForLevel(p.outLevel))
}

// installCompactionLocked publishes a built compaction: it records every input removal and
// output addition in the MANIFEST, swaps the live set, and frees the inputs' pages, all in
// one l.mu critical section so a reader folds either the inputs or the outputs but never
// both (the outputs are the MVCC merge of the inputs, so the two views resolve identically).
// The edits are recorded before the live set is swapped, the LSM's atomic version install.
// The caller holds l.mu and flushMu. It returns the work report.
func (l *LSM) installCompactionLocked(p *compactionPlan, outputs []*segment) (engine.MaintReport, error) {
	for _, seg := range p.in0 {
		if err := l.appendEditLocked(manifestRemove, uint8(p.srcLevel), seg.footer); err != nil {
			return engine.MaintReport{}, err
		}
	}
	for _, seg := range p.in1 {
		if err := l.appendEditLocked(manifestRemove, uint8(p.outLevel), seg.footer); err != nil {
			return engine.MaintReport{}, err
		}
	}
	for _, seg := range outputs {
		if err := l.appendEditLocked(manifestAdd, uint8(p.outLevel), seg.footer); err != nil {
			return engine.MaintReport{}, err
		}
	}

	l.removeSegmentsLocked(p.srcLevel, setOf(p.in0))
	if len(p.in1) > 0 {
		l.removeSegmentsLocked(p.outLevel, setOf(p.in1))
	}
	for _, seg := range outputs {
		l.addSegmentLocked(p.outLevel, seg)
	}

	for _, seg := range p.in0 {
		if err := l.freeSegmentPages(seg); err != nil {
			return engine.MaintReport{}, err
		}
	}
	for _, seg := range p.in1 {
		if err := l.freeSegmentPages(seg); err != nil {
			return engine.MaintReport{}, err
		}
	}

	pageSize := int64(l.pgr.PageSize())
	var after int64
	for _, seg := range outputs {
		after += int64(seg.pages) * pageSize
	}
	reclaimed := p.before - after
	if reclaimed < 0 {
		reclaimed = 0
	}
	return engine.MaintReport{
		PagesCompacted: len(p.in0) + len(p.in1),
		BytesWritten:   after,
		BytesReclaimed: reclaimed,
	}, nil
}

// runCompactionLocked merges a run from the source level into the level below it through the
// three compaction phases, all under l.mu (the host Maintain path holds it). It returns a
// report of the work done, or a zero report when there was nothing to merge.
func (l *LSM) runCompactionLocked(src int, watermark uint64, wholeLevel bool) (engine.MaintReport, error) {
	p, ok := l.planPushDownLocked(compaction{kind: compactPushDown, level: src, wholeLevel: wholeLevel})
	if !ok {
		return engine.MaintReport{}, nil
	}
	outputs, err := l.buildCompaction(p, watermark)
	if err != nil {
		return engine.MaintReport{}, err
	}
	return l.installCompactionLocked(p, outputs)
}

// runSelfMergeLocked folds every run at a tiered level back into one disjoint run through the
// three compaction phases, all under l.mu. It is what the bottom pays for accumulating runs
// instead of merging on every push: it runs once per tierFanout pushes rather than on each
// one. The caller holds l.mu.
func (l *LSM) runSelfMergeLocked(level int, watermark uint64) (engine.MaintReport, error) {
	p, ok := l.planSelfMergeLocked(compaction{kind: compactSelfMerge, level: level})
	if !ok {
		return engine.MaintReport{}, nil
	}
	outputs, err := l.buildCompaction(p, watermark)
	if err != nil {
		return engine.MaintReport{}, err
	}
	return l.installCompactionLocked(p, outputs)
}

// writeSplit drives the merge through a splitter, writing the kept cells into one or more
// size-bounded output segments, each filter sized for the output level by the Monkey budget
// bitsPerKey. It reads only the open-time-immutable split knobs and writes fresh pages, so it
// is safe with l.mu released (the caller holds flushMu, the page-write serializer). An empty
// trailing segment (when the final pull dropped every cell) is reclaimed rather than
// published, so a compaction whose tail is all dead versions leaks no pages.
func (l *LSM) writeSplit(mi *mergeIter, watermark uint64, dropTomb bool, bitsPerKey int, cdc codecID) ([]*segment, error) {
	target := l.segTargetBytes
	if target < 1 {
		target = 1
	}
	sp := &splitter{mi: mi, watermark: watermark, dropTomb: dropTomb, target: target}
	var outs []*segment
	for !sp.exhausted {
		seg, err := writeSegment(l.pgr, bitsPerKey, l.filterKind, cdc, sp.fill)
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
//
// A separated set (KindSetSep) is a base exactly like an inline set: its value lives in
// the value log, but the cell still establishes the version a snapshot resolves to, so it
// shadows the older versions under it the same way. Compaction carries the pointer cell
// through verbatim, which is what makes value separation cheap.
func (s *splitter) keep(ik []byte) bool {
	kind := format.KindOf(ik)
	if kind == format.KindRangeBegin {
		return true
	}
	if format.Version(ik) <= s.watermark {
		if s.keptBase {
			return false
		}
		if kind == format.KindSet || kind == format.KindSetSep || kind == format.KindDelete {
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
