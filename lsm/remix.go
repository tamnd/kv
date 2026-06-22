package lsm

import (
	"sort"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// The REMIX ordered index speeds the range path the way the block index and Bloom
// filters sped the point path (spec 06 §6, spec 11 §5.3). A naive LSM range scan runs
// every source through the k-way merge, so the heap carries one entry per segment and
// pays a comparison and possibly a cursor switch at each key. But a leveled level (L1
// and below) holds key-range-disjoint segments sorted by first key, so the level is
// already one global sorted run cut into size-bounded pieces: only one of its segments
// can hold any given key, and walking the segments in order walks the level in order.
// The merge does not need to see those segments separately.
//
// REMIX records that global order across a level's segments so the scan does not pay a
// per-key heap-merge over them. In kv's single-file layout the order is not a separately
// persisted key array: it is the per-level segment list compaction already keeps sorted
// by first key under its disjoint invariant, plus each segment's footer key bounds. The
// levelSource below is the cursor that walks that list as one ordered stream, so with the
// index on the heap-merge carries one entry per leveled level instead of one per segment.
// For a tree of many small segments per level that shrinks the heap from the segment count
// to the level count, cutting the comparisons and cursor switches a scan pays, which is
// the REMIX win. The result is identical to the per-segment merge, key for key; only the
// number of sources the heap juggles changes.
//
// L0 is the exception: its segments are flushed memtables that overlap in key range, so
// no single ordered walk crosses them and each stays its own source. L0 is bounded by the
// compaction trigger, so its segment count is small and leaving it per-segment costs
// little. The memtable is likewise always its own source.

// levelSource presents one leveled level's disjoint segments as a single ordered cell
// stream, the cursor the REMIX index rests on. It keeps an active segSource over the
// current segment and steps to the next segment when that one is exhausted, so the cells
// it yields run in ascending internal-key order across the whole level. Because the
// segments are disjoint and sorted by first key, that order is the level's global order
// with no merging between segments required.
type levelSource struct {
	pgr  *pager.Pager
	segs []*segment // the level's segments, ascending by first key, disjoint
	cur  int        // index of the active segment, len(segs) when exhausted
	src  segSource  // cursor over segs[cur]
	err  error
}

func (ls *levelSource) valid() bool {
	return ls.err == nil && ls.cur < len(ls.segs) && ls.src.valid()
}

func (ls *levelSource) key() []byte   { return ls.src.key() }
func (ls *levelSource) value() []byte { return ls.src.value() }

// startSegment returns the index of the first segment that may hold target, found by
// binary search over the segments' upper bounds. Because the level is disjoint and sorted,
// the first segment whose largest key is at or after target's user key is the only one
// that can contain it or the next key after it; a nil target starts at the first segment.
func (ls *levelSource) startSegment(target []byte) int {
	if target == nil {
		return 0
	}
	uk := format.UserKey(target)
	return sort.Search(len(ls.segs), func(i int) bool {
		return ls.segs[i].maxKey != nil && format.CompareUser(ls.segs[i].maxKey, uk) >= 0
	})
}

// seekGE positions the source at the first cell whose key is at or after target. It binary
// searches to the segment that may hold target, seeks within it, and steps forward to the
// next segment when that one yields nothing at or after target (which happens only when
// target falls in the gap above a segment's range).
func (ls *levelSource) seekGE(target []byte) error {
	for ls.cur = ls.startSegment(target); ls.cur < len(ls.segs); ls.cur++ {
		ls.src = segSource{pgr: ls.pgr, seg: ls.segs[ls.cur]}
		if err := ls.src.seekGE(target); err != nil {
			ls.err = err
			return err
		}
		if ls.src.valid() {
			return nil
		}
		if ls.src.err != nil {
			ls.err = ls.src.err
			return ls.err
		}
	}
	return nil
}

// next advances within the active segment, crossing into the next segment from its start
// when the current one is spent, until a cell is found or the level is exhausted.
func (ls *levelSource) next() error {
	if ls.err != nil {
		return ls.err
	}
	if err := ls.src.next(); err != nil {
		ls.err = err
		return err
	}
	for !ls.src.valid() {
		if ls.src.err != nil {
			ls.err = ls.src.err
			return ls.err
		}
		ls.cur++
		if ls.cur >= len(ls.segs) {
			return nil
		}
		ls.src = segSource{pgr: ls.pgr, seg: ls.segs[ls.cur]}
		if err := ls.src.seekGE(nil); err != nil {
			ls.err = err
			return err
		}
	}
	return nil
}

// rangeSourcesLocked builds the ordered sources a range fold merges: always the active
// memtable, then the on-disk segments. With the REMIX index off every segment is its own
// source, the original heap-merge. With it on, L0's overlapping segments stay per-segment
// but each leveled level collapses to one levelSource over its disjoint run, so the heap
// carries one entry per level rather than one per segment. The caller holds l.mu.
func (l *LSM) rangeSourcesLocked() []mergeSource {
	sources := []mergeSource{&memSource{sl: l.mem.sl}}
	// Sealed memtables awaiting flush sit between the active memtable and the segments: each
	// is older than the active one but newer than any segment. The most recently sealed (the
	// tail of the queue) is the newer source, so it comes first.
	for i := len(l.imm) - 1; i >= 0; i-- {
		sources = append(sources, &memSource{sl: l.imm[i].mem.sl})
	}
	if !l.rangeIndex {
		for _, seg := range l.allSegmentsLocked() {
			sources = append(sources, &segSource{pgr: l.pgr, seg: seg})
		}
		return sources
	}
	if len(l.levels) > 0 {
		for _, seg := range l.levels[0] {
			sources = append(sources, &segSource{pgr: l.pgr, seg: seg})
		}
	}
	for i := 1; i < len(l.levels); i++ {
		if len(l.levels[i]) == 0 {
			continue
		}
		// Only a leveled level is one disjoint sorted run a single cursor can walk. The
		// tiered bottom accumulates overlapping runs (spec 06 §6), so like L0 its segments
		// must stay separate sources for the heap to merge.
		if l.isTieredLocked(i) {
			for _, seg := range l.levels[i] {
				sources = append(sources, &segSource{pgr: l.pgr, seg: seg})
			}
			continue
		}
		sources = append(sources, &levelSource{pgr: l.pgr, segs: l.levels[i]})
	}
	return sources
}
