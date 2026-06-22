package lsm

import (
	"context"
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// compact runs one Maintain compaction at the given watermark with a budget large
// enough to permit work, the host call that drives a single compaction unit.
func compact(t *testing.T, l *LSM, watermark uint64) engine.MaintReport {
	t.Helper()
	rep, err := l.Maintain(context.Background(), engine.MaintBudget{MaxPages: 1 << 30, Watermark: watermark})
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	return rep
}

// drain runs Maintain repeatedly until no level is over target, settling the whole tree
// so the level invariant and the multi-level shape can be checked.
func drainCompaction(t *testing.T, l *LSM, watermark uint64) {
	t.Helper()
	for {
		if compact(t, l, watermark).PagesCompacted == 0 {
			return
		}
	}
}

// forceCompact merges L0 into the level below it regardless of the fan-in trigger, the
// hook the version-semantics tests use to exercise the drop rule on a small segment set
// without flushing a trigger's worth of segments first.
func forceCompact(t *testing.T, l *LSM, watermark uint64) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.runCompactionLocked(0, watermark, true); err != nil {
		t.Fatalf("compact: %v", err)
	}
}

// forcePushDown pushes one run from a leveled source level down into the level below,
// bypassing the picker so a test can drive a specific level transition.
func forcePushDown(t *testing.T, l *LSM, src int, watermark uint64, wholeLevel bool) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.runCompactionLocked(src, watermark, wholeLevel); err != nil {
		t.Fatalf("push down level %d: %v", src, err)
	}
}

// levelShape reports the segment count per level, trailing empty levels trimmed, the
// fingerprint the reopen test compares.
func levelShape(l *LSM) []int {
	var s []int
	for _, lvl := range l.levelsLocked() {
		s = append(s, len(lvl))
	}
	for len(s) > 0 && s[len(s)-1] == 0 {
		s = s[:len(s)-1]
	}
	return s
}

func sameShape(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// deepestLevel returns the index of the deepest level that holds a segment, or -1 when
// the tree is empty. That level is the tiered bottom under lazy-leveling.
func deepestLevel(l *LSM) int {
	for i := len(l.levelsLocked()) - 1; i >= 1; i-- {
		if len(l.levelsLocked()[i]) > 0 {
			return i
		}
	}
	return -1
}

// assertLeveledInvariant confirms every leveled level is sorted by first key with
// disjoint ranges, the non-overlapping invariant a leveled compaction must preserve. The
// tiered bottom is exempt: it holds several runs that may overlap, which is the whole
// point of tiering it.
func assertLeveledInvariant(t *testing.T, l *LSM) {
	t.Helper()
	bottom := deepestLevel(l)
	for i := 1; i < len(l.levelsLocked()); i++ {
		if i == bottom {
			continue
		}
		segs := l.levelsLocked()[i]
		for j := 1; j < len(segs); j++ {
			if format.CompareUser(segs[j-1].maxKey, segs[j].minKey) >= 0 {
				t.Fatalf("level %d not disjoint: seg %d [%s..%s] overlaps seg %d [%s..%s]",
					i, j-1, segs[j-1].minKey, segs[j-1].maxKey, j, segs[j].minKey, segs[j].maxKey)
			}
		}
	}
}

// TestCompactionMergesToOneSegment flushes several segments and confirms a compaction
// merges them into one, the read fan-in the merge bounds.
func TestCompactionMergesToOneSegment(t *testing.T) {
	l := newLSM(t)

	const segs = 6
	version := uint64(1)
	for s := 0; s < segs; s++ {
		b := engine.NewWriteBatch(version)
		for i := 0; i < 40; i++ {
			b.Set([]byte(fmt.Sprintf("key%05d", s*40+i)), []byte("v"))
		}
		if err := l.Apply(b, version); err != nil {
			t.Fatalf("apply: %v", err)
		}
		l.flushActive(t)
		version++
	}
	if len(l.allSegmentsLocked()) != segs {
		t.Fatalf("expected %d segments before compaction, got %d", segs, len(l.allSegmentsLocked()))
	}

	rep := compact(t, l, 0)
	if rep.PagesCompacted != segs {
		t.Fatalf("report merged %d segments, want %d", rep.PagesCompacted, segs)
	}
	if len(l.allSegmentsLocked()) != 1 {
		t.Fatalf("expected one segment after compaction, got %d", len(l.allSegmentsLocked()))
	}
	// Every key still resolves through the single merged segment.
	for s := 0; s < segs; s++ {
		for i := 0; i < 40; i++ {
			key := fmt.Sprintf("key%05d", s*40+i)
			if v, ok := getAt(t, l, key, version); !ok || string(v) != "v" {
				t.Fatalf("Get(%s) = (%q,%v) after compaction, want v", key, v, ok)
			}
		}
	}
}

// TestCompactionDropsDeadVersions overwrites every key many times across segments,
// then compacts at a watermark above every version, so each key collapses to a single
// surviving version and the segment shrinks.
func TestCompactionDropsDeadVersions(t *testing.T) {
	l := newLSM(t)

	const n = 100
	const rounds = 8
	version := uint64(1)
	for r := 0; r < rounds; r++ {
		b := engine.NewWriteBatch(version)
		for i := 0; i < n; i++ {
			b.Set([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("r%d", r)))
		}
		if err := l.Apply(b, version); err != nil {
			t.Fatalf("apply: %v", err)
		}
		l.flushActive(t)
		version++
	}

	var cellsBefore int
	for _, seg := range l.allSegmentsLocked() {
		cellsBefore += seg.numCells
	}
	if cellsBefore != n*rounds {
		t.Fatalf("expected %d cells before compaction, got %d", n*rounds, cellsBefore)
	}

	// Watermark above every committed version: only the newest version of each key can
	// be observed, so the history collapses to one cell per key.
	compact(t, l, version)
	if len(l.allSegmentsLocked()) != 1 {
		t.Fatalf("expected one segment, got %d", len(l.allSegmentsLocked()))
	}
	if got := l.allSegmentsLocked()[0].numCells; got != n {
		t.Fatalf("compaction kept %d cells, want %d (one newest version per key)", got, n)
	}
	// The surviving value is the newest round's.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%04d", i)
		if v, ok := getAt(t, l, key, version); !ok || string(v) != fmt.Sprintf("r%d", rounds-1) {
			t.Fatalf("Get(%s) = (%q,%v), want r%d", key, v, ok, rounds-1)
		}
	}
}

// TestCompactionKeepsVersionsAboveWatermark confirms a snapshot read still sees an old
// version after a compaction whose watermark sits below that version, the MVCC
// guarantee the version-drop rule must not break.
func TestCompactionKeepsVersionsAboveWatermark(t *testing.T) {
	l := newLSM(t)

	applyBatch(t, l, 10, func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("old")) })
	l.flushActive(t)
	applyBatch(t, l, 20, func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("mid")) })
	l.flushActive(t)
	applyBatch(t, l, 30, func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("new")) })
	l.flushActive(t)

	// Watermark 15: a snapshot at 10 must still resolve, so version 10 cannot be dropped
	// even though 20 and 30 shadow it at newer snapshots.
	forceCompact(t, l, 15)
	if len(l.allSegmentsLocked()) != 1 {
		t.Fatalf("expected one segment, got %d", len(l.allSegmentsLocked()))
	}
	cases := []struct {
		snap uint64
		want string
	}{{10, "old"}, {15, "old"}, {20, "mid"}, {30, "new"}, {100, "new"}}
	for _, c := range cases {
		if v, ok := getAt(t, l, "k", c.snap); !ok || string(v) != c.want {
			t.Fatalf("Get(k)@%d = (%q,%v), want %q", c.snap, v, ok, c.want)
		}
	}
}

// TestCompactionPreservesMerges confirms a merge version below the watermark still
// folds over the base it sits on, so the drop rule never strands a merge operand.
func TestCompactionPreservesMerges(t *testing.T) {
	l := newLSM(t)
	l.SetMergeFunc(concatMerge)

	applyBatch(t, l, 10, func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("base")) })
	l.flushActive(t)
	applyBatch(t, l, 20, func(b *engine.WriteBatch) { b.Merge([]byte("k"), []byte("+x")) })
	l.flushActive(t)
	applyBatch(t, l, 30, func(b *engine.WriteBatch) { b.Merge([]byte("k"), []byte("+y")) })
	l.flushActive(t)

	// Watermark above every version: the group collapses, but the set base must be kept
	// under the merges so the fold still produces base+x+y.
	forceCompact(t, l, 100)
	if v, ok := getAt(t, l, "k", 100); !ok || string(v) != "base+x+y" {
		t.Fatalf("Get(k) = (%q,%v) after compaction, want base+x+y", v, ok)
	}
}

// TestCompactionPreservesRangeDeletes confirms a range-delete marker survives a
// compaction at a high watermark and still removes the keys it covers, since a marker
// is not shadowed by a newer version of its own key.
func TestCompactionPreservesRangeDeletes(t *testing.T) {
	l := newLSM(t)

	for i := 0; i < 10; i++ {
		applyBatch(t, l, uint64(i+1), func(b *engine.WriteBatch) {
			b.Set([]byte(fmt.Sprintf("k%02d", i)), []byte("v"))
		})
	}
	l.flushActive(t)
	applyBatch(t, l, 50, func(b *engine.WriteBatch) { b.DeleteRange([]byte("k03"), []byte("k07")) })
	l.flushActive(t)

	forceCompact(t, l, 100)
	if len(l.allSegmentsLocked()[0].rangeDels) != 1 {
		t.Fatalf("compaction lost the range delete: %d intervals", len(l.allSegmentsLocked()[0].rangeDels))
	}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%02d", i)
		v, ok := getAt(t, l, key, 100)
		covered := i >= 3 && i < 7
		if covered && ok {
			t.Fatalf("Get(%s) = %q, want covered by the range delete after compaction", key, v)
		}
		if !covered && (!ok || string(v) != "v") {
			t.Fatalf("Get(%s) = (%q,%v), want v", key, v, ok)
		}
	}
}

// TestCompactionFreesPages confirms a compaction returns the input segments' pages to
// the freelist, the space reclamation that keeps the file from growing unbounded.
func TestCompactionFreesPages(t *testing.T) {
	fs, pgr := newDurablePager(t)
	l := openLSM(t, pgr)

	const n = 200
	const rounds = 6
	version := uint64(1)
	for r := 0; r < rounds; r++ {
		applyLSN(t, l, version, version, func(wb *engine.WriteBatch) {
			for i := 0; i < n; i++ {
				wb.Set([]byte(fmt.Sprintf("k%04d", i)), []byte("value-bytes"))
			}
		})
		l.flushActive(t)
		version++
	}

	freeBefore := pgr.FreeCount()
	compact(t, l, version)
	if got := pgr.FreeCount(); got <= freeBefore {
		t.Fatalf("free count %d did not grow past %d; compaction freed no pages", got, freeBefore)
	}
	_ = fs
}

// TestCompactionSurvivesReopen compacts, folds the result and its MANIFEST edits to the
// file, and reopens with no WAL, so the only record of the merge is the MANIFEST: the
// reopened engine must show the single merged segment and resolve every key.
func TestCompactionSurvivesReopen(t *testing.T) {
	fs, pgr := newDurablePager(t)
	l := openLSM(t, pgr)

	const segs = 5
	want := map[string]string{}
	version := uint64(1)
	for s := 0; s < segs; s++ {
		applyLSN(t, l, version, version, func(b *engine.WriteBatch) {
			for i := 0; i < 30; i++ {
				key := fmt.Sprintf("key%04d", s*30+i)
				val := fmt.Sprintf("val%04d", s*30+i)
				b.Set([]byte(key), []byte(val))
				want[key] = val
			}
		})
		l.flushActive(t)
		version++
	}

	compact(t, l, 0)
	if len(l.allSegmentsLocked()) != 1 {
		t.Fatalf("expected one segment after compaction, got %d", len(l.allSegmentsLocked()))
	}
	if err := pgr.Checkpoint(l.DurableLSN()); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	pgr2 := reopenPager(t, fs, pgr)
	l2 := openLSM(t, pgr2)
	if len(l2.allSegmentsLocked()) != 1 {
		t.Fatalf("reopened engine has %d segments, want 1 (the merge output)", len(l2.allSegmentsLocked()))
	}
	rd, err := l2.NewReader(engine.Snapshot{Version: version})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	for key, val := range want {
		v, err := rd.Get([]byte(key))
		if err != nil || string(v) != val {
			t.Fatalf("after reopen Get(%s) = (%q,%v), want %q", key, v, err, val)
		}
	}
}

// TestLeveledFormsDeeperLevels ingests enough data under small level targets to build a
// multi-level tree, then confirms the level invariant holds, a recent snapshot resolves
// the newest writes, and an old snapshot still resolves the originals the merges sit on.
func TestLeveledFormsDeeperLevels(t *testing.T) {
	l := newLSM(t)
	l.memtableCap = 1
	l.l0Trigger = 2
	l.levelRatio = 2
	l.l1TargetBytes = 128
	l.segTargetBytes = 64

	const n = 80
	for i := 0; i < n; i++ {
		applyBatch(t, l, uint64(i+1), func(b *engine.WriteBatch) {
			b.Set([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%05d", i)))
		})
		l.flushActive(t)
	}
	// Overwrite the even keys and delete every fifth, at versions above the originals.
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			applyBatch(t, l, uint64(1000+i), func(b *engine.WriteBatch) {
				b.Set([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("w%05d", i)))
			})
			l.flushActive(t)
		}
	}
	for i := 0; i < n; i++ {
		if i%5 == 0 {
			applyBatch(t, l, uint64(2000+i), func(b *engine.WriteBatch) {
				b.Delete([]byte(fmt.Sprintf("k%05d", i)))
			})
			l.flushActive(t)
		}
	}

	drainCompaction(t, l, 0)
	assertLeveledInvariant(t, l)
	if len(l.levelsLocked()) < 3 {
		t.Fatalf("expected a multi-level tree, got shape %v", levelShape(l))
	}

	// Newest snapshot: deletes win, then overwrites, then originals.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%05d", i)
		v, ok := getAt(t, l, key, 9000)
		switch {
		case i%5 == 0:
			if ok {
				t.Fatalf("Get(%s)@9000 = %q, want deleted", key, v)
			}
		case i%2 == 0:
			if !ok || string(v) != fmt.Sprintf("w%05d", i) {
				t.Fatalf("Get(%s)@9000 = (%q,%v), want w%05d", key, v, ok, i)
			}
		default:
			if !ok || string(v) != fmt.Sprintf("v%05d", i) {
				t.Fatalf("Get(%s)@9000 = (%q,%v), want v%05d", key, v, ok, i)
			}
		}
	}
	// Old snapshot, before any overwrite or delete: every key reads its original, proving
	// the merges did not strand the versions beneath them.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%05d", i)
		if v, ok := getAt(t, l, key, uint64(i+1)); !ok || string(v) != fmt.Sprintf("v%05d", i) {
			t.Fatalf("Get(%s)@%d = (%q,%v), want v%05d", key, i+1, v, ok, i)
		}
	}
}

// TestLeveledPartialPick confirms a push-down into a leveled middle level touches only the
// segments its input overlaps, leaving the disjoint neighbours untouched, the property
// that makes leveled compaction incremental. The middle level is made leveled by seeding a
// level below it, since the deepest level is tiered and would add a run rather than merge.
func TestLeveledPartialPick(t *testing.T) {
	l := newLSM(t)
	l.l0Trigger = 1
	l.segTargetBytes = 1      // one version group per output segment
	l.l1TargetBytes = 1 << 50 // huge, so no level descends on its own during the test
	l.tierFanout = 1 << 30    // huge, so the bottom never self-merges during the test

	// Seed an L2 so L1 becomes a leveled middle level: flush a key, add it to L1 as the
	// (then) tiered bottom, then descend that whole level into L2.
	applyBatch(t, l, 1, func(b *engine.WriteBatch) { b.Set([]byte("seed"), []byte("0")) })
	l.flushActive(t)
	forceCompact(t, l, 0)           // L0 -> L1, tiered add (L1 is the bottom)
	forcePushDown(t, l, 1, 0, true) // descend the whole L1 into L2; L1 is now empty and leveled
	if deepestLevel(l) != 2 {
		t.Fatalf("expected L2 to be the bottom, got shape %v", levelShape(l))
	}

	// One L0 segment of three far-apart keys; a leveled merge into the empty L1 splits it
	// into three disjoint single-key segments.
	applyBatch(t, l, 2, func(b *engine.WriteBatch) {
		b.Set([]byte("aaa"), []byte("1"))
		b.Set([]byte("mmm"), []byte("1"))
		b.Set([]byte("zzz"), []byte("1"))
	})
	l.flushActive(t)
	forceCompact(t, l, 0)
	if len(l.levelsLocked()[1]) != 3 {
		t.Fatalf("expected three disjoint L1 segments, got shape %v", levelShape(l))
	}
	low, mid, high := l.levelsLocked()[1][0], l.levelsLocked()[1][1], l.levelsLocked()[1][2]

	// A new write to the middle key only; the leveled merge must rewrite only the middle L1
	// segment and leave the low and high segments as the exact same handles.
	applyBatch(t, l, 3, func(b *engine.WriteBatch) { b.Set([]byte("mmm"), []byte("2")) })
	l.flushActive(t)
	forceCompact(t, l, 0)

	if len(l.levelsLocked()[1]) != 3 || l.levelsLocked()[1][0] != low || l.levelsLocked()[1][2] != high {
		t.Fatalf("partial compaction disturbed the disjoint neighbours, shape %v", levelShape(l))
	}
	if l.levelsLocked()[1][1] == mid {
		t.Fatalf("the overlapped middle segment was not replaced")
	}
	if v, ok := getAt(t, l, "mmm", 5); !ok || string(v) != "2" {
		t.Fatalf("Get(mmm) = (%q,%v), want 2", v, ok)
	}
	for _, key := range []string{"aaa", "zzz"} {
		if v, ok := getAt(t, l, key, 5); !ok || string(v) != "1" {
			t.Fatalf("Get(%s) = (%q,%v), want 1", key, v, ok)
		}
	}
}

// TestLeveledBottomTombstoneDrop confirms a self-merge of the tiered bottom drops a point
// delete outright, since every run is an input and nothing lives below it for the
// tombstone to shadow, so the key and its value both vanish. The set and the delete are
// added as two separate bottom runs, then a self-merge folds them together.
func TestLeveledBottomTombstoneDrop(t *testing.T) {
	l := newLSM(t)
	l.l0Trigger = 1
	l.tierFanout = 2

	// Two separate pushes into the tiered bottom: one run holding the set, one the delete.
	applyBatch(t, l, 1, func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) })
	l.flushActive(t)
	compact(t, l, 10) // L0 -> L1 tiered add: run with the set
	applyBatch(t, l, 2, func(b *engine.WriteBatch) { b.Delete([]byte("k")) })
	l.flushActive(t)
	compact(t, l, 10) // L0 -> L1 tiered add: run with the delete
	if got := len(l.levelsLocked()[1]); got != 2 {
		t.Fatalf("expected two tiered runs at the bottom, got %d (shape %v)", got, levelShape(l))
	}

	// The two runs overlap on key k, so a third Maintain self-merges them and drops the
	// tombstone at the bottom.
	compact(t, l, 10)
	total := 0
	for _, s := range l.allSegmentsLocked() {
		total += s.numCells
	}
	if total != 0 {
		t.Fatalf("bottom self-merge left %d cells, want the tombstone and value both dropped", total)
	}
	if _, ok := getAt(t, l, "k", 100); ok {
		t.Fatalf("Get(k) found a value, want not found after the tombstone drop")
	}
}

// TestLeveledReopenRestoresLevels builds a multi-level tree, folds it and its MANIFEST
// level edits to the file, and reopens with no WAL: the reopened engine must rebuild the
// same per-level shape and resolve every key.
func TestLeveledReopenRestoresLevels(t *testing.T) {
	fs, pgr := newDurablePager(t)
	l := openLSM(t, pgr)
	l.memtableCap = 1
	l.l0Trigger = 2
	l.levelRatio = 2
	l.l1TargetBytes = 128
	l.segTargetBytes = 64

	const n = 80
	want := map[string]string{}
	for i := 0; i < n; i++ {
		v := uint64(i + 1)
		applyLSN(t, l, v, v, func(b *engine.WriteBatch) {
			key := fmt.Sprintf("k%05d", i)
			val := fmt.Sprintf("v%05d", i)
			b.Set([]byte(key), []byte(val))
			want[key] = val
		})
		l.flushActive(t)
	}
	drainCompaction(t, l, 0)
	assertLeveledInvariant(t, l)
	if len(l.levelsLocked()) < 3 {
		t.Fatalf("expected a multi-level tree, got shape %v", levelShape(l))
	}
	shapeBefore := levelShape(l)

	if err := pgr.Checkpoint(l.DurableLSN()); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	pgr2 := reopenPager(t, fs, pgr)
	l2 := openLSM(t, pgr2)
	if got := levelShape(l2); !sameShape(got, shapeBefore) {
		t.Fatalf("reopened level shape %v, want %v", got, shapeBefore)
	}
	assertLeveledInvariant(t, l2)
	rd, err := l2.NewReader(engine.Snapshot{Version: 10000})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	for key, val := range want {
		if v, err := rd.Get([]byte(key)); err != nil || string(v) != val {
			t.Fatalf("after reopen Get(%s) = (%q,%v), want %q", key, v, err, val)
		}
	}
}

// TestCompactionConformance runs the shared oracle suite, then drains compaction at the
// oracle's read-mark and confirms every key still resolves to the oracle's answer, so
// the version-drop and bottom-tombstone rules agree with MVCC resolution.
func TestCompactionConformance(t *testing.T) {
	l := newLSM(t)
	l.memtableCap = 1
	l.l0Trigger = 1

	const n = 150
	var batches []*engine.WriteBatch
	b1 := engine.NewWriteBatch(100)
	for i := 0; i < n; i++ {
		b1.Set([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%05d", i)))
	}
	batches = append(batches, b1)
	b2 := engine.NewWriteBatch(200)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		switch {
		case i%5 == 0:
			b2.Delete(k)
		case i%2 == 0:
			b2.Set(k, []byte(fmt.Sprintf("w%05d", i)))
		}
	}
	batches = append(batches, b2)

	if err := engine.CheckEngine(l, batches, concatMerge); err != nil {
		t.Fatalf("conformance before compaction: %v", err)
	}
	// Seal the tail into L0, then drain at a watermark above every version and re-read at
	// the newest snapshot.
	l.flushActive(t)
	drainCompaction(t, l, 300)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%05d", i)
		v, ok := getAt(t, l, key, 300)
		switch {
		case i%5 == 0:
			if ok {
				t.Fatalf("Get(%s) = %q after compaction, want deleted", key, v)
			}
		case i%2 == 0:
			if !ok || string(v) != fmt.Sprintf("w%05d", i) {
				t.Fatalf("Get(%s) = (%q,%v), want w%05d", key, v, ok, i)
			}
		default:
			if !ok || string(v) != fmt.Sprintf("v%05d", i) {
				t.Fatalf("Get(%s) = (%q,%v), want v%05d", key, v, ok, i)
			}
		}
	}
}
