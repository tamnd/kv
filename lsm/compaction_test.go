package lsm

import (
	"context"
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// compact runs one Maintain compaction at the given watermark with a budget large
// enough to permit work, the host call that drives a merge.
func compact(t *testing.T, l *LSM, watermark uint64) engine.MaintReport {
	t.Helper()
	rep, err := l.Maintain(context.Background(), engine.MaintBudget{MaxPages: 1 << 30, Watermark: watermark})
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	return rep
}

// forceCompact merges the segment set regardless of the fan-in trigger, the hook the
// merge-correctness tests use to exercise the version-drop rule on a small set without
// flushing four segments first.
func forceCompact(t *testing.T, l *LSM, watermark uint64) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.compactLocked(watermark); err != nil {
		t.Fatalf("compact: %v", err)
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
	if len(l.segments) != segs {
		t.Fatalf("expected %d segments before compaction, got %d", segs, len(l.segments))
	}

	rep := compact(t, l, 0)
	if rep.PagesCompacted != segs {
		t.Fatalf("report merged %d segments, want %d", rep.PagesCompacted, segs)
	}
	if len(l.segments) != 1 {
		t.Fatalf("expected one segment after compaction, got %d", len(l.segments))
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
	for _, seg := range l.segments {
		cellsBefore += seg.numCells
	}
	if cellsBefore != n*rounds {
		t.Fatalf("expected %d cells before compaction, got %d", n*rounds, cellsBefore)
	}

	// Watermark above every committed version: only the newest version of each key can
	// be observed, so the history collapses to one cell per key.
	compact(t, l, version)
	if len(l.segments) != 1 {
		t.Fatalf("expected one segment, got %d", len(l.segments))
	}
	if got := l.segments[0].numCells; got != n {
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
	if len(l.segments) != 1 {
		t.Fatalf("expected one segment, got %d", len(l.segments))
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
	if len(l.segments[0].rangeDels) != 1 {
		t.Fatalf("compaction lost the range delete: %d intervals", len(l.segments[0].rangeDels))
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
	if len(l.segments) != 1 {
		t.Fatalf("expected one segment after compaction, got %d", len(l.segments))
	}
	if err := pgr.Checkpoint(l.DurableLSN()); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	pgr2 := reopenPager(t, fs, pgr)
	l2 := openLSM(t, pgr2)
	if len(l2.segments) != 1 {
		t.Fatalf("reopened engine has %d segments, want 1 (the merge output)", len(l2.segments))
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

// TestCompactionConformance runs the shared oracle suite, then compacts at the oracle's
// read-mark and confirms every key still resolves to the oracle's answer, so the
// version-drop rule agrees with MVCC resolution.
func TestCompactionConformance(t *testing.T) {
	l := newLSM(t)
	l.memtableCap = 1

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
	// Compact at a watermark above every version, then re-read at the newest snapshot.
	compact(t, l, 300)
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
