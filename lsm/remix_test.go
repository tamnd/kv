package lsm

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// buildLeveledTree flushes many small memtables and drains compaction into a settled
// multi-level shape whose leveled levels each hold several disjoint segments, the tree
// the REMIX cursor is meant to walk. It returns the engine and the keys it holds, in
// ascending order, so a scan's output can be checked against the truth.
func buildLeveledTree(t *testing.T) (*LSM, []string) {
	t.Helper()
	l := newLSM(t)
	// Cut every version group into its own output segment and hold the level descents off,
	// so a leveled merge into L1 yields one disjoint segment per key, the same deterministic
	// shaping the leveled-compaction tests use. The result is an L1 of many disjoint runs
	// (the leveled level REMIX collapses), an L2 tiered bottom, plus an L0 segment and the
	// memtable, so a scan merges every kind of source at once.
	l.l0Trigger = 1
	l.segTargetBytes = 1
	l.l1TargetBytes = 1 << 50
	l.tierFanout = 1 << 30

	// Seed an L2 so L1 becomes a leveled middle level rather than the tiered bottom: flush a
	// few keys, add them to L1 as the (then) bottom, then descend that whole level into L2.
	applyBatch(t, l, 1, func(b *engine.WriteBatch) {
		for _, i := range []int{50, 150, 250, 350} {
			b.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("seed%05d", i)))
		}
	})
	l.flushActive(t)
	forceCompact(t, l, 0)           // L0 -> L1 tiered add (L1 is the bottom)
	forcePushDown(t, l, 1, 0, true) // descend the whole L1 into L2; L1 is empty and leveled

	// The main body: every key at a newer version, flushed as one L0 segment and merged into
	// the empty L1, which splits it into one disjoint single-key segment per key.
	const keys = 400
	applyBatch(t, l, 2, func(b *engine.WriteBatch) {
		for i := 0; i < keys; i++ {
			b.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("v%05d", i)))
		}
	})
	l.flushActive(t)
	forceCompact(t, l, 0)

	// Leave fresher state above L1: one key overwritten in a new L0 segment, another live in
	// the active memtable, so the merge spans memtable, L0, the L1 levelSource, and L2.
	applyBatch(t, l, 3, func(b *engine.WriteBatch) {
		b.Set([]byte("key00200"), []byte("l0-overwrite"))
	})
	l.flushActive(t)
	applyBatch(t, l, 4, func(b *engine.WriteBatch) {
		b.Set([]byte("key00100"), []byte("mem-overwrite"))
	})

	want := make([]string, keys)
	for i := 0; i < keys; i++ {
		want[i] = fmt.Sprintf("key%05d", i)
	}
	return l, want
}

// hasDisjointLevel reports whether some leveled (non-tiered) level holds at least minSegs
// disjoint segments, the precondition that makes a levelSource cover more than one segment.
// The tiered bottom is excluded because its runs overlap and stay per-segment.
func hasDisjointLevel(l *LSM, minSegs int) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for i := 1; i < len(l.levels); i++ {
		if !l.isTieredLocked(i) && len(l.levels[i]) >= minSegs {
			return true
		}
	}
	return false
}

// keysOf reduces a scan result to just its keys, the part REMIX must reproduce exactly.
func keysOf(pairs [][2]string) []string {
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p[0]
	}
	return out
}

func equalStrings(a, b []string) bool {
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

// TestRangeIndexMatchesPlainMerge is the heart of the slice: over a tree with several
// disjoint segments per level, a scan with the REMIX index on yields byte-for-byte the
// same keys and values as the per-segment merge, across a full scan, bounded scans, a
// prefix scan, and a keys-only scan. The index changes only how many sources the heap
// juggles, never the merged result.
func TestRangeIndexMatchesPlainMerge(t *testing.T) {
	l, want := buildLeveledTree(t)
	if !hasDisjointLevel(l, 2) {
		t.Fatalf("test tree has no leveled level with multiple disjoint segments; nothing for REMIX to collapse")
	}

	cases := []struct {
		name string
		opts engine.IterOptions
	}{
		{"full", engine.IterOptions{}},
		{"lower-bounded", engine.IterOptions{Lower: []byte("key00200")}},
		{"upper-bounded", engine.IterOptions{Upper: []byte("key00400")}},
		{"both-bounded", engine.IterOptions{Lower: []byte("key00150"), Upper: []byte("key00450")}},
		{"prefix", engine.IterOptions{Prefix: []byte("key003")}},
		{"keys-only", engine.IterOptions{KeysOnly: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l.rangeIndex = false
			plain := scanRange(t, l, tc.opts, version(want))
			l.rangeIndex = true
			remix := scanRange(t, l, tc.opts, version(want))
			l.rangeIndex = false

			if len(plain) != len(remix) {
				t.Fatalf("REMIX returned %d pairs, plain merge returned %d", len(remix), len(plain))
			}
			for i := range plain {
				if plain[i] != remix[i] {
					t.Fatalf("pair %d differs: plain %v, REMIX %v", i, plain[i], remix[i])
				}
			}
		})
	}
}

// version returns a snapshot version above every write in the test tree, so a scan sees
// the newest version of each key.
func version(_ []string) uint64 { return 1 << 20 }

// TestRangeIndexFullScanIsComplete checks the REMIX scan against the independent truth,
// not just against the plain merge, so a bug shared by both paths cannot hide: a full
// forward scan returns every key exactly once in order.
func TestRangeIndexFullScanIsComplete(t *testing.T) {
	l, want := buildLeveledTree(t)
	l.rangeIndex = true
	got := keysOf(scanRange(t, l, engine.IterOptions{}, version(want)))
	if !equalStrings(got, want) {
		t.Fatalf("REMIX full scan returned %d keys, want %d; first mismatch reveals the gap", len(got), len(want))
	}
}

// TestLevelSourceWalksDisjointSegments drives a levelSource directly over a level's
// disjoint segments and confirms it yields every cell in internal-key order, the ordered
// walk the heap relies on, and that seekGE lands on the first key at or after a target
// that falls between two segments.
func TestLevelSourceWalksDisjointSegments(t *testing.T) {
	l, _ := buildLeveledTree(t)
	l.mu.RLock()
	defer l.mu.RUnlock()

	var segs []*segment
	for i := 1; i < len(l.levels); i++ {
		if !l.isTieredLocked(i) && len(l.levels[i]) >= 2 {
			segs = l.levels[i]
			break
		}
	}
	if segs == nil {
		t.Fatalf("no leveled level with multiple disjoint segments")
	}

	// A full walk from the start visits the segments' cells in ascending internal-key
	// order with no gap or repeat across the segment boundaries.
	ls := &levelSource{pgr: l.pgr, segs: segs}
	if err := ls.seekGE(nil); err != nil {
		t.Fatalf("seekGE(nil): %v", err)
	}
	var walked [][]byte
	for ls.valid() {
		walked = append(walked, append([]byte(nil), ls.key()...))
		if err := ls.next(); err != nil {
			t.Fatalf("next: %v", err)
		}
	}
	for i := 1; i < len(walked); i++ {
		if format.CompareInternal(walked[i-1], walked[i]) >= 0 {
			t.Fatalf("levelSource out of order at %d: %q then %q", i, walked[i-1], walked[i])
		}
	}
	// The walk covers every cell the segments hold, the sum of their cell counts.
	total := 0
	for _, s := range segs {
		total += s.numCells
	}
	if len(walked) != total {
		t.Fatalf("levelSource walked %d cells, segments hold %d", len(walked), total)
	}

	// seekGE to a key in the middle lands on the first stored key at or after it, even
	// when the target falls in the gap above one segment's range.
	mid := walked[len(walked)/2]
	target := format.EncodeInternalKey(format.UserKey(mid), format.MaxVersion, format.KindDelete)
	ls2 := &levelSource{pgr: l.pgr, segs: segs}
	if err := ls2.seekGE(target); err != nil {
		t.Fatalf("seekGE(mid): %v", err)
	}
	if !ls2.valid() {
		t.Fatalf("seekGE(mid) landed past the end")
	}
	if format.CompareUser(format.UserKey(ls2.key()), format.UserKey(mid)) < 0 {
		t.Fatalf("seekGE landed before the target: got %q, target %q", ls2.key(), mid)
	}
}
