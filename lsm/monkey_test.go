package lsm

import (
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// TestMonkeyBitsForLevel pins the per-level allocation: L0 and L1 get the top budget,
// every level below never gets more bits than the one above, the budget never falls
// below the floor, and a deep level bottoms out at the floor.
func TestMonkeyBitsForLevel(t *testing.T) {
	const T = 10
	if got := bloomBitsForLevel(0, T); got != bloomBitsTop {
		t.Fatalf("L0 bits = %d, want top %d", got, bloomBitsTop)
	}
	if got := bloomBitsForLevel(1, T); got != bloomBitsTop {
		t.Fatalf("L1 bits = %d, want top %d", got, bloomBitsTop)
	}
	prev := bloomBitsForLevel(0, T)
	for lvl := 1; lvl <= 12; lvl++ {
		got := bloomBitsForLevel(lvl, T)
		if got > prev {
			t.Fatalf("bits at level %d (%d) exceed level %d (%d); the budget must not grow with depth", lvl, got, lvl-1, prev)
		}
		if got < bloomBitsFloor {
			t.Fatalf("bits at level %d (%d) fell below the floor %d", lvl, got, bloomBitsFloor)
		}
		prev = got
	}
	if got := bloomBitsForLevel(20, T); got != bloomBitsFloor {
		t.Fatalf("a very deep level got %d bits, want the floor %d", got, bloomBitsFloor)
	}
	if monkeyStep(1) != 1 {
		t.Fatalf("monkeyStep(1) = %d, want 1 (a degenerate ratio still steps down)", monkeyStep(1))
	}
	if monkeyStep(T) < 1 {
		t.Fatalf("monkeyStep(%d) = %d, want at least 1", T, monkeyStep(T))
	}
}

// TestMonkeyDeepSegmentsGetFewerBits drives a compaction whose output lands at L2 and
// confirms the L2 segment carries fewer probes than the L1 segments above it, that the
// shrunken filter still has no false negatives, and that the reduced probe count
// round-trips through the footer on a reopen.
func TestMonkeyDeepSegmentsGetFewerBits(t *testing.T) {
	fs, pgr := newDurablePager(t)
	l := openLSM(t, pgr)
	l.l0Trigger = 1
	l.levelRatio = 10
	l.segTargetBytes = 1 // one version group per output segment

	// Build three disjoint L1 segments by splitting one L0 segment of far-apart keys.
	applyLSN(t, l, 1, 1, func(b *engine.WriteBatch) {
		b.Set([]byte("aaa"), []byte("1"))
		b.Set([]byte("mmm"), []byte("1"))
		b.Set([]byte("zzz"), []byte("1"))
	})
	l.flushActive(t)
	compact(t, l, 0)
	if len(l.levelsLocked()) < 2 || len(l.levelsLocked()[1]) != 3 {
		t.Fatalf("expected three L1 segments, got shape %v", levelShape(l))
	}
	topK := bloomK(bloomBitsForLevel(1, l.levelRatio))
	for _, s := range l.levelsLocked()[1] {
		if k, ok := bloomProbes(s.filter); !ok || k != topK {
			t.Fatalf("L1 segment carries %v probes, want the top budget %d", s.filter, topK)
		}
	}

	// Move one L1 segment down to L2; the output filter must shrink to the L2 budget.
	l.mu.Lock()
	if _, err := l.runCompactionLocked(1, 0, false); err != nil {
		l.mu.Unlock()
		t.Fatalf("compact L1: %v", err)
	}
	l.mu.Unlock()
	if len(l.levelsLocked()) < 3 || len(l.levelsLocked()[2]) == 0 {
		t.Fatalf("expected an L2 segment, got shape %v", levelShape(l))
	}
	deepK := bloomK(bloomBitsForLevel(2, l.levelRatio))
	if deepK >= topK {
		t.Fatalf("test setup: the L2 budget (%d probes) does not undercut L1 (%d)", deepK, topK)
	}
	for _, s := range l.levelsLocked()[2] {
		if k, ok := bloomProbes(s.filter); !ok || k != deepK {
			t.Fatalf("L2 segment carries %v probes, want the reduced budget %d", s.filter, deepK)
		}
		// The smaller filter still reports every key it holds.
		var keys [][]byte
		if err := s.scan(l.pgr, func(ik, _ []byte) bool {
			keys = append(keys, append([]byte(nil), format.UserKey(ik)...))
			return true
		}); err != nil {
			t.Fatalf("scan: %v", err)
		}
		for _, k := range keys {
			if !s.filter.mayContain(k) {
				t.Fatalf("the reduced L2 filter reports a false negative for a key it holds")
			}
		}
	}

	// The reduced probe count round-trips through the footer: a reopen rebuilds the same
	// L2 filter from disk, with no flat default papering over a lost value.
	if err := pgr.Checkpoint(l.DurableLSN(), 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	pgr2 := reopenPager(t, fs, pgr)
	l2 := openLSM(t, pgr2)
	if len(l2.levelsLocked()) < 3 || len(l2.levelsLocked()[2]) == 0 {
		t.Fatalf("reopen lost L2, got shape %v", levelShape(l2))
	}
	for _, s := range l2.levelsLocked()[2] {
		if k, ok := bloomProbes(s.filter); ok && k != deepK {
			t.Fatalf("after reopen the L2 segment carries %d probes, want %d", k, deepK)
		}
	}
}

// bloomProbes returns the probe count of a segment's filter when it is a Bloom filter,
// the Monkey tests' window onto the per-level bit budget. A nil or non-Bloom filter
// returns ok=false, so a test that pins a Bloom budget fails loudly rather than reading a
// zero out of the wrong filter kind.
func bloomProbes(f segFilter) (uint32, bool) {
	bf, ok := f.(*bloomFilter)
	if !ok {
		return 0, false
	}
	return bf.k, true
}
