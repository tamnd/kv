package lsm

import (
	"bytes"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// segKinds scans a segment and returns the kind of every cell in stored order, the way
// the separation tests check whether a value was inlined or turned into a pointer.
func segKinds(t *testing.T, l *LSM, s *segment) []format.Kind {
	t.Helper()
	var kinds []format.Kind
	err := s.scan(l.pgr, func(ik, _ []byte) bool {
		kinds = append(kinds, format.KindOf(ik))
		return true
	})
	if err != nil {
		t.Fatalf("scan segment: %v", err)
	}
	return kinds
}

// scanSeparated resolves the whole keyspace through a reader and returns the key/value
// pairs in order. KeysOnly drives the no-materialize path, where a separated value is
// never fetched from the vLog.
func scanSeparated(t *testing.T, l *LSM, version uint64, keysOnly bool) (keys []string, vals [][]byte) {
	t.Helper()
	rd, err := l.NewReader(engine.Snapshot{Version: version})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	cur, err := rd.NewIter(engine.IterOptions{KeysOnly: keysOnly})
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer cur.Close()
	for ok := cur.First(); ok; ok = cur.Next() {
		lv, err := cur.Value()
		if err != nil {
			t.Fatalf("value: %v", err)
		}
		v, err := lv.Value()
		if err != nil {
			t.Fatalf("materialize: %v", err)
		}
		keys = append(keys, string(cur.Key()))
		vals = append(vals, append([]byte(nil), v...))
	}
	return keys, vals
}

// TestValueSeparationRoundtrip turns on separation with a low threshold, writes a value
// past it, and confirms the flushed cell became a pointer while the value still reads
// back whole through the point path.
func TestValueSeparationRoundtrip(t *testing.T) {
	l := newLSM(t)
	l.valueSepThreshold = 16

	big := bytes.Repeat([]byte("x"), 200)
	applyBatch(t, l, 1, func(b *engine.WriteBatch) { b.Set([]byte("k"), big) })
	l.flushActive(t)

	segs := l.allSegmentsLocked()
	if len(segs) != 1 {
		t.Fatalf("expected one segment, got %d", len(segs))
	}
	kinds := segKinds(t, l, segs[0])
	if len(kinds) != 1 || kinds[0] != format.KindSetSep {
		t.Fatalf("cell kinds = %v, want one setsep", kinds)
	}

	if v, ok := getAt(t, l, "k", 1); !ok || !bytes.Equal(v, big) {
		t.Fatalf("Get(k) = (%d bytes, %v), want the 200-byte value back", len(v), ok)
	}
}

// TestValueLargerThanPageLifts confirms the auto-separation clause: with separation
// nominally off, a value too large to fit a segment cell is still stored, by spilling
// across vLog pages, instead of being rejected the way it was before this slice.
func TestValueLargerThanPageLifts(t *testing.T) {
	l := newLSM(t) // valueSepThreshold stays 0, separation "off"

	usable := l.pgr.Header().UsablePageSize()
	huge := bytes.Repeat([]byte("z"), usable*3) // spans several vLog pages

	applyBatch(t, l, 1, func(b *engine.WriteBatch) { b.Set([]byte("big"), huge) })
	l.flushActive(t) // before this slice writeSegment would have rejected the oversized cell

	kinds := segKinds(t, l, l.allSegmentsLocked()[0])
	if len(kinds) != 1 || kinds[0] != format.KindSetSep {
		t.Fatalf("oversized value was not separated, kinds = %v", kinds)
	}
	if v, ok := getAt(t, l, "big", 1); !ok || !bytes.Equal(v, huge) {
		t.Fatalf("Get(big) lost the page-spanning value: ok=%v len=%d want=%d", ok, len(v), len(huge))
	}
}

// TestThresholdGatesSeparation confirms only values at or above the threshold separate:
// a small value keeps its inline KindSet cell, a large one becomes a pointer.
func TestThresholdGatesSeparation(t *testing.T) {
	l := newLSM(t)
	l.valueSepThreshold = 100

	applyBatch(t, l, 1, func(b *engine.WriteBatch) {
		b.Set([]byte("small"), []byte("tiny"))
		b.Set([]byte("large"), bytes.Repeat([]byte("y"), 200))
	})
	l.flushActive(t)

	got := map[string]format.Kind{}
	err := l.allSegmentsLocked()[0].scan(l.pgr, func(ik, _ []byte) bool {
		got[string(format.UserKey(ik))] = format.KindOf(ik)
		return true
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got["small"] != format.KindSet {
		t.Fatalf("small value separated, kind = %v, want set", got["small"])
	}
	if got["large"] != format.KindSetSep {
		t.Fatalf("large value not separated, kind = %v, want setsep", got["large"])
	}
}

// TestSeparatedPointersSurviveReopen folds a flush with a separated value to the file,
// reopens through a fresh engine with no WAL, and reads the value back: the pointer cell
// in the segment and the vLog pages it names both survived on the checkpoint.
func TestSeparatedPointersSurviveReopen(t *testing.T) {
	fs, pgr := newDurablePager(t)
	l := openLSM(t, pgr)
	l.valueSepThreshold = 16

	big := bytes.Repeat([]byte("w"), 500)
	applyLSN(t, l, 1, 1, func(b *engine.WriteBatch) { b.Set([]byte("k"), big) })
	l.flushActive(t)
	if err := pgr.Checkpoint(l.DurableLSN(), 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	pgr2 := reopenPager(t, fs, pgr)
	l2 := openLSM(t, pgr2)
	if len(l2.allSegmentsLocked()) != 1 {
		t.Fatalf("reopened engine restored no segments")
	}
	if v, ok := getAt(t, l2, "k", 1); !ok || !bytes.Equal(v, big) {
		t.Fatalf("after reopen Get(k) = (%d bytes, %v), want the 500-byte value", len(v), ok)
	}
}

// TestCompactionCarriesPointers confirms compaction moves a separated value's pointer
// without rewriting the value: a pushed-down cell is still a KindSetSep and still
// resolves to the original bytes after it has descended a level.
func TestCompactionCarriesPointers(t *testing.T) {
	l := newLSM(t)
	l.valueSepThreshold = 16
	l.l0Trigger = 1

	big := bytes.Repeat([]byte("q"), 300)
	applyBatch(t, l, 1, func(b *engine.WriteBatch) { b.Set([]byte("k"), big) })
	l.flushActive(t)
	compact(t, l, 0) // push L0 down into the bottom

	bottom := l.levelsLocked()[1]
	if len(bottom) != 1 {
		t.Fatalf("expected one bottom run after compaction, shape %v", levelShape(l))
	}
	kinds := segKinds(t, l, bottom[0])
	if len(kinds) != 1 || kinds[0] != format.KindSetSep {
		t.Fatalf("compaction did not carry the pointer through, kinds = %v", kinds)
	}
	if v, ok := getAt(t, l, "k", 1); !ok || !bytes.Equal(v, big) {
		t.Fatalf("Get(k) after compaction = (%d bytes, %v), want the value intact", len(v), ok)
	}
}

// TestKeysOnlyScanSkipsValue confirms the KeysOnly contract reaches the separated path:
// a key-only scan yields every key with an empty value (the vLog is never read), while a
// normal scan yields the full separated values.
func TestKeysOnlyScanSkipsValue(t *testing.T) {
	l := newLSM(t)
	l.valueSepThreshold = 16

	want := map[string][]byte{
		"a": bytes.Repeat([]byte("1"), 200),
		"b": bytes.Repeat([]byte("2"), 200),
		"c": bytes.Repeat([]byte("3"), 200),
	}
	applyBatch(t, l, 1, func(b *engine.WriteBatch) {
		for k, v := range want {
			b.Set([]byte(k), v)
		}
	})
	l.flushActive(t)

	keys, vals := scanSeparated(t, l, 1, true) // KeysOnly
	if got := len(keys); got != 3 {
		t.Fatalf("keys-only scan returned %d keys, want 3", got)
	}
	for i, k := range keys {
		if len(vals[i]) != 0 {
			t.Fatalf("keys-only scan materialized %q = %d bytes, want empty", k, len(vals[i]))
		}
	}

	keys, vals = scanSeparated(t, l, 1, false) // full scan
	for i, k := range keys {
		if !bytes.Equal(vals[i], want[k]) {
			t.Fatalf("full scan %q = %d bytes, want %d", k, len(vals[i]), len(want[k]))
		}
	}
}
