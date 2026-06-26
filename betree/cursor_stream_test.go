package betree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// TestStreamingCursorCrossesChunkBoundaries drives the forward streaming cursor across many of
// its windows. scanChunkKeys is 128, so a few thousand keys forces dozens of window refills, and
// a correct cursor must hand back every key exactly once in order with no gap or repeat at a
// window seam. It checks a full forward walk, a bounded SeekGE-then-Next scan (the ycsb-e shape
// the streaming path exists for), and a reverse walk (the full-materialization fallback), all
// against the same oracle.
func TestStreamingCursorCrossesChunkBoundaries(t *testing.T) {
	tr := newTreeBig(t)

	const n = 4000 // > 30 windows at scanChunkKeys=128
	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("k%06d", i)) }
	valOf := func(i int) []byte { return []byte(fmt.Sprintf("v%06d-%0120d", i, i)) }

	for base := 0; base < n; base += 200 {
		wb := engine.NewWriteBatch(1)
		for i := base; i < base+200 && i < n; i++ {
			wb.Set(keyOf(i), valOf(i))
		}
		if err := tr.Apply(wb, wb.Version()); err != nil {
			t.Fatalf("apply at %d: %v", base, err)
		}
		if base%800 == 0 {
			if err := tr.Flush(); err != nil {
				t.Fatalf("flush at %d: %v", base, err)
			}
		}
	}
	if err := tr.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}

	rd, err := tr.NewReader(engine.Snapshot{Version: 1})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()

	// Full forward walk: every key in order, crossing every window boundary.
	it, err := rd.NewIter(engine.IterOptions{})
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	want := 0
	for ok := it.First(); ok; ok = it.Next() {
		if string(it.Key()) != string(keyOf(want)) {
			t.Fatalf("forward pos %d key = %q, want %q", want, it.Key(), keyOf(want))
		}
		want++
	}
	if want != n {
		t.Fatalf("forward walk saw %d keys, want %d", want, n)
	}

	// Bounded SeekGE-then-Next from a key mid-keyspace, the streaming hot path: read 50 keys
	// and stop, which must not require materializing the rest.
	it2, err := rd.NewIter(engine.IterOptions{})
	if err != nil {
		t.Fatalf("iter2: %v", err)
	}
	seen := 0
	for ok := it2.SeekGE(keyOf(1500)); ok && seen < 50; ok = it2.Next() {
		if string(it2.Key()) != string(keyOf(1500+seen)) {
			t.Fatalf("bounded scan pos %d key = %q, want %q", seen, it2.Key(), keyOf(1500+seen))
		}
		seen++
	}
	if seen != 50 {
		t.Fatalf("bounded scan saw %d keys, want 50", seen)
	}

	// Reverse walk: every key in descending order through the full fallback.
	it3, err := rd.NewIter(engine.IterOptions{Reverse: true})
	if err != nil {
		t.Fatalf("iter3: %v", err)
	}
	rwant := n - 1
	for ok := it3.First(); ok; ok = it3.Next() {
		if string(it3.Key()) != string(keyOf(rwant)) {
			t.Fatalf("reverse pos: key = %q, want %q", it3.Key(), keyOf(rwant))
		}
		rwant--
	}
	if rwant != -1 {
		t.Fatalf("reverse walk stopped at %d, want -1", rwant)
	}
}

// TestStreamingCursorSkipsDeletedSpans forces a window that folds to nothing: a contiguous run
// wider than scanChunkKeys is deleted at the read snapshot, so the cursor must refill across the
// empty window rather than stop at it. A forward walk must surface exactly the surviving keys in
// order, proving the empty-window refill loop both fires and terminates.
func TestStreamingCursorSkipsDeletedSpans(t *testing.T) {
	tr := newTreeBig(t)

	const n = 1200
	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("d%06d", i)) }
	b := engine.NewWriteBatch(1)
	for i := 0; i < n; i++ {
		b.Set(keyOf(i), []byte(fmt.Sprintf("val-%06d", i)))
	}
	if err := tr.Apply(b, b.Version()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Delete a contiguous span of 400 keys (> 3 windows at scanChunkKeys=128) at version 2.
	wb := engine.NewWriteBatch(2)
	for i := 400; i < 800; i++ {
		wb.Delete(keyOf(i))
	}
	if err := tr.Apply(wb, wb.Version()); err != nil {
		t.Fatalf("delete span: %v", err)
	}
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush deletes: %v", err)
	}

	rd, err := tr.NewReader(engine.Snapshot{Version: 2})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	it, err := rd.NewIter(engine.IterOptions{})
	if err != nil {
		t.Fatalf("iter: %v", err)
	}

	var got []int
	for ok := it.First(); ok; ok = it.Next() {
		var idx int
		if _, err := fmt.Sscanf(string(it.Key()), "d%06d", &idx); err != nil {
			t.Fatalf("bad key %q: %v", it.Key(), err)
		}
		got = append(got, idx)
	}
	// Expect 0..399 and 800..1199, the deleted span absent.
	wantCount := 400 + 400
	if len(got) != wantCount {
		t.Fatalf("survivors = %d, want %d", len(got), wantCount)
	}
	for pos, idx := range got {
		var exp int
		if pos < 400 {
			exp = pos
		} else {
			exp = 800 + (pos - 400)
		}
		if idx != exp {
			t.Fatalf("survivor pos %d = %d, want %d", pos, idx, exp)
		}
	}
}
