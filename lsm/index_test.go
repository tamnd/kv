package lsm

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// applyBatch commits one batch at a version into the active memtable, the engine
// half of a host write, used by the index tests to stage data before a flush.
func applyBatch(t *testing.T, l *LSM, version uint64, fill func(b *engine.WriteBatch)) {
	t.Helper()
	b := engine.NewWriteBatch(version)
	fill(b)
	if err := l.Apply(b, version); err != nil {
		t.Fatalf("apply at version %d: %v", version, err)
	}
}

// getAt resolves a user key at a snapshot through the engine's reader, the point
// path the block index serves.
func getAt(t *testing.T, l *LSM, key string, version uint64) ([]byte, bool) {
	t.Helper()
	rd, err := l.NewReader(engine.Snapshot{Version: version})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	v, err := rd.Get([]byte(key))
	if err == engine.ErrNotFound {
		return nil, false
	}
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return v, true
}

// TestSegmentBlockIndexPointGet flushes enough keys to fill several data pages in one
// segment, then point-reads every key plus keys that fall between, before, and after
// the stored range, exercising the block-index seek and the segment key-range reject.
func TestSegmentBlockIndexPointGet(t *testing.T) {
	l := newLSM(t)
	l.SetMergeFunc(concatMerge)

	const n = 2000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		val := fmt.Sprintf("val%06d", i)
		applyBatch(t, l, uint64(i+1), func(b *engine.WriteBatch) { b.Set([]byte(key), []byte(val)) })
	}
	l.flushActive(t)
	if len(l.allSegmentsLocked()) != 1 {
		t.Fatalf("expected one segment, got %d", len(l.allSegmentsLocked()))
	}
	seg := l.allSegmentsLocked()[0]
	if len(seg.index) < 2 {
		t.Fatalf("expected a multi-page segment, got %d index entries", len(seg.index))
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		want := fmt.Sprintf("val%06d", i)
		if v, ok := getAt(t, l, key, n+1); !ok || string(v) != want {
			t.Fatalf("Get(%s) = (%q,%v), want %q", key, v, ok, want)
		}
	}
	// Keys that do not exist: below the range, above the range, and between two keys.
	for _, miss := range []string{"key999999", "aaa", "key000000x"} {
		if v, ok := getAt(t, l, miss, n+1); ok {
			t.Fatalf("Get(%s) = %q, want missing", miss, v)
		}
	}
}

// TestSegmentGroupSpansPages forces one user key's version group to exceed a single
// data page, so writeSegment spills it onto continuation pages that repeat its first
// key, then confirms the block index still seeks to the group's newest version.
func TestSegmentGroupSpansPages(t *testing.T) {
	l := newLSM(t)
	l.SetMergeFunc(concatMerge)

	// A neighbour on each side so the big group sits in the middle of the run.
	applyBatch(t, l, 1, func(b *engine.WriteBatch) { b.Set([]byte("aaa"), []byte("a")) })
	big := make([]byte, 3000) // each version is most of a 4 KiB page
	const versions = 6
	for v := uint64(2); v < 2+versions; v++ {
		for i := range big {
			big[i] = byte(v)
		}
		val := append([]byte(nil), big...)
		applyBatch(t, l, v, func(b *engine.WriteBatch) { b.Set([]byte("mmm"), val) })
	}
	applyBatch(t, l, 2+versions, func(b *engine.WriteBatch) { b.Set([]byte("zzz"), []byte("z")) })
	l.flushActive(t)

	seg := l.allSegmentsLocked()[0]
	// The big group alone must have forced more pages than there are distinct keys.
	if len(seg.index) < 3 {
		t.Fatalf("expected the big group to span pages, got %d index entries", len(seg.index))
	}

	// The newest version is visible at a high snapshot.
	newest := byte(2 + versions - 1)
	want := bytes.Repeat([]byte{newest}, 3000)
	if v, ok := getAt(t, l, "mmm", 100); !ok || !bytes.Equal(v, want) {
		t.Fatalf("Get(mmm) newest mismatch: ok=%v len=%d firstByte=%d", ok, len(v), firstByte(v))
	}
	// An older snapshot sees an older version, proving every version of the spanning
	// group is reachable, not just the one the seek lands on.
	wantOld := bytes.Repeat([]byte{3}, 3000) // version 3
	if v, ok := getAt(t, l, "mmm", 3); !ok || !bytes.Equal(v, wantOld) {
		t.Fatalf("Get(mmm)@3 mismatch: ok=%v len=%d firstByte=%d", ok, len(v), firstByte(v))
	}
	// The neighbours still resolve, so the spanning group did not corrupt the index.
	if v, ok := getAt(t, l, "aaa", 100); !ok || string(v) != "a" {
		t.Fatalf("Get(aaa) = (%q,%v), want a", v, ok)
	}
	if v, ok := getAt(t, l, "zzz", 100); !ok || string(v) != "z" {
		t.Fatalf("Get(zzz) = (%q,%v), want z", v, ok)
	}
}

func firstByte(b []byte) int {
	if len(b) == 0 {
		return -1
	}
	return int(b[0])
}

// TestSegmentPointGetVersioned flushes overwrites, deletes, and a merge into one
// segment and confirms the point path folds each key's group to the newest visible
// version, the same resolution the full-snapshot path produces.
func TestSegmentPointGetVersioned(t *testing.T) {
	l := newLSM(t)
	l.SetMergeFunc(concatMerge)

	const n = 300
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%05d", i)
		applyBatch(t, l, uint64(i+1), func(b *engine.WriteBatch) { b.Set([]byte(key), []byte("first")) })
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%05d", i)
		applyBatch(t, l, uint64(n+i+1), func(b *engine.WriteBatch) {
			switch i % 3 {
			case 0:
				b.Delete([]byte(key))
			case 1:
				b.Set([]byte(key), []byte("second"))
			default:
				b.Merge([]byte(key), []byte("+"))
			}
		})
	}
	l.flushActive(t)

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%05d", i)
		v, ok := getAt(t, l, key, uint64(2*n+1))
		switch i % 3 {
		case 0:
			if ok {
				t.Fatalf("Get(%s) = %q, want deleted", key, v)
			}
		case 1:
			if !ok || string(v) != "second" {
				t.Fatalf("Get(%s) = (%q,%v), want second", key, v, ok)
			}
		default:
			if !ok || string(v) != "first+" {
				t.Fatalf("Get(%s) = (%q,%v), want first+", key, v, ok)
			}
		}
	}
}

// TestSegmentRangeDelPointGet flushes a range delete into a segment so its interval
// is persisted in the segment's range-delete chain, then confirms the point path
// reads keys the delete covers as absent without scanning the run for the marker.
func TestSegmentRangeDelPointGet(t *testing.T) {
	l := newLSM(t)
	l.SetMergeFunc(concatMerge)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k%03d", i)
		applyBatch(t, l, uint64(i+1), func(b *engine.WriteBatch) { b.Set([]byte(key), []byte("v")) })
	}
	// Delete the band [k010, k040) at a version newer than every set above.
	applyBatch(t, l, 100, func(b *engine.WriteBatch) { b.DeleteRange([]byte("k010"), []byte("k040")) })
	l.flushActive(t)

	if len(l.allSegmentsLocked()[0].rangeDels) != 1 {
		t.Fatalf("expected the range delete persisted in the segment, got %d", len(l.allSegmentsLocked()[0].rangeDels))
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k%03d", i)
		v, ok := getAt(t, l, key, 200)
		covered := i >= 10 && i < 40
		if covered && ok {
			t.Fatalf("Get(%s) = %q, want covered by range delete", key, v)
		}
		if !covered && (!ok || string(v) != "v") {
			t.Fatalf("Get(%s) = (%q,%v), want v", key, v, ok)
		}
	}

	// A later set inside the band resurrects one key above the range delete.
	applyBatch(t, l, 300, func(b *engine.WriteBatch) { b.Set([]byte("k020"), []byte("back")) })
	if v, ok := getAt(t, l, "k020", 400); !ok || string(v) != "back" {
		t.Fatalf("Get(k020) = (%q,%v), want back after resurrect", v, ok)
	}
}

// TestSegmentPointGetShadowing confirms the newest version wins across sources: a key
// flushed into a segment and then overwritten in the memtable reads the memtable
// value, and one overwritten across two segments reads the newer segment.
func TestSegmentPointGetShadowing(t *testing.T) {
	l := newLSM(t)
	l.SetMergeFunc(concatMerge)

	applyBatch(t, l, 1, func(b *engine.WriteBatch) { b.Set([]byte("a"), []byte("seg1")) })
	applyBatch(t, l, 2, func(b *engine.WriteBatch) { b.Set([]byte("b"), []byte("seg1")) })
	l.flushActive(t)
	applyBatch(t, l, 3, func(b *engine.WriteBatch) { b.Set([]byte("a"), []byte("seg2")) })
	l.flushActive(t)
	applyBatch(t, l, 4, func(b *engine.WriteBatch) { b.Set([]byte("b"), []byte("mem")) })

	if len(l.allSegmentsLocked()) != 2 {
		t.Fatalf("expected two segments, got %d", len(l.allSegmentsLocked()))
	}
	if v, ok := getAt(t, l, "a", 100); !ok || string(v) != "seg2" {
		t.Fatalf("Get(a) = (%q,%v), want seg2 (newer segment wins)", v, ok)
	}
	if v, ok := getAt(t, l, "b", 100); !ok || string(v) != "mem" {
		t.Fatalf("Get(b) = (%q,%v), want mem (memtable wins)", v, ok)
	}
	// At an older snapshot the newer versions are invisible.
	if v, ok := getAt(t, l, "a", 1); !ok || string(v) != "seg1" {
		t.Fatalf("Get(a)@1 = (%q,%v), want seg1", v, ok)
	}
}

// TestSeekPageBoundaries pins seekPage's boundary behaviour directly against a small
// hand-built index, the logic a point read depends on to never miss a group.
func TestSeekPageBoundaries(t *testing.T) {
	seg := &segment{index: []indexEntry{
		{firstUser: []byte("c"), page: 10},
		{firstUser: []byte("f"), page: 11},
		{firstUser: []byte("f"), page: 12}, // a group spanning two pages repeats its key
		{firstUser: []byte("m"), page: 13},
	}}
	cases := []struct {
		key  string
		want int
	}{
		{"a", 0}, // below the first separator: start at page 0, scan finds nothing
		{"c", 0}, // exact match on the first page
		{"d", 0}, // between c and f: group lives mid-page on page 0
		{"f", 1}, // exact match, leftmost of the repeated run
		{"g", 2}, // between the f-run and m: page 2 holds the tail of any straddle
		{"m", 3}, // exact match on the last page
		{"z", 3}, // above every separator: start at the last page
	}
	for _, c := range cases {
		if got := seg.seekPage([]byte(c.key)); got != c.want {
			t.Fatalf("seekPage(%q) = %d, want %d", c.key, got, c.want)
		}
	}
}
