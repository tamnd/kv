package lsm

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// TestBloomNoFalseNegatives is the filter's load-bearing invariant: every key added
// must test present. A false negative would let a point read skip a segment that
// holds the key, returning a wrong absent, so this can never fail.
func TestBloomNoFalseNegatives(t *testing.T) {
	f := newBloom(5000, bloomBitsPerKey)
	for i := 0; i < 5000; i++ {
		f.add([]byte(fmt.Sprintf("key%06d", i)))
	}
	for i := 0; i < 5000; i++ {
		if !f.mayContain([]byte(fmt.Sprintf("key%06d", i))) {
			t.Fatalf("false negative for key%06d: an added key tested absent", i)
		}
	}
}

// TestBloomFalsePositiveRate confirms the filter rejects the large majority of keys
// it never saw. Ten bits per key targets roughly one percent false positives, so a
// rate above five percent means the hashing or sizing is broken, not just unlucky.
func TestBloomFalsePositiveRate(t *testing.T) {
	const n = 10000
	f := newBloom(n, bloomBitsPerKey)
	for i := 0; i < n; i++ {
		f.add([]byte(fmt.Sprintf("present%06d", i)))
	}
	fp := 0
	for i := 0; i < n; i++ {
		if f.mayContain([]byte(fmt.Sprintf("absent%06d", i))) {
			fp++
		}
	}
	rate := float64(fp) / float64(n)
	if rate > 0.05 {
		t.Fatalf("false-positive rate %.3f exceeds 0.05; the filter is not discriminating", rate)
	}
}

// TestBloomNilIsConservative confirms a nil filter, the state a segment written
// before this slice or one with no keys carries, passes every key so the segment is
// always read rather than wrongly skipped.
func TestBloomNilIsConservative(t *testing.T) {
	var f *bloomFilter
	if !f.mayContain([]byte("anything")) {
		t.Fatal("nil filter must pass every key so the segment is always read")
	}
	empty := &bloomFilter{}
	if !empty.mayContain([]byte("anything")) {
		t.Fatal("empty filter must pass every key so the segment is always read")
	}
}

// TestSegmentBloomBuiltAndProbed flushes keys into a segment and confirms the segment
// carries a filter that admits every key it holds and rejects keys it never saw, the
// in-memory half of the point-miss skip.
func TestSegmentBloomBuiltAndProbed(t *testing.T) {
	l := newLSM(t)
	l.SetMergeFunc(concatMerge)

	const n = 1000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		applyBatch(t, l, uint64(i+1), func(b *engine.WriteBatch) { b.Set([]byte(key), []byte("v")) })
	}
	l.flushActive(t)
	seg := l.segments[0]
	if seg.filter == nil {
		t.Fatal("flushed segment carries no Bloom filter")
	}
	for i := 0; i < n; i++ {
		if !seg.filter.mayContain([]byte(fmt.Sprintf("key%06d", i))) {
			t.Fatalf("segment filter rejects key%06d, which it holds", i)
		}
	}
	// A key well outside the flushed range should almost always be rejected; with a
	// thousand keys at ten bits each one probe is overwhelmingly likely to miss.
	rejected := 0
	for i := 0; i < 100; i++ {
		if !seg.filter.mayContain([]byte(fmt.Sprintf("absent%06d", i))) {
			rejected++
		}
	}
	if rejected == 0 {
		t.Fatal("segment filter admitted every absent key; it is not discriminating")
	}
}

// TestSegmentBloomSurvivesReopen folds a segment and its filter to the file, reopens
// through a fresh engine with no WAL, and confirms the reloaded filter still admits
// every key the segment holds, so the persisted probe count and bit array round-trip.
func TestSegmentBloomSurvivesReopen(t *testing.T) {
	fs, pgr := newDurablePager(t)
	l := openLSM(t, pgr)

	const n = 800
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		applyLSN(t, l, uint64(i+1), uint64(i+1), func(b *engine.WriteBatch) { b.Set([]byte(key), []byte("v")) })
	}
	l.flushActive(t)
	if err := pgr.Checkpoint(l.DurableLSN()); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	pgr2 := reopenPager(t, fs, pgr)
	l2 := openLSM(t, pgr2)
	if len(l2.segments) == 0 {
		t.Fatal("reopened engine restored no segments")
	}
	seg := l2.segments[0]
	if seg.filter == nil {
		t.Fatal("reopened segment lost its Bloom filter")
	}
	for i := 0; i < n; i++ {
		if !seg.filter.mayContain([]byte(fmt.Sprintf("key%06d", i))) {
			t.Fatalf("reloaded filter rejects key%06d, which the segment holds", i)
		}
	}
}

// TestSegmentBloomGetCorrectness drives the filter through the read path: keys spread
// across several segments must each resolve, and a key never written must read absent,
// so the skip-on-miss never drops a real key. The result must match the snapshot path,
// which consults every segment unconditionally.
func TestSegmentBloomGetCorrectness(t *testing.T) {
	l := newLSM(t)
	l.SetMergeFunc(concatMerge)

	// Three segments, each holding a disjoint key band, so most point reads hit one
	// segment and the filter rejects the other two.
	bands := []string{"aaa", "mmm", "zzz"}
	for s, prefix := range bands {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("%s%04d", prefix, i)
			version := uint64(s*100 + i + 1)
			applyBatch(t, l, version, func(b *engine.WriteBatch) { b.Set([]byte(key), []byte("v")) })
		}
		l.flushActive(t)
	}
	if len(l.segments) != 3 {
		t.Fatalf("expected three segments, got %d", len(l.segments))
	}

	for _, prefix := range bands {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("%s%04d", prefix, i)
			if v, ok := getAt(t, l, key, 1000); !ok || string(v) != "v" {
				t.Fatalf("Get(%s) = (%q,%v), want v", key, v, ok)
			}
		}
	}
	// Keys that were never written, between and beyond the bands, must read absent
	// even though the filter rejects most segments without reading them.
	for _, miss := range []string{"aaa9999", "bbb0001", "nnn0001", "zzz9999", "qqq0000"} {
		if v, ok := getAt(t, l, miss, 1000); ok {
			t.Fatalf("Get(%s) = %q, want missing", miss, v)
		}
	}
}
