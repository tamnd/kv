package lsm

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// scanRange runs a bounded forward scan through the engine reader and returns the
// (key, value) pairs it yields, the streaming-merge path this slice introduced.
func scanRange(t *testing.T, l *LSM, opts engine.IterOptions, version uint64) [][2]string {
	t.Helper()
	rd, err := l.NewReader(engine.Snapshot{Version: version})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	cur, err := rd.NewIter(opts)
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer cur.Close()
	var out [][2]string
	var last []byte
	for ok := cur.First(); ok; ok = cur.Next() {
		k := append([]byte(nil), cur.Key()...)
		if last != nil && bytes.Compare(k, last) <= 0 {
			t.Fatalf("scan out of order: %q after %q", k, last)
		}
		last = k
		lv, err := cur.Value()
		if err != nil {
			t.Fatalf("value: %v", err)
		}
		val, err := lv.Value()
		if err != nil {
			t.Fatalf("lazy value: %v", err)
		}
		out = append(out, [2]string{string(k), string(val)})
	}
	return out
}

// TestMergeBoundedScanAcrossSegments spreads keys across many segments plus the live
// memtable and confirms a bounded scan returns exactly the keys in [lower, upper) in
// order, the streaming merge seeking each source to the lower bound and stopping at
// the upper.
func TestMergeBoundedScanAcrossSegments(t *testing.T) {
	l := newLSM(t)

	const segs = 10
	const perSeg = 100
	version := uint64(1)
	for s := 0; s < segs; s++ {
		b := engine.NewWriteBatch(version)
		for i := 0; i < perSeg; i++ {
			key := fmt.Sprintf("key%05d", s*perSeg+i)
			b.Set([]byte(key), []byte(fmt.Sprintf("v%d", s*perSeg+i)))
		}
		if err := l.Apply(b, version); err != nil {
			t.Fatalf("apply seg %d: %v", s, err)
		}
		l.flushActive(t)
		version++
	}
	// A live memtable write on top, so the scan crosses on-disk and in-memory sources.
	bm := engine.NewWriteBatch(version)
	bm.Set([]byte("key00250"), []byte("memwins"))
	if err := l.Apply(bm, version); err != nil {
		t.Fatalf("apply mem: %v", err)
	}

	got := scanRange(t, l, engine.IterOptions{Lower: []byte("key00200"), Upper: []byte("key00300")}, version)
	if len(got) != 100 {
		t.Fatalf("bounded scan returned %d keys, want 100", len(got))
	}
	if got[0][0] != "key00200" || got[len(got)-1][0] != "key00299" {
		t.Fatalf("bounded scan range = [%s, %s], want [key00200, key00299]", got[0][0], got[len(got)-1][0])
	}
	// The memtable version of key00250 shadows the segment version.
	for _, kv := range got {
		if kv[0] == "key00250" && kv[1] != "memwins" {
			t.Fatalf("key00250 = %q, want memwins (memtable shadows segment)", kv[1])
		}
	}
}

// TestMergeFullScanMatchesKeys confirms an unbounded scan over a multi-segment engine
// returns every live key once, in order, the same set the old gather-and-sort path
// produced.
func TestMergeFullScanMatchesKeys(t *testing.T) {
	l := newLSM(t)

	want := map[string]bool{}
	version := uint64(1)
	for s := 0; s < 8; s++ {
		b := engine.NewWriteBatch(version)
		for i := 0; i < 60; i++ {
			key := fmt.Sprintf("k%04d", s*60+i)
			b.Set([]byte(key), []byte("v"))
			want[key] = true
		}
		if err := l.Apply(b, version); err != nil {
			t.Fatalf("apply: %v", err)
		}
		l.flushActive(t)
		version++
	}

	got := scanRange(t, l, engine.IterOptions{}, version)
	if len(got) != len(want) {
		t.Fatalf("full scan returned %d keys, want %d", len(got), len(want))
	}
	for _, kv := range got {
		if !want[kv[0]] {
			t.Fatalf("full scan returned unexpected key %q", kv[0])
		}
	}
}

// TestMergePrefixScan confirms a prefix scan returns only the keys under the prefix,
// the bounds NewIter derives from Prefix feeding straight into the merge seek.
func TestMergePrefixScan(t *testing.T) {
	l := newLSM(t)
	l.memtableCap = 1 // scatter the prefixes across segments

	for i, key := range []string{"apple", "apricot", "banana", "berry", "cherry", "date"} {
		b := engine.NewWriteBatch(uint64(i + 1))
		b.Set([]byte(key), []byte("v"))
		if err := l.Apply(b, uint64(i+1)); err != nil {
			t.Fatalf("apply: %v", err)
		}
		l.flushActive(t)
	}

	got := scanRange(t, l, engine.IterOptions{Prefix: []byte("ap")}, 100)
	if len(got) != 2 || got[0][0] != "apple" || got[1][0] != "apricot" {
		t.Fatalf("prefix scan = %v, want [apple apricot]", got)
	}
}

// TestMergeScanSpansSpilledGroup forces a user key's version group to span several
// data pages and confirms a scan crossing it yields the group's newest visible version
// once and the neighbours intact, so the page-crossing segment iterator stays aligned.
func TestMergeScanSpansSpilledGroup(t *testing.T) {
	l := newLSM(t)
	l.SetMergeFunc(concatMerge)

	applyBatch(t, l, 1, func(b *engine.WriteBatch) { b.Set([]byte("aaa"), []byte("a")) })
	big := make([]byte, 3000)
	for v := uint64(2); v < 8; v++ {
		for i := range big {
			big[i] = byte(v)
		}
		val := append([]byte(nil), big...)
		applyBatch(t, l, v, func(b *engine.WriteBatch) { b.Set([]byte("mmm"), val) })
	}
	applyBatch(t, l, 8, func(b *engine.WriteBatch) { b.Set([]byte("zzz"), []byte("z")) })
	l.flushActive(t)
	if len(l.allSegmentsLocked()[0].index) < 3 {
		t.Fatalf("expected the big group to span pages, got %d index entries", len(l.allSegmentsLocked()[0].index))
	}

	got := scanRange(t, l, engine.IterOptions{}, 100)
	if len(got) != 3 {
		t.Fatalf("scan returned %d keys, want 3 (aaa, mmm, zzz)", len(got))
	}
	if got[0][0] != "aaa" || got[2][0] != "zzz" {
		t.Fatalf("scan endpoints = [%s, %s], want [aaa, zzz]", got[0][0], got[2][0])
	}
	if got[1][0] != "mmm" || got[1][1] != string(bytes.Repeat([]byte{7}, 3000)) {
		t.Fatalf("mmm did not fold to its newest version across the spilled pages")
	}
}

// TestMergeBoundedScanWithRangeDelete confirms a bounded scan honors a range delete
// that lives in a segment, skipping the covered keys while returning the rest of the
// bounded range, the live-interval set folded inside the streaming merge.
func TestMergeBoundedScanWithRangeDelete(t *testing.T) {
	l := newLSM(t)

	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("k%03d", i)
		applyBatch(t, l, uint64(i+1), func(b *engine.WriteBatch) { b.Set([]byte(key), []byte("v")) })
	}
	applyBatch(t, l, 100, func(b *engine.WriteBatch) { b.DeleteRange([]byte("k010"), []byte("k020")) })
	l.flushActive(t)

	got := scanRange(t, l, engine.IterOptions{Lower: []byte("k005"), Upper: []byte("k025")}, 200)
	for _, kv := range got {
		if kv[0] >= "k010" && kv[0] < "k020" {
			t.Fatalf("scan returned %q, which the range delete covers", kv[0])
		}
	}
	// k005..k009 and k020..k024 survive: ten keys.
	if len(got) != 10 {
		t.Fatalf("bounded scan returned %d keys, want 10 (range delete removed the middle band)", len(got))
	}
}
