package betree

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/tamnd/kv/format"
)

const testUsable = 4096 - 24 // a 4 KiB page minus a generous reserved trailer

// ik builds an internal key for a user key at a version with a kind, so the codec
// tests work in the same key space the engine does.
func ik(user string, version uint64, kind format.Kind) []byte {
	return format.EncodeInternalKey([]byte(user), version, kind)
}

// sortRecords puts records in ascending internal-key order, the precondition
// encodeLeaf documents and the leaf builder will guarantee.
func sortRecords(recs []record) {
	sort.Slice(recs, func(i, j int) bool {
		return format.CompareInternal(recs[i].key, recs[j].key) < 0
	})
}

func TestLeafRoundTrip(t *testing.T) {
	recs := []record{
		{ik("apple", 30, format.KindSet), []byte("green")},
		{ik("apple", 10, format.KindSet), []byte("red")},
		{ik("banana", 20, format.KindSet), []byte("yellow")},
		{ik("cherry", 40, format.KindMerge), []byte("?")},
		{ik("cherry", 30, format.KindSet), []byte("dark")},
		{ik("date", 30, format.KindDelete), nil},
	}
	sortRecords(recs)

	dst := make([]byte, testUsable)
	used, err := encodeLeaf(dst, &leaf{records: recs, left: 7, right: 9, bucketSize: 2})
	if err != nil {
		t.Fatalf("encodeLeaf: %v", err)
	}
	if used > testUsable {
		t.Fatalf("used %d exceeds usable %d", used, testUsable)
	}

	got, err := decodeLeaf(dst)
	if err != nil {
		t.Fatalf("decodeLeaf: %v", err)
	}
	if got.left != 7 || got.right != 9 {
		t.Fatalf("siblings = (%d,%d), want (7,9)", got.left, got.right)
	}
	if len(got.records) != len(recs) {
		t.Fatalf("record count = %d, want %d", len(got.records), len(recs))
	}
	for i := range recs {
		if !bytes.Equal(got.records[i].key, recs[i].key) {
			t.Fatalf("record %d key = %x, want %x", i, got.records[i].key, recs[i].key)
		}
		if !bytes.Equal(got.records[i].val, recs[i].val) {
			t.Fatalf("record %d val = %q, want %q", i, got.records[i].val, recs[i].val)
		}
	}
}

// TestLeafSeekPage checks that the on-page restart-array seek finds every key that
// is present and rejects keys that fall before, between, and after the records,
// which is the property the front-coded bucketed layout exists to provide.
func TestLeafSeekPage(t *testing.T) {
	var recs []record
	for i := 0; i < 50; i++ {
		recs = append(recs, record{ik(fmt.Sprintf("key%03d", i), 10, format.KindSet), []byte(fmt.Sprintf("val%03d", i))})
	}
	sortRecords(recs)

	dst := make([]byte, testUsable)
	if _, err := encodeLeaf(dst, &leaf{records: recs, bucketSize: 7}); err != nil {
		t.Fatalf("encodeLeaf: %v", err)
	}

	for _, r := range recs {
		val, found, err := seekLeafPage(dst, r.key)
		if err != nil {
			t.Fatalf("seek present key: %v", err)
		}
		if !found {
			t.Fatalf("key %x not found", r.key)
		}
		if !bytes.Equal(val, r.val) {
			t.Fatalf("value = %q, want %q", val, r.val)
		}
	}

	// A key below, between, and above the records must all miss cleanly.
	for _, miss := range []string{"key999", "aaa", "key0005half"} {
		_, found, err := seekLeafPage(dst, ik(miss, 10, format.KindSet))
		if err != nil {
			t.Fatalf("seek absent key: %v", err)
		}
		if found {
			t.Fatalf("absent key %q reported found", miss)
		}
	}
}

// TestLeafPageFull checks the codec reports ErrPageFull rather than overrunning when
// the records do not fit, the signal the leaf builder turns into a split.
func TestLeafPageFull(t *testing.T) {
	var recs []record
	for i := 0; i < 200; i++ {
		recs = append(recs, record{ik(fmt.Sprintf("k%05d", i), 1, format.KindSet), bytes.Repeat([]byte("x"), 64)})
	}
	sortRecords(recs)
	small := make([]byte, 256)
	if _, err := encodeLeaf(small, &leaf{records: recs, bucketSize: 16}); err != ErrPageFull {
		t.Fatalf("err = %v, want ErrPageFull", err)
	}
}

func TestInteriorRoundTrip(t *testing.T) {
	in := &interior{
		leftmost: 100,
		pivots: []pivot{
			{ik("d", 0, format.KindSet), 101},
			{ik("m", 0, format.KindSet), 102},
			{ik("t", 0, format.KindSet), 103},
		},
		buffer: []message{
			{kind: byte(format.KindSet), seq: 1, key: ik("a", 10, format.KindSet), val: []byte("av")},
			{kind: byte(format.KindDelete), seq: 2, key: ik("p", 20, format.KindDelete), val: nil},
			{kind: byte(format.KindMerge), seq: 3, key: ik("z", 30, format.KindMerge), val: []byte("zo")},
		},
	}

	dst := make([]byte, testUsable)
	if _, err := encodeInterior(dst, in); err != nil {
		t.Fatalf("encodeInterior: %v", err)
	}
	got, err := decodeInterior(dst)
	if err != nil {
		t.Fatalf("decodeInterior: %v", err)
	}
	if got.leftmost != 100 {
		t.Fatalf("leftmost = %d, want 100", got.leftmost)
	}
	if len(got.pivots) != 3 || len(got.buffer) != 3 {
		t.Fatalf("pivots=%d buffer=%d, want 3,3", len(got.pivots), len(got.buffer))
	}
	for i, p := range in.pivots {
		if !bytes.Equal(got.pivots[i].key, p.key) || got.pivots[i].child != p.child {
			t.Fatalf("pivot %d = (%x,%d), want (%x,%d)", i, got.pivots[i].key, got.pivots[i].child, p.key, p.child)
		}
	}
	for i, m := range in.buffer {
		g := got.buffer[i]
		if g.kind != m.kind || g.seq != m.seq || !bytes.Equal(g.key, m.key) || !bytes.Equal(g.val, m.val) {
			t.Fatalf("message %d mismatch: %+v vs %+v", i, g, m)
		}
	}
}

// TestInteriorRoute checks separator descent: a target below the first pivot routes
// to the leftmost child, and a target at or above a pivot routes to that pivot's
// child until the next pivot takes over.
func TestInteriorRoute(t *testing.T) {
	in := &interior{
		leftmost: 100,
		pivots: []pivot{
			{ik("d", 0, format.KindSet), 101},
			{ik("m", 0, format.KindSet), 102},
			{ik("t", 0, format.KindSet), 103},
		},
	}
	cases := []struct {
		key  string
		want format.PageNo
	}{
		{"a", 100}, {"c", 100},
		{"d", 101}, {"f", 101},
		{"m", 102}, {"s", 102},
		{"t", 103}, {"z", 103},
	}
	for _, c := range cases {
		if got := in.route(ik(c.key, 0, format.KindSet)); got != c.want {
			t.Fatalf("route(%q) = %d, want %d", c.key, got, c.want)
		}
	}
}

// FuzzDecodeLeaf asserts the leaf decoder never panics and never reads past the
// slice on arbitrary input: it either returns a leaf or ErrCorruptNode. The corpus
// is seeded with a valid encoding so the fuzzer starts from a reachable shape, and
// M8 inherits the target. A decoded leaf must re-encode to bytes that decode back
// equal, so the decoder cannot accept a shape the encoder would never emit.
func FuzzDecodeLeaf(f *testing.F) {
	recs := []record{
		{ik("a", 2, format.KindSet), []byte("x")},
		{ik("a", 1, format.KindSet), []byte("y")},
		{ik("bb", 1, format.KindDelete), nil},
	}
	sortRecords(recs)
	seed := make([]byte, testUsable)
	if _, err := encodeLeaf(seed, &leaf{records: recs, bucketSize: 2, left: 1, right: 2}); err != nil {
		f.Fatalf("seed encode: %v", err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Add(make([]byte, 20))

	f.Fuzz(func(t *testing.T, data []byte) {
		lf, err := decodeLeaf(data)
		if err != nil {
			return
		}
		out := make([]byte, len(data))
		if _, err := encodeLeaf(out, lf); err != nil {
			return // a decoded leaf that no longer fits is fine; it must not panic
		}
		again, err := decodeLeaf(out)
		if err != nil {
			t.Fatalf("re-decode of re-encoded leaf failed: %v", err)
		}
		if len(again.records) != len(lf.records) {
			t.Fatalf("record count drift: %d vs %d", len(again.records), len(lf.records))
		}
		for i := range lf.records {
			if !bytes.Equal(again.records[i].key, lf.records[i].key) || !bytes.Equal(again.records[i].val, lf.records[i].val) {
				t.Fatalf("record %d drifted across re-encode", i)
			}
		}
	})
}

// FuzzDecodeInterior is the interior counterpart: arbitrary bytes must yield an
// interior node or ErrCorruptNode, never a panic or an out-of-bounds read.
func FuzzDecodeInterior(f *testing.F) {
	in := &interior{
		leftmost: 9,
		pivots:   []pivot{{ik("m", 0, format.KindSet), 10}},
		buffer:   []message{{kind: byte(format.KindSet), seq: 1, key: ik("a", 1, format.KindSet), val: []byte("v")}},
	}
	seed := make([]byte, testUsable)
	if _, err := encodeInterior(seed, in); err != nil {
		f.Fatalf("seed encode: %v", err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Add(make([]byte, 24))

	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := decodeInterior(data)
		if err != nil {
			return
		}
		out := make([]byte, len(data))
		if _, err := encodeInterior(out, got); err != nil {
			return
		}
		again, err := decodeInterior(out)
		if err != nil {
			t.Fatalf("re-decode of re-encoded interior failed: %v", err)
		}
		if again.leftmost != got.leftmost || len(again.pivots) != len(got.pivots) || len(again.buffer) != len(got.buffer) {
			t.Fatalf("interior drifted across re-encode")
		}
	})
}

// TestLeafRandomRoundTrip throws many random sorted record sets at the leaf codec to
// shake out front-coding and bucket-boundary edge cases the fixed cases miss.
func TestLeafRandomRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 200; iter++ {
		n := rng.Intn(40)
		seen := map[string]bool{}
		var recs []record
		for len(recs) < n {
			k := ik(fmt.Sprintf("k%03d", rng.Intn(60)), uint64(rng.Intn(5)), format.KindSet)
			if seen[string(k)] {
				continue
			}
			seen[string(k)] = true
			recs = append(recs, record{k, []byte(fmt.Sprintf("v%d", rng.Intn(10000)))})
		}
		sortRecords(recs)
		bucketSize := 1 + rng.Intn(8)

		dst := make([]byte, testUsable)
		if _, err := encodeLeaf(dst, &leaf{records: recs, bucketSize: bucketSize}); err != nil {
			t.Fatalf("iter %d encode: %v", iter, err)
		}
		got, err := decodeLeaf(dst)
		if err != nil {
			t.Fatalf("iter %d decode: %v", iter, err)
		}
		if len(got.records) != len(recs) {
			t.Fatalf("iter %d count = %d, want %d", iter, len(got.records), len(recs))
		}
		for i := range recs {
			if !bytes.Equal(got.records[i].key, recs[i].key) || !bytes.Equal(got.records[i].val, recs[i].val) {
				t.Fatalf("iter %d record %d drift", iter, i)
			}
			val, found, err := seekLeafPage(dst, recs[i].key)
			if err != nil || !found || !bytes.Equal(val, recs[i].val) {
				t.Fatalf("iter %d seek record %d: found=%v err=%v", iter, i, found, err)
			}
		}
	}
}
