package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// TestLeafInsertInPlace exercises the slotted in-place edit directly on a page image,
// covering the insert, the same-key overwrite (dead-space reclaim by a later compaction,
// not here), and the not-enough-gap and malformed-page branches. It checks the result by
// decoding the page through the real unmarshalLeaf, so a slot or content-start the edit
// left inconsistent would surface as a decode error or a wrong read-back.
func TestLeafInsertInPlace(t *testing.T) {
	const usable = 256
	page := marshalLeaf(&leaf{next: format.NoPage}, usable)

	// A handful of keys inserted out of order must read back in internal-key order.
	type kv struct{ k, v string }
	ins := []kv{{"d", "4"}, {"a", "1"}, {"c", "3"}, {"b", "2"}}
	for _, e := range ins {
		ik := format.EncodeInternalKey([]byte(e.k), 1, format.KindSet)
		done, ok := leafInsertInPlace(page, usable, ik, []byte(e.v))
		if !ok || !done {
			t.Fatalf("insert %q: done=%v ok=%v, want both true", e.k, done, ok)
		}
	}
	l, err := unmarshalLeaf(page)
	if err != nil {
		t.Fatalf("unmarshal after inserts: %v", err)
	}
	wantK := []string{"a", "b", "c", "d"}
	wantV := []string{"1", "2", "3", "4"}
	if len(l.keys) != len(wantK) {
		t.Fatalf("got %d cells, want %d", len(l.keys), len(wantK))
	}
	for i := range wantK {
		if uk := string(format.UserKey(l.keys[i])); uk != wantK[i] {
			t.Fatalf("cell %d user key %q, want %q", i, uk, wantK[i])
		}
		if string(l.vals[i]) != wantV[i] {
			t.Fatalf("cell %d value %q, want %q", i, l.vals[i], wantV[i])
		}
	}

	// Overwriting the exact internal key repoints the slot to a fresh body and leaves the
	// old one as dead space; the cell count must not grow and the read-back must show the
	// new value.
	ikC := format.EncodeInternalKey([]byte("c"), 1, format.KindSet)
	done, ok := leafInsertInPlace(page, usable, ikC, []byte("CCC"))
	if !ok || !done {
		t.Fatalf("overwrite c: done=%v ok=%v", done, ok)
	}
	l, err = unmarshalLeaf(page)
	if err != nil {
		t.Fatalf("unmarshal after overwrite: %v", err)
	}
	if len(l.keys) != 4 {
		t.Fatalf("overwrite changed cell count to %d, want 4", len(l.keys))
	}
	if got := string(l.vals[2]); got != "CCC" {
		t.Fatalf("overwrite read back %q, want CCC", got)
	}

	// A body too large for the remaining contiguous gap reports done=false with the page
	// untouched (ok=true: the page is well-formed, it just does not fit).
	big := bytes.Repeat([]byte("x"), usable)
	before := append([]byte(nil), page...)
	ikZ := format.EncodeInternalKey([]byte("z"), 1, format.KindSet)
	done, ok = leafInsertInPlace(page, usable, ikZ, big)
	if !ok || done {
		t.Fatalf("oversized insert: done=%v ok=%v, want done=false ok=true", done, ok)
	}
	if !bytes.Equal(page, before) {
		t.Fatal("oversized insert mutated the page; it must leave it untouched for the retry path")
	}

	// A page shorter than the leaf header is malformed: ok=false so the caller falls back
	// to the decode path rather than trusting it.
	if _, ok := leafInsertInPlace(make([]byte, leafHeaderSize-1), usable, ikZ, []byte("v")); ok {
		t.Fatal("short page reported ok=true; want false")
	}
}

// TestLeafInPlaceMatchesReencode runs the same insert stream through both the in-place
// path and the whole-leaf decode/re-encode path and asserts the two leaves read back
// identical. This is the guard that the fast path is a pure performance change: any
// divergence in cell order, values, or the B-link would fail here.
func TestLeafInPlaceMatchesReencode(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("inplace=%v", enabled), func(t *testing.T) {
			old := leafInPlaceEnabled
			leafInPlaceEnabled = enabled
			defer func() { leafInPlaceEnabled = old }()

			bt := newBTree(t, 512, 64)
			b := engine.NewWriteBatch(1)
			for i := 0; i < 300; i++ {
				b.Set([]byte(fmt.Sprintf("key%05d", (i*37)%300)), []byte(fmt.Sprintf("val%05d", i)))
			}
			if err := bt.Apply(b, 1); err != nil {
				t.Fatalf("apply: %v", err)
			}
			rd, err := bt.NewReader(engine.Snapshot{Version: 1})
			if err != nil {
				t.Fatalf("reader: %v", err)
			}
			defer rd.Close()
			cur, err := rd.NewIter(engine.IterOptions{})
			if err != nil {
				t.Fatalf("iter: %v", err)
			}
			var got []string
			for ok := cur.First(); ok; ok = cur.Next() {
				lv, err := cur.Value()
				if err != nil {
					t.Fatalf("value: %v", err)
				}
				v, err := lv.Value()
				if err != nil {
					t.Fatalf("lazy value: %v", err)
				}
				got = append(got, string(cur.Key())+"="+string(v))
			}
			// Highest version of each of the 300 keys wins; rebuild the expected set.
			want := map[string]string{}
			for i := 0; i < 300; i++ {
				want[fmt.Sprintf("key%05d", (i*37)%300)] = fmt.Sprintf("val%05d", i)
			}
			if len(got) != len(want) {
				t.Fatalf("read back %d keys, want %d", len(got), len(want))
			}
			for _, kv := range got {
				eq := bytes.IndexByte([]byte(kv), '=')
				k, v := kv[:eq], kv[eq+1:]
				if want[k] != v {
					t.Fatalf("key %q read %q, want %q", k, v, want[k])
				}
			}
		})
	}
}

// newBenchTree builds a fresh in-memory B-tree for the write benchmarks. It mirrors
// newBTree but takes a testing.TB so a benchmark can reset the tree between spans.
func newBenchTree(tb testing.TB, pageSize int) *BTree {
	tb.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "bench.kv", pager.Options{
		PageSize:    pageSize,
		CacheFrames: 4096,
		Engine:      format.EngineBTree,
	})
	if err != nil {
		tb.Fatalf("create pager: %v", err)
	}
	bt := New(p)
	if err := bt.Open(&engine.Env{}); err != nil {
		tb.Fatalf("open btree: %v", err)
	}
	return bt
}

// benchLeafWrite measures the per-insert cost of the B-tree write path in isolation of
// the WAL and fsync (it drives Engine.Apply directly), so the slotted in-place edit's CPU
// win shows undiluted. It fills a bounded span of distinct keys, resetting the tree each
// span so memory stays flat across b.N, and toggles leafInPlaceEnabled to give a clean
// A/B between the in-place path and the whole-leaf re-encode it replaced (perf/02 F1).
func benchLeafWrite(b *testing.B, inPlace bool, keyOrder func(i int) int) {
	old := leafInPlaceEnabled
	leafInPlaceEnabled = inPlace
	defer func() { leafInPlaceEnabled = old }()

	const span = 50000
	val := bytes.Repeat([]byte("v"), 48)
	var key [12]byte
	copy(key[:4], "key")

	bt := newBenchTree(b, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%span == 0 && i != 0 {
			b.StopTimer()
			bt = newBenchTree(b, 4096)
			b.StartTimer()
		}
		binary.BigEndian.PutUint64(key[4:], uint64(keyOrder(i%span)))
		bb := engine.NewWriteBatch(uint64(i + 1))
		bb.Set(key[:], val)
		if err := bt.Apply(bb, uint64(i+1)); err != nil {
			b.Fatalf("apply: %v", err)
		}
	}
}

// sequential and scrambled key orders within a span. Sequential keys land in the rightmost
// leaf (append-heavy); the scrambled order spreads inserts across leaves, the harder case
// the in-place splice still wins on.
func benchSeq(i int) int      { return i }
func benchScramble(i int) int { return (i * 2654435761) & 0x7fffffff }

func BenchmarkLeafWriteSeqInPlace(b *testing.B)     { benchLeafWrite(b, true, benchSeq) }
func BenchmarkLeafWriteSeqReencode(b *testing.B)    { benchLeafWrite(b, false, benchSeq) }
func BenchmarkLeafWriteRandomInPlace(b *testing.B)  { benchLeafWrite(b, true, benchScramble) }
func BenchmarkLeafWriteRandomReencode(b *testing.B) { benchLeafWrite(b, false, benchScramble) }
