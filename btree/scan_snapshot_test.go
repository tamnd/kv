package btree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// scanAllVia drives a zero-copy batch scan over [lower, upper) at snap through whichever cursor the
// caller supplies, returning every visible key and value copied out so the views do not outlive the
// cursor. Both the reader path and the reader-free snapshot path produce a BatchCursor, so this
// drives either identically.
func scanAllVia(t *testing.T, cur engine.StreamCursor) []engine.KV {
	t.Helper()
	bc := cur.(engine.BatchCursor)
	var out []engine.KV
	dst := make([]engine.KV, 7) // small cap so the fill crosses leaf boundaries
	for {
		n, err := bc.NextBatch(dst, false)
		if err != nil {
			t.Fatalf("next batch: %v", err)
		}
		for i := 0; i < n; i++ {
			out = append(out, engine.KV{
				Key:   append([]byte(nil), dst[i].Key...),
				Value: append([]byte(nil), dst[i].Value...),
			})
		}
		if n < len(dst) {
			break
		}
	}
	return out
}

// TestSnapshotForwardCursorMatchesReaderPath checks that the reader-free SnapshotForwardCursorer
// path returns exactly what the NewReader+ForwardCursorer path returns, key for key, over the same
// tree, snapshot, and bounds. The reader-free path exists only to skip the throwaway reader
// allocation, so its output must be observably identical; this pins that.
func TestSnapshotForwardCursorMatchesReaderPath(t *testing.T) {
	bt := newBTree(t, 512, 16) // small page so the keys span several leaves

	const n = 300
	b := engine.NewWriteBatch(10)
	for i := 0; i < n; i++ {
		b.Set([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i)))
	}
	if err := bt.Apply(b, 10); err != nil {
		t.Fatalf("apply: %v", err)
	}

	cases := []struct{ lo, hi []byte }{
		{nil, nil},
		{[]byte("k0050"), []byte("k0200")},
		{[]byte("k0123"), nil},
		{nil, []byte("k0099")},
	}
	snap := engine.Snapshot{Version: 100}
	for _, c := range cases {
		name := fmt.Sprintf("[%s,%s)", c.lo, c.hi)
		t.Run(name, func(t *testing.T) {
			rd, err := bt.NewReader(snap)
			if err != nil {
				t.Fatalf("new reader: %v", err)
			}
			defer rd.Close()
			readerCur, err := rd.(engine.ForwardCursorer).NewForwardCursor(c.lo, c.hi)
			if err != nil {
				t.Fatalf("reader cursor: %v", err)
			}
			snapCur, err := bt.NewSnapshotForwardCursor(snap, c.lo, c.hi)
			if err != nil {
				t.Fatalf("snapshot cursor: %v", err)
			}

			want := scanAllVia(t, readerCur)
			got := scanAllVia(t, snapCur)
			if len(got) != len(want) {
				t.Fatalf("snapshot path scanned %d keys, reader path %d", len(got), len(want))
			}
			for i := range want {
				if string(got[i].Key) != string(want[i].Key) || string(got[i].Value) != string(want[i].Value) {
					t.Fatalf("entry %d: snapshot (%q,%q) != reader (%q,%q)",
						i, got[i].Key, got[i].Value, want[i].Key, want[i].Value)
				}
			}
		})
	}
}

// TestBTreeImplementsSnapshotForwardCursorer is a compile-and-shape guard: the host scan fast path
// type-asserts the engine to engine.SnapshotForwardCursorer, so the B-tree must satisfy it or the
// fast path silently falls back to the allocating reader path.
func TestBTreeImplementsSnapshotForwardCursorer(t *testing.T) {
	var bt any = &BTree{}
	if _, ok := bt.(engine.SnapshotForwardCursorer); !ok {
		t.Fatal("*BTree does not implement engine.SnapshotForwardCursorer")
	}
}
