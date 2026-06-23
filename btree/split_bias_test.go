package btree

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// leafFill walks the whole B-link leaf chain and returns the leaf count and the average
// per-leaf payload fill as a fraction of the usable page, the measure F3a moves: a
// sequential load that seals full leaves runs near 1.0, a midpoint-split one near 0.5.
func (t *BTree) leafFill(tb testing.TB) (leaves int, avgFill float64) {
	tb.Helper()
	pgno, err := t.leftmostLeaf()
	if err != nil {
		tb.Fatalf("leftmostLeaf: %v", err)
	}
	totalBytes := 0
	for pgno != format.NoPage {
		l, err := t.loadLeaf(pgno)
		if err != nil {
			tb.Fatalf("loadLeaf %d: %v", pgno, err)
		}
		leaves++
		totalBytes += leafEncodedSize(l)
		pgno = l.next
	}
	if leaves == 0 {
		return 0, 0
	}
	return leaves, float64(totalBytes) / float64(leaves*t.usable)
}

// insertKeys applies n keys produced by key(i) at one version through the engine, the way
// a real load drives the split path.
func insertKeys(tb testing.TB, bt *BTree, n int, version uint64, key func(i int) []byte) {
	tb.Helper()
	b := engine.NewWriteBatch(version)
	for i := 0; i < n; i++ {
		b.Set(key(i), []byte(fmt.Sprintf("v%06d", i)))
	}
	if err := bt.Apply(b, version); err != nil {
		tb.Fatalf("apply: %v", err)
	}
}

// TestBiasedSplitSequentialFill is the load-bearing check for F3a: inserting ascending
// keys must seal nearly full leaves rather than the half-full ones a midpoint split would
// leave. It asserts the average leaf fill clears 0.8 (a midpoint split sits near 0.5) and
// that the leaf count is within one of the bulk-load packed minimum for the same keys, so
// the insert path now matches the cold-load density on a sequential stream.
func TestBiasedSplitSequentialFill(t *testing.T) {
	const n = 2000
	key := func(i int) []byte { return []byte(fmt.Sprintf("k%08d", i)) }

	bt := newBTree(t, 1024, 256)
	insertKeys(t, bt, n, 7, key)
	leaves, fill := bt.leafFill(t)
	t.Logf("sequential insert: %d leaves, avg fill %.3f", leaves, fill)
	if fill < 0.8 {
		t.Fatalf("sequential insert leaf fill = %.3f, want >= 0.8 (midpoint split would be ~0.5)", fill)
	}

	// The packed minimum: bulk-load the same keys and count its leaves. The biased insert
	// path should land within one leaf of it.
	packed := newBTree(t, 1024, 256)
	cells := make([]struct{ ik, val []byte }, n)
	for i := 0; i < n; i++ {
		cells[i] = struct{ ik, val []byte }{
			ik:  format.EncodeInternalKey(key(i), 7, format.KindSet),
			val: []byte(fmt.Sprintf("v%06d", i)),
		}
	}
	if err := packed.BulkLoad(feedFrom(cells)); err != nil {
		t.Fatalf("bulk load: %v", err)
	}
	packedLeaves, packedFill := packed.leafFill(t)
	t.Logf("bulk-load packed: %d leaves, avg fill %.3f", packedLeaves, packedFill)
	if leaves > packedLeaves+1 {
		t.Fatalf("sequential insert used %d leaves, bulk-load packed %d (want within 1)", leaves, packedLeaves)
	}

	// Every key still reads back: density must not cost correctness.
	rd, _ := bt.NewReader(engine.Snapshot{Version: 7})
	defer rd.Close()
	for i := 0; i < n; i++ {
		v, err := rd.Get(key(i))
		if err != nil {
			t.Fatalf("get %q: %v", key(i), err)
		}
		if want := fmt.Sprintf("v%06d", i); string(v) != want {
			t.Fatalf("get %q = %q, want %q", key(i), v, want)
		}
	}
}

// TestBiasedSplitDescendingFill checks the symmetric prepend case: inserting descending
// keys (every overflow lands at the left end) must also seal full leaves through the
// prepend bias, not half-full ones.
func TestBiasedSplitDescendingFill(t *testing.T) {
	const n = 2000
	key := func(i int) []byte { return []byte(fmt.Sprintf("k%08d", n-1-i)) }

	bt := newBTree(t, 1024, 256)
	insertKeys(t, bt, n, 3, key)
	leaves, fill := bt.leafFill(t)
	t.Logf("descending insert: %d leaves, avg fill %.3f", leaves, fill)
	if fill < 0.8 {
		t.Fatalf("descending insert leaf fill = %.3f, want >= 0.8", fill)
	}

	rd, _ := bt.NewReader(engine.Snapshot{Version: 3})
	defer rd.Close()
	for i := 0; i < n; i++ {
		if _, err := rd.Get(key(i)); err != nil {
			t.Fatalf("get %q: %v", key(i), err)
		}
	}
}

// TestBiasedSplitRandomUnchanged checks the bias does not disturb a random workload: a
// shuffled insert order still splits at the balanced midpoint, so its fill stays in the
// classic random-B+tree band (roughly 0.5 to 0.8) and every key reads back. This pins the
// fall-through: only the pure end-insert cases take the biased cut.
func TestBiasedSplitRandomUnchanged(t *testing.T) {
	const n = 2000
	perm := rand.New(rand.NewSource(1)).Perm(n)
	key := func(i int) []byte { return []byte(fmt.Sprintf("k%08d", perm[i])) }

	bt := newBTree(t, 1024, 256)
	insertKeys(t, bt, n, 5, key)
	leaves, fill := bt.leafFill(t)
	t.Logf("random insert: %d leaves, avg fill %.3f", leaves, fill)
	if fill < 0.45 || fill > 0.85 {
		t.Fatalf("random insert leaf fill = %.3f, want in [0.45, 0.85]", fill)
	}

	rd, _ := bt.NewReader(engine.Snapshot{Version: 5})
	defer rd.Close()
	for i := 0; i < n; i++ {
		if _, err := rd.Get(key(i)); err != nil {
			t.Fatalf("get %q: %v", key(i), err)
		}
	}
}
