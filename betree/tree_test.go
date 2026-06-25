package betree

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// depth returns the number of node levels from the root down to a leaf, following
// the leftmost spine. A flat single-leaf tree is depth 1; one interior over leaves is
// depth 2; an interior over interiors is depth 3. It exists so a test can assert the
// write path actually built a multi-level tree rather than a single overfull leaf.
func (t *Tree) depth(tb testing.TB) int {
	tb.Helper()
	d := 0
	pgno := t.root()
	for {
		d++
		typ, err := t.pageType(pgno)
		if err != nil {
			tb.Fatalf("page type at %d: %v", pgno, err)
		}
		if typ == format.PageBTreeLeaf {
			return d
		}
		in, err := t.loadInterior(pgno)
		if err != nil {
			tb.Fatalf("load interior at %d: %v", pgno, err)
		}
		pgno = in.leftmost
	}
}

// TestTreeGrowsMultiLevel inserts enough keys, at a small page, that the run cannot
// be one or two leaves: the tree must split leaves, split the interior they hang
// under, and grow a new root over that. It asserts the tree reached at least three
// levels, so the split-propagation and new-root paths in propagateSplit are genuinely
// exercised, then reads every key back to prove the multi-level tree routes correctly.
func TestTreeGrowsMultiLevel(t *testing.T) {
	tr := newTreeSized(t, vfs.NewMem(), 512)

	const n = 4000
	const perBatch = 200
	ver := uint64(0)
	for base := 0; base < n; base += perBatch {
		ver++
		b := engine.NewWriteBatch(ver)
		for i := base; i < base+perBatch && i < n; i++ {
			b.Set([]byte(fmt.Sprintf("key%06d", i)), []byte(fmt.Sprintf("val%06d", i)))
		}
		if err := tr.Apply(b, ver); err != nil {
			t.Fatalf("apply batch at %d: %v", base, err)
		}
	}

	if d := tr.depth(t); d < 3 {
		t.Fatalf("tree depth = %d, want >= 3 (interior splits and a new root)", d)
	}

	rd, err := tr.NewReader(engine.Snapshot{Version: ver})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%06d", i))
		v, err := rd.Get(k)
		if err != nil {
			t.Fatalf("key %q in multi-level tree: %v", k, err)
		}
		if want := fmt.Sprintf("val%06d", i); string(v) != want {
			t.Fatalf("key %q = %q, want %q", k, v, want)
		}
	}
}

// TestDeepTreeConformance runs the differential oracle against a core at a tiny page
// across many seeds, so most streams build a multi-level tree and the random mix of
// sets, deletes, merges, and range deletes is folded identically to the model at
// every snapshot. It is the multi-leaf check pushed down to a page small enough that
// the interior spine itself splits.
func TestDeepTreeConformance(t *testing.T) {
	for seed := int64(1); seed <= 30; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			tr := newTreeSized(t, vfs.NewMem(), 512)
			batches := randomBatches(rand.New(rand.NewSource(seed)))
			if err := engine.CheckEngine(tr, batches, concatMerge); err != nil {
				t.Fatalf("conformance (seed %d): %v", seed, err)
			}
		})
	}
}

// TestReopenMultiLevel grows a multi-level tree, checkpoints, closes, reopens, and
// reads every key back. It proves the interior spine, not just the leaf chain,
// survives the round trip: the new root and the split interiors are all on disk and
// the reopened core descends them correctly.
func TestReopenMultiLevel(t *testing.T) {
	fs := vfs.NewMem()
	tr := newTreeSized(t, fs, 512)

	const n = 3000
	b := engine.NewWriteBatch(9)
	for i := 0; i < n; i++ {
		b.Set([]byte(fmt.Sprintf("k%06d", i)), []byte(fmt.Sprintf("v%06d", i)))
	}
	if err := tr.Apply(b, 9); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Drain the hot tail onto pages so the whole key space is in the tree spine, not
	// resting in the heap: the depth check below asserts the on-disk spine is
	// multi-level, and the direct checkpoint that follows has no logical WAL to replay a
	// tail-resident write from.
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush tail: %v", err)
	}
	if d := tr.depth(t); d < 3 {
		t.Fatalf("pre-reopen depth = %d, want >= 3", d)
	}
	if err := tr.pgr.Checkpoint(0, 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := tr.pgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := pager.Open(fs, "test.kv", pager.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	tr2 := New(p2)
	if err := tr2.Open(&engine.Env{}); err != nil {
		t.Fatalf("reopen betree: %v", err)
	}
	if d := tr2.depth(t); d < 3 {
		t.Fatalf("post-reopen depth = %d, want >= 3", d)
	}
	rd, err := tr2.NewReader(engine.Snapshot{Version: 9})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		v, err := rd.Get(k)
		if err != nil {
			t.Fatalf("key %q after reopen: %v", k, err)
		}
		if want := fmt.Sprintf("v%06d", i); string(v) != want {
			t.Fatalf("key %q = %q, want %q", k, v, want)
		}
	}
}
