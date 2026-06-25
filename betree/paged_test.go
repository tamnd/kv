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

// newTreeSized opens a Bε-tree core over a fresh in-memory database with a chosen
// page size. A small page forces the run to span several leaf pages, which is what
// exercises the sibling-linked run, the greedy leaf packing, and the cross-leaf
// version groups that a single-page run never reaches.
func newTreeSized(t *testing.T, fs vfs.FS, pageSize int) *Tree {
	t.Helper()
	p, err := pager.Create(fs, "test.kv", pager.Options{
		PageSize:    pageSize,
		CacheFrames: 16,
		Engine:      format.EngineBeta,
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	tr := New(p)
	if err := tr.Open(&engine.Env{}); err != nil {
		t.Fatalf("open betree: %v", err)
	}
	return tr
}

// TestConformanceMultiLeaf runs the randomized differential check against a core
// with a small page, so most streams overflow a single leaf and the run grows to
// several sibling-linked pages. Conformance must hold identically across the page
// boundaries: a user key whose versions straddle two leaves still folds the same as
// one whose versions sit in a single leaf.
func TestConformanceMultiLeaf(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
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

// TestReopenAfterCheckpoint writes enough keys to span many leaves, checkpoints,
// closes, reopens the file, and checks every key is still readable. It proves the
// run is genuinely on disk: the leaf pages, the sibling links, and the engine root
// in the header all survive the round trip, and Open on an existing root rebinds to
// the run without rewriting it.
func TestReopenAfterCheckpoint(t *testing.T) {
	fs := vfs.NewMem()
	tr := newTreeSized(t, fs, 512)

	const n = 300
	b := engine.NewWriteBatch(7)
	for i := 0; i < n; i++ {
		b.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i)))
	}
	if err := tr.Apply(b, 7); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Drain the hot tail onto pages: this test drives the pager directly with no logical
	// WAL to replay, so a write left in the tail would be lost across the checkpoint.
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush tail: %v", err)
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
	rd, err := tr2.NewReader(engine.Snapshot{Version: 7})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%05d", i))
		v, err := rd.Get(k)
		if err != nil {
			t.Fatalf("key %q after reopen: %v", k, err)
		}
		if want := fmt.Sprintf("val%05d", i); string(v) != want {
			t.Fatalf("key %q = %q, want %q", k, v, want)
		}
	}
}

// TestRangeDeleteReopen applies a range delete, checkpoints, reopens, and checks the
// covered keys are still absent. The interval set is not persisted as such: it is
// rebuilt from the range-begin marker cells in the run at read time, so this proves
// the markers survive the round trip and the rebuild fires after a reopen.
func TestRangeDeleteReopen(t *testing.T) {
	fs := vfs.NewMem()
	tr := newTreeSized(t, fs, 512)

	b := engine.NewWriteBatch(5)
	for i := 0; i < 10; i++ {
		b.Set([]byte(fmt.Sprintf("k%02d", i)), []byte("v"))
	}
	if err := tr.Apply(b, 5); err != nil {
		t.Fatalf("apply sets: %v", err)
	}
	bd := engine.NewWriteBatch(10)
	bd.DeleteRange([]byte("k03"), []byte("k07")) // covers k03..k06
	if err := tr.Apply(bd, 10); err != nil {
		t.Fatalf("apply range delete: %v", err)
	}
	// Drain the hot tail onto pages before the direct checkpoint (no logical WAL here).
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush tail: %v", err)
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
	rd, err := tr2.NewReader(engine.Snapshot{Version: 10})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("k%02d", i))
		_, err := rd.Get(k)
		covered := i >= 3 && i <= 6
		if covered && err != engine.ErrNotFound {
			t.Fatalf("covered key %q after reopen: err = %v, want ErrNotFound", k, err)
		}
		if !covered && err != nil {
			t.Fatalf("uncovered key %q after reopen: %v", k, err)
		}
	}
}

// TestEmptyRunReads checks that a freshly opened core with an empty root leaf reads
// cleanly: a point read misses and a scan is empty, with no decode error from the
// zero-record leaf the root starts as.
func TestEmptyRunReads(t *testing.T) {
	tr := newTreeSized(t, vfs.NewMem(), 512)
	rd, err := tr.NewReader(engine.Snapshot{Version: 1})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	if _, err := rd.Get([]byte("nope")); err != engine.ErrNotFound {
		t.Fatalf("empty Get: err = %v, want ErrNotFound", err)
	}
	cur, err := rd.NewIter(engine.IterOptions{})
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer cur.Close()
	if cur.First() {
		t.Fatalf("empty run scan yielded a key: %q", cur.Key())
	}
}
