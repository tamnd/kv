package btree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// newBufferedBTree is newBTree with the Bε write path turned on, so the same
// conformance and split tests run against the buffered engine. The page size is left
// to the caller: a tiny page forces the flush cascade and interior splits the buffered
// path exists to exercise.
func newBufferedBTree(t *testing.T, pageSize, cacheFrames int) *BTree {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.kv", pager.Options{
		PageSize:    pageSize,
		CacheFrames: cacheFrames,
		Engine:      format.EngineBTree,
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	bt := New(p)
	if err := bt.Open(&engine.Env{Options: engine.EngineOptions{BufferedInserts: true}}); err != nil {
		t.Fatalf("open buffered btree: %v", err)
	}
	if !bt.buffered {
		t.Fatal("BufferedInserts option did not enable the buffered path")
	}
	return bt
}

// TestBufferedConformanceBasic runs the same versioned set/delete/merge mix as the
// unbuffered basic test through the buffered engine. Since CheckEngine compares every
// point read and scan against the model oracle, a buffered message that resolved to the
// wrong version, or one a read failed to pick up off the path, fails here.
func TestBufferedConformanceBasic(t *testing.T) {
	bt := newBufferedBTree(t, 4096, 16)

	b1 := engine.NewWriteBatch(10)
	b1.Set([]byte("apple"), []byte("red"))
	b1.Set([]byte("banana"), []byte("yellow"))
	b1.Set([]byte("cherry"), []byte("dark"))

	b2 := engine.NewWriteBatch(20)
	b2.Set([]byte("apple"), []byte("green"))
	b2.Delete([]byte("banana"))
	b2.Merge([]byte("cherry"), []byte("!"))

	b3 := engine.NewWriteBatch(30)
	b3.Merge([]byte("cherry"), []byte("?"))
	b3.Set([]byte("date"), []byte("brown"))

	if err := engine.CheckEngine(bt, []*engine.WriteBatch{b1, b2, b3}, concatMerge); err != nil {
		t.Fatalf("buffered conformance: %v", err)
	}
}

// TestBufferedConformanceCascade drives many distinct keys through the buffered engine
// at a tiny page size, so the root buffer overflows repeatedly and the flush cascade
// has to split leaves many ways, split interiors structurally, and grow the root by
// more than one level. CheckEngine then proves the tree the cascade built reads exactly
// like the model oracle, point and scan.
func TestBufferedConformanceCascade(t *testing.T) {
	bt := newBufferedBTree(t, 512, 64)

	const n = 2000
	b := engine.NewWriteBatch(5)
	for i := 0; i < n; i++ {
		b.Set([]byte(fmt.Sprintf("key%06d", i)), []byte(fmt.Sprintf("val%06d", i)))
	}
	if err := engine.CheckEngine(bt, []*engine.WriteBatch{b}, nil); err != nil {
		t.Fatalf("buffered cascade conformance: %v", err)
	}
	// The cascade must really have built a multi-level tree for this to test anything.
	typ, err := bt.typeOf(bt.root())
	if err != nil {
		t.Fatalf("typeOf root: %v", err)
	}
	if typ != format.PageBTreeInterior {
		t.Fatalf("expected an interior root after %d keys at a tiny page, got %v", n, typ)
	}
	if bt.pgr.DBSize() < 10 {
		t.Fatalf("expected the cascade to allocate many pages, db size = %d", bt.pgr.DBSize())
	}
}

// TestBufferedConformanceChurn mixes overwrites, deletes, and merges across versions in
// the buffered engine, the case where buffered messages of different kinds for the same
// key must fold in version order against whatever already reached the leaf.
func TestBufferedConformanceChurn(t *testing.T) {
	bt := newBufferedBTree(t, 1024, 64)

	const n = 800
	var batches []*engine.WriteBatch
	b1 := engine.NewWriteBatch(10)
	for i := 0; i < n; i++ {
		b1.Set([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%05d", i)))
	}
	batches = append(batches, b1)

	// Each batch touches every key at most once, the invariant the transaction layer
	// upholds: overwrite one residue class, delete a disjoint one, leave the rest.
	b2 := engine.NewWriteBatch(20)
	for i := 0; i < n; i++ {
		switch i % 3 {
		case 0:
			b2.Set([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("w%05d", i)))
		case 1:
			b2.Delete([]byte(fmt.Sprintf("k%05d", i)))
		}
	}
	batches = append(batches, b2)

	b3 := engine.NewWriteBatch(30)
	for i := 2; i < n; i += 3 {
		b3.Merge([]byte(fmt.Sprintf("k%05d", i)), []byte("+"))
	}
	batches = append(batches, b3)

	if err := engine.CheckEngine(bt, batches, concatMerge); err != nil {
		t.Fatalf("buffered churn conformance: %v", err)
	}
}

// TestBufferedReadSeesUnflushedMessage pins the read-path guarantee directly: a write
// that is still parked in the root buffer, not yet flushed to its leaf, must still be
// visible to a point read and a scan. It builds an interior root, then injects one key
// small enough to stay in the buffer, and checks the leaf that would hold it does not,
// while the reader does.
func TestBufferedReadSeesUnflushedMessage(t *testing.T) {
	bt := newBufferedBTree(t, 512, 64)

	// Insert until the root is an interior node, so the next write buffers rather than
	// descending directly through the lone-leaf path.
	ver := uint64(10)
	i := 0
	for {
		typ, err := bt.typeOf(bt.root())
		if err != nil {
			t.Fatalf("typeOf: %v", err)
		}
		if typ == format.PageBTreeInterior {
			break
		}
		b := engine.NewWriteBatch(ver)
		b.Set([]byte(fmt.Sprintf("seed%05d", i)), []byte("x"))
		if err := bt.Apply(b, ver); err != nil {
			t.Fatalf("apply seed: %v", err)
		}
		ver += 10
		i++
		if i > 10000 {
			t.Fatal("root never became interior")
		}
	}

	// One more write, distinctive and small, which should land in the root buffer and
	// stay there (a single small message cannot exceed the buffer budget).
	key := []byte("seed00000") // sorts to the front, exercises the leftmost child path
	b := engine.NewWriteBatch(ver)
	b.Set(key, []byte("BUFFERED"))
	if err := bt.Apply(b, ver); err != nil {
		t.Fatalf("apply buffered key: %v", err)
	}

	root, err := bt.loadInterior(bt.root())
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	found := false
	for _, mk := range root.msgKeys {
		if format.CompareUser(format.UserKey(mk), key) == 0 && format.Version(mk) == ver {
			found = true
		}
	}
	if !found {
		t.Fatal("the freshly written key did not stay in the root buffer; cannot test the unflushed read path")
	}

	// The leaf that covers the key must not hold this new version yet (it is still in the
	// buffer), so a read that ignored buffers would miss it.
	leafPg, err := bt.leafCovering(key)
	if err != nil {
		t.Fatalf("leafCovering: %v", err)
	}
	l, err := bt.loadLeaf(leafPg)
	if err != nil {
		t.Fatalf("loadLeaf: %v", err)
	}
	for j := range l.keys {
		if format.CompareUser(format.UserKey(l.keys[j]), key) == 0 && format.Version(l.keys[j]) == ver {
			t.Fatal("the buffered version already reached the leaf; the test no longer exercises an unflushed read")
		}
	}

	// The reader must nonetheless see the buffered write, both by point read and by scan.
	rd, err := bt.NewReader(engine.Snapshot{Version: ver + 1, Now: ver + 1})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer rd.Close()
	got, err := rd.Get(key)
	if err != nil {
		t.Fatalf("Get of a buffered key: %v", err)
	}
	if string(got) != "BUFFERED" {
		t.Fatalf("Get of a buffered key = %q, want BUFFERED", got)
	}

	it, err := rd.NewIter(engine.IterOptions{})
	if err != nil {
		t.Fatalf("NewIter: %v", err)
	}
	defer it.Close()
	seen := false
	for ok := it.First(); ok; ok = it.Next() {
		if format.CompareUser(it.Key(), key) == 0 {
			lv, err := it.Value()
			if err != nil {
				t.Fatalf("Value: %v", err)
			}
			vv, err := lv.Value()
			if err != nil {
				t.Fatalf("lazy Value: %v", err)
			}
			if string(vv) != "BUFFERED" {
				t.Fatalf("scan value for buffered key = %q, want BUFFERED", vv)
			}
			seen = true
		}
	}
	if !seen {
		t.Fatal("scan did not surface the buffered key")
	}
}
