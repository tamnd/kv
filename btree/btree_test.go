package btree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// newBTree creates a fresh in-memory database and returns an opened B-tree core
// over it. pageSize is small in the split tests so a modest key count forces the
// tree to grow past a single leaf.
func newBTree(t *testing.T, pageSize, cacheFrames int) *BTree {
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
	if err := bt.Open(&engine.Env{}); err != nil {
		t.Fatalf("open btree: %v", err)
	}
	return bt
}

// concatMerge is a deterministic merge resolver: it appends the operand to the
// existing value. The oracle uses the same function, so a conforming engine must
// fold merges identically.
func concatMerge(existing, operand []byte) []byte {
	out := make([]byte, 0, len(existing)+len(operand))
	out = append(out, existing...)
	out = append(out, operand...)
	return out
}

// TestConformanceBasic drives a small mix of sets, deletes, and merges across
// several versions through the conformance oracle at a roomy page size (no splits).
func TestConformanceBasic(t *testing.T) {
	bt := newBTree(t, 4096, 16)

	var batches []*engine.WriteBatch

	b1 := engine.NewWriteBatch(10)
	b1.Set([]byte("apple"), []byte("red"))
	b1.Set([]byte("banana"), []byte("yellow"))
	b1.Set([]byte("cherry"), []byte("dark"))
	batches = append(batches, b1)

	b2 := engine.NewWriteBatch(20)
	b2.Set([]byte("apple"), []byte("green")) // overwrite
	b2.Delete([]byte("banana"))              // tombstone
	b2.Merge([]byte("cherry"), []byte("!"))  // merge on top of a set
	batches = append(batches, b2)

	b3 := engine.NewWriteBatch(30)
	b3.Merge([]byte("cherry"), []byte("?")) // second operand
	b3.Set([]byte("date"), []byte("brown"))
	batches = append(batches, b3)

	if err := engine.CheckEngine(bt, batches, concatMerge); err != nil {
		t.Fatalf("conformance: %v", err)
	}
}

// TestConformanceSingleVersionSplits inserts many distinct keys at a tiny page
// size so the tree splits leaves and grows interior levels, then checks the engine
// still matches the oracle for point reads and scans.
func TestConformanceSingleVersionSplits(t *testing.T) {
	bt := newBTree(t, 512, 16)

	const n = 300
	b := engine.NewWriteBatch(5)
	for i := 0; i < n; i++ {
		b.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i)))
	}
	if err := engine.CheckEngine(bt, []*engine.WriteBatch{b}, nil); err != nil {
		t.Fatalf("conformance: %v", err)
	}
	// The tree must actually have grown past one page for this to be a real test.
	if bt.pgr.DBSize() < 5 {
		t.Fatalf("expected the tree to split into several pages, db size = %d", bt.pgr.DBSize())
	}
}

// TestConformanceVersionedSplits combines version history (overwrites, deletes,
// merges) with a small page size so version groups land across many split leaves.
func TestConformanceVersionedSplits(t *testing.T) {
	bt := newBTree(t, 512, 8)

	const n = 200
	var batches []*engine.WriteBatch

	// Version 100: set every key.
	b1 := engine.NewWriteBatch(100)
	for i := 0; i < n; i++ {
		b1.Set([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%05d", i)))
	}
	batches = append(batches, b1)

	// Version 200: overwrite evens, delete multiples of 5, merge multiples of 3.
	b2 := engine.NewWriteBatch(200)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		switch {
		case i%5 == 0:
			b2.Delete(k)
		case i%2 == 0:
			b2.Set(k, []byte(fmt.Sprintf("w%05d", i)))
		case i%3 == 0:
			b2.Merge(k, []byte("+"))
		}
	}
	batches = append(batches, b2)

	// Version 300: a second merge wave over the survivors.
	b3 := engine.NewWriteBatch(300)
	for i := 0; i < n; i++ {
		if i%3 == 0 {
			b3.Merge([]byte(fmt.Sprintf("k%05d", i)), []byte("*"))
		}
	}
	batches = append(batches, b3)

	if err := engine.CheckEngine(bt, batches, concatMerge); err != nil {
		t.Fatalf("conformance: %v", err)
	}
}

// TestRangeAndPrefixScan exercises bounded and prefix iteration after splits.
func TestRangeAndPrefixScan(t *testing.T) {
	bt := newBTree(t, 512, 16)
	b := engine.NewWriteBatch(1)
	for i := 0; i < 100; i++ {
		b.Set([]byte(fmt.Sprintf("a%03d", i)), []byte("x"))
		b.Set([]byte(fmt.Sprintf("b%03d", i)), []byte("y"))
	}
	if err := bt.Apply(b, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}
	rd, err := bt.NewReader(engine.Snapshot{Version: 1})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()

	// Prefix "a" must yield exactly the 100 a-keys, in order.
	cur, err := rd.NewIter(engine.IterOptions{Prefix: []byte("a")})
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	var count int
	var last []byte
	for ok := cur.First(); ok; ok = cur.Next() {
		k := append([]byte(nil), cur.Key()...)
		if k[0] != 'a' {
			t.Fatalf("prefix scan leaked key %q", k)
		}
		if last != nil && string(k) <= string(last) {
			t.Fatalf("prefix scan out of order: %q after %q", k, last)
		}
		last = k
		count++
	}
	cur.Close()
	if count != 100 {
		t.Fatalf("prefix scan got %d keys, want 100", count)
	}

	// Bounded scan [a050, a060) yields ten keys.
	cur2, _ := rd.NewIter(engine.IterOptions{Lower: []byte("a050"), Upper: []byte("a060")})
	count = 0
	for ok := cur2.First(); ok; ok = cur2.Next() {
		count++
	}
	cur2.Close()
	if count != 10 {
		t.Fatalf("bounded scan got %d keys, want 10", count)
	}
}

// TestConformanceRangeDelete drives sets, a range deletion, and re-sets above it
// through the oracle at a tiny page size, so the range-delete marker cell and the
// keys it covers land in different leaves. CheckEngine compares the B-tree to the
// oracle at every commit version, which exercises the snapshot before the delete
// (keys present), at the delete (covered keys absent), and after a re-set (the
// newer version survives the older range delete).
func TestConformanceRangeDelete(t *testing.T) {
	bt := newBTree(t, 512, 16)

	var batches []*engine.WriteBatch

	// Enough keys at a small page size to span several leaves, so the marker cell
	// and the keys it covers do not share a single leaf.
	b1 := engine.NewWriteBatch(10)
	for i := 0; i < 60; i++ {
		b1.Set([]byte(fmt.Sprintf("k%02d", i)), []byte("one"))
	}
	batches = append(batches, b1)

	// Delete the half-open interval [k20, k40): covers k20..k39, leaves k40.
	b2 := engine.NewWriteBatch(20)
	b2.DeleteRange([]byte("k20"), []byte("k40"))
	batches = append(batches, b2)

	// Re-set two covered keys above the range delete; both must reappear.
	b3 := engine.NewWriteBatch(30)
	b3.Set([]byte("k30"), []byte("two"))
	batches = append(batches, b3)

	b4 := engine.NewWriteBatch(40)
	b4.Set([]byte("k25"), []byte("three"))
	batches = append(batches, b4)

	if err := engine.CheckEngine(bt, batches, concatMerge); err != nil {
		t.Fatalf("conformance: %v", err)
	}
}

// TestConformanceRangeDeleteMerge checks merges interacting with a range delete: a
// merge committed above the range delete folds over an empty base (the range delete
// wiped the prior versions), while a merge below it is erased.
func TestConformanceRangeDeleteMerge(t *testing.T) {
	bt := newBTree(t, 4096, 16)

	var batches []*engine.WriteBatch

	b1 := engine.NewWriteBatch(10)
	b1.Set([]byte("p"), []byte("base"))
	b1.Set([]byte("out"), []byte("keep")) // outside the deleted range
	batches = append(batches, b1)

	b2 := engine.NewWriteBatch(15)
	b2.Merge([]byte("p"), []byte("+a")) // folds to "base+a" before the delete
	batches = append(batches, b2)

	b3 := engine.NewWriteBatch(20)
	b3.DeleteRange([]byte("p"), []byte("q")) // covers p, not out
	batches = append(batches, b3)

	b4 := engine.NewWriteBatch(25)
	b4.Merge([]byte("p"), []byte("+b")) // folds over empty base -> "+b"
	batches = append(batches, b4)

	if err := engine.CheckEngine(bt, batches, concatMerge); err != nil {
		t.Fatalf("conformance: %v", err)
	}
}

// TestRangeDeleteReopen applies a range delete, checkpoints, reopens the file, and
// checks the covered keys are still absent -- the interval set is rebuilt from the
// marker cells at Open, not held only in memory.
func TestRangeDeleteReopen(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.kv", pager.Options{PageSize: 512, CacheFrames: 16, Engine: format.EngineBTree})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	bt := New(p)
	if err := bt.Open(&engine.Env{}); err != nil {
		t.Fatalf("open: %v", err)
	}
	b := engine.NewWriteBatch(5)
	for i := 0; i < 10; i++ {
		b.Set([]byte(fmt.Sprintf("k%02d", i)), []byte("v"))
	}
	if err := bt.Apply(b, 5); err != nil {
		t.Fatalf("apply sets: %v", err)
	}
	// Delete at a later version than the sets, so the covered keys are unambiguously
	// older than the range delete.
	bd := engine.NewWriteBatch(10)
	bd.DeleteRange([]byte("k03"), []byte("k07")) // covers k03..k06
	if err := bt.Apply(bd, 10); err != nil {
		t.Fatalf("apply range delete: %v", err)
	}
	if err := p.Checkpoint(0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := pager.Open(fs, "test.kv", pager.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	bt2 := New(p2)
	if err := bt2.Open(&engine.Env{}); err != nil {
		t.Fatalf("reopen btree: %v", err)
	}
	rd, _ := bt2.NewReader(engine.Snapshot{Version: 10})
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

// TestReopenAfterCheckpoint checkpoints a split tree, reopens the file, and checks
// the data is still all there -- the root and every page survived the round trip.
func TestReopenAfterCheckpoint(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.kv", pager.Options{PageSize: 512, CacheFrames: 16, Engine: format.EngineBTree})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	bt := New(p)
	if err := bt.Open(&engine.Env{}); err != nil {
		t.Fatalf("open: %v", err)
	}
	const n = 250
	b := engine.NewWriteBatch(7)
	for i := 0; i < n; i++ {
		b.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i)))
	}
	if err := bt.Apply(b, 7); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := p.Checkpoint(0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := pager.Open(fs, "test.kv", pager.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	bt2 := New(p2)
	if err := bt2.Open(&engine.Env{}); err != nil {
		t.Fatalf("reopen btree: %v", err)
	}
	rd, _ := bt2.NewReader(engine.Snapshot{Version: 7})
	defer rd.Close()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%05d", i))
		got, err := rd.Get(k)
		if err != nil {
			t.Fatalf("get %q after reopen: %v", k, err)
		}
		if want := fmt.Sprintf("val%05d", i); string(got) != want {
			t.Fatalf("get %q = %q, want %q", k, got, want)
		}
	}
}
