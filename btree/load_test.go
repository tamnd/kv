package btree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// feedFrom returns a pull function over the given internal-key/value cells, the shape
// BulkLoad consumes.
func feedFrom(cells []struct{ ik, val []byte }) func() ([]byte, []byte, bool) {
	i := 0
	return func() ([]byte, []byte, bool) {
		if i >= len(cells) {
			return nil, nil, false
		}
		c := cells[i]
		i++
		return c.ik, c.val, true
	}
}

// setCells builds n ascending Set cells (key NNNNNN -> val NNNNNN) at one version, the
// shape a dump replays into a fresh database.
func setCells(n int, version uint64) []struct{ ik, val []byte } {
	cells := make([]struct{ ik, val []byte }, n)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		cells[i] = struct{ ik, val []byte }{
			ik:  format.EncodeInternalKey(k, version, format.KindSet),
			val: []byte(fmt.Sprintf("v%06d", i)),
		}
	}
	return cells
}

// TestBulkLoadBuildsReadableTree bulk-loads many ascending keys into a small-page tree,
// forcing a multi-level build, and checks every key reads back at the load version and a
// missing key reads absent. It also walks the B-link leaf chain through a range scan to
// confirm the sibling pointers were linked left to right.
func TestBulkLoadBuildsReadableTree(t *testing.T) {
	bt := newBTree(t, 512, 64) // tiny pages so the build spans many leaves and interior levels

	const n = 500
	if err := bt.BulkLoad(feedFrom(setCells(n, 7))); err != nil {
		t.Fatalf("bulk load: %v", err)
	}

	rd, err := bt.NewReader(engine.Snapshot{Version: 7})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%06d", i)
		v, err := rd.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %q: %v", k, err)
		}
		if want := fmt.Sprintf("v%06d", i); string(v) != want {
			t.Fatalf("get %q = %q, want %q", k, v, want)
		}
	}
	if _, err := rd.Get([]byte("k999999")); err != engine.ErrNotFound {
		t.Fatalf("missing key err = %v, want ErrNotFound", err)
	}

	// A full ascending scan must visit every key in order, which only holds if the leaf
	// B-link chain is intact.
	cur, err := rd.NewIter(engine.IterOptions{})
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer cur.Close()
	count := 0
	for ok := cur.First(); ok; ok = cur.Next() {
		want := fmt.Sprintf("k%06d", count)
		if string(cur.Key()) != want {
			t.Fatalf("scan position %d = %q, want %q", count, cur.Key(), want)
		}
		count++
	}
	if count != n {
		t.Fatalf("scan visited %d keys, want %d", count, n)
	}
}

// TestBulkLoadMatchesInsert loads the same keys two ways -- bulk-loaded into one tree,
// inserted one at a time into another -- and asserts both resolve every key identically,
// so the bottom-up build is observationally equal to the insert path.
func TestBulkLoadMatchesInsert(t *testing.T) {
	cells := setCells(300, 4)

	loaded := newBTree(t, 512, 64)
	if err := loaded.BulkLoad(feedFrom(cells)); err != nil {
		t.Fatalf("bulk load: %v", err)
	}

	inserted := newBTree(t, 512, 64)
	b := engine.NewWriteBatch(4)
	for _, c := range cells {
		b.Set(format.UserKey(c.ik), c.val)
	}
	if err := inserted.Apply(b, 4); err != nil {
		t.Fatalf("apply: %v", err)
	}

	lr, _ := loaded.NewReader(engine.Snapshot{Version: 4})
	defer lr.Close()
	ir, _ := inserted.NewReader(engine.Snapshot{Version: 4})
	defer ir.Close()
	for _, c := range cells {
		k := format.UserKey(c.ik)
		lv, le := lr.Get(k)
		iv, ie := ir.Get(k)
		if le != ie || string(lv) != string(iv) {
			t.Fatalf("key %q: loaded (%q,%v) != inserted (%q,%v)", k, lv, le, iv, ie)
		}
	}
}

// TestBulkLoadEmptyStream loads nothing and checks the tree stays a valid empty store: a
// read finds no key and a later insert still works.
func TestBulkLoadEmptyStream(t *testing.T) {
	bt := newBTree(t, 512, 64)
	if err := bt.BulkLoad(feedFrom(nil)); err != nil {
		t.Fatalf("bulk load empty: %v", err)
	}
	rd, _ := bt.NewReader(engine.Snapshot{Version: 1})
	if _, err := rd.Get([]byte("nope")); err != engine.ErrNotFound {
		t.Fatalf("empty load get = %v, want ErrNotFound", err)
	}
	rd.Close()

	b := engine.NewWriteBatch(1)
	b.Set([]byte("a"), []byte("1"))
	if err := bt.Apply(b, 1); err != nil {
		t.Fatalf("apply after empty load: %v", err)
	}
}

// TestBulkLoadSingleLeaf loads a handful of keys that fit one page, so the root stays a
// leaf, and checks they read back. It guards the no-interior-level path.
func TestBulkLoadSingleLeaf(t *testing.T) {
	bt := newBTree(t, 4096, 16)
	cells := setCells(3, 9)
	if err := bt.BulkLoad(feedFrom(cells)); err != nil {
		t.Fatalf("bulk load: %v", err)
	}
	rd, _ := bt.NewReader(engine.Snapshot{Version: 9})
	defer rd.Close()
	for _, c := range cells {
		k := format.UserKey(c.ik)
		if v, err := rd.Get(k); err != nil || string(v) != string(c.val) {
			t.Fatalf("get %q = %q,%v, want %q", k, v, err, c.val)
		}
	}
}
