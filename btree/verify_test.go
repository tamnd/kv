package btree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// applyKeys inserts n distinct keys at a single version, enough at a tiny page size to
// force the tree to split into interior levels.
func applyKeys(t *testing.T, bt *BTree, n int) {
	t.Helper()
	b := engine.NewWriteBatch(1)
	for i := 0; i < n; i++ {
		b.Set([]byte(fmt.Sprintf("key%05d", i)), []byte("v"))
	}
	if err := bt.Apply(b, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

// firstLeaf descends the left spine to a leaf page number, so a test can corrupt a real
// leaf without knowing the tree's shape.
func firstLeaf(t *testing.T, bt *BTree) format.PageNo {
	t.Helper()
	pgno := bt.root()
	for {
		typ, err := bt.typeOf(pgno)
		if err != nil {
			t.Fatalf("typeOf %d: %v", pgno, err)
		}
		if typ == format.PageBTreeLeaf {
			return pgno
		}
		in, err := bt.loadInterior(pgno)
		if err != nil {
			t.Fatalf("loadInterior %d: %v", pgno, err)
		}
		pgno = in.children[0]
	}
}

// TestVerifyHealthyTree builds a multi-level tree and checks the verifier passes it: no
// problems, every page reachable once, the key count matches what was inserted, and the
// page accounting balances.
func TestVerifyHealthyTree(t *testing.T) {
	bt := newBTree(t, 512, 64)
	const n = 300
	applyKeys(t, bt, n)

	rep, err := bt.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("healthy tree reported %d problems: %+v", len(rep.Problems), rep.Problems)
	}
	if rep.Keys != n {
		t.Fatalf("verify saw %d keys, want %d", rep.Keys, n)
	}
	if rep.PagesVisited < 3 {
		t.Fatalf("verify visited %d pages, want a multi-level tree (>= 3)", rep.PagesVisited)
	}
	// header(1) + reachable + free must equal the page count for a sound file.
	if got := 1 + rep.PagesVisited + rep.FreePages; uint32(got) != rep.PageCount {
		t.Fatalf("accounting 1+%d+%d = %d != page count %d", rep.PagesVisited, rep.FreePages, got, rep.PageCount)
	}
}

// TestVerifyDetectsBadPageType corrupts a leaf's type byte and confirms the verifier
// reports a structure problem rather than passing or panicking, the corruption-detection
// obligation (spec 23 §3, §7).
func TestVerifyDetectsBadPageType(t *testing.T) {
	bt := newBTree(t, 512, 64)
	applyKeys(t, bt, 300)
	leaf := firstLeaf(t, bt)

	fr, err := bt.pgr.Get(leaf, pager.Write)
	if err != nil {
		t.Fatalf("get leaf %d: %v", leaf, err)
	}
	fr.Data()[0] = 0xFF // not a valid page type
	bt.pgr.Unpin(fr, true)

	rep, err := bt.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if rep.OK() {
		t.Fatal("verify passed a tree with a corrupted page type")
	}
	if !hasClass(rep, "structure") {
		t.Fatalf("want a structure problem, got %+v", rep.Problems)
	}
}

// TestVerifyDetectsDoubleAlloc puts a page that is reachable from the tree onto the
// freelist and confirms the verifier reports it as both reachable and free, plus the
// resulting space-accounting mismatch.
func TestVerifyDetectsDoubleAlloc(t *testing.T) {
	bt := newBTree(t, 512, 64)
	applyKeys(t, bt, 300)
	leaf := firstLeaf(t, bt)

	bt.pgr.Free(leaf) // a live page must never be on the freelist

	rep, err := bt.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if rep.OK() {
		t.Fatal("verify passed a tree with a reachable page on the freelist")
	}
	if !hasClass(rep, "double-alloc") {
		t.Fatalf("want a double-alloc problem, got %+v", rep.Problems)
	}
	if !hasClass(rep, "space") {
		t.Fatalf("want a space-accounting problem, got %+v", rep.Problems)
	}
}

// hasClass reports whether the report contains a problem of the given class.
func hasClass(rep *engine.VerifyReport, class string) bool {
	for _, p := range rep.Problems {
		if p.Class == class {
			return true
		}
	}
	return false
}
