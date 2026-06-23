package btree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
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

// TestVerifyDetectsOrderViolation duplicates a key inside a leaf so two adjacent internal
// keys are no longer strictly ascending, and confirms the verifier reports an order problem.
// Equal adjacent keys are an ordering fault and nothing else, so the report isolates the
// class.
func TestVerifyDetectsOrderViolation(t *testing.T) {
	bt := newBTree(t, 512, 64)
	applyKeys(t, bt, 300)
	leaf := firstLeaf(t, bt)

	l, err := bt.loadLeaf(leaf)
	if err != nil {
		t.Fatalf("load leaf %d: %v", leaf, err)
	}
	if len(l.keys) < 2 {
		t.Fatalf("leaf %d has %d keys, need at least 2 to break ordering", leaf, len(l.keys))
	}
	// Make cell 1 equal to cell 0: a strictly-ascending check fails on equality.
	l.keys[1] = append([]byte(nil), l.keys[0]...)
	if err := bt.storeLeaf(leaf, l); err != nil {
		t.Fatalf("store leaf %d: %v", leaf, err)
	}

	rep, err := bt.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if rep.OK() {
		t.Fatal("verify passed a leaf with non-ascending keys")
	}
	if !hasClass(rep, "order") {
		t.Fatalf("want an order problem, got %+v", rep.Problems)
	}
}

// TestVerifyDetectsBoundsViolation lowers the root's first separator below every real key, so
// the leftmost child subtree now routes keys that sit at or above the upper bound the parent
// claims for it, and confirms the verifier reports a bounds problem. Only the separator bytes
// change, so the in-order key sequence stays sound and the report isolates the bounds class.
func TestVerifyDetectsBoundsViolation(t *testing.T) {
	bt := newBTree(t, 512, 64)
	applyKeys(t, bt, 300)

	root := bt.root()
	typ, err := bt.typeOf(root)
	if err != nil {
		t.Fatalf("typeOf root %d: %v", root, err)
	}
	if typ != format.PageBTreeInterior {
		t.Fatalf("root %d is not interior; need a multi-level tree", root)
	}
	in, err := bt.loadInterior(root)
	if err != nil {
		t.Fatalf("load interior %d: %v", root, err)
	}
	if len(in.seps) < 1 {
		t.Fatalf("root %d has no separators", root)
	}
	// "a" sorts below every "key%05d", so the leftmost subtree's hi bound is now violated by
	// all of its keys, while the separators stay strictly ascending (no order fault).
	in.seps[0] = []byte("a")
	if err := bt.storeInterior(root, in); err != nil {
		t.Fatalf("store interior %d: %v", root, err)
	}

	rep, err := bt.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if rep.OK() {
		t.Fatal("verify passed a tree whose child keys escape their parent's bound")
	}
	if !hasClass(rep, "bounds") {
		t.Fatalf("want a bounds problem, got %+v", rep.Problems)
	}
}

// TestVerifyDetectsFreelistViolation puts an out-of-range page number on the freelist and
// confirms the verifier reports a freelist problem. An out-of-range entry is skipped before
// the space accounting tallies it, so the file still balances and the report isolates the
// freelist class.
func TestVerifyDetectsFreelistViolation(t *testing.T) {
	bt := newBTree(t, 512, 64)
	applyKeys(t, bt, 300)

	// A page number well past the file's high-water mark can never be a real free page.
	bt.pgr.Free(bt.pgr.DBSize() + 1000)

	rep, err := bt.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if rep.OK() {
		t.Fatal("verify passed a freelist holding an out-of-range page")
	}
	if !hasClass(rep, "freelist") {
		t.Fatalf("want a freelist problem, got %+v", rep.Problems)
	}
}

// TestVerifyDetectsChecksumCorruption flushes a checksummed tree to disk, flips a content
// byte inside a leaf page on disk so its stored checksum no longer matches, and confirms
// the verifier reports a "checksum" problem, the torn-write/bit-rot class (spec 02 §3.2,
// spec 23 §3). The flip changes neither the page type nor key order, so the report
// isolates the checksum class: it is the only detector for a page that is otherwise
// structurally perfect but silently wrong in a data byte.
func TestVerifyDetectsChecksumCorruption(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.kv", pager.Options{
		PageSize:    512,
		CacheFrames: 64,
		Engine:      format.EngineBTree,
		Checksum:    format.ChecksumCRC32C,
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	bt := New(p)
	if err := bt.Open(&engine.Env{}); err != nil {
		t.Fatalf("open btree: %v", err)
	}
	applyKeys(t, bt, 300)
	leaf := firstLeaf(t, bt)

	// Flush every dirty page to disk with a valid checksum, then corrupt one content byte
	// of the leaf on disk behind the pager's back.
	if err := p.Checkpoint(0, 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	f, err := fs.Open("test.kv", vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	page := make([]byte, 512)
	off := int64(leaf-1) * 512
	if _, err := f.ReadAt(page, off); err != nil {
		t.Fatalf("read leaf %d: %v", leaf, err)
	}
	page[256] ^= 0xFF // a content byte: not the type byte (0) and not the trailer
	if _, err := f.WriteAt(page, off); err != nil {
		t.Fatalf("write leaf %d: %v", leaf, err)
	}
	if err := f.Sync(vfs.SyncFull); err != nil {
		t.Fatalf("sync: %v", err)
	}
	f.Close()

	rep, err := bt.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if rep.OK() {
		t.Fatal("verify passed a tree with a bit-flipped leaf page")
	}
	if !hasClass(rep, "checksum") {
		t.Fatalf("want a checksum problem, got %+v", rep.Problems)
	}
}

// TestVerifyChecksumCleanFile confirms the checksum sweep passes a freshly checkpointed
// checksummed file: every live page is stamped and verifies, so the sweep adds no false
// positives on a sound database.
func TestVerifyChecksumCleanFile(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.kv", pager.Options{
		PageSize:    512,
		CacheFrames: 64,
		Engine:      format.EngineBTree,
		Checksum:    format.ChecksumCRC32C,
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	bt := New(p)
	if err := bt.Open(&engine.Env{}); err != nil {
		t.Fatalf("open btree: %v", err)
	}
	applyKeys(t, bt, 300)
	if err := p.Checkpoint(0, 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	rep, err := bt.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("checksummed clean file reported problems: %+v", rep.Problems)
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
