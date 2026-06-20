package btree

import (
	"errors"
	"fmt"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// Verify implements engine.Verifier: it walks the tree from the root and checks every
// structural invariant a sound B-tree must hold, then reconciles the reachable pages
// against the freelist and the file size (spec 23 §3, §7). It returns an error only when
// an I/O failure stops the walk; every structural violation is recorded as a problem so
// one pass fully diagnoses a corrupt file rather than halting at the first fault.
//
// The invariants checked:
//   - every page is a valid B-tree node type (structure);
//   - keys within a leaf and separators within an interior node ascend strictly, and the
//     whole in-order key sequence ascends strictly (order);
//   - every key in a child subtree lies within the [lo, hi) user-key bound its parent
//     routes to it (bounds);
//   - every child pointer is a real page in [1, page count] (structure);
//   - no page is reachable from more than one parent (structure: a cycle or shared
//     subtree would corrupt updates);
//   - no page is both reachable and on the freelist (double-alloc);
//   - the freelist holds no duplicate or out-of-range page (freelist);
//   - header(1) + reachable + free == page count, so no page leaks and none is
//     double-counted (space).
func (t *BTree) Verify() (*engine.VerifyReport, error) {
	rep := &engine.VerifyReport{
		PageCount: t.pgr.DBSize(),
		FreePages: t.pgr.FreeCount(),
	}
	visited := make(map[uint32]bool)

	// Sweep every live page's checksum first so a torn write or bit rot is reported as a
	// "checksum" problem (spec 02 §3.2) even on a page the structural walk would refuse to
	// descend into. The structural walk that follows tolerates a checksum failure rather
	// than aborting, so one pass diagnoses both the corruption and any structure it breaks.
	t.verifyChecksums(rep)

	root := t.root()
	if root != format.NoPage {
		var prev []byte // last internal key seen in-order, for the global ordering check
		if err := t.verifyNode(root, nil, nil, visited, &prev, rep); err != nil {
			return nil, err
		}
	}
	rep.PagesVisited = len(visited)

	t.verifyFreelist(visited, rep)
	return rep, nil
}

// verifyChecksums reads every allocated, non-free page raw and checks its stored
// checksum, recording a "checksum" problem per mismatch. It is the detector for the
// torn-write/bit-rot corruption class (spec 23 §3): a page can pass every structural
// invariant and still be silently wrong in a value byte, which only the checksum
// catches. It is a no-op on a file created without checksums, and it skips freelist
// pages (whose stale bytes carry no current checksum) and all-zero holes.
func (t *BTree) verifyChecksums(rep *engine.VerifyReport) {
	algo := t.pgr.ChecksumAlgo()
	if algo == format.ChecksumNone {
		return
	}
	free := make(map[uint32]bool)
	for _, p := range t.pgr.FreePages() {
		free[p] = true
	}
	for pgno := uint32(1); pgno <= rep.PageCount; pgno++ {
		if free[pgno] {
			continue
		}
		page, err := t.pgr.ReadRaw(pgno)
		if err != nil {
			// A short read at the tail means the page is not materialized on disk yet: a
			// high-water page above the synced file length after a crash, or a hole. That is
			// not corruption (the structural walk catches a live pointer into it); skip it.
			continue
		}
		if allZeroPage(page) {
			continue
		}
		if err := format.VerifyPageChecksum(page, algo); err != nil {
			rep.Add("checksum", pgno, "page checksum mismatch (torn write or bit rot)")
		}
	}
}

// allZeroPage reports whether a page is entirely zero, an uninitialized hole that
// carries no checksum and is not corruption.
func allZeroPage(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// verifyNode recursively checks the subtree rooted at pgno, whose keys must all fall in
// the user-key range [lo, hi) (a nil bound means unbounded on that side). It threads prev,
// the last internal key seen in left-to-right order, so the in-order key sequence is
// proven strictly ascending across page boundaries, not just within a page. It records
// problems rather than returning them; it returns an error only for an I/O failure.
func (t *BTree) verifyNode(pgno format.PageNo, lo, hi []byte, visited map[uint32]bool, prev *[]byte, rep *engine.VerifyReport) error {
	if pgno == 0 || pgno > t.pgr.DBSize() {
		rep.Add("structure", pgno, "child pointer out of range")
		return nil
	}
	if visited[pgno] {
		rep.Add("structure", pgno, "page reachable from more than one parent (cycle or shared subtree)")
		return nil
	}
	visited[pgno] = true

	typ, err := t.typeOf(pgno)
	if err != nil {
		// A checksum failure on this page was already recorded by the checksum sweep;
		// stop descending the unreadable subtree rather than aborting the whole walk, so
		// the report still lists every other problem found.
		if errors.Is(err, format.ErrCorrupt) {
			return nil
		}
		return fmt.Errorf("btree verify: read page %d: %w", pgno, err)
	}
	switch typ {
	case format.PageBTreeLeaf:
		l, err := t.loadLeaf(pgno)
		if err != nil {
			if errors.Is(err, format.ErrCorrupt) {
				return nil
			}
			return fmt.Errorf("btree verify: load leaf %d: %w", pgno, err)
		}
		t.verifyLeaf(pgno, l, lo, hi, prev, rep)
	case format.PageBTreeInterior:
		in, err := t.loadInterior(pgno)
		if err != nil {
			if errors.Is(err, format.ErrCorrupt) {
				return nil
			}
			return fmt.Errorf("btree verify: load interior %d: %w", pgno, err)
		}
		if err := t.verifyInterior(pgno, in, lo, hi, visited, prev, rep); err != nil {
			return err
		}
	default:
		rep.Add("structure", pgno, fmt.Sprintf("unexpected page type 0x%02x for a tree page", byte(typ)))
	}
	return nil
}

// verifyLeaf checks a leaf's cells: each user key within [lo, hi), the internal-key
// sequence strictly ascending within the leaf and against the running global predecessor,
// and counts the live cells.
func (t *BTree) verifyLeaf(pgno format.PageNo, l *leaf, lo, hi []byte, prev *[]byte, rep *engine.VerifyReport) {
	if len(l.keys) != len(l.vals) {
		rep.Add("structure", pgno, fmt.Sprintf("leaf has %d keys but %d values", len(l.keys), len(l.vals)))
	}
	for i, ik := range l.keys {
		uk := format.UserKey(ik)
		if lo != nil && format.CompareUser(uk, lo) < 0 {
			rep.Add("bounds", pgno, fmt.Sprintf("key %q below the subtree lower bound %q", uk, lo))
		}
		if hi != nil && format.CompareUser(uk, hi) >= 0 {
			rep.Add("bounds", pgno, fmt.Sprintf("key %q at or above the subtree upper bound %q", uk, hi))
		}
		if i > 0 && format.CompareInternal(l.keys[i-1], ik) >= 0 {
			rep.Add("order", pgno, fmt.Sprintf("internal keys not strictly ascending at cell %d", i))
		}
		if *prev != nil && format.CompareInternal(*prev, ik) >= 0 {
			rep.Add("order", pgno, fmt.Sprintf("internal key at cell %d not greater than the previous leaf's last key", i))
		}
		*prev = ik
		rep.Keys++
	}
}

// verifyInterior checks an interior node's separators (strictly ascending, within
// [lo, hi)) and recurses into each child with the narrowed bound it routes to. child[i]
// covers [sep[i-1], sep[i]) with sep[-1] = lo and sep[len] = hi, so the bounds tighten as
// the walk descends and a key landing in the wrong subtree is caught at the leaf.
func (t *BTree) verifyInterior(pgno format.PageNo, in *interior, lo, hi []byte, visited map[uint32]bool, prev *[]byte, rep *engine.VerifyReport) error {
	if len(in.children) != len(in.seps)+1 {
		rep.Add("structure", pgno, fmt.Sprintf("interior has %d separators but %d children (want %d)", len(in.seps), len(in.children), len(in.seps)+1))
	}
	for i := 1; i < len(in.seps); i++ {
		if format.CompareUser(in.seps[i-1], in.seps[i]) >= 0 {
			rep.Add("order", pgno, fmt.Sprintf("separators not strictly ascending at index %d", i))
		}
	}
	for i, sep := range in.seps {
		if lo != nil && format.CompareUser(sep, lo) < 0 {
			rep.Add("bounds", pgno, fmt.Sprintf("separator %q below the node lower bound %q", sep, lo))
		}
		if hi != nil && format.CompareUser(sep, hi) >= 0 {
			rep.Add("bounds", pgno, fmt.Sprintf("separator %q at or above the node upper bound %q", sep, hi))
		}
		_ = i
	}
	for i, child := range in.children {
		clo, chi := lo, hi
		if i > 0 {
			clo = in.seps[i-1]
		}
		if i < len(in.seps) {
			chi = in.seps[i]
		}
		if err := t.verifyNode(child, clo, chi, visited, prev, rep); err != nil {
			return err
		}
	}
	return nil
}

// verifyFreelist reconciles the freelist against the reachable set and the file size: no
// duplicate or out-of-range free page, no page both free and reachable, and the page
// accounting balances (header + reachable + free == page count). It runs on the fresh
// in-memory freelist the pager loaded at open, which folds the drained trunk pages back
// in, so right after open the three sets partition the file exactly (spec 09 §2).
func (t *BTree) verifyFreelist(visited map[uint32]bool, rep *engine.VerifyReport) {
	free := t.pgr.FreePages()
	seen := make(map[uint32]bool, len(free))
	for _, p := range free {
		if p == 0 || p > rep.PageCount {
			rep.Add("freelist", p, "freelist entry out of range")
			continue
		}
		if seen[p] {
			rep.Add("freelist", p, "page listed more than once on the freelist")
			continue
		}
		seen[p] = true
		if visited[p] {
			rep.Add("double-alloc", p, "page is both reachable from the tree and on the freelist")
		}
	}
	// header page 1 + reachable + free should account for the whole file.
	accounted := uint64(1) + uint64(len(visited)) + uint64(len(seen))
	if accounted != uint64(rep.PageCount) {
		rep.Add("space", 0, fmt.Sprintf("page accounting does not balance: header(1) + reachable(%d) + free(%d) = %d, want page count %d",
			len(visited), len(seen), accounted, rep.PageCount))
	}
}
