package btree

import (
	"fmt"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// BulkLoad implements engine.BulkLoader: it replaces an empty tree's contents with a
// B+tree built bottom-up over cells delivered in ascending internal-key order. Each
// leaf is packed to the page in one pass and its B-link right-sibling pointer is set as
// the chain grows, then the interior levels are built from the leaf separators. Every
// page is written once, so a cold load never pays the repeated descents and node splits
// the insert path costs (spec 05 §3, spec 15 §6).
//
// It is only valid on a freshly opened, empty engine: it builds a fresh tree, installs
// its root, and returns the pre-existing empty root page to the freelist. The host
// (db.Load) guarantees the database has no commits and makes the build durable with a
// checkpoint, so a crash before that checkpoint leaves the database empty.
func (t *BTree) BulkLoad(next func() (ik, value []byte, ok bool)) error {
	oldRoot := t.root()

	var (
		leafPages []format.PageNo // page number of each sealed leaf, left to right
		leafFirst [][]byte        // first user key of each sealed leaf
		pending   *leaf           // a sealed leaf whose right-sibling is not yet known
		pendingNo format.PageNo
	)

	// sealLeaf reserves a page for l and defers its write until the next leaf is sealed,
	// so l.next can be set to that leaf's page number first. The previous pending leaf,
	// now that its sibling is known, is written here.
	sealLeaf := func(l *leaf) error {
		no, err := t.reservePage()
		if err != nil {
			return err
		}
		if pending != nil {
			pending.next = no
			if err := t.storeLeaf(pendingNo, pending); err != nil {
				return err
			}
		}
		leafPages = append(leafPages, no)
		leafFirst = append(leafFirst, append([]byte(nil), format.UserKey(l.keys[0])...))
		pending, pendingNo = l, no
		return nil
	}

	cur := &leaf{}
	for {
		ik, value, ok := next()
		if !ok {
			break
		}
		if nodeHeaderSize+len(ik)+len(value)+8 > t.pageSize {
			return fmt.Errorf("btree: entry of %d bytes exceeds page (overflow values are deferred)", len(ik)+len(value))
		}
		cur.keys = append(cur.keys, append([]byte(nil), ik...))
		cur.vals = append(cur.vals, append([]byte(nil), value...))
		if len(marshalLeaf(cur)) <= t.pageSize {
			continue
		}

		// The leaf overflowed with the cell just added. Seal it at the last user-key
		// boundary so a version group is never split across two leaves, carrying the
		// trailing group into the next leaf. When the whole leaf is one group, cut before
		// the last cell to make progress -- the degenerate giant-group case the insert
		// path also cuts rather than leave oversized (spec 05 §3.2).
		sp := lastGroupBoundary(cur)
		if sp == 0 {
			sp = len(cur.keys) - 1
		}
		sealed := &leaf{
			keys: append([][]byte(nil), cur.keys[:sp]...),
			vals: append([][]byte(nil), cur.vals[:sp]...),
		}
		carry := &leaf{
			keys: append([][]byte(nil), cur.keys[sp:]...),
			vals: append([][]byte(nil), cur.vals[sp:]...),
		}
		if err := sealLeaf(sealed); err != nil {
			return err
		}
		cur = carry
	}

	if len(cur.keys) > 0 {
		if err := sealLeaf(cur); err != nil {
			return err
		}
	}
	// The last leaf has no right sibling; write it now.
	if pending != nil {
		pending.next = format.NoPage
		if err := t.storeLeaf(pendingNo, pending); err != nil {
			return err
		}
	}

	if len(leafPages) == 0 {
		// Empty stream: leave the empty root in place, nothing to reclaim.
		return nil
	}

	// Build interior levels bottom-up from the leaf separators until a single root
	// remains. The separator before leaf i+1 is its first user key, so the separators of
	// a level are the first keys of its children after the first.
	children := leafPages
	seps := leafFirst[1:]
	for len(children) > 1 {
		var err error
		children, seps, err = t.packInteriorLevel(children, seps)
		if err != nil {
			return err
		}
	}
	t.setRoot(children[0])

	// The pre-existing empty root is now unreferenced.
	if oldRoot != format.NoPage && oldRoot != children[0] {
		t.pgr.Free(oldRoot)
	}
	return nil
}

// packInteriorLevel packs one level of interior nodes over the given children and the
// separators between them (len(seps) == len(children)-1). It fills each node to the page
// and promotes the separator that sits between a full node and the next as that node's
// separator at the parent level. It returns the parent level's children (one page per
// node packed) and the promoted separators between them.
func (t *BTree) packInteriorLevel(children []format.PageNo, seps [][]byte) ([]format.PageNo, [][]byte, error) {
	var parentChildren []format.PageNo
	var parentSeps [][]byte

	i := 0
	for i < len(children) {
		in := &interior{children: []format.PageNo{children[i]}}
		j := i
		for j+1 < len(children) {
			cand := &interior{
				seps:     append(append([][]byte(nil), in.seps...), seps[j]),
				children: append(append([]format.PageNo(nil), in.children...), children[j+1]),
			}
			// Keep at least one separator per node so progress is guaranteed even when a
			// single child plus separator already fills the page.
			if len(in.seps) >= 1 && len(marshalInterior(cand)) > t.pageSize {
				break
			}
			in = cand
			j++
		}
		no, err := t.storeInteriorNew(in)
		if err != nil {
			return nil, nil, err
		}
		parentChildren = append(parentChildren, no)
		if j+1 < len(children) {
			// seps[j] separates this node from the next; it routes between them one level
			// up rather than living inside either node.
			parentSeps = append(parentSeps, append([]byte(nil), seps[j]...))
		}
		i = j + 1
	}
	return parentChildren, parentSeps, nil
}

// reservePage allocates a page and returns its number, leaving it zeroed and unpinned so
// a later storeLeaf can write the real content once its right-sibling pointer is known.
func (t *BTree) reservePage() (format.PageNo, error) {
	pgno, fr, err := t.pgr.Allocate()
	if err != nil {
		return 0, err
	}
	t.pgr.Unpin(fr, true)
	return pgno, nil
}

// lastGroupBoundary returns the index of the last user-key group start in l (the first
// cell of the rightmost user key), or 0 when every cell shares one user key.
func lastGroupBoundary(l *leaf) int {
	b := 0
	for i := 1; i < len(l.keys); i++ {
		if format.CompareUser(format.UserKey(l.keys[i]), format.UserKey(l.keys[i-1])) != 0 {
			b = i
		}
	}
	return b
}

// compile-time check that BTree provides the bulk-load fast path.
var _ engine.BulkLoader = (*BTree)(nil)
