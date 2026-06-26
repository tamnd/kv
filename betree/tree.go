package betree

// This file is M0's interior-routed write path: it turns the flat sibling-linked run
// the paged slice landed into a real Bε-tree, so an insert descends from the root
// through interior nodes to one leaf in O(log n) page touches instead of rewriting
// the whole run. The descent, the leaf split, and the split propagation are modeled
// directly on the shipped B+tree (btree/btree.go), reusing the proven algorithm over
// the generation-2 node codecs from node.go rather than inventing new logic.
//
// What this lands, and what it leaves. The leaf is still the unit of mutation: an
// insert decodes the target leaf, splices the cell in, and re-encodes it, splitting
// when the re-encode overflows the page. That keeps the slice small and obviously
// correct; the in-place page splice that avoids the decode, and the buffered-message
// push that defers the leaf touch, are M1. Routing is by full internal key, so a
// user key's versions land in internal-key order across the run, and the read side
// still gathers a version group by walking sibling leaves rather than by descent.
// Interior nodes carry an empty message buffer here: the codec reserves the room and
// the buffer stays empty until M1 starts pushing messages into it.

import (
	"fmt"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// root reports the page that roots the tree, and setRoot records a new root. The durable
// root store (root.go) stays the source of truth so a reopen finds the tree without a side
// file: the pager header's EngineRoot for the single-shard core, the shard-directory slot
// for a sub-tree, the same way the shipped btree roots itself in the header. But a latch-free
// reader cannot read that durable home while a writer's growRoot stores into it without a data
// race, so setRoot also publishes the root into an atomic mirror (Tree.rootPgno) that the read
// path loads. PageNo is an alias for uint32, so it crosses the atomic word unchanged.
func (t *Tree) root() format.PageNo { return format.PageNo(t.rootPgno.Load()) }
func (t *Tree) setRoot(p format.PageNo) {
	t.rstore.store(p)
	t.rootPgno.Store(uint32(p))
}

// emptyRoot materializes a fresh empty leaf and installs it as the root. Open calls
// it on a fresh database so the tree always has a valid root leaf to descend to,
// which keeps every read and write path free of a null-root special case.
func (t *Tree) emptyRoot() error {
	pgno, err := t.storeLeafNew(&leaf{bucketSize: defaultBucketSize})
	if err != nil {
		return err
	}
	t.setRoot(pgno)
	return nil
}

// pageType reports the node type of pgno without decoding the whole node, so the
// descent can branch on interior-versus-leaf from the common header alone.
func (t *Tree) pageType(pgno format.PageNo) (format.PageType, error) {
	fr, err := t.pgr.Get(pgno, pager.Read)
	if err != nil {
		return 0, err
	}
	// Read the common header under fillGate so this raw byte read cannot race a writer's
	// in-place rewrite of the same frame. pageType is on both paths: a writer calling it
	// down its descent holds wmu and is the only mutator, but a latch-free reader reaching
	// it through the snapshot gather has no such exclusion, so the gate is what keeps the
	// header read clean against a concurrent writePage. The shared RLock lets every reader
	// decode in parallel; only the writer's brief memcpy excludes them.
	t.fillGate.RLock()
	if len(fr.Data()) < format.CommonHeaderSize {
		t.fillGate.RUnlock()
		t.pgr.Unpin(fr, false)
		return 0, ErrCorruptNode
	}
	typ := format.DecodeCommonHeader(fr.Data()).Type
	t.fillGate.RUnlock()
	t.pgr.Unpin(fr, false)
	return typ, nil
}

// loadInterior pins, decodes, and unpins an interior page. decodeInterior copies
// every key, so the returned node owns its bytes and outlives the pin.
func (t *Tree) loadInterior(pgno format.PageNo) (*interior, error) {
	fr, err := t.pgr.Get(pgno, pager.Read)
	if err != nil {
		return nil, err
	}
	in, derr := decodeInterior(fr.Data()[:t.pgr.UsablePageSize()])
	t.pgr.Unpin(fr, false)
	return in, derr
}

// encBufGet draws a page-sized scratch buffer for one node encode from the pool
// (betree.go encBuf) and returns it with the pool ref the caller hands back through
// encBufPut. A persisted encode passes zero=true so the buffer's slack is cleared, because
// the encoder writes only up to its content length and leaves the tail untouched; clearing
// it keeps a page's unused tail deterministic and free of bytes from a prior pooled encode,
// exactly as the make([]byte, ...) this replaces did. A trial-fit encode whose bytes are
// discarded passes zero=false and skips the clear, which is the hot once-per-record path the
// pack loop runs. The returned slice spans the usable page area; the pool buffer's capacity
// is that size, so the reslice never exceeds it.
//
// The ref is returned for the caller to defer encBufPut on, rather than a release closure,
// because a returned closure captures ref and escapes to the heap, one small allocation on
// every encode and every trial fit. Returning the already-allocated ref and deferring a plain
// method keeps the hot path free of that per-call allocation.
func (t *Tree) encBufGet(zero bool) (buf []byte, ref *[]byte) {
	ref = t.encBuf.get()
	b := (*ref)[:t.pgr.UsablePageSize()]
	if zero {
		for i := range b {
			b[i] = 0
		}
	}
	return b, ref
}

// encBufPut returns a buffer drawn by encBufGet to the pool. It is the deferred counterpart
// to encBufGet; keeping it a named method rather than a closure is what lets the deferred call
// stay allocation-free on the encode path.
func (t *Tree) encBufPut(ref *[]byte) { t.encBuf.put(ref) }

// writeLeaf re-encodes lf into the existing page pgno. The caller has already
// checked the leaf fits; an overflow here is a programming error, not a split
// signal, so it surfaces as an error rather than being retried.
func (t *Tree) writeLeaf(pgno format.PageNo, lf *leaf) error {
	dst, ref := t.encBufGet(true)
	defer t.encBufPut(ref)
	if _, err := encodeLeaf(dst, lf); err != nil {
		return err
	}
	return t.writePage(pgno, dst)
}

// writeInterior re-encodes in into the existing page pgno, with the same
// already-fits contract as writeLeaf.
func (t *Tree) writeInterior(pgno format.PageNo, in *interior) error {
	dst, ref := t.encBufGet(true)
	defer t.encBufPut(ref)
	if _, err := encodeInterior(dst, in); err != nil {
		return err
	}
	return t.writePage(pgno, dst)
}

// writePage copies a fully encoded usable-area image into the existing page pgno
// under a write pin. body spans the usable area; the pager re-stamps the checksum
// trailer on writeback, so the bytes past the usable area are its concern, not this
// codec's.
func (t *Tree) writePage(pgno format.PageNo, body []byte) error {
	fr, err := t.pgr.Get(pgno, pager.Write)
	if err != nil {
		return err
	}
	// The memcpy and the decoded-box clear run under fillGate.Lock, the exclusive side of
	// the gate a cold reader takes shared for its decode-and-publish. Get already cleared
	// the box when it took the write pin, which keeps a hot reader off stale bytes; the
	// second clear here, after the new bytes land and under the same gate, closes the cold
	// window where a reader could decode the old bytes and publish a box describing bytes
	// this write just overwrote. With both the memcpy and the clear inside the gate, a cold
	// reader serialized against this either decodes the new bytes or finds an empty box and
	// cold-decodes them next; it never republishes a stale view. This narrow byte-level
	// serialization is the one thing the optimistic gen protocol cannot launder on the
	// pre-arena substrate; M6's off-heap arena removes it (doc 05 section 5).
	t.fillGate.Lock()
	copy(fr.Data(), body)
	fr.ClearDecoded()
	t.fillGate.Unlock()
	t.pgr.Unpin(fr, true)
	return nil
}

// storeLeafNew allocates a fresh page and writes lf into it, returning its number.
// It is the new-page counterpart to writeLeaf, used when a split creates a sibling.
func (t *Tree) storeLeafNew(lf *leaf) (format.PageNo, error) {
	dst, ref := t.encBufGet(true)
	defer t.encBufPut(ref)
	if _, err := encodeLeaf(dst, lf); err != nil {
		return 0, err
	}
	pgno, fr, err := t.pgr.Allocate()
	if err != nil {
		return 0, err
	}
	// Same fillGate rule as writePage: every copy into a frame's bytes runs under the
	// gate's write side so a latch-free reader's cold decode cannot race it. The page is
	// freshly allocated and not yet linked into the tree, so no reader reaches it during
	// this write, but holding to the one rule for every byte write is what lets the
	// detector establish a happens-before for every write-versus-cold-read pair without a
	// per-site reachability argument.
	t.fillGate.Lock()
	copy(fr.Data(), dst)
	fr.ClearDecoded()
	t.fillGate.Unlock()
	t.pgr.Unpin(fr, true)
	return pgno, nil
}

// storeInteriorNew allocates a fresh page and writes in into it, used when a split
// propagates and grows a new interior or a new root.
func (t *Tree) storeInteriorNew(in *interior) (format.PageNo, error) {
	dst, ref := t.encBufGet(true)
	defer t.encBufPut(ref)
	if _, err := encodeInterior(dst, in); err != nil {
		return 0, err
	}
	pgno, fr, err := t.pgr.Allocate()
	if err != nil {
		return 0, err
	}
	t.fillGate.Lock()
	copy(fr.Data(), dst)
	fr.ClearDecoded()
	t.fillGate.Unlock()
	t.pgr.Unpin(fr, true)
	return pgno, nil
}

// leftmostLeaf descends from the root following the leftmost child of every interior
// until it reaches a leaf, returning that leaf's page. It is how the read-side run
// walk finds the head of the sibling chain now that the root can be an interior node
// rather than the first leaf.
func (t *Tree) leftmostLeaf() (format.PageNo, error) {
	pgno := t.root()
	seen := map[format.PageNo]bool{}
	for {
		if seen[pgno] {
			return 0, fmt.Errorf("betree: interior spine cycles at page %d", pgno)
		}
		seen[pgno] = true
		typ, err := t.pageType(pgno)
		if err != nil {
			return 0, err
		}
		if typ == format.PageBTreeLeaf {
			return pgno, nil
		}
		in, err := t.viewInterior(pgno)
		if err != nil {
			return 0, err
		}
		pgno = in.leftmost
	}
}

// insertOne routes key/val to its leaf and inserts it, splitting and propagating the
// split upward when the leaf overflows the page. It is the per-cell write primitive
// Apply drives, replacing the whole-run rewrite. The descent records the interior
// pages it passes and the child slot it took at each, so a leaf split can splice the
// new separator into the right parent on the way back up without a second search.
func (t *Tree) insertOne(key, val []byte) error {
	usable := t.pgr.UsablePageSize()

	// Reject a cell too large to ever fit a leaf up front. Spilling an oversized value
	// to the value log is a deliberately deferred format feature (doc 06 section 4),
	// so until it lands an oversized cell is a clear error rather than an infinite
	// split loop.
	if _, err := encodeLeaf(make([]byte, usable), &leaf{records: []record{{key: key, val: val}}, bucketSize: defaultBucketSize}); err != nil {
		return fmt.Errorf("betree: cell does not fit in a page (key %x, value %d bytes): %w", key, len(val), err)
	}

	// Descend to the target leaf, recording the interior spine and the slot taken at
	// each level for the split-propagation walk back up.
	var path []format.PageNo
	var slots []int
	pgno := t.root()
	for {
		typ, err := t.pageType(pgno)
		if err != nil {
			return err
		}
		if typ == format.PageBTreeLeaf {
			break
		}
		in, err := t.loadInterior(pgno)
		if err != nil {
			return err
		}
		idx := in.childIndex(key)
		path = append(path, pgno)
		slots = append(slots, idx)
		pgno = in.childPage(idx)
	}

	lf, err := t.readLeaf(pgno)
	if err != nil {
		return err
	}
	lf.insertRecord(key, val)

	// The common case: the leaf still fits, so rewrite it in place and stop.
	if _, err := encodeLeaf(make([]byte, usable), lf); err == nil {
		return t.writeLeaf(pgno, lf)
	} else if err != ErrPageFull {
		return err
	}

	// Overflow: split the leaf at its midpoint. The right half becomes a new page; the
	// left half stays on the original page so the old right sibling's left link, which
	// already points here, stays valid. The sibling chain is relinked left to right.
	sp := len(lf.records) / 2
	if sp == 0 {
		sp = 1
	}
	right := &leaf{
		records:    append([]record(nil), lf.records[sp:]...),
		left:       pgno,
		right:      lf.right,
		bucketSize: defaultBucketSize,
	}
	rpgno, err := t.storeLeafNew(right)
	if err != nil {
		return err
	}
	left := &leaf{
		records:    append([]record(nil), lf.records[:sp]...),
		left:       lf.left,
		right:      rpgno,
		bucketSize: defaultBucketSize,
	}
	if err := t.writeLeaf(pgno, left); err != nil {
		return err
	}

	sep := right.records[0].key
	return t.propagateSplit(path, slots, sep, rpgno)
}

// propagateSplit threads a child split up the recorded spine. At each level it
// splices the new separator and child into the parent at the slot the descent took;
// if the parent overflows it splits the interior at its midpoint, pushing the middle
// pivot's key further up, and continues. When the split runs past the original root,
// it grows a new root over the two halves. This mirrors the shipped btree's
// propagate, with leftmost+pivots standing in for that node's children+separators.
func (t *Tree) propagateSplit(path []format.PageNo, slots []int, sep []byte, newChild format.PageNo) error {
	usable := t.pgr.UsablePageSize()

	for i := len(path) - 1; i >= 0; i-- {
		ppgno := path[i]
		in, err := t.loadInterior(ppgno)
		if err != nil {
			return err
		}
		in.insertPivotAt(slots[i], sep, newChild)

		if _, err := encodeInterior(make([]byte, usable), in); err == nil {
			return t.writeInterior(ppgno, in)
		} else if err != ErrPageFull {
			return err
		}

		// The interior overflowed: split it. The middle pivot's key rises to the parent;
		// its child becomes the right node's leftmost, so no separator is lost.
		mid := len(in.pivots) / 2
		upSep := append([]byte(nil), in.pivots[mid].key...)
		rightIn := &interior{
			leftmost: in.pivots[mid].child,
			pivots:   append([]pivot(nil), in.pivots[mid+1:]...),
		}
		leftIn := &interior{
			leftmost: in.leftmost,
			pivots:   append([]pivot(nil), in.pivots[:mid]...),
		}
		rpgno, err := t.storeInteriorNew(rightIn)
		if err != nil {
			return err
		}
		if err := t.writeInterior(ppgno, leftIn); err != nil {
			return err
		}
		sep, newChild = upSep, rpgno
	}

	// The split reached past the root: grow a new root whose leftmost child is the old
	// root (now the left half) and whose single pivot routes to the new right half.
	newRoot := &interior{
		leftmost: t.root(),
		pivots:   []pivot{{key: append([]byte(nil), sep...), child: newChild}},
	}
	rpgno, err := t.storeInteriorNew(newRoot)
	if err != nil {
		return err
	}
	t.setRoot(rpgno)
	return nil
}
