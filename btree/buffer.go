package btree

import (
	"fmt"

	"github.com/tamnd/kv/format"
)

// The Bε (buffered) write path (spec 05 §4). In the default engine an insert descends
// to its leaf and dirties a whole page to change a few bytes; for write-heavier
// workloads that pays an avoidable write-amplification tax. Bε mode parks each insert
// as a message in the root interior's buffer instead, and flushes a node's buffer one
// level down in a batch only when it fills. A key reaches its leaf after O(log n)
// flushes, but each flush moves many messages per page write, so the per-key write
// amplification drops toward LSM territory while point reads stay O(log n) (a read
// also checks the buffers along its root-to-leaf path, a bounded, cache-resident cost).
//
// The mode is opt-in (EngineOptions.BufferedInserts) and off by default. With it off
// every interior buffer is empty, marshalInterior writes no buffer section, and this
// file is never entered, so the read-latency-first default path is byte-for-byte
// unchanged.
//
// Budgets. An interior page splits its usable bytes between pivots (separators and
// child pointers, which set the tree's fanout and height) and the message buffer.
// bufBudget caps the buffer; a node whose buffer exceeds it flushes. pivotBudget caps
// the pivots; absorbing child splits past it splits the interior structurally. The two
// budgets sum to usable, so a stored node (buffer at most bufBudget, pivots at most
// pivotBudget) always fits its page. Reserving half the page for buffer halves the
// interior fanout relative to the unbuffered tree, a deliberate Bε trade: a little more
// height in exchange for batched, amortized writes.

func (t *BTree) bufBudget() int   { return t.usable / 2 }
func (t *BTree) pivotBudget() int { return t.usable - t.bufBudget() }

// message is one buffered insert/delete: a full internal-key cell, identical in form
// to a leaf cell, so MVCC resolution treats a parked message exactly like a flushed one.
type message struct {
	ik  []byte
	val []byte
}

// childSplit is a (separator, new child) pair a subtree hands back to its parent after
// a flush forced it to split: user keys >= sep now live in child, which the parent
// splices in beside the child it split out of.
type childSplit struct {
	sep   []byte
	child format.PageNo
}

// bufferedApply installs one entry through the Bε path. While the tree is a lone leaf
// root there is no interior to buffer into, so it inserts directly; that small-tree
// case never benefits from buffering anyway. Once the root is an interior, the entry
// becomes a message in the root buffer, and a flush cascade carries it down only when
// buffers fill.
func (t *BTree) bufferedApply(ik, value []byte) error {
	if nodeHeaderSize+len(ik)+len(value)+8 > t.usable {
		return fmt.Errorf("btree: entry of %d bytes exceeds page (overflow values are deferred)", len(ik)+len(value))
	}
	root := t.root()
	typ, err := t.typeOf(root)
	if err != nil {
		return err
	}
	if typ == format.PageBTreeLeaf {
		return t.insertOne(ik, value)
	}
	splits, err := t.applyMessages(root, []message{{ik: ik, val: value}})
	if err != nil {
		return err
	}
	if len(splits) == 0 {
		return nil
	}
	// The flush cascaded a split up through the root: grow a new level over the old
	// root (which kept its page as the leftmost child) and the siblings it split out.
	children := []format.PageNo{root}
	var seps [][]byte
	for _, cs := range splits {
		seps = append(seps, cs.sep)
		children = append(children, cs.child)
	}
	return t.installRoot(children, seps)
}

// installRoot makes (children, seps) the new root, splitting it across levels if the
// fan-out from a wide cascade overflows a single interior page. It recurses upward
// until the top interior fits, so the tree can grow more than one level in a single
// flush.
func (t *BTree) installRoot(children []format.PageNo, seps [][]byte) error {
	in := &interior{seps: seps, children: children}
	if len(marshalInterior(in)) <= t.pivotBudget() {
		rp, err := t.storeInteriorNew(in)
		if err != nil {
			return err
		}
		t.setRoot(rp)
		return nil
	}
	pieces, ups := splitInteriorPieces(in, t.pivotBudget())
	pgnos := make([]format.PageNo, len(pieces))
	for i, pc := range pieces {
		p, err := t.storeInteriorNew(pc)
		if err != nil {
			return err
		}
		pgnos[i] = p
	}
	return t.installRoot(pgnos, ups)
}

// applyMessages routes a batch of messages into the subtree rooted at pgno and returns
// the splits, if any, the parent must absorb. It is the recursive heart of the flush
// cascade: at a leaf the messages land as cells; at an interior they join the buffer,
// which flushes one level further only when it overflows.
func (t *BTree) applyMessages(pgno format.PageNo, msgs []message) ([]childSplit, error) {
	typ, err := t.typeOf(pgno)
	if err != nil {
		return nil, err
	}
	if typ == format.PageBTreeLeaf {
		return t.applyLeafMessages(pgno, msgs)
	}
	return t.applyInteriorMessages(pgno, msgs)
}

// applyLeafMessages writes a batch of messages into a leaf, splitting it into a chain
// of leaves when the batch overflows the page. The leftmost piece keeps the leaf's
// page number so the parent pointer to it stays valid; the rest are returned as splits.
func (t *BTree) applyLeafMessages(pgno format.PageNo, msgs []message) ([]childSplit, error) {
	l, err := t.loadLeaf(pgno)
	if err != nil {
		return nil, err
	}
	for _, m := range msgs {
		l.insert(m.ik, m.val)
	}
	if leafEncodedSize(l) <= t.usable {
		return nil, t.storeLeaf(pgno, l)
	}

	pieces := splitLeafPieces(l, t.usable)
	if len(pieces) == 1 {
		// A single user key's version group outgrew a page and has no clean split point;
		// store it oversized rather than cut a group, as the unbuffered path also does.
		return nil, t.storeLeaf(pgno, pieces[0])
	}

	pgnos := make([]format.PageNo, len(pieces))
	pgnos[0] = pgno
	for i := 1; i < len(pieces); i++ {
		np, err := t.storeLeafNew(pieces[i])
		if err != nil {
			return nil, err
		}
		pgnos[i] = np
	}
	// Chain the right-sibling links across the new pieces, then persist each with the
	// corrected next pointer (the last piece already carries the original next).
	for i := 0; i < len(pieces)-1; i++ {
		pieces[i].next = pgnos[i+1]
	}
	var splits []childSplit
	for i := 0; i < len(pieces); i++ {
		if err := t.storeLeaf(pgnos[i], pieces[i]); err != nil {
			return nil, err
		}
		if i > 0 {
			splits = append(splits, childSplit{
				sep:   append([]byte(nil), format.UserKey(pieces[i].keys[0])...),
				child: pgnos[i],
			})
		}
	}
	return splits, nil
}

// applyInteriorMessages buffers a batch into an interior node, and flushes only if the
// buffer now exceeds its budget. A flush partitions the buffer by child, recurses into
// each touched child, absorbs the child splits that come back, and finally splits the
// node itself if the absorbed separators overflowed the pivot budget.
func (t *BTree) applyInteriorMessages(pgno format.PageNo, msgs []message) ([]childSplit, error) {
	in, err := t.loadInterior(pgno)
	if err != nil {
		return nil, err
	}
	for _, m := range msgs {
		in.bufferInsert(m.ik, m.val)
	}
	if in.msgBytes() <= t.bufBudget() {
		return nil, t.storeInterior(pgno, in)
	}

	// Flush: route every buffered message to the child whose subtree owns it.
	groups := make(map[int][]message, len(in.children))
	for i := range in.msgKeys {
		ci := in.childFor(format.UserKey(in.msgKeys[i]))
		groups[ci] = append(groups[ci], message{ik: in.msgKeys[i], val: in.msgVals[i]})
	}
	in.msgKeys, in.msgVals = nil, nil

	splitsByChild := make(map[int][]childSplit)
	for ci := 0; ci < len(in.children); ci++ {
		g := groups[ci]
		if len(g) == 0 {
			continue
		}
		sp, err := t.applyMessages(in.children[ci], g)
		if err != nil {
			return nil, err
		}
		if len(sp) > 0 {
			splitsByChild[ci] = sp
		}
	}
	if len(splitsByChild) > 0 {
		in.absorbSplits(splitsByChild)
	}

	// The buffer is empty now, so the node is pivots only; split it structurally if the
	// absorbed separators pushed it past the pivot budget.
	if len(marshalInterior(in)) <= t.pivotBudget() {
		return nil, t.storeInterior(pgno, in)
	}
	pieces, ups := splitInteriorPieces(in, t.pivotBudget())
	pgnos := make([]format.PageNo, len(pieces))
	pgnos[0] = pgno
	for i := 1; i < len(pieces); i++ {
		np, err := t.storeInteriorNew(pieces[i])
		if err != nil {
			return nil, err
		}
		pgnos[i] = np
	}
	var splits []childSplit
	for i := 0; i < len(pieces); i++ {
		if i == 0 {
			if err := t.storeInterior(pgno, pieces[0]); err != nil {
				return nil, err
			}
			continue
		}
		splits = append(splits, childSplit{sep: ups[i-1], child: pgnos[i]})
	}
	return splits, nil
}

// msgBytes is the size the message buffer adds to the interior's encoding: the count
// varint plus each message cell. It mirrors marshalInterior's buffer section exactly so
// the flush threshold is measured in the same bytes the page actually stores.
func (in *interior) msgBytes() int {
	if len(in.msgKeys) == 0 {
		return 0
	}
	n := uvarintLen(uint64(len(in.msgKeys)))
	for i := range in.msgKeys {
		n += uvarintLen(uint64(len(in.msgKeys[i]))) + len(in.msgKeys[i])
		n += uvarintLen(uint64(len(in.msgVals[i]))) + len(in.msgVals[i])
	}
	return n
}

// absorbSplits rebuilds the node's children and separators, splicing each child's
// returned splits in right after that child. Child i's new right-siblings all sort
// below separator i, so they slot between child i and separator i.
func (in *interior) absorbSplits(splitsByChild map[int][]childSplit) {
	var nc []format.PageNo
	var ns [][]byte
	for i, child := range in.children {
		nc = append(nc, child)
		for _, cs := range splitsByChild[i] {
			ns = append(ns, cs.sep)
			nc = append(nc, cs.child)
		}
		if i < len(in.seps) {
			ns = append(ns, in.seps[i])
		}
	}
	in.children, in.seps = nc, ns
}

// splitLeafPieces divides an overflowing leaf into a chain of leaves each within
// usable, advancing every cut to a user-key boundary so a version group is never split
// (leaf.splitPoint). The pieces are returned in key order; only the last carries the
// original right-sibling pointer, and the caller wires the rest after allocating pages.
// A single oversized version group has no clean cut, so the leaf is returned whole.
func splitLeafPieces(l *leaf, usable int) []*leaf {
	var pieces []*leaf
	cur := l
	for leafEncodedSize(cur) > usable {
		sp := cur.splitPoint()
		if sp == 0 {
			break
		}
		left := &leaf{keys: cur.keys[:sp], vals: cur.vals[:sp]}
		right := &leaf{keys: cur.keys[sp:], vals: cur.vals[sp:], next: cur.next}
		pieces = append(pieces, left)
		cur = right
	}
	pieces = append(pieces, cur)
	return pieces
}

// splitInteriorPieces divides an interior whose pivots overflow budget into a chain of
// interiors each within budget, pushing the middle separator of every cut up to the
// parent. It returns the pieces in key order and the up-separators between them, so the
// parent learns one separator per new sibling. The node must carry no buffered messages
// (a flush clears the buffer before any structural split).
func splitInteriorPieces(in *interior, budget int) ([]*interior, [][]byte) {
	var pieces []*interior
	var ups [][]byte
	cur := in
	for len(marshalInterior(cur)) > budget && len(cur.seps) >= 1 {
		mid := len(cur.seps) / 2
		ups = append(ups, append([]byte(nil), cur.seps[mid]...))
		left := &interior{seps: cur.seps[:mid], children: cur.children[:mid+1]}
		right := &interior{seps: cur.seps[mid+1:], children: cur.children[mid+1:]}
		pieces = append(pieces, left)
		cur = right
	}
	pieces = append(pieces, cur)
	return pieces, ups
}

// uvarintLen returns the number of bytes the unsigned varint encoding of x occupies,
// the size accounting marshalInterior's buffer section relies on.
func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}
