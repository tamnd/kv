package betree

// This file is M1's buffered-message write path: the Bε part of the tree, where a
// write becomes a message that lands in the highest owning node's buffer and rides
// down to the leaves in batched flushes instead of descending per cell (doc 02
// sections 1 and 2, decisions D2 and D3). It replaces M0's descend-to-the-leaf
// insert as the engine's write path while keeping M0's leaf split and node codecs
// underneath. The amortization is the whole point: messages pile up in a node's
// buffer until it fills, then a flush moves a whole page of them down one level in
// one batch, so a random key costs a fraction of a page write amortized rather than
// one page write per level (doc 02 section 2's B^(1-e) win).
//
// The correctness lever that lets this land incrementally under the same oracle: a
// message's resolved value does not depend on where the message physically sits.
// Resolution is by commit version through the shared fold (format.Fold), so a
// message in an interior buffer and the same message merged into a leaf fold to the
// same answer. The read path (paged.go) therefore gathers buffered messages and
// leaf records into one fold and stays bit-for-bit identical to M0's resolution
// while the write path moves underneath it. That is the alongside discipline applied
// inside M1: the buffered tree is verified against the same model the unbuffered
// tree answered to, so a flush that reordered or dropped a message fails the diff.
//
// What this lands, and what it leaves. The buffer flushes when a node fills (the
// page is the high-water mark, so a flush moves a full page of messages, maximal
// amortization per flush), to the heaviest child (the standard Be-tree rule that
// keeps each flush fat). Splits during a flush are handled by draining the buffer
// empty first so a split always sees a bufferless node, which keeps the split logic
// the same shape as M0's. The in-memory mutable hot tail that absorbs overwrite
// churn before it ever becomes a message (doc 02 section 3, tail.go) rolls its sealed
// runs over into this on-disk buffered tree through applyToTree.

import (
	"github.com/tamnd/kv/format"
)

// childSplit is a new (separator, page) pair produced when a node split under a
// flush: the separator is the new page's smallest key, and the page sits
// immediately to the right of the node that split. The parent splices it into its
// pivots. A single flush into one child can produce several, in left-to-right key
// order, when the merged run overflows the child by more than one page.
type childSplit struct {
	sep  []byte
	page format.PageNo
}

// applyToTree merges one sorted, internal-key-deduplicated run of messages into the
// tree from the root and grows the root if it splits. It is the path the hot tail's
// rollover takes into the on-disk tree, and a one-message run is the same path a
// single insert takes, differing only in run length. A leaf root has no buffer, so
// each message merges straight into the leaf through M0's insertOne, which splits and
// grows an interior root exactly as before; once an interior root exists the run
// pushes into its buffer through pushDown, which flushes and may split the root, in
// which case a new root is grown over the pieces. The run must be sorted by internal
// key and free of exact-internal-key duplicates; the tail seal guarantees both.
func (t *Tree) applyToTree(msgs []message) error {
	if len(msgs) == 0 {
		return nil
	}
	root := t.root()
	typ, err := t.pageType(root)
	if err != nil {
		return err
	}
	if typ == format.PageBTreeLeaf {
		// A leaf root has nowhere to buffer, so merge each message into the leaf on the
		// proven M0 path. The first message that overflows it grows an interior root; the
		// rest of this run then descends that root, and the next rollover buffers at it.
		for _, m := range msgs {
			if err := t.insertOne(m.key, m.val); err != nil {
				return err
			}
		}
		return nil
	}
	splits, err := t.pushDown(root, msgs)
	if err != nil {
		return err
	}
	if len(splits) > 0 {
		return t.growRoot(root, splits)
	}
	return nil
}

// pushDown merges a sorted run of messages into the subtree rooted at pgno and
// returns the child splits pgno produced, if any, for the parent to splice in. For a
// leaf it merges the run into the records and repacks. For an interior it merges the
// run into the buffer, flushes the heaviest child while the node overflows the page,
// and repacks. The run must be sorted by internal key and free of internal-key
// duplicates; mergeMessages and the buffer maintenance guarantee that.
func (t *Tree) pushDown(pgno format.PageNo, msgs []message) ([]childSplit, error) {
	typ, err := t.pageType(pgno)
	if err != nil {
		return nil, err
	}

	if typ == format.PageBTreeLeaf {
		lf, err := t.readLeaf(pgno)
		if err != nil {
			return nil, err
		}
		for _, m := range msgs {
			lf.insertRecord(m.key, m.val)
		}
		return t.packLeaf(pgno, lf)
	}

	in, err := t.loadInterior(pgno)
	if err != nil {
		return nil, err
	}
	in.buffer = mergeMessages(in.buffer, msgs)

	// Flush the heaviest child while the node does not fit the page. Each flush
	// removes one child's messages from the buffer, so the buffer strictly shrinks and
	// the loop ends in at most one pass per child with pending messages. A flush can
	// split the flushed child, whose new pivots are spliced into this node, which is
	// why the loop re-derives the heaviest child from the updated node every pass.
	for !t.interiorFits(in) && len(in.buffer) > 0 {
		c := heaviestChild(in)
		run := sliceBufferForChild(in, c)
		if len(run) == 0 {
			break
		}
		splits, err := t.pushDown(in.childPage(c), run)
		if err != nil {
			return nil, err
		}
		// The new pages sit to the right of child c. childPage(c) corresponds to pivot
		// insert position c (slot 0 is leftmost, slot i+1 is pivots[i].child), so the
		// splits splice in at c, c+1, ... in left-to-right order.
		for i, s := range splits {
			in.insertPivotAt(c+i, s.sep, s.page)
		}
	}

	return t.packInterior(pgno, in)
}

// packLeaf writes lf back to pgno, splitting it across new pages when it overflows.
// It returns a childSplit for every page past the first, in key order, so the parent
// can splice them in. The first piece stays on pgno, which keeps the left sibling's
// forward link valid without a second write, and the pieces are chained left to right
// with the last piece's right link inheriting lf's old right sibling.
func (t *Tree) packLeaf(pgno format.PageNo, lf *leaf) ([]childSplit, error) {
	if t.leafFits(lf) {
		return nil, t.writeLeaf(pgno, lf)
	}

	chunks, err := t.splitRecordsToFit(lf.records)
	if err != nil {
		return nil, err
	}

	// Reserve the sibling page numbers first so every forward link can point at a real
	// page before that page is written. The first chunk reuses pgno; chunks 1..n-1 get
	// fresh numbers, materialized below through GetAllocated (the no-read fresh-page
	// write), since their on-disk bytes are dead.
	pages := make([]format.PageNo, len(chunks))
	pages[0] = pgno
	for i := 1; i < len(chunks); i++ {
		pages[i] = t.pgr.AllocateNumber()
	}

	var splits []childSplit
	for i, chunk := range chunks {
		left := lf.left
		if i > 0 {
			left = pages[i-1]
		}
		right := lf.right
		if i < len(chunks)-1 {
			right = pages[i+1]
		}
		piece := &leaf{records: chunk, left: left, right: right, bucketSize: defaultBucketSize}
		if i == 0 {
			if err := t.writeLeaf(pages[i], piece); err != nil {
				return nil, err
			}
		} else {
			if err := t.writeLeafAllocated(pages[i], piece); err != nil {
				return nil, err
			}
			splits = append(splits, childSplit{sep: append([]byte(nil), chunk[0].key...), page: pages[i]})
		}
	}
	return splits, nil
}

// writeLeafAllocated writes lf into a page reserved by AllocateNumber, materializing
// the frame through GetAllocated so the dead prior bytes are not read back. It is the
// fresh-page counterpart to writeLeaf, which rewrites an existing page in place.
func (t *Tree) writeLeafAllocated(pgno format.PageNo, lf *leaf) error {
	dst := make([]byte, t.pgr.UsablePageSize())
	if _, err := encodeLeaf(dst, lf); err != nil {
		return err
	}
	fr, err := t.pgr.GetAllocated(pgno)
	if err != nil {
		return err
	}
	copy(fr.Data(), dst)
	t.pgr.Unpin(fr, true)
	return nil
}

// packInterior writes in back to pgno, splitting its pivots across new pages when it
// overflows. By the time packInterior runs the flush loop in pushDown has drained the
// buffer to the point the node fits, or emptied it entirely, so a split here operates
// on a node whose overflow is pivot count, not buffer bytes. The split therefore
// partitions the remaining buffer (usually empty) by separator and divides the pivots
// like an ordinary B-tree interior split, returning a childSplit per new page.
func (t *Tree) packInterior(pgno format.PageNo, in *interior) ([]childSplit, error) {
	if t.interiorFits(in) {
		return nil, t.writeInterior(pgno, in)
	}

	pieces, seps, err := t.splitInteriorToFit(in)
	if err != nil {
		return nil, err
	}

	// The first piece rewrites the existing page in place; the rest are fresh pages.
	// Interior nodes carry no sibling links, so a fresh piece can be allocated and
	// written in one step (storeInteriorNew) without reserving its number ahead of time.
	var splits []childSplit
	for i, piece := range pieces {
		if i == 0 {
			if err := t.writeInterior(pgno, piece); err != nil {
				return nil, err
			}
			continue
		}
		p, err := t.storeInteriorNew(piece)
		if err != nil {
			return nil, err
		}
		splits = append(splits, childSplit{sep: seps[i-1], page: p})
	}
	return splits, nil
}

// growRoot installs a new interior root over a root page that split. The new root's
// leftmost child is the old root (now the leftmost piece), and the split pages become
// its pivots. The new root can itself overflow when the old root split into many
// pieces, so this packs it and loops, growing another level until a single root page
// holds the pivots.
func (t *Tree) growRoot(oldRoot format.PageNo, splits []childSplit) error {
	for {
		nr := &interior{leftmost: oldRoot}
		for i, s := range splits {
			nr.insertPivotAt(i, s.sep, s.page)
		}
		// A new root has no buffer and almost always fits in one page; store it fresh.
		if t.interiorFits(nr) {
			pgno, err := t.storeInteriorNew(nr)
			if err != nil {
				return err
			}
			t.setRoot(pgno)
			return nil
		}
		// Rare: the old root split into more pieces than a single new root can name.
		// Split the new root's (bufferless) pivots into fresh pages and grow another
		// level, with the leftmost piece carrying up as the next iteration's old root.
		pieces, seps, err := t.splitInteriorToFit(nr)
		if err != nil {
			return err
		}
		first, err := t.storeInteriorNew(pieces[0])
		if err != nil {
			return err
		}
		next := make([]childSplit, 0, len(pieces)-1)
		for i := 1; i < len(pieces); i++ {
			p, err := t.storeInteriorNew(pieces[i])
			if err != nil {
				return err
			}
			next = append(next, childSplit{sep: seps[i-1], page: p})
		}
		oldRoot, splits = first, next
	}
}

// heaviestChild returns the child slot with the most pending message bytes in the
// buffer. Flushing the heaviest child reclaims the most buffer space per flush and
// keeps each flush fat, which is what holds the amortization ratio high (doc 02
// section 2). Slot indices run 0..len(pivots); slot 0 is the leftmost child.
func heaviestChild(in *interior) int {
	weights := make([]int, len(in.pivots)+1)
	for _, m := range in.buffer {
		c := in.childIndex(m.key)
		weights[c] += len(m.key) + len(m.val)
	}
	best, bestW := 0, -1
	for c, w := range weights {
		if w > bestW {
			best, bestW = c, w
		}
	}
	return best
}

// sliceBufferForChild removes and returns the messages routed to child slot c, in
// key order. The buffer stays sorted by internal key and the pivots partition the key
// space, so one child's messages are a contiguous run; this pulls that run out and
// leaves the rest of the buffer intact and still sorted.
func sliceBufferForChild(in *interior, c int) []message {
	kept := in.buffer[:0:0]
	var run []message
	for _, m := range in.buffer {
		if in.childIndex(m.key) == c {
			run = append(run, m)
		} else {
			kept = append(kept, m)
		}
	}
	in.buffer = kept
	return run
}

// mergeMessages merges two sorted message runs into one sorted run, collapsing exact
// internal-key duplicates to the later message. Only exact internal-key matches
// collapse: two versions of the same user key carry different internal keys (the
// inverted version is part of the key), so MVCC versions are all preserved and only a
// replayed identical commit is deduplicated, the same idempotency insertRecord gives
// the leaf.
func mergeMessages(a, b []message) []message {
	if len(b) == 0 {
		return a
	}
	out := make([]message, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		cmp := format.CompareInternal(a[i].key, b[j].key)
		switch {
		case cmp < 0:
			out = append(out, a[i])
			i++
		case cmp > 0:
			out = append(out, b[j])
			j++
		default:
			// Same internal key: the later run (b) wins, the same last-writer rule the
			// leaf record splice uses.
			out = append(out, b[j])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// leafFits reports whether lf encodes within the usable page area.
func (t *Tree) leafFits(lf *leaf) bool {
	_, err := encodeLeaf(make([]byte, t.pgr.UsablePageSize()), lf)
	return err == nil
}

// interiorFits reports whether in encodes within the usable page area.
func (t *Tree) interiorFits(in *interior) bool {
	_, err := encodeInterior(make([]byte, t.pgr.UsablePageSize()), in)
	return err == nil
}

// splitRecordsToFit greedily packs records into chunks that each encode within a
// page, returning the chunks in key order. A single record that cannot fit a page
// alone is the oversized-cell case the write path rejects up front, so it surfaces
// as an error here rather than an endless split.
func (t *Tree) splitRecordsToFit(records []record) ([][]record, error) {
	var chunks [][]record
	start := 0
	for start < len(records) {
		end := start
		for end < len(records) {
			trial := &leaf{records: records[start : end+1], bucketSize: defaultBucketSize}
			if t.leafFits(trial) {
				end++
				continue
			}
			break
		}
		if end == start {
			return nil, ErrPageFull // a single record overflows a page on its own
		}
		chunk := append([]record(nil), records[start:end]...)
		chunks = append(chunks, chunk)
		start = end
	}
	return chunks, nil
}

// splitInteriorToFit divides an overflowing interior into pieces that each encode
// within a page, returning the pieces in key order and the separators that rise
// between them. The pivots are packed greedily; at each break the boundary pivot's
// key rises as a separator and its child becomes the next piece's leftmost, the
// standard interior split with no separator lost. The buffer is partitioned by the
// separators so each message stays with the piece whose key range owns it.
func (t *Tree) splitInteriorToFit(in *interior) ([]*interior, [][]byte, error) {
	var pieces []*interior
	var seps [][]byte

	leftmost := in.leftmost
	start := 0
	for start <= len(in.pivots) {
		// Grow the current piece one pivot at a time until the next pivot would
		// overflow the page, testing the encoded size with the piece's share of the
		// buffer included so a buffer-heavy node still splits to fit.
		end := start
		for end < len(in.pivots) {
			lo := pieceLowKey(in, start)
			// A piece's pivots run [start,end); test fit with one more pivot included,
			// carrying that pivot's share of the buffer so a buffer-heavy node still
			// splits to fit.
			trialPlus := &interior{
				leftmost: leftmost,
				pivots:   in.pivots[start : end+1],
				buffer:   bufferInRange(in.buffer, lo, in.pivots[end].key),
			}
			if t.interiorFits(trialPlus) {
				end++
				continue
			}
			break
		}

		if end == len(in.pivots) {
			// The tail piece takes every remaining pivot.
			lo := pieceLowKey(in, start)
			piece := &interior{
				leftmost: leftmost,
				pivots:   append([]pivot(nil), in.pivots[start:]...),
				buffer:   bufferFrom(in.buffer, lo),
			}
			pieces = append(pieces, piece)
			break
		}

		if end == start {
			return nil, nil, ErrPageFull // a single pivot plus its child overflows a page
		}

		lo := pieceLowKey(in, start)
		hiSep := in.pivots[end].key
		piece := &interior{
			leftmost: leftmost,
			pivots:   append([]pivot(nil), in.pivots[start:end]...),
			buffer:   bufferInRange(in.buffer, lo, hiSep),
		}
		pieces = append(pieces, piece)
		seps = append(seps, append([]byte(nil), hiSep...))
		// The boundary pivot's key rose as the separator; its child becomes the next
		// piece's leftmost, so the pivot itself is consumed by the boundary.
		leftmost = in.pivots[end].child
		start = end + 1
	}

	return pieces, seps, nil
}

// pieceLowKey returns the inclusive low bound of the piece that starts at pivot
// index start: nil (no lower bound) for the first piece, else the separator that
// opened it. It bounds which buffer messages belong to the piece on the low side.
func pieceLowKey(in *interior, start int) []byte {
	if start == 0 {
		return nil
	}
	return in.pivots[start-1].key
}

// bufferInRange returns the buffer messages whose internal key falls in [lo, hi):
// lo nil means no lower bound, hi nil means no upper bound. The buffer is sorted, so
// this is the contiguous slice the bounds cut out, copied so the pieces own their
// messages.
func bufferInRange(buf []message, lo, hi []byte) []message {
	var out []message
	for _, m := range buf {
		if lo != nil && format.CompareInternal(m.key, lo) < 0 {
			continue
		}
		if hi != nil && format.CompareInternal(m.key, hi) >= 0 {
			continue
		}
		out = append(out, m)
	}
	return out
}

// bufferFrom returns the buffer messages whose internal key is >= lo (lo nil means
// all of them), the tail-piece counterpart to bufferInRange.
func bufferFrom(buf []message, lo []byte) []message {
	return bufferInRange(buf, lo, nil)
}
