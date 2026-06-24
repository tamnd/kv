package btree

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// nodeHeaderSize is the fixed node header: the 8-byte common header (spec 02 §3.1)
// plus a 4-byte pointer. For a leaf the pointer is the right-sibling (the B-link,
// spec 05 §2); for an interior node it is the rightmost child.
const nodeHeaderSize = format.CommonHeaderSize + 4

// leafHeaderSize is the leaf-only header: the shared node header plus a 2-byte
// content-start pointer (the lowest byte offset a live cell body occupies). The
// slot array begins right after it. An interior node has no content-start field, so
// its header stays nodeHeaderSize.
const leafHeaderSize = nodeHeaderSize + 2

// A leaf page is slotted (spec 05 §2): a cell-pointer (slot) array grows down from
// the header in ascending key order, and cell bodies grow up from the end of the
// usable region in arbitrary physical order, with free space in the middle. Inserting
// a cell appends its body into the free space, splices one 2-byte slot into the array,
// and bumps the count, touching only the bytes that change instead of re-encoding every
// existing cell. The layout is:
//
//	[0:8]            common header (type, cell count)
//	[8:12]           next sibling page number (the B-link)
//	[12:14]          content start: offset of the lowest live cell body
//	[14 : 14+2n]     slot array, n big-endian uint16 body offsets, ascending by key
//	  ... free ...
//	[contentStart : usable]   cell bodies, each (uvarint klen, key, uvarint vlen, val)
//
// An overwrite or delete leaves its old body as dead space the slot no longer names;
// a later insert that does not fit the contiguous gap compacts the page (repacking the
// live bodies) before splitting, so dead space is reclaimed without a structural change.

// leaf is the decoded form of a B-tree leaf page: the slotted bytes read back into
// parallel key/value slices. The read path and the split/gc/buffer paths work on this
// decoded form; the hot insert path edits the slotted bytes in place and never builds
// it. Leaves hold full internal keys (user_key || ^version || kind) so a user key's
// version group sorts together, newest first (the version is stored inverted).
type leaf struct {
	keys [][]byte // internal keys, ascending by format.CompareInternal
	vals [][]byte // inline values, parallel to keys (overflow is deferred)
	next format.PageNo

	// cleanState caches the leaf-intrinsic "clean Sets" predicate (every cell a plain Set with
	// strictly ascending distinct user keys, so every version group has size one), computed once
	// and reused. The scan fast path and the GC fast path both ask this per leaf, and on a short
	// scan op recomputing it walked the whole leaf with a key compare per cell, which a CPU profile
	// showed costing as much as the read itself. A decoded leaf is immutable and shared across
	// readers (viewLeaf hands back one cached decode), so the value is computed lazily and stored
	// atomically the same way interior.childRefs caches its swizzles: 0 unknown, 1 clean, 2 dirty.
	// The result is deterministic, so a concurrent double-compute is harmless and the store is
	// idempotent.
	cleanState atomic.Int32
}

const (
	leafCleanUnknown int32 = 0
	leafCleanClean   int32 = 1
	leafCleanDirty   int32 = 2
)

// intrinsicCleanSets reports the leaf-only part of the clean-Sets predicate: every cell a plain
// Set and the user keys strictly ascending and distinct, so each version group has exactly one
// member. It excludes the range-delete check, which is not a property of the leaf and which callers
// apply separately. The answer is cached on first computation since the leaf is immutable.
func (l *leaf) intrinsicCleanSets() bool {
	switch l.cleanState.Load() {
	case leafCleanClean:
		return true
	case leafCleanDirty:
		return false
	}
	clean := true
	var prev []byte
	for i := range l.keys {
		ik := l.keys[i]
		if format.KindOf(ik) != format.KindSet {
			clean = false
			break
		}
		uk := format.UserKey(ik)
		if i > 0 && format.CompareUser(prev, uk) >= 0 {
			clean = false
			break
		}
		prev = uk
	}
	if clean {
		l.cleanState.Store(leafCleanClean)
	} else {
		l.cleanState.Store(leafCleanDirty)
	}
	return clean
}

// interior is the decoded form of a B-tree interior page. It holds n separators and
// n+1 children: child[i] covers keys in [sep[i-1], sep[i]) with sep[-1] = -inf and
// sep[n] = +inf, so child[n] (the rightmost) covers [sep[n-1], +inf). Separators are
// USER keys; routing never needs the version (spec 05 §2.1).
//
// In Bε mode (spec 05 §4) an interior node also carries a message buffer: pending
// inserts and deletes parked here instead of descending to their leaf, flushed one
// level down in a batch when the buffer fills. Messages are full internal-key cells,
// exactly like leaf cells, so MVCC and the engine seam are unchanged. The buffer is
// empty in the default (unbuffered) mode, and an interior page written with no
// messages encodes identically to before this slice, so the default path is untouched.
type interior struct {
	seps     [][]byte        // n separator user keys, ascending
	children []format.PageNo // n+1 child pages
	msgKeys  [][]byte        // Bε buffered message internal keys, ascending by CompareInternal
	msgVals  [][]byte        // buffered message values, parallel to msgKeys
	// childRefs is the swizzle cache: childRefs[i] holds the decoded box of child i once a
	// descent has resolved it, so the next descent reaches the child by following the pointer
	// instead of going back through the pager's shard lock and resident-page map (perf/12 F2).
	// It is parallel to children and populated lazily by the read descent. A slot is valid only
	// while its box reports Live(); a write, eviction, or rebind of the child marks the box dead
	// and the descent falls back to the pager and re-swizzles. The slots are atomic because
	// concurrent readers of this shared immutable node may each store a freshly resolved box
	// (always the same box for a given resident child, so the store is idempotent). It is only
	// set on interiors decoded by unmarshalInterior, the only nodes the read descent traverses.
	childRefs []atomic.Pointer[pager.DecodedNode]
}

// cellBodySize reports the encoded length of one leaf cell body: the key length
// varint, the key, the value length varint, and the value.
func cellBodySize(key, val []byte) int {
	return format.UvarintLen(uint64(len(key))) + len(key) +
		format.UvarintLen(uint64(len(val))) + len(val)
}

// leafEncodedSize reports the slotted bytes l would occupy: the header, one slot per
// cell, and every cell body. It replaces the old len(marshalLeaf(l)) the fit and
// overflow checks used, since a marshaled slotted page is always usable-sized (the
// free gap is part of the image) and so its length no longer signals overflow.
func leafEncodedSize(l *leaf) int {
	sz := leafHeaderSize
	for i := range l.keys {
		sz += 2 + cellBodySize(l.keys[i], l.vals[i])
	}
	return sz
}

// appendCellBody writes one cell body (klen, key, vlen, val) at out and returns the
// extended slice. It is the single encoder both the in-place insert and the full
// marshal share, so the on-disk cell shape is defined in one place.
func appendCellBody(out []byte, key, val []byte) []byte {
	out = format.AppendUvarint(out, uint64(len(key)))
	out = append(out, key...)
	out = format.AppendUvarint(out, uint64(len(val)))
	out = append(out, val...)
	return out
}

// marshalLeaf encodes l into a full usable-sized slotted page image: bodies packed
// against the end, the slot array in key order at the front, free space between. The
// caller must have checked leafEncodedSize(l) <= usable; a leaf that does not fit must
// split instead. A fresh empty leaf encodes with content start at usable and no slots.
func marshalLeaf(l *leaf, usable int) []byte {
	page := make([]byte, usable)
	format.CommonHeader{Type: format.PageBTreeLeaf, CellCount: uint16(len(l.keys))}.Encode(page)
	binary.BigEndian.PutUint32(page[format.CommonHeaderSize:nodeHeaderSize], l.next)
	content := usable
	for i := range l.keys {
		body := appendCellBody(nil, l.keys[i], l.vals[i])
		content -= len(body)
		copy(page[content:], body)
		binary.BigEndian.PutUint16(page[leafHeaderSize+2*i:], uint16(content))
	}
	binary.BigEndian.PutUint16(page[nodeHeaderSize:leafHeaderSize], uint16(content))
	return page
}

// nodeReader is a bounds-checked cursor over a node page. Every read advances the offset and reports
// an error rather than panicking when the page is shorter than the structure it claims to hold, so a
// corrupt or type-confused page that reaches a decoder is rejected with format.ErrCorrupt instead of
// crashing the process (spec 23 §5: a malformed input must never panic or over-read). The pager's
// checksum is the first guard against corruption; this is the defense in depth for the case a
// checksum-valid page is still structurally wrong, which type confusion (a leaf reached as an
// interior) produces without ever flipping a byte.
type nodeReader struct {
	p   []byte
	off int
}

func (r *nodeReader) uint32() (uint32, error) {
	if r.off < 0 || r.off+4 > len(r.p) {
		return 0, format.ErrCorrupt
	}
	v := binary.BigEndian.Uint32(r.p[r.off:])
	r.off += 4
	return v, nil
}

func (r *nodeReader) uvarint() (uint64, error) {
	if r.off < 0 || r.off > len(r.p) {
		return 0, format.ErrCorrupt
	}
	v, n := format.Uvarint(r.p[r.off:])
	if n <= 0 {
		return 0, format.ErrCorrupt
	}
	r.off += n
	return v, nil
}

// bytes copies the next n bytes. It checks n against the whole page length before the addition, so a
// length large enough to overflow int when added to the offset cannot slip past the bound.
func (r *nodeReader) bytes(n uint64) ([]byte, error) {
	if r.off < 0 || n > uint64(len(r.p)) || r.off+int(n) > len(r.p) {
		return nil, format.ErrCorrupt
	}
	b := append([]byte(nil), r.p[r.off:r.off+int(n)]...)
	r.off += int(n)
	return b, nil
}

// unmarshalLeaf decodes a slotted leaf page into parallel key/value slices, walking the
// slot array in key order and following each slot to its cell body. It returns
// format.ErrCorrupt when a slot offset or a body length runs off the page rather than
// panicking on an out-of-range read (spec 23 §5).
func unmarshalLeaf(p []byte) (*leaf, error) {
	if len(p) < leafHeaderSize {
		return nil, format.ErrCorrupt
	}
	h := format.DecodeCommonHeader(p)
	l := &leaf{next: binary.BigEndian.Uint32(p[format.CommonHeaderSize:nodeHeaderSize])}
	n := int(h.CellCount)
	if leafHeaderSize+2*n > len(p) {
		return nil, format.ErrCorrupt
	}
	for i := 0; i < n; i++ {
		off := int(binary.BigEndian.Uint16(p[leafHeaderSize+2*i:]))
		if off < leafHeaderSize+2*n || off > len(p) {
			return nil, format.ErrCorrupt
		}
		r := &nodeReader{p: p, off: off}
		klen, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		key, err := r.bytes(klen)
		if err != nil {
			return nil, err
		}
		vlen, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		val, err := r.bytes(vlen)
		if err != nil {
			return nil, err
		}
		l.keys = append(l.keys, key)
		l.vals = append(l.vals, val)
	}
	return l, nil
}

// leafSlotKey returns the internal key of the cell named by slot i as a subslice of the
// page (no copy), for the in-place search to compare against without decoding the whole
// leaf. It reports false if the slot offset or key length runs off the page.
func leafSlotKey(p []byte, n, i int) ([]byte, bool) {
	if leafHeaderSize+2*(i+1) > len(p) {
		return nil, false
	}
	off := int(binary.BigEndian.Uint16(p[leafHeaderSize+2*i:]))
	if off < leafHeaderSize+2*n || off >= len(p) {
		return nil, false
	}
	klen, m := format.Uvarint(p[off:])
	if m <= 0 {
		return nil, false
	}
	start := off + m
	end := start + int(klen)
	if end > len(p) || end < start {
		return nil, false
	}
	return p[start:end], true
}

// leafSlotSearch returns the slot index of the first cell whose internal key is >= ik
// (the insert position), and whether that cell's key equals ik. It binary-searches the
// slot array, reading each candidate's key in place. A malformed slot reports !ok so the
// caller falls back to the decode path rather than trusting a bad page.
func leafSlotSearch(p []byte, n int, ik []byte) (idx int, found, ok bool) {
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		key, kok := leafSlotKey(p, n, mid)
		if !kok {
			return 0, false, false
		}
		if format.CompareInternal(key, ik) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < n {
		key, kok := leafSlotKey(p, n, lo)
		if !kok {
			return 0, false, false
		}
		if format.CompareInternal(key, ik) == 0 {
			return lo, true, true
		}
	}
	return lo, false, true
}

// interiorChildInPlace finds the child subtree that covers userKey by walking the interior
// page's separator cells directly, without decoding the node into an interior struct. It is the
// interior analog of leafSlotSearch: the hot insert descent routes through it so each interior
// level allocates nothing, instead of the loadInterior path that builds an interior with a fresh
// copy of every separator just to binary-search them (perf/11 W2). The cells are variable-length
// and packed with no offset array, so this is a forward scan rather than a binary search; an
// interior holds at most a few hundred small separators, and a scan that copies nothing beats a
// binary search that allocates every separator. It returns the same child loadInterior's
// childFor would: the child of the first separator strictly greater than userKey, or the
// rightmost child when userKey is at least every separator. ok=false signals a malformed page so
// the caller falls back to the decode path, the same contract leafInsertInPlace uses. The Bε
// message buffer that may follow the pivot cells does not affect routing and is never read here.
func interiorChildInPlace(p []byte, userKey []byte) (child format.PageNo, ok bool) {
	if len(p) < nodeHeaderSize {
		return 0, false
	}
	n := int(format.DecodeCommonHeader(p).CellCount)
	off := nodeHeaderSize
	for i := 0; i < n; i++ {
		// cell: [4-byte child][uvarint separator length][separator bytes]
		if off+4 > len(p) {
			return 0, false
		}
		c := binary.BigEndian.Uint32(p[off:])
		off += 4
		slen, m := format.Uvarint(p[off:])
		if m <= 0 {
			return 0, false
		}
		off += m
		end := off + int(slen)
		if end < off || end > len(p) {
			return 0, false
		}
		sep := p[off:end]
		off = end
		if format.CompareUser(userKey, sep) < 0 {
			return c, true
		}
	}
	// userKey is at least every separator: route to the rightmost child in the header pointer.
	return binary.BigEndian.Uint32(p[format.CommonHeaderSize:nodeHeaderSize]), true
}

// leafInsertInPlace inserts (ik, value) into the slotted leaf page directly, the hot
// path's whole point: instead of decoding the leaf, inserting into a slice, and
// re-encoding every cell, it appends one cell body into the free gap and splices one
// slot into the array, touching only the bytes that change. An existing identical
// internal key is overwritten in place (its old body becomes dead space), which keeps
// WAL redo idempotent.
//
// It returns done=true when the edit landed. It returns done=false, with the page
// untouched, when the cell does not fit the contiguous free gap; the caller then decodes
// the page and either compacts-and-reinserts (dead space reclaimed) or splits (genuinely
// full), the rare slow path. ok=false signals a malformed page so the caller falls back
// to the decode path. The caller holds the page under a write pin.
func leafInsertInPlace(p []byte, usable int, ik, value []byte) (done, ok bool) {
	if len(p) < leafHeaderSize || usable > len(p) {
		return false, false
	}
	n := int(format.DecodeCommonHeader(p).CellCount)
	if leafHeaderSize+2*n > usable {
		return false, false
	}
	idx, found, sok := leafSlotSearch(p, n, ik)
	if !sok {
		return false, false
	}
	content := int(binary.BigEndian.Uint16(p[nodeHeaderSize:leafHeaderSize]))
	if content < leafHeaderSize+2*n || content > usable {
		return false, false
	}
	bodyLen := cellBodySize(ik, value)

	if found {
		// Overwrite: write a new body and repoint the slot. The old body stays as dead
		// space a later compaction reclaims. Needs room for the body alone, no new slot.
		gap := content - (leafHeaderSize + 2*n)
		if gap < bodyLen {
			return false, true
		}
		newOff := content - bodyLen
		copyCellBody(p, newOff, ik, value)
		binary.BigEndian.PutUint16(p[leafHeaderSize+2*idx:], uint16(newOff))
		binary.BigEndian.PutUint16(p[nodeHeaderSize:leafHeaderSize], uint16(newOff))
		return true, true
	}

	// Insert: needs room for the body plus one new slot, which the slot array's growth by
	// two bytes also consumes from the gap.
	gap := content - (leafHeaderSize + 2*n)
	if gap < bodyLen+2 {
		return false, true
	}
	newOff := content - bodyLen
	copyCellBody(p, newOff, ik, value)
	// Splice the slot at idx: shift the slots from idx..n right by one (two bytes).
	slotBase := leafHeaderSize
	copy(p[slotBase+2*(idx+1):slotBase+2*(n+1)], p[slotBase+2*idx:slotBase+2*n])
	binary.BigEndian.PutUint16(p[slotBase+2*idx:], uint16(newOff))
	binary.BigEndian.PutUint16(p[nodeHeaderSize:leafHeaderSize], uint16(newOff))
	h := format.DecodeCommonHeader(p)
	h.CellCount = uint16(n + 1)
	h.Encode(p)
	return true, true
}

// copyCellBody writes one cell body at offset off in page p.
func copyCellBody(p []byte, off int, key, val []byte) {
	o := off
	o += format.PutUvarint(p[o:], uint64(len(key)))
	o += copy(p[o:], key)
	o += format.PutUvarint(p[o:], uint64(len(val)))
	copy(p[o:], val)
}

// marshalInterior encodes an interior node: header (with rightmost child in the
// pointer slot), then per non-rightmost child a cell of (4-byte child, varint
// separator length, separator bytes). In Bε mode a non-empty message buffer is
// appended after the pivot cells as a varint message count followed by that many
// (varint klen, key, varint vlen, value) cells. When the buffer is empty nothing is
// appended, so an unbuffered interior encodes byte for byte as it did before this
// slice and unmarshalInterior reads the absent count back as zero from the page's
// trailing zero padding.
func marshalInterior(in *interior) []byte {
	out := make([]byte, nodeHeaderSize)
	format.CommonHeader{Type: format.PageBTreeInterior, CellCount: uint16(len(in.seps))}.Encode(out)
	binary.BigEndian.PutUint32(out[format.CommonHeaderSize:nodeHeaderSize], in.children[len(in.children)-1])
	var ptr [4]byte
	for i := range in.seps {
		binary.BigEndian.PutUint32(ptr[:], in.children[i])
		out = append(out, ptr[:]...)
		out = format.AppendUvarint(out, uint64(len(in.seps[i])))
		out = append(out, in.seps[i]...)
	}
	if len(in.msgKeys) > 0 {
		out = format.AppendUvarint(out, uint64(len(in.msgKeys)))
		for i := range in.msgKeys {
			out = format.AppendUvarint(out, uint64(len(in.msgKeys[i])))
			out = append(out, in.msgKeys[i]...)
			out = format.AppendUvarint(out, uint64(len(in.msgVals[i])))
			out = append(out, in.msgVals[i]...)
		}
	}
	return out
}

// unmarshalInterior decodes an interior page, returning format.ErrCorrupt when the bytes do not
// describe a well-formed interior node rather than panicking on an out-of-range read. The garbage
// cell count a type-confused page presents is exactly what made the old unchecked loop over-read; the
// bounds-checked reader turns that into a clean rejection.
func unmarshalInterior(p []byte) (*interior, error) {
	if len(p) < nodeHeaderSize {
		return nil, format.ErrCorrupt
	}
	h := format.DecodeCommonHeader(p)
	in := &interior{}
	r := &nodeReader{p: p, off: nodeHeaderSize}
	for i := 0; i < int(h.CellCount); i++ {
		child, err := r.uint32()
		if err != nil {
			return nil, err
		}
		slen, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		sep, err := r.bytes(slen)
		if err != nil {
			return nil, err
		}
		in.children = append(in.children, child)
		in.seps = append(in.seps, sep)
	}
	in.children = append(in.children, binary.BigEndian.Uint32(p[format.CommonHeaderSize:nodeHeaderSize]))
	// The Bε message buffer follows the pivot cells. An unbuffered interior wrote no buffer section, so
	// the count reads back as zero from the page's zero padding.
	if r.off < len(p) {
		mcount, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		for i := 0; i < int(mcount); i++ {
			klen, err := r.uvarint()
			if err != nil {
				return nil, err
			}
			key, err := r.bytes(klen)
			if err != nil {
				return nil, err
			}
			vlen, err := r.uvarint()
			if err != nil {
				return nil, err
			}
			val, err := r.bytes(vlen)
			if err != nil {
				return nil, err
			}
			in.msgKeys = append(in.msgKeys, key)
			in.msgVals = append(in.msgVals, val)
		}
	}
	// Size the swizzle cache to the child count once, here at decode, so the read descent only
	// ever loads and stores slots and never grows the slice. Slots start nil (no child resolved
	// yet) and fill in as descents pass through (perf/12 F2).
	in.childRefs = make([]atomic.Pointer[pager.DecodedNode], len(in.children))
	return in, nil
}

// bufferInsert parks (internalKey, value) in the interior's message buffer in sorted
// internal-key order, overwriting an identical internal key so a redo of the same
// committed batch re-injects the message as a no-op, the same idempotence leaf.insert
// gives the leaf (spec 08 §3). The buffer stays ascending by CompareInternal so a
// flush can partition it by child with a single forward pass and a read can fold it
// with the leaf group without re-sorting.
func (in *interior) bufferInsert(internalKey, value []byte) {
	idx := in.bufferSearch(internalKey)
	if idx < len(in.msgKeys) && format.CompareInternal(in.msgKeys[idx], internalKey) == 0 {
		in.msgVals[idx] = append([]byte(nil), value...)
		return
	}
	in.msgKeys = append(in.msgKeys, nil)
	copy(in.msgKeys[idx+1:], in.msgKeys[idx:])
	in.msgKeys[idx] = append([]byte(nil), internalKey...)

	in.msgVals = append(in.msgVals, nil)
	copy(in.msgVals[idx+1:], in.msgVals[idx:])
	in.msgVals[idx] = append([]byte(nil), value...)
}

// bufferSearch returns the index of the first buffered message key >= internalKey.
func (in *interior) bufferSearch(internalKey []byte) int {
	lo, hi := 0, len(in.msgKeys)
	for lo < hi {
		mid := (lo + hi) / 2
		if format.CompareInternal(in.msgKeys[mid], internalKey) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// childFor returns the index of the child subtree that covers userKey: the first
// separator strictly greater than userKey, or the rightmost child if userKey is
// >= every separator.
func (in *interior) childFor(userKey []byte) int {
	lo, hi := 0, len(in.seps)
	for lo < hi {
		mid := (lo + hi) / 2
		if format.CompareUser(userKey, in.seps[mid]) < 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

// insertChild splits the child at index p into (oldChild stays, newChild) with the
// given separator: keys < sep stay in oldChild (index p), keys >= sep go to
// newChild (inserted at p+1). seps and children grow by one each.
func (in *interior) insertChild(p int, sep []byte, newChild format.PageNo) {
	in.seps = append(in.seps, nil)
	copy(in.seps[p+1:], in.seps[p:])
	in.seps[p] = append([]byte(nil), sep...)

	in.children = append(in.children, 0)
	copy(in.children[p+2:], in.children[p+1:])
	in.children[p+1] = newChild
}

// leafInsert inserts (internalKey, value) into l in sorted position. If an entry
// with the identical internal key already exists it is overwritten, which is what
// makes WAL redo idempotent: replaying a committed batch re-inserts the same
// versioned key as a no-op (spec 08 §3).
func (l *leaf) insert(internalKey, value []byte) {
	idx := l.search(internalKey)
	if idx < len(l.keys) && format.CompareInternal(l.keys[idx], internalKey) == 0 {
		l.vals[idx] = append([]byte(nil), value...)
		return
	}
	l.keys = append(l.keys, nil)
	copy(l.keys[idx+1:], l.keys[idx:])
	l.keys[idx] = append([]byte(nil), internalKey...)

	l.vals = append(l.vals, nil)
	copy(l.vals[idx+1:], l.vals[idx:])
	l.vals[idx] = append([]byte(nil), value...)
}

// search returns the index of the first key >= internalKey (lower bound).
func (l *leaf) search(internalKey []byte) int {
	lo, hi := 0, len(l.keys)
	for lo < hi {
		mid := (lo + hi) / 2
		if format.CompareInternal(l.keys[mid], internalKey) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// splitPoint chooses where to divide an oversized leaf: the midpoint, advanced
// forward to the next user-key boundary so a single user key's version group is
// never cut across two leaves (spec 05 §3.2 keeps groups intact). It returns an
// index in [1, len) or 0 if no clean boundary exists (a single giant version
// group, which M1 leaves oversized rather than corrupting the group).
func (l *leaf) splitPoint() int {
	mid := len(l.keys) / 2
	for i := mid; i < len(l.keys); i++ {
		if format.CompareUser(format.UserKey(l.keys[i]), format.UserKey(l.keys[i-1])) != 0 {
			return i
		}
	}
	for i := mid; i > 0; i-- {
		if format.CompareUser(format.UserKey(l.keys[i]), format.UserKey(l.keys[i-1])) != 0 {
			return i
		}
	}
	return 0
}

// splitPointBiased chooses where to divide an oversized leaf given where the cell that
// overflowed it was inserted. A balanced midpoint split is the right call for a random
// insert, but it is the wrong call for a sequential one: under monotonically increasing
// keys every overflow inserts at the right end, and a midpoint split leaves the left leaf
// half full forever, so an append-only load wastes about half its leaf space (spec 05
// F3a, the classic B+tree sequential-insert problem). When the overflowing cell is the
// rightmost and forms its own user-key group, this keeps it alone on the right so the
// left leaf seals nearly full; the symmetric prepend case keeps it alone on the left.
// Both biased cuts are group-clean by construction (the lone cell is its own group), and
// the leaf they leave behind is a subset of the pre-insert leaf, which fit, so neither
// can overflow. Any other insert falls back to the balanced split, so a non-sequential
// workload is unchanged. insertedAt is the index the new cell landed at in l.keys.
func (l *leaf) splitPointBiased(insertedAt int) int {
	n := len(l.keys)
	// Append: the new cell is the rightmost and starts a new group (its user key differs
	// from the cell before it). Seal everything before it into a full left leaf.
	if insertedAt == n-1 && n >= 2 &&
		format.CompareUser(format.UserKey(l.keys[n-1]), format.UserKey(l.keys[n-2])) != 0 {
		return n - 1
	}
	// Prepend: the new cell is the leftmost and starts a new group. Seal everything after
	// it into a full right leaf, leaving the new cell alone on the left.
	if insertedAt == 0 && n >= 2 &&
		format.CompareUser(format.UserKey(l.keys[1]), format.UserKey(l.keys[0])) != 0 {
		return 1
	}
	return l.splitPoint()
}
