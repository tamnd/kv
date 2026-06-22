package btree

import (
	"encoding/binary"

	"github.com/tamnd/kv/format"
)

// nodeHeaderSize is the fixed node header: the 8-byte common header (spec 02 §3.1)
// plus a 4-byte pointer. For a leaf the pointer is the right-sibling (the B-link,
// spec 05 §2); for an interior node it is the rightmost child.
const nodeHeaderSize = format.CommonHeaderSize + 4

// leaf is the decoded form of a B-tree leaf page. M1 decodes a whole page into
// this struct, mutates it, and re-encodes -- correctness over the in-place slotted
// edit the spec's layout (spec 05 §2) ultimately wants. Leaves hold full internal
// keys (user_key || ^version || kind) so a user key's version group sorts together,
// newest first (the version is stored inverted).
type leaf struct {
	keys [][]byte // internal keys, ascending by format.CompareInternal
	vals [][]byte // inline values, parallel to keys (overflow is deferred)
	next format.PageNo
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
}

// marshalLeaf encodes l to its on-disk bytes (header + cells). The caller checks
// len(out) <= pageSize before committing it to a page; an over-long result means
// the leaf must split.
func marshalLeaf(l *leaf) []byte {
	out := make([]byte, nodeHeaderSize)
	format.CommonHeader{Type: format.PageBTreeLeaf, CellCount: uint16(len(l.keys))}.Encode(out)
	binary.BigEndian.PutUint32(out[format.CommonHeaderSize:nodeHeaderSize], l.next)
	for i := range l.keys {
		out = format.AppendUvarint(out, uint64(len(l.keys[i])))
		out = append(out, l.keys[i]...)
		out = format.AppendUvarint(out, uint64(len(l.vals[i])))
		out = append(out, l.vals[i]...)
	}
	return out
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

// unmarshalLeaf decodes a leaf page, returning format.ErrCorrupt when the bytes do not describe a
// well-formed leaf rather than panicking on an out-of-range read.
func unmarshalLeaf(p []byte) (*leaf, error) {
	if len(p) < nodeHeaderSize {
		return nil, format.ErrCorrupt
	}
	h := format.DecodeCommonHeader(p)
	l := &leaf{next: binary.BigEndian.Uint32(p[format.CommonHeaderSize:nodeHeaderSize])}
	r := &nodeReader{p: p, off: nodeHeaderSize}
	for i := 0; i < int(h.CellCount); i++ {
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
