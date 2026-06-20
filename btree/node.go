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
type interior struct {
	seps     [][]byte        // n separator user keys, ascending
	children []format.PageNo // n+1 child pages
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

// unmarshalLeaf decodes a leaf page.
func unmarshalLeaf(p []byte) *leaf {
	h := format.DecodeCommonHeader(p)
	l := &leaf{next: binary.BigEndian.Uint32(p[format.CommonHeaderSize:nodeHeaderSize])}
	off := nodeHeaderSize
	for i := 0; i < int(h.CellCount); i++ {
		klen, n := format.Uvarint(p[off:])
		off += n
		key := append([]byte(nil), p[off:off+int(klen)]...)
		off += int(klen)
		vlen, n := format.Uvarint(p[off:])
		off += n
		val := append([]byte(nil), p[off:off+int(vlen)]...)
		off += int(vlen)
		l.keys = append(l.keys, key)
		l.vals = append(l.vals, val)
	}
	return l
}

// marshalInterior encodes an interior node: header (with rightmost child in the
// pointer slot), then per non-rightmost child a cell of (4-byte child, varint
// separator length, separator bytes).
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
	return out
}

// unmarshalInterior decodes an interior page.
func unmarshalInterior(p []byte) *interior {
	h := format.DecodeCommonHeader(p)
	in := &interior{}
	off := nodeHeaderSize
	for i := 0; i < int(h.CellCount); i++ {
		child := binary.BigEndian.Uint32(p[off:])
		off += 4
		slen, n := format.Uvarint(p[off:])
		off += n
		sep := append([]byte(nil), p[off:off+int(slen)]...)
		off += int(slen)
		in.children = append(in.children, child)
		in.seps = append(in.seps, sep)
	}
	in.children = append(in.children, binary.BigEndian.Uint32(p[format.CommonHeaderSize:nodeHeaderSize]))
	return in
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
