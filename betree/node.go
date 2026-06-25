package betree

import (
	"encoding/binary"
	"errors"

	"github.com/tamnd/kv/format"
)

// This file is the generation-2 on-disk node codec for the Bε-tree core: the
// front-coded bucketed leaf (tag 0x03) and the pivot-plus-buffer interior node
// (tag 0x02) that redesign doc 06 section 2 specifies. It is the physical layout
// the later M0 PRs lay over the pager; here it is a self-contained encoder and
// decoder over a usable-page byte slice, so it can be exercised exhaustively by
// round-trip and fuzz tests before any of it is on the live read or write path.
// Keeping it decoupled from the in-memory skeleton (betree.go) is deliberate: the
// skeleton stays correct and green while the real format is built and proven
// underneath it, which is the alongside-then-flip discipline applied within M0.
//
// Scope here matches M0: inline values only, no overflow or value-log pointer
// (doc 06 section 4, off by default), and the interior buffer stores its messages
// sequentially rather than behind the sorted cell-pointer array a flush's binary
// search will later want. Those are additive format features the generation-2
// container reserves room for; none of them changes the envelope this file lays
// down. The checksum trailer is the pager's concern: this codec works inside the
// usable area the pager hands it (page size minus the reserved trailer), exactly as
// doc 06 section 2 requires, so a region can never grow into the trailer.

var (
	// ErrPageFull means the encoded node does not fit in the usable page area. The
	// caller's response is a split (interior or leaf), the standard B-tree overflow
	// path; the codec only reports the condition.
	ErrPageFull = errors.New("betree: node does not fit in page")
	// ErrCorruptNode means a decode hit bytes that cannot be a valid generation-2
	// node: a wrong tag, a truncated field, a front-coded record that shares more
	// bytes than its predecessor holds, or a restart offset out of range. Every
	// decode path fails closed with this rather than reading past the slice, which
	// is the property the migration-reader fuzz (M0) depends on.
	ErrCorruptNode = errors.New("betree: corrupt node")
)

// defaultBucketSize is the front-coding restart interval: a leaf bucket holds this
// many records, the first stored with its full key and the rest front-coded against
// the record before them (doc 06 section 2). Sixteen is the LevelDB/RocksDB data
// block default and the balance doc 06 names: a 16-way binary search over restarts
// then at most a 16-record scan, so front coding buys cache density at a bounded
// scan cost.
const defaultBucketSize = 16

// record is one leaf key/value pair. key is the full internal key
// (user_key || ^version || kind), so versions of a user key sort adjacently and
// front coding compresses the shared user-key prefix of consecutive versions.
type record struct {
	key []byte
	val []byte
}

// leaf is the decoded form of a leaf node: its records in ascending internal-key
// order, its sibling links for the scan cursor, and its bucket size.
type leaf struct {
	records    []record
	left       format.PageNo // left-sibling leaf, 0 if leftmost
	right      format.PageNo // right-sibling leaf, 0 if rightmost
	bucketSize int
}

// leaf header layout, after the 8-byte common header (doc 06 section 2):
//
//	+8  bucket count        (2)
//	+10 bucket size         (2)
//	+12 right-sibling page  (4)
//	+16 left-sibling page   (4)
//	+20 bucket restart array, then the record region
const leafHeaderEnd = 20

// lcp returns the number of leading bytes a and b share.
func lcp(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// encodeLeaf writes lf into dst, the usable page area, and returns the number of
// bytes used. It returns ErrPageFull if the front-coded records plus the restart
// array do not fit. Records must already be in ascending internal-key order; the
// caller (the tree's leaf builder) guarantees that, and the codec does not re-sort.
func encodeLeaf(dst []byte, lf *leaf) (int, error) {
	bucketSize := lf.bucketSize
	if bucketSize <= 0 {
		bucketSize = defaultBucketSize
	}
	n := len(lf.records)
	if n > 0xFFFF {
		return 0, ErrPageFull // more records than the 16-bit cell count can name
	}
	bucketCount := 0
	if n > 0 {
		bucketCount = (n + bucketSize - 1) / bucketSize
	}

	// Build the record region and the restart offsets together. A bucket boundary
	// (every bucketSize records) resets front coding: the first record in a bucket
	// stores its full key so the bucket is independently seekable.
	restartArrayBytes := bucketCount * 2
	regionStart := leafHeaderEnd + restartArrayBytes
	restarts := make([]int, bucketCount)
	var rec []byte
	var prev []byte
	for i, r := range lf.records {
		if i%bucketSize == 0 {
			restarts[i/bucketSize] = regionStart + len(rec)
			rec = format.AppendUvarint(rec, 0) // shared = 0 marks a bucket start
			rec = format.AppendUvarint(rec, uint64(len(r.key)))
			rec = append(rec, r.key...)
		} else {
			s := lcp(prev, r.key)
			suf := r.key[s:]
			rec = format.AppendUvarint(rec, uint64(s))
			rec = format.AppendUvarint(rec, uint64(len(suf)))
			rec = append(rec, suf...)
		}
		rec = format.AppendUvarint(rec, uint64(len(r.val)))
		rec = append(rec, r.val...)
		prev = r.key
	}

	total := regionStart + len(rec)
	if total > len(dst) {
		return 0, ErrPageFull
	}

	format.CommonHeader{Type: format.PageBTreeLeaf, CellCount: uint16(n)}.Encode(dst)
	binary.BigEndian.PutUint16(dst[8:10], uint16(bucketCount))
	binary.BigEndian.PutUint16(dst[10:12], uint16(bucketSize))
	binary.BigEndian.PutUint32(dst[12:16], lf.right)
	binary.BigEndian.PutUint32(dst[16:20], lf.left)
	for b, off := range restarts {
		binary.BigEndian.PutUint16(dst[leafHeaderEnd+b*2:], uint16(off))
	}
	copy(dst[regionStart:], rec)
	return total, nil
}

// decodeLeaf parses a leaf page. It fails closed with ErrCorruptNode on any
// truncation or inconsistency rather than reading past src.
func decodeLeaf(src []byte) (*leaf, error) {
	if len(src) < leafHeaderEnd {
		return nil, ErrCorruptNode
	}
	ch := format.DecodeCommonHeader(src)
	if ch.Type != format.PageBTreeLeaf {
		return nil, ErrCorruptNode
	}
	n := int(ch.CellCount)
	bucketCount := int(binary.BigEndian.Uint16(src[8:10]))
	bucketSize := int(binary.BigEndian.Uint16(src[10:12]))
	right := binary.BigEndian.Uint32(src[12:16])
	left := binary.BigEndian.Uint32(src[16:20])
	if n > 0 && bucketSize <= 0 {
		return nil, ErrCorruptNode
	}
	if n > 0 && bucketCount != (n+bucketSize-1)/bucketSize {
		return nil, ErrCorruptNode
	}

	off := leafHeaderEnd + bucketCount*2
	if off > len(src) {
		return nil, ErrCorruptNode
	}
	records := make([]record, 0, n)
	var prev []byte
	for i := 0; i < n; i++ {
		shared, m := format.Uvarint(src[off:])
		if m <= 0 {
			return nil, ErrCorruptNode
		}
		off += m
		if i%bucketSize == 0 && shared != 0 {
			return nil, ErrCorruptNode
		}
		l, m := format.Uvarint(src[off:])
		if m <= 0 || l > uint64(len(src)) || off+m+int(l) > len(src) {
			return nil, ErrCorruptNode
		}
		off += m
		suf := src[off : off+int(l)]
		off += int(l)

		var key []byte
		if shared == 0 {
			key = append([]byte(nil), suf...)
		} else {
			if shared > uint64(len(prev)) {
				return nil, ErrCorruptNode
			}
			key = make([]byte, 0, int(shared)+len(suf))
			key = append(key, prev[:shared]...)
			key = append(key, suf...)
		}

		vlen, m := format.Uvarint(src[off:])
		if m <= 0 || vlen > uint64(len(src)) || off+m+int(vlen) > len(src) {
			return nil, ErrCorruptNode
		}
		off += m
		val := append([]byte(nil), src[off:off+int(vlen)]...)
		off += int(vlen)

		records = append(records, record{key: key, val: val})
		prev = key
	}
	return &leaf{records: records, left: left, right: right, bucketSize: bucketSize}, nil
}

// seekLeafPage searches a leaf page for target using the on-page restart array,
// without decoding the whole leaf: it binary-searches the bucket restart keys to
// pick the one bucket whose range covers target, then linear-scans that bucket
// reconstructing front-coded keys. It returns the value and true on an exact
// internal-key match. This is what proves the bucketed front-coded layout stays
// seekable; the engine's version-resolving read is built over it in M3.
func seekLeafPage(src, target []byte) (val []byte, found bool, err error) {
	if len(src) < leafHeaderEnd {
		return nil, false, ErrCorruptNode
	}
	ch := format.DecodeCommonHeader(src)
	if ch.Type != format.PageBTreeLeaf {
		return nil, false, ErrCorruptNode
	}
	n := int(ch.CellCount)
	bucketCount := int(binary.BigEndian.Uint16(src[8:10]))
	bucketSize := int(binary.BigEndian.Uint16(src[10:12]))
	if n == 0 {
		return nil, false, nil
	}
	if bucketSize <= 0 || bucketCount != (n+bucketSize-1)/bucketSize {
		return nil, false, ErrCorruptNode
	}

	// firstKey reads the full key of bucket b directly from its restart offset.
	firstKey := func(b int) ([]byte, int, error) {
		if leafHeaderEnd+b*2+2 > len(src) {
			return nil, 0, ErrCorruptNode
		}
		off := int(binary.BigEndian.Uint16(src[leafHeaderEnd+b*2:]))
		shared, m := format.Uvarint(src[min(off, len(src)):])
		if off >= len(src) || m <= 0 || shared != 0 {
			return nil, 0, ErrCorruptNode
		}
		off += m
		klen, m := format.Uvarint(src[off:])
		if m <= 0 || klen > uint64(len(src)) || off+m+int(klen) > len(src) {
			return nil, 0, ErrCorruptNode
		}
		off += m
		return src[off : off+int(klen)], off + int(klen), nil
	}

	// Binary search the restart keys for the last bucket whose first key is <= target.
	lo, hi := 0, bucketCount
	for lo < hi {
		mid := (lo + hi) / 2
		fk, _, ferr := firstKey(mid)
		if ferr != nil {
			return nil, false, ferr
		}
		if format.CompareInternal(fk, target) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return nil, false, nil // target sorts before the first key
	}
	bucket := lo - 1

	// Linear-scan the chosen bucket, reconstructing each front-coded key.
	fk, off, ferr := firstKey(bucket)
	if ferr != nil {
		return nil, false, ferr
	}
	prev := append([]byte(nil), fk...)
	first := bucket * bucketSize
	last := first + bucketSize
	if last > n {
		last = n
	}
	for i := first; i < last; i++ {
		var key []byte
		if i == first {
			key = prev
		} else {
			shared, m := format.Uvarint(src[off:])
			if m <= 0 {
				return nil, false, ErrCorruptNode
			}
			off += m
			sl, m := format.Uvarint(src[off:])
			if m <= 0 || sl > uint64(len(src)) || off+m+int(sl) > len(src) || shared > uint64(len(prev)) {
				return nil, false, ErrCorruptNode
			}
			off += m
			key = make([]byte, 0, int(shared)+int(sl))
			key = append(key, prev[:shared]...)
			key = append(key, src[off:off+int(sl)]...)
			off += int(sl)
		}
		vlen, m := format.Uvarint(src[off:])
		if m <= 0 || vlen > uint64(len(src)) || off+m+int(vlen) > len(src) {
			return nil, false, ErrCorruptNode
		}
		off += m
		value := src[off : off+int(vlen)]
		off += int(vlen)

		cmp := format.CompareInternal(key, target)
		if cmp == 0 {
			return append([]byte(nil), value...), true, nil
		}
		if cmp > 0 {
			return nil, false, nil // passed where target would be
		}
		prev = key
	}
	return nil, false, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// message is one buffered Bε message addressed to a key in an interior node's
// subtree (doc 06 section 2). kind is the operation (set/delete/merge/range), seq
// orders messages for one key oldest to newest, key is the full internal key, and
// val is the inline payload (empty for a delete).
type message struct {
	kind byte
	seq  uint64
	key  []byte
	val  []byte
}

// pivot is one interior routing entry: a separator key and the child holding keys
// at or above it (and below the next pivot).
type pivot struct {
	key   []byte
	child format.PageNo
}

// interior is the decoded form of an interior node: its leftmost child, its sorted
// pivots, and its pending message buffer.
type interior struct {
	leftmost format.PageNo
	pivots   []pivot
	buffer   []message
}

// interior header layout, after the 8-byte common header (doc 06 section 2):
//
//	+8  buffer cell count    (2)
//	+10 buffer bytes used    (2)
//	+12 pivot region end     (2)  offset where pivots stop and the buffer begins
//	+14 leftmost child page  (4)
//	+18 buffer free hint     (2)
//	+20 reserved             (4)
//	+24 pivot cell-pointer array, pivot cells, then the buffer region
const interiorHeaderEnd = 24

// encodeInterior writes in into dst, the usable page area, and returns bytes used.
// It returns ErrPageFull if the pivots plus the buffered messages do not fit. The
// pivot cell-pointer array makes the pivots binary-searchable in place; the buffer
// is stored as a sequential message run in M0 (the sorted buffer cell-pointer array
// a flush's binary search wants is an additive M1 concern).
func encodeInterior(dst []byte, in *interior) (int, error) {
	np := len(in.pivots)
	if np > 0xFFFF || len(in.buffer) > 0xFFFF {
		return 0, ErrPageFull
	}
	ptrArrayBytes := np * 2
	pivotStart := interiorHeaderEnd + ptrArrayBytes

	// Encode the pivot cells, recording each cell's absolute offset for the pointer
	// array.
	offsets := make([]int, np)
	var cells []byte
	for i, p := range in.pivots {
		offsets[i] = pivotStart + len(cells)
		cells = format.AppendUvarint(cells, uint64(len(p.key)))
		cells = append(cells, p.key...)
		var c [4]byte
		binary.BigEndian.PutUint32(c[:], p.child)
		cells = append(cells, c[:]...)
	}
	pivotRegionEnd := pivotStart + len(cells)

	// Encode the buffer messages sequentially after the pivots.
	var buf []byte
	for _, m := range in.buffer {
		buf = append(buf, m.kind)
		buf = format.AppendUvarint(buf, m.seq)
		buf = format.AppendUvarint(buf, uint64(len(m.key)))
		buf = append(buf, m.key...)
		buf = format.AppendUvarint(buf, uint64(len(m.val)))
		buf = append(buf, m.val...)
	}

	total := pivotRegionEnd + len(buf)
	if total > len(dst) || pivotRegionEnd > 0xFFFF {
		return 0, ErrPageFull
	}

	format.CommonHeader{Type: format.PageBTreeInterior, CellCount: uint16(np)}.Encode(dst)
	binary.BigEndian.PutUint16(dst[8:10], uint16(len(in.buffer)))
	binary.BigEndian.PutUint16(dst[10:12], uint16(len(buf)))
	binary.BigEndian.PutUint16(dst[12:14], uint16(pivotRegionEnd))
	binary.BigEndian.PutUint32(dst[14:18], in.leftmost)
	binary.BigEndian.PutUint16(dst[18:20], 0)
	binary.BigEndian.PutUint32(dst[20:24], 0)
	for i, off := range offsets {
		binary.BigEndian.PutUint16(dst[interiorHeaderEnd+i*2:], uint16(off))
	}
	copy(dst[pivotStart:], cells)
	copy(dst[pivotRegionEnd:], buf)
	return total, nil
}

// decodeInterior parses an interior page, failing closed with ErrCorruptNode on any
// truncation or inconsistency.
func decodeInterior(src []byte) (*interior, error) {
	if len(src) < interiorHeaderEnd {
		return nil, ErrCorruptNode
	}
	ch := format.DecodeCommonHeader(src)
	if ch.Type != format.PageBTreeInterior {
		return nil, ErrCorruptNode
	}
	np := int(ch.CellCount)
	bufCount := int(binary.BigEndian.Uint16(src[8:10]))
	pivotRegionEnd := int(binary.BigEndian.Uint16(src[12:14]))
	leftmost := binary.BigEndian.Uint32(src[14:18])

	ptrArrayEnd := interiorHeaderEnd + np*2
	if ptrArrayEnd > len(src) {
		return nil, ErrCorruptNode
	}
	pivots := make([]pivot, 0, np)
	for i := 0; i < np; i++ {
		off := int(binary.BigEndian.Uint16(src[interiorHeaderEnd+i*2:]))
		if off >= len(src) {
			return nil, ErrCorruptNode
		}
		klen, m := format.Uvarint(src[off:])
		if m <= 0 || klen > uint64(len(src)) || off+m+int(klen)+4 > len(src) {
			return nil, ErrCorruptNode
		}
		off += m
		key := append([]byte(nil), src[off:off+int(klen)]...)
		off += int(klen)
		child := binary.BigEndian.Uint32(src[off : off+4])
		pivots = append(pivots, pivot{key: key, child: child})
	}

	if pivotRegionEnd > len(src) {
		return nil, ErrCorruptNode
	}
	off := pivotRegionEnd
	buffer := make([]message, 0, bufCount)
	for i := 0; i < bufCount; i++ {
		if off+1 > len(src) {
			return nil, ErrCorruptNode
		}
		kind := src[off]
		off++
		seq, m := format.Uvarint(src[off:])
		if m <= 0 {
			return nil, ErrCorruptNode
		}
		off += m
		klen, m := format.Uvarint(src[off:])
		if m <= 0 || klen > uint64(len(src)) || off+m+int(klen) > len(src) {
			return nil, ErrCorruptNode
		}
		off += m
		key := append([]byte(nil), src[off:off+int(klen)]...)
		off += int(klen)
		vlen, m := format.Uvarint(src[off:])
		if m <= 0 || vlen > uint64(len(src)) || off+m+int(vlen) > len(src) {
			return nil, ErrCorruptNode
		}
		off += m
		val := append([]byte(nil), src[off:off+int(vlen)]...)
		off += int(vlen)
		buffer = append(buffer, message{kind: kind, seq: seq, key: key, val: val})
	}
	return &interior{leftmost: leftmost, pivots: pivots, buffer: buffer}, nil
}

// route returns the child page that owns target: the child to the right of the
// last pivot whose key is <= target, or the leftmost child when target sorts below
// every pivot. The pivots are in ascending internal-key order, so this is the
// standard separator-key descent that keeps point-read latency at the B-tree floor.
func (in *interior) route(target []byte) format.PageNo {
	child := in.leftmost
	for _, p := range in.pivots {
		if format.CompareInternal(target, p.key) >= 0 {
			child = p.child
		} else {
			break
		}
	}
	return child
}
