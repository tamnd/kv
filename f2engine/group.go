package f2engine

import "github.com/tamnd/kv/format"

// cell is one stored version of a user key: the commit version, the mutation kind,
// and the raw value bytes the batch carried (for a TTL set, the expiry-framed value;
// for a delete or a range marker, empty or the marker payload). A key's cells are kept
// newest-first, the order format.Fold consumes them in.
type cell struct {
	version uint64
	kind    format.Kind
	value   []byte
}

// encodeGroup serializes a key's cells into the single opaque value f2 stores under
// that key. The layout is a uvarint cell count, then per cell a uvarint version, one
// kind byte, a uvarint value length, and the value bytes. dst is appended to and
// returned so the caller can reuse a scratch buffer across writes.
func encodeGroup(dst []byte, cells []cell) []byte {
	dst = format.AppendUvarint(dst, uint64(len(cells)))
	for _, c := range cells {
		dst = format.AppendUvarint(dst, c.version)
		dst = append(dst, byte(c.kind))
		dst = format.AppendUvarint(dst, uint64(len(c.value)))
		dst = append(dst, c.value...)
	}
	return dst
}

// decodeGroup parses what encodeGroup wrote. The returned cells alias src, so a caller
// that retains a value past src's lifetime must copy it. ok is false on a truncated or
// internally inconsistent buffer, which the caller treats as a corrupt group rather
// than an empty one.
func decodeGroup(src []byte) (cells []cell, ok bool) {
	count, n := format.Uvarint(src)
	if n <= 0 {
		return nil, false
	}
	src = src[n:]
	cells = make([]cell, 0, count)
	for i := uint64(0); i < count; i++ {
		v, n := format.Uvarint(src)
		if n <= 0 {
			return nil, false
		}
		src = src[n:]
		if len(src) < 1 {
			return nil, false
		}
		k := format.Kind(src[0])
		src = src[1:]
		vl, n := format.Uvarint(src)
		if n <= 0 || uint64(len(src)-n) < vl {
			return nil, false
		}
		src = src[n:]
		cells = append(cells, cell{version: v, kind: k, value: src[:vl]})
		src = src[vl:]
	}
	return cells, true
}
