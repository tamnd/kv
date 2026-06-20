package format

// A value pointer is what a cell stores instead of the value bytes when the engine
// separates a value out of the segment into the value log (WiscKey, spec 06 §7, spec
// 02 §8.3). The cell's kind is KindSetSep, and its value field is the encoded pointer
// below rather than the literal bytes. Compaction then moves the small pointer instead
// of rewriting the value, which is the whole point of separation: the value bytes are
// written once, sequentially, into the vLog and never copied again by a merge.
//
// The pointer names where the value lives: the first vLog page of the record, the byte
// offset of the value within that page's body, and the value's length. A value larger
// than a page body spans several pages, chained through the common header's overflow
// slot, so the reader follows the chain from (page, offset) for length bytes. The
// length travels in the pointer so a key-only scan, or a LazyValue that is never
// materialized, learns the value size without touching the vLog.

// ValuePointer locates a separated value in the value log.
type ValuePointer struct {
	Page   uint32 // first vLog page holding the value
	Offset uint32 // byte offset of the value within that page's body
	Length uint32 // value length in bytes
}

// AppendValuePointer appends the encoding of p to dst and returns the extended slice.
// The three fields are varint-packed, so a small page number and offset cost only a
// couple of bytes each, keeping the pointer cell tiny.
func AppendValuePointer(dst []byte, p ValuePointer) []byte {
	dst = AppendUvarint(dst, uint64(p.Page))
	dst = AppendUvarint(dst, uint64(p.Offset))
	dst = AppendUvarint(dst, uint64(p.Length))
	return dst
}

// DecodeValuePointer parses a value pointer from the front of b. It returns ok ==
// false when b is truncated, so a malformed pointer cell is rejected rather than
// dereferenced into a wrong region.
func DecodeValuePointer(b []byte) (ValuePointer, bool) {
	page, n1 := Uvarint(b)
	if n1 <= 0 {
		return ValuePointer{}, false
	}
	b = b[n1:]
	off, n2 := Uvarint(b)
	if n2 <= 0 {
		return ValuePointer{}, false
	}
	b = b[n2:]
	length, n3 := Uvarint(b)
	if n3 <= 0 {
		return ValuePointer{}, false
	}
	return ValuePointer{Page: uint32(page), Offset: uint32(off), Length: uint32(length)}, true
}
