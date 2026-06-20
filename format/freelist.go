package format

import "encoding/binary"

// The freelist recycles freed pages so a single-file database reclaims space
// without deleting files (spec 02 §4). It is a chain of trunk pages, each
// listing free leaf page numbers. The header points at the first trunk
// (offset 32) and holds the total free count (offset 36).
//
// Trunk page layout (tag 0x08):
//
//	offset 0  : common header (8 bytes, tag PageFreelistTrunk)
//	offset 8  : next trunk page (4 bytes, 0 if last)
//	offset 12 : count k of leaf page numbers in this trunk (4 bytes)
//	offset 16 : k big-endian page numbers
const (
	trunkNextOff  = CommonHeaderSize     // 8
	trunkCountOff = CommonHeaderSize + 4 // 12
	trunkArrayOff = CommonHeaderSize + 8 // 16
)

// TrunkCapacity reports how many leaf page numbers a trunk page can hold given
// the usable page size.
func TrunkCapacity(usable int) int {
	if usable <= trunkArrayOff {
		return 0
	}
	return (usable - trunkArrayOff) / 4
}

// TrunkPage is a decoded view over a freelist trunk page.
type TrunkPage struct {
	Next  PageNo
	Leafs []PageNo
}

// EncodeTrunk writes a trunk page into p (which must be the full page buffer).
func EncodeTrunk(p []byte, t TrunkPage) {
	CommonHeader{Type: PageFreelistTrunk}.Encode(p)
	binary.BigEndian.PutUint32(p[trunkNextOff:], t.Next)
	binary.BigEndian.PutUint32(p[trunkCountOff:], uint32(len(t.Leafs)))
	off := trunkArrayOff
	for _, leaf := range t.Leafs {
		binary.BigEndian.PutUint32(p[off:], leaf)
		off += 4
	}
}

// DecodeTrunk reads a trunk page from p. count is clamped to the page's capacity
// to bound a corrupt count field.
func DecodeTrunk(p []byte, usable int) TrunkPage {
	next := binary.BigEndian.Uint32(p[trunkNextOff:])
	count := int(binary.BigEndian.Uint32(p[trunkCountOff:]))
	if cap := TrunkCapacity(usable); count > cap {
		count = cap
	}
	leafs := make([]PageNo, count)
	off := trunkArrayOff
	for i := 0; i < count; i++ {
		leafs[i] = binary.BigEndian.Uint32(p[off:])
		off += 4
	}
	return TrunkPage{Next: next, Leafs: leafs}
}
