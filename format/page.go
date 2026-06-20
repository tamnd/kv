package format

import "encoding/binary"

// PageType is the one-byte tag that begins every non-header page (spec 02 §3).
type PageType byte

const (
	PageFree          PageType = 0x00 // on the freelist; contents undefined
	PageHeaderMeta    PageType = 0x01 // page 1: header + engine root region
	PageBTreeInterior PageType = 0x02 // B-tree interior node
	PageBTreeLeaf     PageType = 0x03 // B-tree leaf node
	PageOverflow      PageType = 0x04 // a link in an overflow chain
	PageLSMBlock      PageType = 0x05 // an LSM segment data/index/filter block
	PageLSMManifest   PageType = 0x06 // a page of the embedded MANIFEST
	PageVLog          PageType = 0x07 // a value-log page (WiscKey)
	PageFreelistTrunk PageType = 0x08 // a freelist trunk page
	PagePointerMap    PageType = 0x09 // reverse child->parent pointers for vacuum
)

// String renders a PageType for diagnostics.
func (t PageType) String() string {
	switch t {
	case PageFree:
		return "free"
	case PageHeaderMeta:
		return "header"
	case PageBTreeInterior:
		return "btree-interior"
	case PageBTreeLeaf:
		return "btree-leaf"
	case PageOverflow:
		return "overflow"
	case PageLSMBlock:
		return "lsm-block"
	case PageLSMManifest:
		return "lsm-manifest"
	case PageVLog:
		return "vlog"
	case PageFreelistTrunk:
		return "freelist-trunk"
	case PagePointerMap:
		return "pointer-map"
	default:
		return "reserved"
	}
}

// PageNo is a 1-based page number; 0 is the null/none sentinel used in all
// internal pointers (spec 02 §1).
type PageNo = uint32

// NoPage is the null page pointer.
const NoPage PageNo = 0

// CommonHeaderSize is the size of the generic page header that precedes the
// engine-specific payload on every non-header page (spec 02 §3.1).
const CommonHeaderSize = 8

// CommonHeader is the 8-byte preamble shared by all non-header pages.
type CommonHeader struct {
	Type      PageType // byte 0
	Flags     byte     // byte 1, engine-defined
	CellCount uint16   // bytes 2-3, for cell-structured pages
	Overflow  PageNo   // bytes 4-7, overflow continuation page (overflow pages)
}

// Encode writes the common header into the first 8 bytes of p.
func (h CommonHeader) Encode(p []byte) {
	p[0] = byte(h.Type)
	p[1] = h.Flags
	binary.BigEndian.PutUint16(p[2:4], h.CellCount)
	binary.BigEndian.PutUint32(p[4:8], h.Overflow)
}

// DecodeCommonHeader reads the common header from the first 8 bytes of p.
func DecodeCommonHeader(p []byte) CommonHeader {
	return CommonHeader{
		Type:      PageType(p[0]),
		Flags:     p[1],
		CellCount: binary.BigEndian.Uint16(p[2:4]),
		Overflow:  binary.BigEndian.Uint32(p[4:8]),
	}
}

// Page-size bounds (spec 02 §1). 65536 cannot fit in 16 bits and is stored as
// the literal 1 in the header's page-size field.
const (
	MinPageSize     = 512
	MaxPageSize     = 65536
	DefaultPageSize = 4096
)

// ValidPageSize reports whether n is a legal page size: a power of two within
// [MinPageSize, MaxPageSize].
func ValidPageSize(n int) bool {
	if n < MinPageSize || n > MaxPageSize {
		return false
	}
	return n&(n-1) == 0
}

// EncodePageSize maps a page size to its 2-byte header representation, encoding
// 65536 as 1.
func EncodePageSize(n int) uint16 {
	if n == MaxPageSize {
		return 1
	}
	return uint16(n)
}

// DecodePageSize maps the 2-byte header representation back to a page size.
func DecodePageSize(v uint16) int {
	if v == 1 {
		return MaxPageSize
	}
	return int(v)
}
