package hashlog

import (
	"encoding/binary"
	"errors"
)

// This file is the on-disk extent header (spec 2070 doc 03 section 5), the piece M1
// deferred and M5 needs: recovery walks each shard's log from the file alone, and the
// header is what makes a log extent self-describing. Each log extent begins with this
// fixed header, then its body holds the shard's page bytes. The header records which
// shard owns the extent, where it sits in the shard's chain (prev and next links), and
// the logical address of its first body byte, so recovery can group the file's extents
// by shard, order them by logical address, and replay each shard's records without a
// separate chain table.
//
// The header forces the on-disk extent to be larger than a page: an extent is a header
// followed by a PageSize body, so the on-disk extent stride is PageSize +
// extentHeaderBytes. The logical-address arithmetic the read path turns on is
// unchanged (a logical address still divides by PageSize to a page index and masks to
// an in-page offset); only the extent-id-to-file-offset mapping gains the header
// offset. This is the reconciliation the M1 spec-resolution note promised: the earlier
// "ExtentSize equals PageSize" was an M0/M1 simplification that held only until an
// on-disk header had a first reader, which is recovery.

const (
	// extentMagic identifies a hashlog extent header. Eight bytes so the header opens
	// with one fixed-width read, the same shape as the superblock and snapshot magics.
	extentMagic = "HLOGEX\x00\x00"

	// extentHeaderBytes is the on-disk size reserved for an extent header at the front
	// of every extent. Doc 03 section 5 lays the header fields out in 52 bytes; it is
	// rounded up to 64 so the page body that follows starts at a 64-byte boundary within
	// the extent, which keeps the body's own alignment clean (doc 03 section 8). The
	// unused tail bytes are reserved and zero.
	extentHeaderBytes = 64

	// Extent kinds (doc 03 section 5 field table). A log extent holds a shard's records;
	// the checkpoint and oversize kinds are reserved here and used by M4's snapshot run
	// (checkpoint) and M9's oversize values.
	extentKindLog          = 0
	extentKindCheckpoint   = 1
	extentKindOversize     = 2
	extentKindOversizeCont = 3
)

// errBadExtentHeader is the single sentinel for a torn or malformed extent header, so a
// caller can tell "this extent was never a valid log extent" from a real I/O error. A
// crash mid-grow (the header written but no record synced, or a torn header) decodes to
// this, and recovery treats such an extent as empty (doc 03 section 4: reconciliation
// returns it to the free stack).
var errBadExtentHeader = errors.New("hashlog: extent header invalid")

// extentHeader is the in-memory form of an extent's on-disk header. prevExtent and
// nextExtent are the chain links (-1 at the head or tail), baseAddr is the logical
// address of the extent's first body byte (so a record's logical address is baseAddr
// plus its offset within the body), and genStamp is the allocator generation when the
// extent was handed out, a belt-and-suspenders check that an extent was not silently
// reused under a stale reference.
type extentHeader struct {
	kind       uint16
	shardID    int32
	prevExtent int64
	nextExtent int64
	baseAddr   int64
	genStamp   uint64
}

// encodeExtentHeader serialises a header into exactly extentHeaderBytes bytes: the
// fixed fields little-endian, then a CRC32C over the bytes before it, then zero padding
// to the reserved size. The CRC covers only the used header bytes (doc 03 section 5:
// headerCRC over bytes [0..48)), separate from the per-record CRCs in the body, so a
// torn header is detectable on its own.
func encodeExtentHeader(h extentHeader) []byte {
	buf := make([]byte, extentHeaderBytes)
	copy(buf[0:8], extentMagic)
	binary.LittleEndian.PutUint16(buf[8:10], h.kind)
	// buf[10:12] reserved, zero.
	binary.LittleEndian.PutUint32(buf[12:16], uint32(h.shardID))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(h.prevExtent))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(h.nextExtent))
	binary.LittleEndian.PutUint64(buf[32:40], uint64(h.baseAddr))
	binary.LittleEndian.PutUint64(buf[40:48], h.genStamp)
	crc := crc32c(buf[0:48])
	binary.LittleEndian.PutUint32(buf[48:52], crc)
	// buf[52:64] reserved, zero.
	return buf
}

// decodeExtentHeader parses an extent header. It is fail-closed (doc 08 section 4.4):
// it checks the length, the magic, and the header CRC, and returns errBadExtentHeader
// on any mismatch, so a torn or never-written extent is rejected cleanly rather than
// driving recovery off a garbage link. It never panics and never reads out of bounds.
func decodeExtentHeader(buf []byte) (extentHeader, error) {
	if len(buf) < extentHeaderBytes {
		return extentHeader{}, errBadExtentHeader
	}
	if string(buf[0:8]) != extentMagic {
		return extentHeader{}, errBadExtentHeader
	}
	want := binary.LittleEndian.Uint32(buf[48:52])
	if crc32c(buf[0:48]) != want {
		return extentHeader{}, errBadExtentHeader
	}
	return extentHeader{
		kind:       binary.LittleEndian.Uint16(buf[8:10]),
		shardID:    int32(binary.LittleEndian.Uint32(buf[12:16])),
		prevExtent: int64(binary.LittleEndian.Uint64(buf[16:24])),
		nextExtent: int64(binary.LittleEndian.Uint64(buf[24:32])),
		baseAddr:   int64(binary.LittleEndian.Uint64(buf[32:40])),
		genStamp:   binary.LittleEndian.Uint64(buf[40:48]),
	}, nil
}
