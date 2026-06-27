package hashlog

import (
	"encoding/binary"
	"errors"
)

// The durable record format (spec 2070 doc 03 section 6, D3; doc 04 section 2, D4).
// It extends the in-memory record with a leading LSN, a flags byte, and a trailing
// CRC32C, so a record on the one file is self-describing for recovery and the
// compactor's liveness check. The format is used only on the durable write path; the
// memory-only store keeps the leaner encodeRecord so its benchmarked ceiling does not
// move (doc 08 section 1.2). The two stay close on purpose: the durable encoder is
// the in-memory encoder with the header and CRC added around the same key/value body.
//
// Layout, every fixed-width integer little-endian:
//
//	lsn     uint64           8 bytes
//	keyLen  uvarint          1..10 bytes
//	valLen  uvarint          1..10 bytes
//	flags   uint8            1 byte
//	key     raw              keyLen bytes
//	value   raw              valLen bytes
//	crc32c  uint32           4 bytes, CRC32C over lsn..value inclusive
const (
	recordLSNSize   = 8
	recordFlagsSize = 1
	recordCRCSize   = crcSize // 4, the same CRC32C primitive as the superblock

	// recordFixedOverhead is every byte of a record that is not the key, the value, or
	// the two length varints: the lsn, the flags, and the trailing CRC.
	recordFixedOverhead = recordLSNSize + recordFlagsSize + recordCRCSize
)

// Record flag bits (doc 03 section 6).
const (
	// flagTombstone marks a delete record: valLen is zero and no value bytes follow.
	// A delete appends a tombstone rather than mutating in place so it is recoverable
	// from the log (D7). M2 defines and round-trips the bit; the durable delete path
	// that writes it lands with recovery at M5.
	flagTombstone = 1 << 0
	// flagOversize marks a record whose value spans extents instead of living inline;
	// the inline bytes hold an oversize descriptor and valLen is the descriptor length
	// (section 7). Defined here, written at M9.
	flagOversize = 1 << 1
)

var errBadRecord = errors.New("hashlog: malformed durable record")

// durableRecordLen returns the encoded size of a durable record for a key and value.
func durableRecordLen(key, value []byte) int {
	return recordFixedOverhead +
		uvarintLen(uint64(len(key))) + uvarintLen(uint64(len(value))) +
		len(key) + len(value)
}

// durableValOff returns the offset of the value's first byte from the record start,
// the property D3 preserves so a resident GET slices the value with no decode. The
// index points at recordStart + durableValOff, exactly as the in-memory set computes
// its valOff with the lsn and flags added in.
func durableValOff(key, value []byte) int {
	return recordLSNSize +
		uvarintLen(uint64(len(key))) + uvarintLen(uint64(len(value))) +
		recordFlagsSize + len(key)
}

// encodeDurableRecord writes a durable record into dst (at least durableRecordLen
// long) and returns the bytes written. It is the in-memory encoder with the lsn and
// flags before the body and the CRC32C after it. It allocates nothing, so it runs on
// the append path under the shard write lock into the reusable scratch the same way
// encodeRecord does.
func encodeDurableRecord(dst []byte, lsn uint64, key, value []byte, flags byte) int {
	binary.LittleEndian.PutUint64(dst, lsn)
	n := recordLSNSize
	n += binary.PutUvarint(dst[n:], uint64(len(key)))
	n += binary.PutUvarint(dst[n:], uint64(len(value)))
	dst[n] = flags
	n++
	n += copy(dst[n:], key)
	n += copy(dst[n:], value)
	crc := crc32c(dst[:n])
	binary.LittleEndian.PutUint32(dst[n:], crc)
	n += recordCRCSize
	return n
}

// decodeDurableRecord reads one record from the front of buf. It is the path recovery
// and the compactor take, so it is fail-closed: it validates every length against the
// buffer before indexing, never panics on arbitrary bytes, and rejects a record whose
// CRC does not cover its bytes (doc 05 fail-closed, doc 08 section 4.4). It returns
// the decoded fields and n, the total record length, so a scan advances by n.
func decodeDurableRecord(buf []byte) (lsn uint64, flags byte, key, value []byte, n int, err error) {
	// Smallest possible record: lsn + two single-byte length varints + flags + CRC,
	// with an empty key and value.
	if len(buf) < recordLSNSize+2+recordFlagsSize+recordCRCSize {
		return 0, 0, nil, nil, 0, errBadRecord
	}
	p := 0
	lsn = binary.LittleEndian.Uint64(buf[p:])
	p += recordLSNSize

	klen, kn := binary.Uvarint(buf[p:])
	if kn <= 0 {
		return 0, 0, nil, nil, 0, errBadRecord
	}
	p += kn
	vlen, vn := binary.Uvarint(buf[p:])
	if vn <= 0 {
		return 0, 0, nil, nil, 0, errBadRecord
	}
	p += vn

	// Bound both lengths against the buffer before any arithmetic, so an absurd varint
	// can never drive an out-of-range index or an int overflow.
	if klen > uint64(len(buf)) || vlen > uint64(len(buf)) {
		return 0, 0, nil, nil, 0, errBadRecord
	}
	if p >= len(buf) { // need the flags byte
		return 0, 0, nil, nil, 0, errBadRecord
	}
	flags = buf[p]
	p++

	end := p + int(klen) + int(vlen)
	if end+recordCRCSize > len(buf) {
		return 0, 0, nil, nil, 0, errBadRecord
	}
	key = buf[p : p+int(klen)]
	value = buf[p+int(klen) : end]

	want := binary.LittleEndian.Uint32(buf[end:])
	if crc32c(buf[:end]) != want {
		return 0, 0, nil, nil, 0, errBadRecord
	}
	return lsn, flags, key, value, end + recordCRCSize, nil
}
