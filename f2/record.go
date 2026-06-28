package f2

import (
	"encoding/binary"
	"hash/crc32"
)

// The durable mode uses a self-describing, CRC-protected record so a crash leaves
// a recoverable file: every record carries a checksum over its own bytes, and a
// torn tail record (a write the crash cut short) fails its checksum and ends the
// shard's replay at exactly the right point. The memory-only mode keeps the lean
// uvarint format in log.go, which carries no checksum because a process that is
// still running never reads a torn record.
//
// Durable record layout:
//
//	flags    1 byte    bit 0 is the tombstone, marking a logged delete
//	keyLen   uvarint
//	valLen   uvarint
//	key      keyLen bytes
//	value    valLen bytes
//	crc      4 bytes   crc32c over flags through value
//
// A delete is a real record, not just an index edit, so recovery sees the
// deletion and does not resurrect the key from its earlier value record.
//
// flagValid is set on every real record's flags byte. It is what lets replay
// stop at the real tail: the unwritten remainder of a page is zero, and the CRC
// of an all-zero span is itself zero, so a zero span would otherwise decode as a
// valid empty record. Requiring the marker bit makes a zero byte end the scan
// before the CRC is even consulted.
const (
	flagValid     byte = 1 << 7
	flagTombstone byte = 1 << 0
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// durableRecordLen is the on-log size of a durable record for the given key and
// value, used to decide page fit before encoding.
func durableRecordLen(key, value []byte) int {
	return 1 + uvarintLen(uint64(len(key))) + uvarintLen(uint64(len(value))) + len(key) + len(value) + 4
}

// encodeDurable writes a durable record into dst, which must be at least
// durableRecordLen long, and returns the number of bytes written.
func encodeDurable(dst []byte, key, value []byte, tombstone bool) int {
	w := 0
	dst[w] = flagValid
	if tombstone {
		dst[w] |= flagTombstone
	}
	w++
	w += binary.PutUvarint(dst[w:], uint64(len(key)))
	w += binary.PutUvarint(dst[w:], uint64(len(value)))
	copy(dst[w:], key)
	w += len(key)
	copy(dst[w:], value)
	w += len(value)
	crc := crc32.Checksum(dst[:w], crcTable)
	binary.LittleEndian.PutUint32(dst[w:], crc)
	w += 4
	return w
}

// decodeDurable reads a durable record at the start of b. It returns the record's
// key, value, tombstone flag, and total byte length, plus whether the record is
// intact. A record fails closed: a truncated buffer, an impossible length, or a
// checksum mismatch all report ok=false, which is how recovery finds the crash
// point at the tail of a shard's log.
func decodeDurable(b []byte) (key, value []byte, tombstone bool, n int, ok bool) {
	if len(b) < 1 || b[0]&flagValid == 0 {
		// Empty buffer, or a zero/garbage byte where a record should start: the end
		// of the written records in this page.
		return nil, nil, false, 0, false
	}
	flags := b[0]
	p := 1
	klen, a := binary.Uvarint(b[p:])
	if a <= 0 {
		return nil, nil, false, 0, false
	}
	p += a
	vlen, c := binary.Uvarint(b[p:])
	if c <= 0 {
		return nil, nil, false, 0, false
	}
	p += c
	end := p + int(klen) + int(vlen)
	if klen > uint64(len(b)) || vlen > uint64(len(b)) || end+4 > len(b) {
		return nil, nil, false, 0, false
	}
	want := binary.LittleEndian.Uint32(b[end:])
	if crc32.Checksum(b[:end], crcTable) != want {
		return nil, nil, false, 0, false
	}
	key = b[p : p+int(klen)]
	value = b[p+int(klen) : end]
	return key, value, flags&flagTombstone != 0, end + 4, true
}
