package hashlog

import (
	"encoding/binary"
	"errors"
)

// This file is the index snapshot: the periodic on-disk artifact of the two-artifact
// durability model (spec 2070 doc 05 sections 2-4, D8). A snapshot is the resident
// hash index written as tuples, one section per shard: per live key the location and
// nothing else, no values (the values stay in the log, where the location points).
// Recovery (M5) loads the snapshot, then replays each shard's log delta from the
// recorded frontier forward; M4 is the writer and the commit, not the reader.
//
// The snapshot stores tuples, not the table structure (doc 05 section 2): the slot
// count, the probe order, and the table hash are in-memory details free to change
// between versions, so a snapshot that froze them could not be loaded by a later
// binary. A tuple snapshot is also the minimum size (one entry per live key, no empty
// slots, no tombstone debris) and recovery rebuilds a fresh table from it, which is a
// compacting rebuild for free.

const (
	// snapMagic identifies a hashlog index snapshot stream. Eight bytes so the header
	// starts with one fixed-width read.
	snapMagic = "HLOGSN\x00\x00"

	// snapFormatVersion is the snapshot on-disk format version. A decoder rejects a
	// version it does not understand rather than guessing.
	snapFormatVersion = 1

	// snapHeaderSize is the fixed stream header: magic, version, shard count,
	// generation, stream length, directory offset, sections offset.
	snapHeaderSize = 40

	// snapDirEntrySize is one shard's directory entry: section offset, section length,
	// live key count, frontier LSN, section CRC32C, reserved.
	snapDirEntrySize = 40

	// snapEntryFixed is the fixed prefix of a snapshot entry: flags, addr, vlen. The
	// key follows, length-prefixed with a uvarint.
	snapEntryFixed = 13

	// snapSectionTrailer is a section's trailer: the entry count and the CRC32C over the
	// section (entries plus the count).
	snapSectionTrailer = 12
)

// errBadSnapshot is the single sentinel for any malformed or torn snapshot stream, so
// a caller can tell "this snapshot is unusable" from a real I/O error. The decoder is
// fail-closed (doc 05 section 8, FuzzDecodeSnapshot): it validates every length and
// offset against the buffer before indexing and returns this on any mismatch.
var errBadSnapshot = errors.New("hashlog: snapshot invalid")

// snapTuple is one live key as captured for the snapshot: the key bytes, the value
// location the index holds, and the reserved flags byte. No value and no per-key LSN
// (the section's frontier LSN is the shared version stamp, see the implementation
// note).
type snapTuple struct {
	key   []byte
	loc   valLoc
	flags byte
}

// snapSection is one shard's captured cut: its live tuples and the frontier LSN the
// snapshot is current as of (the consistent-cut F_shard, doc 05 section 3).
type snapSection struct {
	shard       int
	frontierLSN uint64
	tuples      []snapTuple
}

// decodedSnapshot is what decodeSnapshot returns: the generation the snapshot belongs
// to (cross-checked against the superblock slot by recovery) and the per-shard
// sections in shard order.
type decodedSnapshot struct {
	generation uint64
	sections   []snapSection
}

// snapSectionSize returns the encoded byte length of one shard's section: its entries
// back to back plus the trailer. encodeSnapshot uses it to lay out the section offsets
// before allocating the stream, so each section can be written straight into its final
// place in the stream with no intermediate per-section buffer.
func snapSectionSize(s snapSection) int {
	n := 0
	for i := range s.tuples {
		n += snapEntryFixed + uvarintLen(uint64(len(s.tuples[i].key))) + len(s.tuples[i].key)
	}
	return n + snapSectionTrailer
}

// encodeSnapSectionInto serialises one shard's section directly into dst (which must be
// exactly snapSectionSize(s) bytes): the entries back to back, then a trailer of the
// entry count and a CRC32C over everything before the CRC. It returns the same CRC so
// the caller can mirror it into the directory without rescanning the section, letting a
// reader verify the section against the directory before trusting its bytes.
func encodeSnapSectionInto(dst []byte, s snapSection) uint32 {
	off := 0
	for i := range s.tuples {
		t := &s.tuples[i]
		dst[off] = t.flags
		binary.LittleEndian.PutUint64(dst[off+1:off+9], uint64(t.loc.addr))
		binary.LittleEndian.PutUint32(dst[off+9:off+13], t.loc.vlen)
		off += snapEntryFixed
		off += binary.PutUvarint(dst[off:], uint64(len(t.key)))
		off += copy(dst[off:], t.key)
	}
	binary.LittleEndian.PutUint64(dst[off:off+8], uint64(len(s.tuples)))
	crc := crc32c(dst[:off+8])
	binary.LittleEndian.PutUint32(dst[off+8:off+12], crc)
	return crc
}

// encodeSnapshot serialises a whole snapshot stream: the header, then the shard
// directory, then the sections in shard order. The stream is one contiguous byte
// region (the implementation writes it across a contiguous extent run, so recovery
// reads it back as one region with no extent chain to walk, see the implementation
// note). sections must have one entry per shard, indexed by shard id.
//
// Sections are sized in a first pass and then encoded straight into their final slice
// of the one stream buffer, so a key is copied into the stream exactly once. There is no
// intermediate per-section buffer, which matters at scale: a shard holding millions of
// live keys would otherwise materialise its whole section a second time before the copy
// into the stream (audit S7).
func encodeSnapshot(shardCount int, generation uint64, sections []snapSection) []byte {
	dirOff := snapHeaderSize
	sectionsOff := dirOff + shardCount*snapDirEntrySize
	total := sectionsOff
	offs := make([]int, shardCount)
	sizes := make([]int, shardCount)
	for i := 0; i < shardCount; i++ {
		sz := snapSectionSize(sections[i])
		offs[i] = total
		sizes[i] = sz
		total += sz
	}

	buf := make([]byte, total)
	copy(buf[0:8], snapMagic)
	binary.LittleEndian.PutUint16(buf[8:10], snapFormatVersion)
	// buf[10:12] reserved, zero.
	binary.LittleEndian.PutUint32(buf[12:16], uint32(shardCount))
	binary.LittleEndian.PutUint64(buf[16:24], generation)
	binary.LittleEndian.PutUint64(buf[24:32], uint64(total))
	binary.LittleEndian.PutUint32(buf[32:36], uint32(dirOff))
	binary.LittleEndian.PutUint32(buf[36:40], uint32(sectionsOff))

	for i := 0; i < shardCount; i++ {
		crc := encodeSnapSectionInto(buf[offs[i]:offs[i]+sizes[i]], sections[i])
		d := dirOff + i*snapDirEntrySize
		binary.LittleEndian.PutUint64(buf[d:d+8], uint64(offs[i]))
		binary.LittleEndian.PutUint64(buf[d+8:d+16], uint64(sizes[i]))
		binary.LittleEndian.PutUint64(buf[d+16:d+24], uint64(len(sections[i].tuples)))
		binary.LittleEndian.PutUint64(buf[d+24:d+32], sections[i].frontierLSN)
		binary.LittleEndian.PutUint32(buf[d+32:d+36], crc)
		// buf[d+36:d+40] reserved, zero.
	}
	return buf
}

// decodeSnapshot parses a whole snapshot stream. It is fail-closed (doc 05 section 8):
// it validates the header, bounds every directory entry and section against the
// buffer, verifies each section's CRC32C before decoding it, and returns errBadSnapshot
// on any mismatch. It never panics, never reads out of bounds, and never allocates
// unboundedly from a length field read off disk.
func decodeSnapshot(buf []byte) (*decodedSnapshot, error) {
	if len(buf) < snapHeaderSize {
		return nil, errBadSnapshot
	}
	if string(buf[0:8]) != snapMagic {
		return nil, errBadSnapshot
	}
	if binary.LittleEndian.Uint16(buf[8:10]) != snapFormatVersion {
		return nil, errBadSnapshot
	}
	shardCount := int(binary.LittleEndian.Uint32(buf[12:16]))
	if shardCount <= 0 || shardCount > maxShardCount {
		return nil, errBadSnapshot
	}
	generation := binary.LittleEndian.Uint64(buf[16:24])
	streamLen := binary.LittleEndian.Uint64(buf[24:32])
	dirOff := int(binary.LittleEndian.Uint32(buf[32:36]))
	sectionsOff := int(binary.LittleEndian.Uint32(buf[36:40]))

	if streamLen != uint64(len(buf)) {
		return nil, errBadSnapshot
	}
	if dirOff != snapHeaderSize {
		return nil, errBadSnapshot
	}
	dirEnd := dirOff + shardCount*snapDirEntrySize
	if dirEnd < dirOff || dirEnd > len(buf) {
		return nil, errBadSnapshot
	}
	if sectionsOff != dirEnd {
		return nil, errBadSnapshot
	}

	sections := make([]snapSection, shardCount)
	for i := 0; i < shardCount; i++ {
		d := dirOff + i*snapDirEntrySize
		sOff := int64(binary.LittleEndian.Uint64(buf[d : d+8]))
		sLen := int64(binary.LittleEndian.Uint64(buf[d+8 : d+16]))
		keyCount := binary.LittleEndian.Uint64(buf[d+16 : d+24])
		frontierLSN := binary.LittleEndian.Uint64(buf[d+24 : d+32])
		dirCRC := binary.LittleEndian.Uint32(buf[d+32 : d+36])

		if sOff < int64(sectionsOff) || sLen < int64(snapSectionTrailer) {
			return nil, errBadSnapshot
		}
		if sOff+sLen < sOff || sOff+sLen > int64(len(buf)) {
			return nil, errBadSnapshot
		}
		body := buf[sOff : sOff+sLen]
		if crc32c(body[:len(body)-crcSize]) != dirCRC {
			return nil, errBadSnapshot
		}
		tuples, err := decodeSnapSection(body, keyCount)
		if err != nil {
			return nil, err
		}
		sections[i] = snapSection{shard: i, frontierLSN: frontierLSN, tuples: tuples}
	}
	return &decodedSnapshot{generation: generation, sections: sections}, nil
}

// decodeSnapSection decodes one section body (entries plus trailer), verifying the
// trailer CRC and cross-checking the entry count against the directory's wantCount.
// Every length is bounded against the section before it is used to index, so a corrupt
// key length cannot drive an out-of-bounds read.
func decodeSnapSection(body []byte, wantCount uint64) ([]snapTuple, error) {
	if len(body) < snapSectionTrailer {
		return nil, errBadSnapshot
	}
	crcPos := len(body) - crcSize
	countPos := crcPos - 8
	want := binary.LittleEndian.Uint32(body[crcPos:])
	if crc32c(body[:crcPos]) != want {
		return nil, errBadSnapshot
	}
	entryCount := binary.LittleEndian.Uint64(body[countPos:crcPos])
	if entryCount != wantCount {
		return nil, errBadSnapshot
	}
	// Each entry is at least snapEntryFixed+1 bytes, so the count cannot exceed the
	// section size; cap the preallocation so a bogus count cannot drive a huge alloc
	// before the per-entry bounds check stops the loop.
	capHint := countPos / (snapEntryFixed + 1)
	if uint64(capHint) > entryCount {
		capHint = int(entryCount)
	}
	tuples := make([]snapTuple, 0, capHint)
	off := 0
	for i := uint64(0); i < entryCount; i++ {
		if off+snapEntryFixed > countPos {
			return nil, errBadSnapshot
		}
		flags := body[off]
		addr := int64(binary.LittleEndian.Uint64(body[off+1 : off+9]))
		vlen := binary.LittleEndian.Uint32(body[off+9 : off+13])
		off += snapEntryFixed
		klen, n := binary.Uvarint(body[off:countPos])
		if n <= 0 {
			return nil, errBadSnapshot
		}
		off += n
		if uint64(off)+klen > uint64(countPos) {
			return nil, errBadSnapshot
		}
		key := append([]byte(nil), body[off:off+int(klen)]...)
		off += int(klen)
		tuples = append(tuples, snapTuple{key: key, loc: valLoc{addr: addr, vlen: vlen}, flags: flags})
	}
	if off != countPos {
		return nil, errBadSnapshot
	}
	return tuples, nil
}
