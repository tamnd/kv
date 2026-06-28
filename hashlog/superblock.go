package hashlog

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// superblock is the in-memory form of one checkpoint slot (doc 03 section 3). The
// file's first SBSize bytes hold two of these, written alternately so a crash
// mid-write always leaves one intact (the LMDB and SQLite double-meta discipline).
//
// At M0 the frontier array, snapshotRoot, and lsnHighWater are placeholders the
// later milestones fill (M3 the frontier, M4 the snapshot, M2 the LSN high-water);
// M0 reserves the fields, guarantees they round-trip, and guarantees they are
// covered by the slot CRC and committed atomically with the rest of the slot.
type superblock struct {
	generation   uint64
	extentSize   uint64
	extentCount  uint64
	lsnHighWater uint64
	snapshotRoot int64 // extent id of the index snapshot root, or -1
	snapshotLen  uint64
	shardCount   uint32

	// frontiers has one entry per shard. It is len shardCount.
	frontiers []shardFrontier

	// free is the allocator's free-extent-id stack, persisted inline (M0). The
	// overflow chain for a free list too large to fit inline is M4.
	free []int64
}

// shardFrontier is the per-shard durable cut a checkpoint records: the highest LSN
// known durable in the shard, and where the shard's log tail is so recovery can
// find the end of the chain without walking it from the head (doc 03 section 3).
type shardFrontier struct {
	frontierLSN uint64
	tailExtent  int64
	tailOff     uint64
}

// newSuperblock builds a fresh generation-0 superblock for a brand-new file: no
// extents, an empty free list, every shard's frontier zeroed, no snapshot.
func newSuperblock(shardCount int, extentSize int64) *superblock {
	return &superblock{
		generation:   0,
		extentSize:   uint64(extentSize),
		extentCount:  0,
		lsnHighWater: 0,
		snapshotRoot: -1,
		snapshotLen:  0,
		shardCount:   uint32(shardCount),
		frontiers:    make([]shardFrontier, shardCount),
		free:         nil,
	}
}

// encode serialises the superblock into exactly one slot's worth of bytes for the
// given slotID (0 for A, 1 for B). The fixed fields are little-endian, the frontier
// array follows the header, the inline free ids follow the frontier array, and the
// trailing 4 bytes are the CRC32C over everything before them (doc 03 section 3).
func (sb *superblock) encode(slotID int) ([]byte, error) {
	shardCount := int(sb.shardCount)
	if shardCount <= 0 || shardCount > maxShardCount {
		return nil, fmt.Errorf("hashlog: superblock shardCount %d out of range", shardCount)
	}
	if len(sb.frontiers) != shardCount {
		return nil, fmt.Errorf("hashlog: superblock has %d frontiers, want %d", len(sb.frontiers), shardCount)
	}
	if len(sb.free) > inlineFreeCapacity(shardCount) {
		return nil, fmt.Errorf("hashlog: free list of %d does not fit inline (cap %d); overflow chain is M4",
			len(sb.free), inlineFreeCapacity(shardCount))
	}

	slot := superblockSlotSize(shardCount)
	buf := make([]byte, slot)

	copy(buf[0:8], sbMagic)
	binary.LittleEndian.PutUint16(buf[8:10], sbFormatVersion)
	binary.LittleEndian.PutUint16(buf[10:12], uint16(slotID))
	// buf[12:16] reserved, zero.
	binary.LittleEndian.PutUint64(buf[16:24], sb.generation)
	binary.LittleEndian.PutUint64(buf[24:32], sb.extentSize)
	binary.LittleEndian.PutUint64(buf[32:40], sb.extentCount)
	binary.LittleEndian.PutUint64(buf[40:48], sb.lsnHighWater)
	binary.LittleEndian.PutUint64(buf[48:56], ^uint64(0)) // freeListExtent: -1, inline only at M0
	binary.LittleEndian.PutUint64(buf[56:64], uint64(len(sb.free)))
	binary.LittleEndian.PutUint64(buf[64:72], uint64(sb.snapshotRoot))
	binary.LittleEndian.PutUint64(buf[72:80], sb.snapshotLen)
	binary.LittleEndian.PutUint32(buf[80:84], sb.shardCount)
	binary.LittleEndian.PutUint32(buf[84:88], uint32(frontierArrayOff))
	binary.LittleEndian.PutUint32(buf[88:92], uint32(freeInlineOff(shardCount)))
	// buf[92:96] reserved2, zero.

	off := frontierArrayOff
	for _, f := range sb.frontiers {
		binary.LittleEndian.PutUint64(buf[off:off+8], f.frontierLSN)
		binary.LittleEndian.PutUint64(buf[off+8:off+16], uint64(f.tailExtent))
		binary.LittleEndian.PutUint64(buf[off+16:off+24], f.tailOff)
		off += frontierEntrySize
	}

	off = freeInlineOff(shardCount)
	for _, id := range sb.free {
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(id))
		off += 8
	}

	crc := crc32c(buf[:slot-crcSize])
	binary.LittleEndian.PutUint32(buf[slot-crcSize:slot], crc)
	return buf, nil
}

// errBadSuperblock is returned by decodeSuperblock for any malformed or torn slot.
// It is a single sentinel so callers can tell "this slot is unusable" from a real
// I/O error.
var errBadSuperblock = errors.New("hashlog: superblock slot invalid")

// decodeSuperblock parses one slot's bytes into a superblock. It is the fail-closed
// decoder of doc 08 section 4.4: it validates length, magic, version, and every
// length and offset against the slot bounds before allocating, verifies the CRC,
// and returns errBadSuperblock on any mismatch. It never panics, never reads out of
// bounds, and never allocates unboundedly from an attacker-chosen field.
func decodeSuperblock(buf []byte) (*superblock, error) {
	// Need at least the fixed header plus a CRC to read shardCount, which decides the
	// true slot size.
	if len(buf) < sbSlotHeaderSize+crcSize {
		return nil, errBadSuperblock
	}
	if string(buf[0:8]) != sbMagic {
		return nil, errBadSuperblock
	}
	if binary.LittleEndian.Uint16(buf[8:10]) != sbFormatVersion {
		return nil, errBadSuperblock
	}

	shardCount := int(binary.LittleEndian.Uint32(buf[80:84]))
	if shardCount <= 0 || shardCount > maxShardCount {
		return nil, errBadSuperblock
	}
	slot := superblockSlotSize(shardCount)
	if len(buf) < slot {
		return nil, errBadSuperblock
	}

	// Verify the CRC over the slot before trusting any field past the header.
	want := binary.LittleEndian.Uint32(buf[slot-crcSize : slot])
	if crc32c(buf[:slot-crcSize]) != want {
		return nil, errBadSuperblock
	}

	frontierOff := int(binary.LittleEndian.Uint32(buf[84:88]))
	freeOff := int(binary.LittleEndian.Uint32(buf[88:92]))
	freeListLen := binary.LittleEndian.Uint64(buf[56:64])
	freeListExtent := int64(binary.LittleEndian.Uint64(buf[48:56]))

	// The free-list overflow chain is M4; an M0 slot is always inline.
	if freeListExtent != -1 {
		return nil, errBadSuperblock
	}

	// Bound every variable region against the slot before allocating.
	if frontierOff != frontierArrayOff {
		return nil, errBadSuperblock
	}
	if freeOff != freeInlineOff(shardCount) {
		return nil, errBadSuperblock
	}
	frontierEnd := frontierOff + shardCount*frontierEntrySize
	if frontierEnd > slot-crcSize {
		return nil, errBadSuperblock
	}
	if freeListLen > uint64(inlineFreeCapacity(shardCount)) {
		return nil, errBadSuperblock
	}
	freeEnd := freeOff + int(freeListLen)*8
	if freeEnd > slot-crcSize {
		return nil, errBadSuperblock
	}

	sb := &superblock{
		generation:   binary.LittleEndian.Uint64(buf[16:24]),
		extentSize:   binary.LittleEndian.Uint64(buf[24:32]),
		extentCount:  binary.LittleEndian.Uint64(buf[32:40]),
		lsnHighWater: binary.LittleEndian.Uint64(buf[40:48]),
		snapshotRoot: int64(binary.LittleEndian.Uint64(buf[64:72])),
		snapshotLen:  binary.LittleEndian.Uint64(buf[72:80]),
		shardCount:   uint32(shardCount),
		frontiers:    make([]shardFrontier, shardCount),
		free:         make([]int64, freeListLen),
	}

	off := frontierOff
	for i := range sb.frontiers {
		sb.frontiers[i] = shardFrontier{
			frontierLSN: binary.LittleEndian.Uint64(buf[off : off+8]),
			tailExtent:  int64(binary.LittleEndian.Uint64(buf[off+8 : off+16])),
			tailOff:     binary.LittleEndian.Uint64(buf[off+16 : off+24]),
		}
		off += frontierEntrySize
	}

	off = freeOff
	for i := range sb.free {
		sb.free[i] = int64(binary.LittleEndian.Uint64(buf[off : off+8]))
		off += 8
	}

	return sb, nil
}

// pickNewer returns the valid slot with the higher generation, the current durable
// checkpoint (doc 03 section 3 alternation rule). Either argument may be nil (a torn
// slot that failed decode). It returns nil only if both are nil.
func pickNewer(a, b *superblock) *superblock {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case a.generation >= b.generation:
		return a
	default:
		return b
	}
}
