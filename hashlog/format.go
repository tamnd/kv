package hashlog

import "hash/crc32"

// This file holds the shared on-disk format constants and arithmetic for the
// durable single-file layout (spec 2070 doc 03). The durable layout is opt-in: it
// is reached only when a Store is opened against a Path, and the memory-only
// DefaultTunables path never touches any of it.

// crc32cTable is the one checksum primitive for the whole on-disk format: the
// superblock slot, the extent header, and the log record all use CRC32C
// (Castagnoli), which has hardware acceleration on amd64 and arm64 and is a stdlib
// algorithm with no dependency (doc 03 section 6).
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// crc32c returns the Castagnoli CRC32C of b.
func crc32c(b []byte) uint32 {
	return crc32.Checksum(b, crc32cTable)
}

// Superblock layout constants (doc 03 section 3).
const (
	// sbMagic identifies a hashlog superblock slot. Eight bytes so it is a single
	// fixed-width read at the head of a slot.
	sbMagic = "HLOGSB\x00\x00"

	// sbFormatVersion is the on-disk format version. It starts at 1 and a reader
	// rejects a slot whose version it does not understand.
	sbFormatVersion = 1

	// sbSlotHeaderSize is the fixed-field prefix of a slot, before the per-shard
	// frontier array (doc 03 section 3 field table: 0..96).
	sbSlotHeaderSize = 96

	// frontierEntrySize is the per-shard frontier tuple: frontierLSN, tailExtent,
	// tailOff, each 8 bytes.
	frontierEntrySize = 24

	// sbBlockSize is the device block the slot is rounded up to. 4 KiB is the modern
	// device block and the largest size we can hope a single write lands as one unit.
	sbBlockSize = 4096

	// crcSize is the trailing CRC32C width on every checksummed structure.
	crcSize = 4

	// maxShardCount bounds shardCount read from disk so a malformed superblock cannot
	// drive an unbounded allocation during decode (fail-closed, doc 08 section 4.4).
	maxShardCount = 1 << 20
)

// superblockSlotSize returns the byte size of one superblock slot for a given shard
// count: the fixed header plus the per-shard frontier array plus the trailing CRC,
// rounded up to a whole number of 4 KiB blocks, at least one block.
//
// Doc 03 section 3 names "4 KiB by default" but also that the slot is rounded up to
// hold the variable-length frontier array. At the default 256 shards the frontier
// array alone is 256*24 = 6144 bytes, past 4 KiB, so the slot is computed, not
// pinned: a small shard count keeps the 4 KiB slot the doc names, 256 shards get an
// 8 KiB slot (recorded in the implementation README spec-resolution note).
func superblockSlotSize(shardCount int) int {
	raw := sbSlotHeaderSize + shardCount*frontierEntrySize + crcSize
	blocks := (raw + sbBlockSize - 1) / sbBlockSize
	if blocks < 1 {
		blocks = 1
	}
	return blocks * sbBlockSize
}

// superblockSize returns the byte size of the whole superblock region (two slots)
// for a shard count. The extent pool begins at this offset.
func superblockSize(shardCount int) int64 {
	return int64(2 * superblockSlotSize(shardCount))
}

// frontierArrayOff is the byte offset within a slot where the per-shard frontier
// array begins. It sits right after the fixed header.
const frontierArrayOff = sbSlotHeaderSize

// freeInlineOff returns the byte offset within a slot where the inline free-id list
// begins: right after the frontier array.
func freeInlineOff(shardCount int) int {
	return sbSlotHeaderSize + shardCount*frontierEntrySize
}

// inlineFreeCapacity returns how many free extent ids fit inline in a slot, after
// the header, the frontier array, and the trailing CRC. A free list larger than
// this needs the overflow chain (deferred to M4).
func inlineFreeCapacity(shardCount int) int {
	slot := superblockSlotSize(shardCount)
	avail := slot - freeInlineOff(shardCount) - crcSize
	if avail < 0 {
		return 0
	}
	return avail / 8
}

// extentByteOffset returns the byte offset in the file of extent id's first byte:
// the superblock region, then id whole extents. The superblock never moves and the
// file never grows by a partial extent, so this mapping is a constant for the life
// of the file (doc 03 section 1). It is the whole addressing story at the file
// level; there is no second-level extent directory on disk.
func extentByteOffset(sbSize, extentSize, id int64) int64 {
	return sbSize + id*extentSize
}

// isPowerOfTwo reports whether x is a positive power of two.
func isPowerOfTwo(x int) bool {
	return x > 0 && x&(x-1) == 0
}
