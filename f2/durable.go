package f2

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// The single durable file is the only on-disk design. It does two jobs at once.
// It is the larger-than-memory backing: a shard keeps a budget of pages in RAM
// and an evicted page is just a page already written here, reread by offset. And
// it is the durability substrate: records carry a CRC, pages carry a header, and
// a double-buffered superblock plus block-by-block recovery rebuild the store
// after a crash. The Durability dial only decides when the file is fsynced, so
// the same layout serves a non-durable larger-than-memory store (None) and a
// crash-safe one (Normal or Full) with no structural difference.
//
// File layout:
//
//	[0, sbSize)              superblock slot A
//	[sbSize, 2*sbSize)       superblock slot B
//	[dataStart, ...)         data blocks, one page each, numbered from 0
//
// The two superblock slots are written alternately so a torn superblock write
// leaves the previous good one intact; recovery picks the slot with the highest
// sequence that still checksums. Each data block opens with a header naming its
// shard and page index, so recovery can rebuild every shard from the file alone
// without trusting any in-memory directory, which is what lets a Full crash
// recover writes the last superblock never knew about.
const (
	// blockHeaderV1 is the original block header: magic, shard, page index, and a
	// CRC over the first twelve bytes. A file written before generations existed
	// carries these, and recovery still reads them, treating every such block as
	// generation zero.
	blockHeaderV1 = 16
	// blockHeaderSize is the current block header: the v1 fields plus a generation,
	// with the CRC over the first sixteen bytes. It is the size new pages reserve at
	// the front, so records start at this offset in a freshly written page.
	blockHeaderSize = 20
	sbSize          = 4096
	numSuperblocks  = 2
	dataStart       = int64(sbSize * numSuperblocks)

	sbMagic    uint32 = 0x32424446 // "FDB2"
	bhMagic    uint32 = 0x32485046 // "FPH2"
	durVersion uint32 = 2          // 2 added the per-block generation; 1 files still open
)

// durableFile owns the one file and hands out blocks. The mutex guards the block
// allocator, the sequence counter, and the superblock writes, all of which are
// rare (a page boundary or a checkpoint), so it never sits on the per-record path.
type durableFile struct {
	f        *os.File
	pageSize int64
	shards   int
	dial     Durability

	mu        sync.Mutex
	allocHigh int64   // next never-used data block
	free      []int64 // blocks a compaction retired, available for reuse
	seq       uint64

	// syncCount counts device barriers issued and syncNanos accumulates their wall
	// time, so Stats can report whether the Full dial is disk-bound. Both are read
	// without the lock, so they are atomics. syncHook, when set, replaces the platform
	// barrier in a test so a benchmark or a counter assertion does not pay F_FULLFSYNC.
	syncCount atomic.Int64
	syncNanos atomic.Int64
	syncHook  func(*os.File) error
}

// sync issues a device barrier and records its count and wall time. Every barrier in
// the durable path routes through here so the fsync accounting is complete. It takes
// no lock, so writeSuperblock (which holds d.mu) can call it without deadlocking.
func (d *durableFile) sync() error {
	d.syncCount.Add(1)
	start := time.Now()
	var err error
	if d.syncHook != nil {
		err = d.syncHook(d.f)
	} else {
		err = platformSyncData(d.f)
	}
	d.syncNanos.Add(int64(time.Since(start)))
	return err
}

// blockOffset is the byte offset of data block b in the file.
func (d *durableFile) blockOffset(b int64) int64 { return dataStart + b*d.pageSize }

// allocBlock reserves a data block, reusing one a compaction retired before
// extending the high-water. Reuse keeps the file from growing past the live page
// count under steady overwrite once compaction is freeing blocks. It is the only
// cross-shard write-side contention point and is taken once per page, not once per
// record. Popping a freed block before bumping allocHigh is the alloc-before-free
// ordering recovery relies on: every block id below allocHigh is either live in a
// surviving generation or sitting on the free list, never both and never lost.
func (d *durableFile) allocBlock() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if n := len(d.free); n > 0 {
		b := d.free[n-1]
		d.free = d.free[:n-1]
		return b
	}
	b := d.allocHigh
	d.allocHigh++
	return b
}

// freeBlock returns a block to the free list. The compactor calls it only after
// the epoch gate has cleared, so no lock-free reader can still be holding bytes
// from the block, which is what makes reuse safe (doc 06 section 6.2).
func (d *durableFile) freeBlock(b int64) {
	d.mu.Lock()
	d.free = append(d.free, b)
	d.mu.Unlock()
}

// writeBlockHeader stamps a page buffer's first bytes with the magic, the owning
// shard, the page index, the generation, and a header checksum, so recovery can
// attribute the block to a shard and generation and reject a block that was
// allocated but never written. The generation is what a whole-shard compaction
// bumps so recovery can prefer a rewritten generation over the one it replaced.
func writeBlockHeader(buf []byte, shardID, pageIndex int, gen uint32) {
	binary.LittleEndian.PutUint32(buf[0:], bhMagic)
	binary.LittleEndian.PutUint32(buf[4:], uint32(shardID))
	binary.LittleEndian.PutUint32(buf[8:], uint32(pageIndex))
	binary.LittleEndian.PutUint32(buf[12:], gen)
	binary.LittleEndian.PutUint32(buf[16:], crc32.Checksum(buf[0:16], crcTable))
}

// parseBlockHeader reads a block header back, reporting ok=false for a block that
// is unallocated (zero), garbage, or has a bad header checksum. It accepts both
// the current layout (with a generation) and the original one (no generation),
// returning the header's byte length so recovery knows where records start in the
// block, and the generation, which is zero for an original-layout block. The
// current layout is tried first; its CRC covers four more bytes, so a stray match
// against an original-layout block is a 2^-32 event the fallback would catch.
func parseBlockHeader(buf []byte) (shardID, pageIndex int, gen uint32, hdrLen int, ok bool) {
	if len(buf) < blockHeaderV1 || binary.LittleEndian.Uint32(buf[0:]) != bhMagic {
		return 0, 0, 0, 0, false
	}
	if len(buf) >= blockHeaderSize &&
		binary.LittleEndian.Uint32(buf[16:]) == crc32.Checksum(buf[0:16], crcTable) {
		return int(binary.LittleEndian.Uint32(buf[4:])),
			int(binary.LittleEndian.Uint32(buf[8:])),
			binary.LittleEndian.Uint32(buf[12:]), blockHeaderSize, true
	}
	if binary.LittleEndian.Uint32(buf[12:]) == crc32.Checksum(buf[0:12], crcTable) {
		return int(binary.LittleEndian.Uint32(buf[4:])),
			int(binary.LittleEndian.Uint32(buf[8:])), 0, blockHeaderV1, true
	}
	return 0, 0, 0, 0, false
}

// writeSuperblock advances the sequence and writes the current allocator
// high-water to the next slot, fsyncing unless the dial is None. The alternation
// of slots is the crash safety: a torn write never destroys the prior good slot.
func (d *durableFile) writeSuperblock() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seq++
	buf := make([]byte, sbSize)
	binary.LittleEndian.PutUint32(buf[0:], sbMagic)
	binary.LittleEndian.PutUint32(buf[4:], durVersion)
	binary.LittleEndian.PutUint32(buf[8:], uint32(d.pageSize))
	binary.LittleEndian.PutUint32(buf[12:], uint32(d.shards))
	binary.LittleEndian.PutUint64(buf[16:], d.seq)
	binary.LittleEndian.PutUint64(buf[24:], uint64(d.allocHigh))
	binary.LittleEndian.PutUint32(buf[32:], crc32.Checksum(buf[0:32], crcTable))
	slot := int64(d.seq % numSuperblocks)
	if _, err := d.f.WriteAt(buf, slot*sbSize); err != nil {
		return err
	}
	if d.dial != DurabilityNone {
		return d.sync()
	}
	return nil
}

// syncDir fsyncs the directory holding path so a freshly created file's directory
// entry is itself durable. Without it a crash right after create can lose the
// entry that names the file, and recovery would then find no file and silently
// treat the store as brand new, losing the whole acknowledged workload.
func syncDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// superblock is the parsed content of a superblock slot.
type superblock struct {
	pageSize  int64
	shards    int
	seq       uint64
	allocHigh int64
	valid     bool
}

// readSuperblock returns the newest valid superblock across the two slots, or an
// invalid zero value when neither slot checksums (a fresh or destroyed file).
func readSuperblock(f *os.File) superblock {
	var best superblock
	for i := int64(0); i < numSuperblocks; i++ {
		buf := make([]byte, sbSize)
		n, _ := f.ReadAt(buf, i*sbSize)
		if n < 36 || binary.LittleEndian.Uint32(buf[0:]) != sbMagic {
			continue
		}
		if binary.LittleEndian.Uint32(buf[32:]) != crc32.Checksum(buf[0:32], crcTable) {
			continue
		}
		seq := binary.LittleEndian.Uint64(buf[16:])
		if !best.valid || seq > best.seq {
			best = superblock{
				pageSize:  int64(binary.LittleEndian.Uint32(buf[8:])),
				shards:    int(binary.LittleEndian.Uint32(buf[12:])),
				seq:       seq,
				allocHigh: int64(binary.LittleEndian.Uint64(buf[24:])),
				valid:     true,
			}
		}
	}
	return best
}

// fileBlocks reports how many data blocks the file currently spans, used by
// recovery to scan past the last superblock's high-water and find pages a Full
// write fsynced after the last checkpoint.
func (d *durableFile) fileBlocks() (int64, error) {
	fi, err := d.f.Stat()
	if err != nil {
		return 0, err
	}
	size := fi.Size()
	if size <= dataStart {
		return 0, nil
	}
	// Round up: a Full write leaves the tail page partial, so the last block can
	// be shorter than a page, and flooring would drop it from the scan.
	return (size - dataStart + d.pageSize - 1) / d.pageSize, nil
}
