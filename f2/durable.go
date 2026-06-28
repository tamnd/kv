package f2

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
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
	blockHeaderSize = 16
	sbSize          = 4096
	numSuperblocks  = 2
	dataStart       = int64(sbSize * numSuperblocks)

	sbMagic    uint32 = 0x32424446 // "FDB2"
	bhMagic    uint32 = 0x32485046 // "FPH2"
	durVersion uint32 = 1
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
	allocHigh int64 // next free data block
	seq       uint64
}

// blockOffset is the byte offset of data block b in the file.
func (d *durableFile) blockOffset(b int64) int64 { return dataStart + b*d.pageSize }

// allocBlock reserves the next data block. It is the only cross-shard write-side
// contention point and is taken once per page, not once per record.
func (d *durableFile) allocBlock() int64 {
	d.mu.Lock()
	b := d.allocHigh
	d.allocHigh++
	d.mu.Unlock()
	return b
}

// writeBlockHeader stamps a page buffer's first bytes with the magic, the owning
// shard, the page index, and a header checksum, so recovery can attribute the
// block to a shard and reject a block that was allocated but never written.
func writeBlockHeader(buf []byte, shardID, pageIndex int) {
	binary.LittleEndian.PutUint32(buf[0:], bhMagic)
	binary.LittleEndian.PutUint32(buf[4:], uint32(shardID))
	binary.LittleEndian.PutUint32(buf[8:], uint32(pageIndex))
	binary.LittleEndian.PutUint32(buf[12:], crc32.Checksum(buf[0:12], crcTable))
}

// parseBlockHeader reads a block header back, reporting ok=false for a block that
// is unallocated (zero), garbage, or has a bad header checksum.
func parseBlockHeader(buf []byte) (shardID, pageIndex int, ok bool) {
	if len(buf) < blockHeaderSize || binary.LittleEndian.Uint32(buf[0:]) != bhMagic {
		return 0, 0, false
	}
	if binary.LittleEndian.Uint32(buf[12:]) != crc32.Checksum(buf[0:12], crcTable) {
		return 0, 0, false
	}
	return int(binary.LittleEndian.Uint32(buf[4:])), int(binary.LittleEndian.Uint32(buf[8:])), true
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
		return platformSyncData(d.f)
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
