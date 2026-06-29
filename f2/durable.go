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
	durVersion uint32 = 3          // 3 added the index snapshot pointer; 2 and 1 files still open

	// sbSnapOffset is where the index snapshot fields begin, after the version-2
	// core and its CRC. They carry their own CRC over [sbSnapOffset, sbSnapCRC), so a
	// version-2 reader that stops at the core CRC ignores them and a torn snapshot
	// pointer reads as no snapshot. Layout: snapRoot(8) snapSeq(8) snapShards(4) crc(4).
	sbSnapOffset = 36
	sbSnapCRC    = 56
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

	// snapRoot points at the committed index snapshot's first block (-1 for none),
	// snapSeq stamps the chain so a stale or torn one is rejected, snapShards is the
	// shard count it covers, and snapBlocks is the chain's block ids held in memory so
	// a re-checkpoint can free the prior chain after committing the next one. All are
	// guarded by mu, written only at a checkpoint.
	snapRoot   int64
	snapSeq    uint64
	snapShards int
	snapBlocks []int64

	// syncCount counts device barriers issued and syncNanos accumulates their wall
	// time, so Stats can report whether the Full dial is disk-bound. Both are read
	// without the lock, so they are atomics. syncHook, when set, replaces the platform
	// barrier in a test so a benchmark or a counter assertion does not pay F_FULLFSYNC.
	syncCount atomic.Int64
	syncNanos atomic.Int64
	syncHook  func(*os.File) error

	// Group commit (audit L4). Under the Full dial every shard fsyncs the shared file
	// once per record, yet one device barrier flushes every shard's pending writes, so
	// a caller that arrives while a barrier is in flight joins one batch and shares the
	// next barrier rather than issuing its own. smu and gcCond guard the batch state and
	// are independent of mu, so writeSuperblock can still sync while holding mu. syncing
	// is true while a batch's leader runs the barrier; cur is the batch accepting joiners,
	// nil when none is pending.
	smu     sync.Mutex
	gcCond  *sync.Cond
	syncing bool
	cur     *syncBatch
}

// syncBatch is one group-commit batch: the writers whose records a single device
// barrier flushes together. The batch's leader runs the one barrier, stores its
// error, marks it done, and wakes the rest, each of which returns this batch's
// error. Each caller captures its batch by pointer on entry, so a later batch's
// error never aliases onto an earlier batch's waiters.
type syncBatch struct {
	err  error
	done bool
}

// sync flushes the file to the device, coalescing concurrent callers into one
// barrier. A caller joins the batch currently accepting joiners (creating it if
// none) and either leads it, issuing the single barrier that covers everyone in the
// batch, or waits for its leader. The leader detaches the batch under smu before
// starting the barrier, and every batched record's WriteAt completed before its
// writer entered sync, so the barrier is guaranteed to start after every write it is
// meant to flush. This turns N shards fsyncing per record under the Full dial into
// far fewer device barriers without weakening the durability any caller is promised.
// It takes only smu, never mu, so writeSuperblock (which holds mu) can call it.
func (d *durableFile) sync() error {
	d.smu.Lock()
	if d.cur == nil {
		d.cur = &syncBatch{}
	}
	b := d.cur
	for !b.done {
		if d.syncing {
			d.gcCond.Wait()
			continue
		}
		// Become the leader for this batch. Detach it so callers arriving during the
		// barrier form the next batch, then run the one barrier without holding smu.
		d.syncing = true
		d.cur = nil
		d.smu.Unlock()
		err := d.barrier()
		d.smu.Lock()
		b.err = err
		b.done = true
		d.syncing = false
		d.gcCond.Broadcast()
	}
	err := b.err
	d.smu.Unlock()
	return err
}

// barrier issues one device barrier and records its count and wall time. Every
// barrier in the durable path routes through here so the fsync accounting is
// complete. It takes no lock, so the group-commit leader runs it with smu released.
func (d *durableFile) barrier() error {
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
	// The index snapshot pointer lives past the version-2 core CRC with its own CRC,
	// so an older reader ignores it and a torn pointer reads as no snapshot. snapRoot is
	// -1 until the first checkpoint writes a chain.
	binary.LittleEndian.PutUint64(buf[sbSnapOffset:], uint64(d.snapRoot))
	binary.LittleEndian.PutUint64(buf[sbSnapOffset+8:], d.snapSeq)
	binary.LittleEndian.PutUint32(buf[sbSnapOffset+16:], uint32(d.snapShards))
	binary.LittleEndian.PutUint32(buf[sbSnapCRC:], crc32.Checksum(buf[sbSnapOffset:sbSnapCRC], crcTable))
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

	// snapRoot is the committed index snapshot chain's first block, -1 when the file
	// carries no snapshot (a version-2 file, or a torn snapshot pointer). snapSeq and
	// snapShards describe the chain. snapValid records that the snapshot CRC checked,
	// so recovery never follows a partial pointer.
	snapRoot   int64
	snapSeq    uint64
	snapShards int
	snapValid  bool
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
				snapRoot:  -1,
			}
			// The snapshot pointer is present only on a version-3 slot and only when its
			// own CRC checks; otherwise the slot reads as carrying no snapshot.
			if n >= sbSnapCRC+4 &&
				binary.LittleEndian.Uint32(buf[sbSnapCRC:]) == crc32.Checksum(buf[sbSnapOffset:sbSnapCRC], crcTable) {
				best.snapRoot = int64(binary.LittleEndian.Uint64(buf[sbSnapOffset:]))
				best.snapSeq = binary.LittleEndian.Uint64(buf[sbSnapOffset+8:])
				best.snapShards = int(binary.LittleEndian.Uint32(buf[sbSnapOffset+16:]))
				best.snapValid = best.snapRoot >= 0
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
