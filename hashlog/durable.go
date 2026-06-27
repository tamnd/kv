package hashlog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// durableFile is the one file a durable hashlog Store lives in (spec 2070 doc 03).
// At M0 it owns the superblock and the extent allocator: it opens or creates the
// file, lays down two checkpoint slots, reconstructs the allocator from the valid
// slot, and commits a new checkpoint by writing the older slot and fsyncing. It does
// not yet hold the per-shard log (M1) or any user value, so opening a durableFile
// does not touch the memory-only Store path.
type durableFile struct {
	f          *os.File
	path       string
	shardCount int
	extentSize int64
	slotSize   int
	sbSize     int64

	alloc *allocator

	// lsn is the per-store monotonic log sequence number (D4): every durable record
	// carries the next value, and recovery and compaction order by it. It is seeded on
	// open from the superblock's lsnHighWater and advanced by nextLSN on each durable
	// append. commit writes the current value back as the new high water.
	lsn atomic.Uint64

	// growMu guards file growth so concurrent shards extending the file for newly
	// allocated extents never shrink it past each other. fileEnd is the current file
	// size, advanced only forward.
	growMu  sync.Mutex
	fileEnd int64

	// sb is the current durable superblock (the content of the newer slot), and
	// newerSlot is which physical slot (0 for A, 1 for B) currently holds it. A commit
	// writes the other slot, so the last committed checkpoint is never in flight.
	sb        *superblock
	newerSlot int
}

// validateDurableTunables checks the durable-mode knobs (doc 03 section 10). It
// defaults ExtentSize to PageSize and enforces ExtentSize == PageSize, a power of
// two, and Path and Dir mutually exclusive. It returns the resolved tunables so the
// caller uses the defaulted ExtentSize.
func validateDurableTunables(t Tunables) (Tunables, error) {
	if t.Path == "" {
		return t, errors.New("hashlog: durable mode needs a Path")
	}
	if t.Dir != "" {
		return t, errors.New("hashlog: Path and Dir are mutually exclusive")
	}
	if t.ExtentSize == 0 {
		t.ExtentSize = t.PageSize
	}
	if t.ExtentSize != t.PageSize {
		return t, fmt.Errorf("hashlog: ExtentSize %d must equal PageSize %d", t.ExtentSize, t.PageSize)
	}
	if !isPowerOfTwo(t.ExtentSize) {
		return t, fmt.Errorf("hashlog: ExtentSize %d must be a power of two", t.ExtentSize)
	}
	if t.Shards <= 0 || t.Shards > maxShardCount {
		return t, fmt.Errorf("hashlog: Shards %d out of range", t.Shards)
	}
	return t, nil
}

// openDurableFile opens the file at path for a given shard count and extent size,
// creating it with two generation-0 slots if it does not yet exist, or reading and
// validating the existing superblock and reconstructing the allocator from it.
func openDurableFile(path string, shardCount int, extentSize int64) (*durableFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	d := &durableFile{
		f:          f,
		path:       path,
		shardCount: shardCount,
		extentSize: extentSize,
		slotSize:   superblockSlotSize(shardCount),
		sbSize:     superblockSize(shardCount),
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size() == 0 {
		if err := d.initFresh(); err != nil {
			f.Close()
			return nil, err
		}
		return d, nil
	}
	if err := d.readExisting(); err != nil {
		f.Close()
		return nil, err
	}
	return d, nil
}

// initFresh lays down two valid generation-0 slots on a brand-new file and stands up
// an empty allocator. Both slots are written and fsynced so a crash right after
// creation still leaves a recoverable file.
func (d *durableFile) initFresh() error {
	sb := newSuperblock(d.shardCount, d.extentSize)
	for slot := 0; slot < 2; slot++ {
		buf, err := sb.encode(slot)
		if err != nil {
			return err
		}
		if _, err := d.f.WriteAt(buf, int64(slot*d.slotSize)); err != nil {
			return err
		}
	}
	if err := d.f.Sync(); err != nil {
		return err
	}
	d.sb = sb
	d.newerSlot = 0
	d.alloc = newAllocator(0, nil)
	d.fileEnd = d.sbSize
	return nil
}

// readExisting reads both slots, picks the valid highest generation, validates it
// against the open-time tunables, and reconstructs the allocator from it.
func (d *durableFile) readExisting() error {
	buf := make([]byte, d.sbSize)
	if _, err := d.f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	// A short read (a torn creation) leaves the tail zeroed, so the affected slot
	// fails its CRC and decodes to nil, which is the fail-closed path.
	slotA, _ := decodeSuperblock(buf[:d.slotSize])
	slotB, _ := decodeSuperblock(buf[d.slotSize:d.sbSize])
	newer := pickNewer(slotA, slotB)
	if newer == nil {
		return errors.New("hashlog: no valid superblock slot")
	}
	if int(newer.shardCount) != d.shardCount {
		return fmt.Errorf("hashlog: file has %d shards, opened with %d", newer.shardCount, d.shardCount)
	}
	if int64(newer.extentSize) != d.extentSize {
		return fmt.Errorf("hashlog: file has extent size %d, opened with %d", newer.extentSize, d.extentSize)
	}
	d.sb = newer
	if newer == slotA {
		d.newerSlot = 0
	} else {
		d.newerSlot = 1
	}
	d.alloc = newAllocator(int64(newer.extentCount), newer.free)
	d.lsn.Store(newer.lsnHighWater)
	fi, err := d.f.Stat()
	if err != nil {
		return err
	}
	d.fileEnd = fi.Size()
	return nil
}

// nextLSN returns the next log sequence number for a durable append. It is the
// per-store monotonic counter (D4), advanced with one atomic add so a writer on any
// shard gets a unique increasing value without holding a store-wide lock.
func (d *durableFile) nextLSN() uint64 {
	return d.lsn.Add(1)
}

// commit writes a new checkpoint into the older slot with a generation one higher
// than the current durable one, then fsyncs. The fsync is the atomic commit point:
// before it the durable state is the prior slot, after it the just-written slot wins
// by generation (doc 03 section 3). At M0 the frontier and snapshot fields carry
// over from the prior superblock as placeholders; the allocator state is what M0
// actually advances.
func (d *durableFile) commit() error {
	count, free := d.alloc.counts()
	if len(free) > inlineFreeCapacity(d.shardCount) {
		return fmt.Errorf("hashlog: free list of %d exceeds inline capacity %d; overflow chain is M4",
			len(free), inlineFreeCapacity(d.shardCount))
	}
	nsb := &superblock{
		generation:   d.sb.generation + 1,
		extentSize:   uint64(d.extentSize),
		extentCount:  uint64(count),
		lsnHighWater: d.lsn.Load(),
		snapshotRoot: d.sb.snapshotRoot,
		snapshotLen:  d.sb.snapshotLen,
		shardCount:   uint32(d.shardCount),
		frontiers:    d.sb.frontiers,
		free:         free,
	}
	older := 1 - d.newerSlot
	buf, err := nsb.encode(older)
	if err != nil {
		return err
	}
	if _, err := d.f.WriteAt(buf, int64(older*d.slotSize)); err != nil {
		return err
	}
	if err := d.f.Sync(); err != nil {
		return err
	}
	d.sb = nsb
	d.newerSlot = older
	return nil
}

// extentOffset returns the byte offset in the file of extent id's first byte.
func (d *durableFile) extentOffset(id int64) int64 {
	return extentByteOffset(d.sbSize, d.extentSize, id)
}

// growExtent extends the file so extent id exists in it, if it does not already. It
// is only-grow and concurrency-safe: two shards allocating and growing at once never
// shrink the file past each other (a freed-and-reused id is already in the file, so
// its end is below fileEnd and this is a no-op). The bytes through the extent's end
// exist before the log path writes records into them.
func (d *durableFile) growExtent(id int64) error {
	end := d.extentOffset(id) + d.extentSize
	d.growMu.Lock()
	defer d.growMu.Unlock()
	if end <= d.fileEnd {
		return nil
	}
	if err := d.f.Truncate(end); err != nil {
		return err
	}
	d.fileEnd = end
	return nil
}

// Close closes the file. The durableFile must not be used afterward.
func (d *durableFile) Close() error {
	if d.f == nil {
		return nil
	}
	err := d.f.Close()
	d.f = nil
	return err
}
