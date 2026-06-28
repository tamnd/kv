package hashlog

import (
	"encoding/binary"
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
	extentSize int64 // the page/body size: bytes of an extent available for records
	stride     int64 // the on-disk extent size: extentSize plus the extent header
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

	// fileEndAtomic mirrors fileEnd as an atomic so the oversize read path (which bounds a
	// descriptor's cont run against the file end while holding only a shard read lock) can read
	// the size without taking growMu, which a concurrent shard may hold to extend the file. It
	// is advanced in lockstep with fileEnd under growMu, so a lock-free reader sees a value at
	// or below the true end, the safe under-count direction the bound needs (M9).
	fileEndAtomic atomic.Int64

	// syncCount counts device barriers issued, so a test can assert the dial's flush
	// points (None issues none, Full one per SET). syncHook, when set, replaces the
	// real barrier; the crash scaffold uses it to freeze the file image at an fsync
	// boundary. Production leaves it nil and goes straight to platformSyncData.
	syncCount atomic.Int64
	syncHook  func(*os.File) error

	// sb is the current durable superblock (the content of the newer slot), and
	// newerSlot is which physical slot (0 for A, 1 for B) currently holds it. A commit
	// writes the other slot, so the last committed checkpoint is never in flight. sb is
	// reassigned by a commit with no shard lock held, so it is only read single-threaded
	// (open, recovery) or from inside the committing goroutine itself.
	sb        *superblock
	newerSlot int

	// gen mirrors sb.generation as an atomic so the log-extent header writer (which runs
	// concurrently with a commit, under a shard lock the commit does not take) can stamp
	// the current generation without racing the sb pointer reassignment. It is advanced
	// in lockstep with every sb assignment. The stamp is informational at this milestone
	// (recovery does not validate it), so reading the pre- or post-commit value is equally
	// correct; the atomic only exists to make that read data-race free.
	gen atomic.Uint64

	// existed is true when the file already held a superblock on open, so the Store
	// must run recovery (M5) to rebuild each shard's index from the checkpoint plus the
	// log tail. A brand-new file (initFresh) leaves it false: there is nothing to
	// recover.
	existed bool

	// Checkpoint state (M4, doc 05). snapRoot and snapCount are the currently committed
	// index snapshot's contiguous extent run, freed when the next checkpoint supersedes
	// it; snapRoot is -1 when no snapshot has committed yet. snapBytes is the last
	// snapshot's stream length and bytesSinceCkpt counts durable record bytes appended
	// since the last checkpoint, both observability counters (doc 08 section 1.4).
	snapRoot       int64
	snapCount      int64
	snapBytes      atomic.Int64
	bytesSinceCkpt atomic.Int64

	// freeRoot and freeCount are the currently committed free-list overflow run, the
	// twin of snapRoot/snapCount for the allocator's free stack (doc 03 section 3). When
	// the free list outgrows the inline slot capacity a checkpoint writes it into a
	// contiguous extent run and records the head here; the next checkpoint rotates this
	// run free once its replacement is durable. freeRoot is -1 when the live free list
	// still fits inline and no run is committed.
	freeRoot  int64
	freeCount int64
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
	if t.Durability < DurabilityNone || t.Durability > DurabilityFull {
		return t, fmt.Errorf("hashlog: Durability %d out of range", t.Durability)
	}
	if t.CheckpointBytes == 0 {
		t.CheckpointBytes = 256 << 20
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
		stride:     extentSize + extentHeaderBytes,
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
	d.existed = true
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
	d.gen.Store(sb.generation)
	d.newerSlot = 0
	d.alloc = newAllocator(0, nil)
	d.fileEnd = d.sbSize
	d.fileEndAtomic.Store(d.fileEnd)
	d.snapRoot = -1
	d.snapCount = 0
	d.freeRoot = -1
	d.freeCount = 0
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
	d.gen.Store(newer.generation)
	if newer == slotA {
		d.newerSlot = 0
	} else {
		d.newerSlot = 1
	}
	d.alloc = newAllocator(int64(newer.extentCount), newer.free)
	d.lsn.Store(newer.lsnHighWater)
	// Reconstruct the committed snapshot's extent run so the next checkpoint frees it.
	// The run is contiguous (it was allocated as one, M4), so its extent count is the
	// stream length rounded up to whole extents.
	d.snapRoot = newer.snapshotRoot
	if newer.snapshotRoot >= 0 && newer.snapshotLen > 0 {
		d.snapCount = (int64(newer.snapshotLen) + d.stride - 1) / d.stride
	} else {
		d.snapRoot = -1
		d.snapCount = 0
	}
	d.snapBytes.Store(int64(newer.snapshotLen))
	// Reconstruct the committed free-list overflow run the same way, so the next
	// checkpoint rotates it free. Its extent count is the id count times 8 bytes rounded
	// up to whole strides. The run's ids themselves are advisory: recover rebuilds the
	// allocator from the physical scan (M5), and the open-time allocator below is seeded
	// from the inline list (empty for an overflow file) only as a placeholder until then.
	d.freeRoot = newer.freeListExtent
	if newer.freeListExtent >= 0 && newer.freeListLen > 0 {
		d.freeCount = (int64(newer.freeListLen)*8 + d.stride - 1) / d.stride
	} else {
		d.freeRoot = -1
		d.freeCount = 0
	}
	fi, err := d.f.Stat()
	if err != nil {
		return err
	}
	d.fileEnd = fi.Size()
	d.fileEndAtomic.Store(d.fileEnd)
	return nil
}

// nextLSN returns the next log sequence number for a durable append. It is the
// per-store monotonic counter (D4), advanced with one atomic add so a writer on any
// shard gets a unique increasing value without holding a store-wide lock.
func (d *durableFile) nextLSN() uint64 {
	return d.lsn.Add(1)
}

// syncData issues one true device barrier on the file (fdatasync on Linux,
// F_FULLFSYNC on macOS, doc 04 section 8) and counts it. A frontier advance is gated
// on this returning, so the barrier is the real one, not a plain Sync that on macOS
// would not flush the drive cache. The injectable hook lets a test freeze the file
// image at the boundary.
func (d *durableFile) syncData() error {
	d.syncCount.Add(1)
	if d.syncHook != nil {
		return d.syncHook(d.f)
	}
	return platformSyncData(d.f)
}

// commit writes a new checkpoint into the older slot with a generation one higher
// than the current durable one, then fsyncs. The fsync is the atomic commit point:
// before it the durable state is the prior slot, after it the just-written slot wins
// by generation (doc 03 section 3). At M0 the frontier and snapshot fields carry
// over from the prior superblock as placeholders; the allocator state is what M0
// actually advances.
func (d *durableFile) commit() error {
	_, free := d.alloc.counts()
	inline, freeExt, freeLen, err := d.persistFreeList(free, true)
	if err != nil {
		return err
	}
	// Re-read the count after persistFreeList: an overflow run may have grown the pool.
	count, _ := d.alloc.counts()
	nsb := &superblock{
		generation:     d.sb.generation + 1,
		extentSize:     uint64(d.extentSize),
		extentCount:    uint64(count),
		lsnHighWater:   d.lsn.Load(),
		snapshotRoot:   d.sb.snapshotRoot,
		snapshotLen:    d.sb.snapshotLen,
		shardCount:     uint32(d.shardCount),
		frontiers:      d.sb.frontiers,
		free:           inline,
		freeListExtent: freeExt,
		freeListLen:    freeLen,
	}
	return d.writeCheckpointSlot(nsb, d.f.Sync)
}

// persistFreeList decides how the allocator's free list is recorded in the next
// checkpoint slot (doc 03 section 3). A list that fits the inline slot capacity rides
// inline (freeListExtent -1); a larger one is written into a contiguous extent run and
// only its head and id count go in the slot. It returns the inline ids (nil for the
// overflow case), the head extent id (-1 inline), and the id count the slot records.
// The barrier flag forwards the durability dial: under Normal and Full the overflow run
// is synced before the slot that points at it commits, so a crash cannot leave the slot
// referencing an unwritten run.
func (d *durableFile) persistFreeList(free []int64, barrier bool) (inline []int64, freeExt int64, freeLen uint64, err error) {
	if len(free) <= inlineFreeCapacity(d.shardCount) {
		// The list fits inline now. If a prior checkpoint had spilled it to a run, that run
		// is superseded: drop it back to the allocator. The on-disk slot we are about to
		// write records the ids inline, and recovery rebuilds the allocator from the
		// physical scan regardless, so freeing the run in memory here leaks nothing.
		if d.freeRoot >= 0 {
			d.alloc.freeRun(d.freeRoot, d.freeCount)
			d.freeRoot = -1
			d.freeCount = 0
		}
		return free, -1, uint64(len(free)), nil
	}
	root, err := d.writeFreeList(free, barrier)
	if err != nil {
		return nil, -1, 0, err
	}
	return nil, root, uint64(len(free)), nil
}

// writeFreeList serialises the free-id list into a freshly allocated contiguous extent
// run and, when barrier is set, fsyncs it (the free-list twin of writeSnapshot). It then
// rotates the previously committed run free, only after the new one is durable and never
// reusing the old run for the new write, so a crash before the superblock commit leaves
// the old run intact for the prior checkpoint and orphans the half-written new one for
// recovery to reclaim. It returns the new run's head extent id.
func (d *durableFile) writeFreeList(free []int64, barrier bool) (int64, error) {
	stream := make([]byte, len(free)*8)
	for i, id := range free {
		binary.LittleEndian.PutUint64(stream[i*8:i*8+8], uint64(id))
	}
	n := (int64(len(stream)) + d.stride - 1) / d.stride
	if n < 1 {
		n = 1
	}
	root, _ := d.alloc.allocRun(n)
	if err := d.growExtent(root + n - 1); err != nil {
		d.alloc.freeRun(root, n)
		return 0, err
	}
	if _, err := d.f.WriteAt(stream, d.extentOffset(root)); err != nil {
		d.alloc.freeRun(root, n)
		return 0, err
	}
	if barrier {
		if err := d.syncData(); err != nil {
			d.alloc.freeRun(root, n)
			return 0, err
		}
	}
	if d.freeRoot >= 0 {
		d.alloc.freeRun(d.freeRoot, d.freeCount)
	}
	d.freeRoot = root
	d.freeCount = n
	return root, nil
}

// writeCheckpointSlot writes nsb into the older of the two superblock slots (never the
// newer, so the last committed checkpoint stays intact as the fallback) and then runs
// the durability barrier sync. That barrier is the atomic commit point (doc 05 section
// 4 step 6, the LMDB meta-page flip): before it returns the new checkpoint does not
// exist for recovery, after it returns the new slot wins by generation. In-memory sb
// and newerSlot are advanced only after the barrier succeeds, so a failed sync leaves
// the durableFile describing the prior committed checkpoint.
func (d *durableFile) writeCheckpointSlot(nsb *superblock, sync func() error) error {
	older := 1 - d.newerSlot
	buf, err := nsb.encode(older)
	if err != nil {
		return err
	}
	if _, err := d.f.WriteAt(buf, int64(older*d.slotSize)); err != nil {
		return err
	}
	if err := sync(); err != nil {
		return err
	}
	d.sb = nsb
	d.gen.Store(nsb.generation)
	d.newerSlot = older
	return nil
}

// writeSnapshot writes a snapshot stream into a freshly allocated contiguous extent
// run and, when barrier is set, fsyncs it (doc 05 section 4 steps 2-4). It then frees
// the previously committed snapshot's run back to the allocator so the next checkpoint
// can reuse it. The previous run is freed only after the new snapshot is durable, and
// the new run is never the previous run, so a crash before the superblock commit
// leaves the old snapshot intact and the half-written new one orphaned and reclaimed.
// It returns the new run's first extent id, which the superblock records as the
// snapshot root.
func (d *durableFile) writeSnapshot(stream []byte, barrier bool) (int64, error) {
	n := (int64(len(stream)) + d.stride - 1) / d.stride
	if n < 1 {
		n = 1
	}
	root, _ := d.alloc.allocRun(n)
	if err := d.growExtent(root + n - 1); err != nil {
		d.alloc.freeRun(root, n)
		return 0, err
	}
	if _, err := d.f.WriteAt(stream, d.extentOffset(root)); err != nil {
		d.alloc.freeRun(root, n)
		return 0, err
	}
	if barrier {
		if err := d.syncData(); err != nil {
			d.alloc.freeRun(root, n)
			return 0, err
		}
	}
	if d.snapRoot >= 0 {
		d.alloc.freeRun(d.snapRoot, d.snapCount)
	}
	d.snapRoot = root
	d.snapCount = n
	return root, nil
}

// commitCheckpoint records a new checkpoint: it builds the superblock with the new
// generation, the snapshot location, the per-shard frontiers, the allocator free list
// (now reflecting the freed superseded snapshot and the in-use new one), and the LSN
// high-water, then writes and barriers the slot (doc 05 section 4 step 5-6). On success
// it resets the bytes-since-checkpoint counter and records the snapshot size.
//
// pending holds the extents a compaction pass retired and this checkpoint should record
// durably free (M8, doc 06 section 7.3): they are written into the slot's free list and,
// only after the commit barrier returns, pushed onto the allocator's in-memory free stack
// so they become reusable. Freeing them in memory only after the durable record commits
// is what excludes the dangling-pointer-on-reuse hazard: a crash before the commit
// recovers the prior checkpoint, whose snapshot may still point into a pending extent, and
// that extent's bytes are intact because it was never reused (doc 06 section 7.4). The
// whole free list, base plus pending, rides the inline slot when it fits and spills to the
// free-list overflow run when it does not (doc 03 section 3), so a large reclamation
// always lands in one checkpoint. The overflow return is retained for the caller's API but
// is now always empty.
func (d *durableFile) commitCheckpoint(snapRoot int64, snapLen uint64, frontiers []shardFrontier, pending []int64, barrier bool, sync func() error) (overflow []int64, err error) {
	_, free := d.alloc.counts()
	// The pending extents are not yet in the allocator's free stack (they are freed in
	// memory only after the commit barrier below), so the durable free list this checkpoint
	// records is the current free stack plus pending, the post-commit free set.
	durFree := append(append([]int64(nil), free...), pending...)
	inline, freeExt, freeLen, err := d.persistFreeList(durFree, barrier)
	if err != nil {
		return nil, err
	}
	// Re-read the count after persistFreeList: an overflow run may have grown the pool.
	count, _ := d.alloc.counts()
	nsb := &superblock{
		generation:     d.sb.generation + 1,
		extentSize:     uint64(d.extentSize),
		extentCount:    uint64(count),
		lsnHighWater:   d.lsn.Load(),
		snapshotRoot:   snapRoot,
		snapshotLen:    snapLen,
		shardCount:     uint32(d.shardCount),
		frontiers:      frontiers,
		free:           inline,
		freeListExtent: freeExt,
		freeListLen:    freeLen,
	}
	if err := d.writeCheckpointSlot(nsb, sync); err != nil {
		return nil, err
	}
	// The durable record is committed, so the retired extents are now safe to reuse: push
	// them onto the in-memory free stack. A later alloc (compaction's own copy appends, or
	// an ordinary roll) hands them back out, which is what keeps the file bounded under
	// churn rather than growing an extent per overwrite.
	for _, id := range pending {
		d.alloc.freeExtent(id)
	}
	d.snapBytes.Store(int64(snapLen))
	d.bytesSinceCkpt.Store(0)
	return nil, nil
}

// extentOffset returns the byte offset in the file of extent id's first byte, the start
// of its header. Extents are spaced one stride apart, so consecutive ids are contiguous
// and a snapshot run reads back as one region.
func (d *durableFile) extentOffset(id int64) int64 {
	return extentByteOffset(d.sbSize, d.stride, id)
}

// logBodyOffset returns the byte offset of a log extent's first body byte: past the
// extent header. A shard's page bytes are written here, so a logical address maps to
// logBodyOffset(extent) plus the in-page offset (doc 03 section 5).
func (d *durableFile) logBodyOffset(id int64) int64 {
	return d.extentOffset(id) + extentHeaderBytes
}

// extentBodyOffset returns the byte offset of any extent's first body byte, past the
// header. It is logBodyOffset generalised to a non-log extent: the M9 oversize-cont
// extents hold raw value bytes in their bodies and read and write from here (doc 03
// section 7). The arithmetic is identical; the separate name keeps the log read path and
// the oversize read path each reading through a self-documenting call.
func (d *durableFile) extentBodyOffset(id int64) int64 {
	return d.extentOffset(id) + extentHeaderBytes
}

// growExtent extends the file so extent id exists in it, if it does not already. It
// is only-grow and concurrency-safe: two shards allocating and growing at once never
// shrink the file past each other (a freed-and-reused id is already in the file, so
// its end is below fileEnd and this is a no-op). The bytes through the extent's end
// exist before the log path writes records into them.
func (d *durableFile) growExtent(id int64) error {
	end := d.extentOffset(id) + d.stride
	d.growMu.Lock()
	defer d.growMu.Unlock()
	if end <= d.fileEnd {
		return nil
	}
	if err := d.f.Truncate(end); err != nil {
		return err
	}
	d.fileEnd = end
	d.fileEndAtomic.Store(end)
	return nil
}

// writeLogExtentHeader writes the self-describing header at the front of a freshly
// allocated log extent (doc 03 section 5): its kind, the owning shard, the previous
// extent in the shard's chain, the logical base address of its first body byte, and the
// allocator generation stamp. It is written before any record body lands in the extent,
// so recovery scanning the file can identify the extent as this shard's and place it at
// its base address. The next link is left at -1; recovery orders by base address and
// does not follow forward links at M5.
func (d *durableFile) writeLogExtentHeader(id int64, shardID int, prev, baseAddr int64) error {
	h := extentHeader{
		kind:       extentKindLog,
		shardID:    int32(shardID),
		prevExtent: prev,
		nextExtent: -1,
		baseAddr:   baseAddr,
		genStamp:   d.gen.Load(),
	}
	_, err := d.f.WriteAt(encodeExtentHeader(h), d.extentOffset(id))
	return err
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
