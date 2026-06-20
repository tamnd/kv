// Package lsm implements the log-structured merge engine (spec 06), the core a
// workload opts into when write throughput dominates. It satisfies the same
// engine.Engine seam as the B-tree core (spec 04), so every layer above the seam
// (transactions, iterators, cache, API, CLI, server) drives it unchanged.
//
// The engine is built in vertical slices. The write path is an arena-backed
// skip-list memtable that Apply inserts into; when it fills, the engine flushes it
// to an immutable on-disk segment and starts a fresh one, so memory stays bounded.
// The read path folds the memtable and every segment into one snapshot view through
// the shared MVCC resolution the oracle and the B-tree core use. Durability splits
// between the WAL and the MANIFEST: a flush writes a segment and records it in the
// MANIFEST, the embedded catalog anchored at the header's engine root, and reports
// the flushed batch's LSN through DurableLSN so the host reclaims the WAL behind it.
// On open the engine loads the MANIFEST to rebuild the segment set, then the WAL tail
// replays the batches committed after the last checkpoint into a fresh memtable over
// it. A point read seeks each source for one key, the memtable through its skip list
// and each segment through a persisted block index, and folds just that key's group;
// a per-segment Bloom filter lets a point miss skip a segment without touching its
// index. The range and iteration path still folds the full snapshot. What remains for
// later slices is leveled compaction, the streaming heap-merge, and value separation.
package lsm

import (
	"bytes"
	"context"
	"sort"
	"sync"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// defaultArenaCap is the memtable arena's initial size. It grows geometrically, so
// this is a starting point that avoids both over-allocating for a tiny database and
// repeatedly doubling for a large one.
const defaultArenaCap = 1 << 20 // 1 MiB

// defaultMemtableCap is the byte size at which the active memtable is flushed to an
// on-disk segment (spec 06 §2, WithMemtableSize). It bounds how much applied data
// lives in memory at once.
const defaultMemtableCap = 64 << 20 // 64 MiB

// LSM is the log-structured merge engine handle (spec 06 §9).
type LSM struct {
	pgr *pager.Pager

	mu          sync.RWMutex
	mem         *memtable
	segments    []*segment
	memtableCap int

	// LSN tracking for the durable-mark seam. pendingLSN is the commit LSN the host
	// noted for the batch about to be applied; memMaxLSN is the largest LSN of any
	// batch in the active memtable; durableLSN is the largest LSN whose data a flush
	// has captured into an on-disk segment the MANIFEST records.
	pendingLSN uint64
	memMaxLSN  uint64
	durableLSN uint64

	merge func(existing, operand []byte) []byte
	env   *engine.Env
}

// New returns an LSM core bound to a pager. The pager is the durable substrate
// flushes write segments to and that this core reads them back from.
func New(pgr *pager.Pager) *LSM {
	return &LSM{pgr: pgr, mem: newMemtable(defaultArenaCap), memtableCap: defaultMemtableCap}
}

// Kind implements engine.Engine.
func (l *LSM) Kind() engine.Kind { return engine.LSM }

// Open implements engine.Engine. It loads the MANIFEST, rebuilding the segment set the
// last checkpoint recorded, and seeds the durable mark from the pager's checkpoint
// boundary, since those segments hold every batch durable through it. Open runs before
// redo, so the WAL tail then replays the batches committed after that boundary into a
// fresh memtable layered over the loaded segments, with no overlap between the two.
func (l *LSM) Open(env *engine.Env) error {
	l.env = env
	l.mu.Lock()
	defer l.mu.Unlock()
	if env != nil && env.Options.MemtableSize > 0 {
		l.memtableCap = env.Options.MemtableSize
	}
	l.durableLSN = l.pgr.CheckpointLSN()
	return l.loadManifestLocked()
}

// Close implements engine.Engine. It drops the in-memory state; the host checkpoints
// before close, folding the segment and MANIFEST pages a flush wrote, so the next open
// rebuilds the segment set from the MANIFEST and replays only the WAL tail past the
// checkpoint into a fresh memtable.
func (l *LSM) Close() error { return nil }

// SetMergeFunc installs the merge resolver used during read-time version
// resolution, the same hook the B-tree core and the oracle expose.
func (l *LSM) SetMergeFunc(f func(existing, operand []byte) []byte) {
	l.mu.Lock()
	l.merge = f
	l.mu.Unlock()
}

// Apply implements engine.Engine: it inserts every entry into the active memtable.
// The batch is already durable in the WAL, so the insert is pure in-memory work.
// When the memtable grows past its cap the engine flushes it to an on-disk segment
// and starts a fresh one, so the resident set stays bounded (spec 06 §2). The flush
// runs synchronously under the write lock; the sealed-queue and background flush
// that hide its latency are a later optimization.
func (l *LSM) Apply(batch *engine.WriteBatch, commitVersion uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mem.count() > 0 && l.mem.size() >= l.memtableCap {
		if err := l.flushLocked(); err != nil {
			return err
		}
	}
	for _, e := range batch.Entries() {
		l.mem.set(e.InternalKey, e.Value)
	}
	// Track the largest WAL LSN now resident in the active memtable, so a later flush
	// knows how far the segment it writes lets the host reclaim the log.
	if l.pendingLSN > l.memMaxLSN {
		l.memMaxLSN = l.pendingLSN
	}
	return nil
}

// NoteLSN records the WAL commit LSN of the batch the host is about to Apply, the
// optional seam the host calls only for an engine that tracks a durable mark (spec 06
// §4). The host calls it under its single writer lock immediately before Apply, both
// for a live commit and for each batch redo replays, so the LSN the next Apply folds
// into the memtable is always this one. The B-tree core, which needs no durable mark,
// does not implement it.
func (l *LSM) NoteLSN(lsn uint64) {
	l.mu.Lock()
	l.pendingLSN = lsn
	l.mu.Unlock()
}

// flushLocked writes the active memtable to a new on-disk segment, appends the
// segment to the live set, and swaps in an empty memtable. The caller holds l.mu.
// The segment's pages are dirtied through the pager and folded by the next
// checkpoint, the same path every engine write takes; until the MANIFEST records
// the segment (a later slice) the live set is in-memory only and the WAL remains
// the sole cross-restart record, so durability is unchanged.
func (l *LSM) flushLocked() error {
	mem := l.mem
	sealedLSN := l.memMaxLSN
	seg, err := writeSegment(l.pgr, func(emit func(ik, val []byte) bool) {
		mem.scan(emit)
	})
	if err != nil {
		return err
	}
	if seg.numCells > 0 {
		// Record the segment in the MANIFEST before publishing it to the live set, so a
		// segment a reader can see is always one the catalog will name after a restart.
		if err := l.appendEditLocked(manifestAdd, seg.footer); err != nil {
			return err
		}
		l.segments = append(l.segments, seg)
	}
	// Every batch in the sealed memtable now lives in the segment, so the host may
	// reclaim the WAL up to its largest LSN once the checkpoint folds these pages. The
	// fold happens before the WAL resets, so advancing the mark here is safe even though
	// the pages are not yet on disk; a crash before the fold simply loses this in-memory
	// advance and the kept WAL replays the batches again.
	if sealedLSN > l.durableLSN {
		l.durableLSN = sealedLSN
	}
	l.mem = newMemtable(defaultArenaCap)
	l.memMaxLSN = 0
	return nil
}

// Maintain implements engine.Engine. Compaction and flush land in later slices; for
// now the memtable-only core has no background work.
func (l *LSM) Maintain(ctx context.Context, budget engine.MaintBudget) (engine.MaintReport, error) {
	return engine.MaintReport{}, nil
}

// Stats implements engine.Engine, reporting the memtable footprint plus the on-disk
// segment pages as the physical size. Live-key and amplification accounting sharpen
// once compaction and the watermark drop shadowed versions.
func (l *LSM) Stats() engine.EngineStats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	physical := int64(l.mem.size())
	pageSize := int64(l.pgr.PageSize())
	for _, seg := range l.segments {
		physical += int64(seg.pages) * pageSize
	}
	return engine.EngineStats{
		PhysicalBytes: physical,
		Amplification: 1,
	}
}

// Reclaim implements engine.Engine: nothing on disk to reclaim yet.
func (l *LSM) Reclaim(budget int) (int, error) { return 0, nil }

// DurableLSN reports the highest WAL LSN whose effects the engine has persisted to
// the main file, the mark past which the host must not fold and reset the WAL
// (spec 06 §4). The LSM core's applied writes live in the memtable until a flush turns
// it into an on-disk segment the MANIFEST records; the mark is the largest LSN any
// such flush has captured. It starts at the pager's checkpoint boundary, since the
// segments Open loaded hold every batch durable through it, and advances on each
// flush. The host folds to this mark and keeps the WAL frames past it, replaying them
// into a fresh memtable on the next open. The B-tree core, whose every Apply lands in
// pages the checkpoint folds, does not implement this method and the host folds the
// entire log.
func (l *LSM) DurableLSN() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.durableLSN
}

// RecoverFinished implements engine.Engine. The LSM core rebuilds its segment set from
// the MANIFEST at Open, before redo, so by here the on-disk runs and the redone
// memtable are both in place and there is nothing left to do. The hook remains the
// point a later slice will settle any recovery-time invariants the level structure
// needs.
func (l *LSM) RecoverFinished(lastVersion uint64) error { return nil }

// NewReader implements engine.Engine.
func (l *LSM) NewReader(snap engine.Snapshot) (engine.Reader, error) {
	return &reader{l: l, snap: snap}, nil
}

// resolved is one user key and its MVCC-resolved value at a snapshot.
type resolved struct {
	uk  []byte
	val []byte
}

// srcCell is one internal-key/value pair gathered from a source during a snapshot.
type srcCell struct {
	ik  []byte
	val []byte
}

// snapshot folds every source (the active memtable and each on-disk segment) into
// the sorted, visible (userKey, value) view at snap: for each user key the newest
// version <= snap.Version, tombstones removed, merges folded, range deletes applied.
// It gathers all cells, sorts them into internal-key order, and resolves each user
// key's version group with the shared format.Fold, so any divergence from the oracle
// or the B-tree core is a bug in this plumbing, never in resolution. Gathering and
// sorting the whole keyspace per read is the bring-up shape; the streaming heap-merge
// across sources (spec 06 §9) replaces it once the level structure exists.
func (l *LSM) snapshot(snap engine.Snapshot) ([]resolved, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var cells []srcCell
	var rangeDels []format.RangeDel
	collect := func(ik, val []byte) bool {
		ikc := append([]byte(nil), ik...)
		valc := append([]byte(nil), val...)
		cells = append(cells, srcCell{ik: ikc, val: valc})
		// A range-delete marker is stored as a cell in every source; reconstruct its
		// interval here so range deletes from segments fold exactly as memtable ones do.
		if format.KindOf(ikc) == format.KindRangeBegin {
			rangeDels = append(rangeDels, format.RangeDel{
				Lo:      append([]byte(nil), format.UserKey(ikc)...),
				Hi:      valc,
				Version: format.Version(ikc),
			})
		}
		return true
	}
	l.mem.scan(collect)
	for _, seg := range l.segments {
		if err := seg.scan(l.pgr, collect); err != nil {
			return nil, err
		}
	}

	// Order the merged cells: user ascending, version descending, kind ascending, so
	// each user key's versions arrive contiguous and newest-first.
	sort.SliceStable(cells, func(i, j int) bool {
		return format.CompareInternal(cells[i].ik, cells[j].ik) < 0
	})

	var out []resolved
	var i int
	for i < len(cells) {
		uk := format.UserKey(cells[i].ik)
		var ops []format.Op
		j := i
		for j < len(cells) && bytes.Equal(format.UserKey(cells[j].ik), uk) {
			op, ok := format.OpFromCell(cells[j].ik, cells[j].val, snap.Now)
			j++
			if !ok {
				continue // range markers resolve through rangeDels, not as ops
			}
			ops = append(ops, op)
		}
		i = j

		rd := format.NewestCoveringRangeDel(rangeDels, uk, snap.Version)
		val, ok := format.Fold(ops, snap.Version, rd, l.merge)
		if !ok {
			continue
		}
		out = append(out, resolved{
			uk:  append([]byte(nil), uk...),
			val: append([]byte(nil), val...),
		})
	}
	return out, nil
}

// reader is a point/range read view at a fixed snapshot. It resolves the memtable
// once and serves reads from the materialized view, the same shape as the model
// engine's reader; the segment slices replace this with a streaming merge across
// memtable and on-disk levels.
type reader struct {
	l    *LSM
	snap engine.Snapshot
}

// Get resolves one user key without materializing the whole keyspace: it gathers
// the key's version group from the memtable, seeking its skip list, and from each
// segment, seeking its block index, then folds that group with the range deletes
// that cover the key. Only the pages that may hold the key are read, so a point read
// no longer scans every source. The range and iteration path still folds the full
// snapshot until the streaming heap-merge lands.
func (r *reader) Get(userKey []byte) ([]byte, error) {
	l := r.l
	l.mu.RLock()
	defer l.mu.RUnlock()

	var cells []srcCell
	collect := func(ik, val []byte) bool {
		cells = append(cells, srcCell{
			ik:  append([]byte(nil), ik...),
			val: append([]byte(nil), val...),
		})
		return true
	}
	l.mem.getGroup(userKey, collect)
	for _, seg := range l.segments {
		// The Bloom filter answers a miss definitively, so a segment it rejects holds
		// no version of the key and its block index need never be touched. A segment
		// without a filter has a nil one, whose mayContain passes, so it is still read.
		if !seg.filter.mayContain(userKey) {
			continue
		}
		if err := seg.getGroup(l.pgr, userKey, collect); err != nil {
			return nil, err
		}
	}

	// Order the gathered group newest version first, the order Fold expects.
	sort.SliceStable(cells, func(i, j int) bool {
		return format.CompareInternal(cells[i].ik, cells[j].ik) < 0
	})
	var ops []format.Op
	for _, c := range cells {
		op, ok := format.OpFromCell(c.ik, c.val, r.snap.Now)
		if !ok {
			continue // range markers resolve through rangeDels, not as ops
		}
		ops = append(ops, op)
	}

	// The deletes that may cover the key live in the memtable's live set and in each
	// segment's persisted range-delete list, so the fold sees every covering marker
	// without scanning the runs for them.
	rd := format.NewestCoveringRangeDel(l.liveRangeDels(), userKey, r.snap.Version)
	val, ok := format.Fold(ops, r.snap.Version, rd, l.merge)
	if !ok {
		return nil, engine.ErrNotFound
	}
	return append([]byte(nil), val...), nil
}

// liveRangeDels gathers every range-delete interval the engine holds, from the
// memtable's live set and each segment's persisted list, the set the fold filters to
// the markers that cover a key. The caller holds l.mu.
func (l *LSM) liveRangeDels() []format.RangeDel {
	dels := make([]format.RangeDel, 0, len(l.mem.rangeDels))
	dels = append(dels, l.mem.rangeDels...)
	for _, seg := range l.segments {
		dels = append(dels, seg.rangeDels...)
	}
	return dels
}

func (r *reader) NewIter(opts engine.IterOptions) (engine.Cursor, error) {
	view, err := r.l.snapshot(r.snap)
	if err != nil {
		return nil, err
	}
	lower, upper := opts.Lower, opts.Upper
	if len(opts.Prefix) > 0 {
		lower = opts.Prefix
		upper = format.PrefixSuccessor(opts.Prefix)
	}
	var filtered []resolved
	for _, e := range view {
		if lower != nil && bytes.Compare(e.uk, lower) < 0 {
			continue
		}
		if upper != nil && bytes.Compare(e.uk, upper) >= 0 {
			continue
		}
		filtered = append(filtered, e)
	}
	return &cursor{view: filtered, pos: -1, reverse: opts.Reverse}, nil
}

func (r *reader) Close() error { return nil }

// cursor walks a pre-resolved snapshot view; bounds and prefix are already applied,
// and reverse flips the direction of First/Last/Next/Prev.
type cursor struct {
	view    []resolved
	pos     int
	reverse bool
}

func (c *cursor) First() bool {
	if c.reverse {
		c.pos = len(c.view) - 1
	} else {
		c.pos = 0
	}
	return c.Valid()
}

func (c *cursor) Last() bool {
	if c.reverse {
		c.pos = 0
	} else {
		c.pos = len(c.view) - 1
	}
	return c.Valid()
}

func (c *cursor) Next() bool {
	if c.reverse {
		c.pos--
	} else {
		c.pos++
	}
	return c.Valid()
}

func (c *cursor) Prev() bool {
	if c.reverse {
		c.pos++
	} else {
		c.pos--
	}
	return c.Valid()
}

func (c *cursor) SeekGE(userKey []byte) bool {
	idx := sort.Search(len(c.view), func(i int) bool {
		return bytes.Compare(c.view[i].uk, userKey) >= 0
	})
	c.pos = idx
	return c.Valid()
}

func (c *cursor) SeekLT(userKey []byte) bool {
	idx := sort.Search(len(c.view), func(i int) bool {
		return bytes.Compare(c.view[i].uk, userKey) >= 0
	})
	c.pos = idx - 1
	return c.Valid()
}

func (c *cursor) Valid() bool { return c.pos >= 0 && c.pos < len(c.view) }

func (c *cursor) Key() []byte {
	if !c.Valid() {
		return nil
	}
	return c.view[c.pos].uk
}

func (c *cursor) InternalKey() []byte {
	if !c.Valid() {
		return nil
	}
	return format.EncodeInternalKey(c.view[c.pos].uk, format.MaxVersion, format.KindSet)
}

func (c *cursor) Value() (engine.LazyValue, error) {
	if !c.Valid() {
		return engine.LazyValue{}, nil
	}
	return engine.InlineValue(c.view[c.pos].val), nil
}

func (c *cursor) Error() error { return nil }
func (c *cursor) Close() error { return nil }
