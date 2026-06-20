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
// index. The range and iteration path streams a k-way merge across the sources. The
// segments are organized into levels (L0 overlapping, L1 down disjoint), and Maintain
// runs a level-aware compaction that merges a level into the one below, dropping dead
// versions at the watermark and tombstones at the bottom. Each segment's Bloom filter is
// sized by the Monkey allocation for the level it is written at, spending more bits on
// the small shallow levels and fewer on the large deep ones. A large value is separated
// out of the segment into a value log at flush, leaving a pointer in the cell so a later
// compaction moves the pointer instead of rewriting the value (WiscKey), which also lets
// the engine hold a value larger than a page. What remains for later slices is the value
// log's garbage collection and the REMIX range index.
package lsm

import (
	"bytes"
	"context"
	"fmt"
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

	mu  sync.RWMutex
	mem *memtable
	// levels is the on-disk tree: levels[0] is L0, the overlapping run of flushed
	// memtables, and levels[i>=1] hold key-range-disjoint segments sorted by minKey,
	// the classic leveled invariant. A read folds every segment regardless of level;
	// the level structure exists so compaction can bound read fan-in and reclaim the
	// space shadowed versions waste.
	levels      [][]*segment
	memtableCap int

	// Compaction policy knobs (spec 06 §6). l0Trigger is the L0 segment count that
	// makes L0 a compaction candidate; levelRatio T is the size multiple between
	// adjacent levels; l1TargetBytes is L1's size target, from which each deeper level's
	// target grows by T; segTargetBytes is the size a compaction output segment is cut
	// at, so a level holds several runs and a later compaction touches only the
	// overlapping subset.
	l0Trigger      int
	levelRatio     int
	l1TargetBytes  int64
	segTargetBytes int
	// tierFanout is the run count the largest (deepest) level accumulates before it
	// self-merges. That level is tiered, not leveled: a compaction from above adds its
	// output as another run rather than merging it into a single sorted run, so the
	// bottom (where most of the data lives) is rewritten a factor of tierFanout less
	// often. The smaller levels stay leveled, one run each, where read and space cost
	// dominate. This is the Dostoevsky lazy-leveling shape (spec 06 §6).
	tierFanout int
	// compactCursor[i] is the user key the next level-i compaction starts at or after,
	// rotating the picked segment across the level so work spreads instead of hammering
	// one key range.
	compactCursor map[int][]byte

	// vlog is the WiscKey value log: when value separation is on, a flush writes a large
	// value into it and the segment cell keeps only a pointer, so compaction moves the
	// pointer instead of rewriting the value (spec 06 §7). valueSepThreshold is the value
	// size at or above which a flush separates; zero means separation is off, except that
	// a value too large to fit a segment cell is always separated so the cell ceiling
	// never rejects a write.
	vlog              *vlog
	valueSepThreshold int

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
	return &LSM{
		pgr:            pgr,
		mem:            newMemtable(defaultArenaCap),
		memtableCap:    defaultMemtableCap,
		l0Trigger:      defaultL0Trigger,
		levelRatio:     defaultLevelRatio,
		l1TargetBytes:  defaultL1TargetBytes,
		segTargetBytes: defaultSegTargetBytes,
		tierFanout:     defaultTierFanout,
		compactCursor:  map[int][]byte{},
		vlog:           newVLog(pgr),
	}
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
	if env != nil && env.Options.ValueSepThreshold > 0 {
		l.valueSepThreshold = env.Options.ValueSepThreshold
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
	// A flush is the one place values separate. Each cell the memtable yields is passed
	// through separateForFlush, which writes a large value into the vLog and rewrites the
	// cell as a KindSetSep pointer; small values and non-set kinds pass through untouched.
	// Separating only at flush, never at compaction, is the WiscKey win: once a value is in
	// the vLog every later compaction of its key moves the pointer, not the bytes.
	var sepErr error
	seg, err := writeSegment(l.pgr, bloomBitsForLevel(0, l.levelRatio), func(emit func(ik, val []byte) bool) {
		mem.scan(func(ik, val []byte) bool {
			oik, oval, e := l.separateForFlush(ik, val)
			if e != nil {
				sepErr = e
				return false
			}
			return emit(oik, oval)
		})
	})
	if err != nil {
		return err
	}
	if sepErr != nil {
		return sepErr
	}
	// Persist the vLog tail before the segment becomes visible, so every pointer the
	// segment carries resolves to durable bytes the moment a reader can see it.
	if err := l.vlog.sync(); err != nil {
		return err
	}
	if seg.numCells > 0 {
		// Record the segment in the MANIFEST before publishing it to the live set, so a
		// segment a reader can see is always one the catalog will name after a restart. A
		// flushed segment enters L0, the overlapping level that receives memtables.
		if err := l.appendEditLocked(manifestAdd, 0, seg.footer); err != nil {
			return err
		}
		l.addSegmentLocked(0, seg)
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

// separateForFlush decides whether a cell's value moves to the value log and, if so,
// rewrites the cell. Only a plain set separates: a tombstone has no value, and a merge
// operand or a TTL-framed value keeps its existing in-cell encoding (a value of either
// kind larger than a page is still rejected by the segment writer, the documented bound
// this slice does not lift). When shouldSeparate says yes, the value is appended to the
// vLog and the cell becomes a KindSetSep whose value field is the encoded pointer, the
// same user key and version. The caller holds l.mu.
func (l *LSM) separateForFlush(ik, val []byte) ([]byte, []byte, error) {
	if format.KindOf(ik) != format.KindSet || !l.shouldSeparate(ik, val) {
		return ik, val, nil
	}
	ptr, err := l.vlog.append(val)
	if err != nil {
		return nil, nil, err
	}
	nik := format.EncodeInternalKey(format.UserKey(ik), format.Version(ik), format.KindSetSep)
	return nik, format.AppendValuePointer(nil, ptr), nil
}

// shouldSeparate is true when the cell's value should move to the value log: when it
// reaches the configured threshold, or, regardless of the threshold, when the whole
// cell would not fit a segment data page. The second clause is what lets the engine
// store a value larger than a page at all, so it holds even with separation nominally
// off (threshold zero).
func (l *LSM) shouldSeparate(ik, val []byte) bool {
	if l.valueSepThreshold > 0 && len(val) >= l.valueSepThreshold {
		return true
	}
	maxCell := l.pgr.Header().UsablePageSize() - segDataHeaderSize
	cellLen := uvarintLen(uint64(len(ik))) + len(ik) + uvarintLen(uint64(len(val))) + len(val)
	return cellLen > maxCell
}

// materializeOp turns one gathered cell into the format.Op the shared resolver folds,
// dereferencing a separated value through the vLog along the way. A KindSetSep cell is
// presented to Fold as an ordinary set: under keysOnly the value is left nil and the
// vLog is never touched, honoring the KeysOnly contract that a key-only scan reads no
// value; otherwise the pointer is followed and the literal bytes fill the op. Every
// other kind goes straight through format.OpFromCell, so the resolver, the oracle, and
// the B-tree core never see a KindSetSep. The returned op owns its value bytes, so the
// caller may keep it past the next source advance. The caller holds l.mu.
func (l *LSM) materializeOp(ik, val []byte, now uint64, keysOnly bool) (format.Op, bool, error) {
	if format.KindOf(ik) == format.KindSetSep {
		op := format.Op{Version: format.Version(ik), Kind: format.KindSet}
		if keysOnly {
			return op, true, nil
		}
		ptr, ok := format.DecodeValuePointer(val)
		if !ok {
			return format.Op{}, false, fmt.Errorf("lsm: corrupt value pointer in cell")
		}
		bytes, err := l.vlog.read(ptr)
		if err != nil {
			return format.Op{}, false, err
		}
		op.Value = bytes
		return op, true, nil
	}
	op, ok := format.OpFromCell(ik, val, now)
	if ok && op.Value != nil {
		op.Value = append([]byte(nil), op.Value...)
	}
	return op, ok, nil
}

// Maintain implements engine.Engine. It runs at most one compaction per call: the
// lazy-leveling policy scores every level and runs the most urgent action, pushing a run
// from a leveled level down into the level below, self-merging the tiered bottom when its
// runs have stacked too deep, or descending the bottom when it has outgrown its target
// (spec 06 §6). Every merge drops the versions no reader at or below the watermark can
// still observe, and a self-merge of the bottom also drops the point tombstones nothing
// lives under. A zero budget, the host's "do nothing" signal, is honored by skipping the
// work. Running one compaction unit per call, with each output segment cut at
// segTargetBytes, keeps a single Maintain from monopolizing I/O; the host calls it
// repeatedly to work a backlog down.
func (l *LSM) Maintain(ctx context.Context, budget engine.MaintBudget) (engine.MaintReport, error) {
	if budget.MaxPages <= 0 && budget.MaxBytes <= 0 {
		return engine.MaintReport{}, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.pickCompactionLocked()
	switch c.kind {
	case compactPushDown:
		return l.runCompactionLocked(c.level, budget.Watermark, c.wholeLevel)
	case compactSelfMerge:
		return l.runSelfMergeLocked(c.level, budget.Watermark)
	default:
		return engine.MaintReport{}, nil
	}
}

// Stats implements engine.Engine, reporting the memtable footprint plus the on-disk
// segment pages as the physical size. Live-key and amplification accounting sharpen
// once compaction and the watermark drop shadowed versions.
func (l *LSM) Stats() engine.EngineStats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	physical := int64(l.mem.size())
	pageSize := int64(l.pgr.PageSize())
	for _, seg := range l.allSegmentsLocked() {
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

// foldRange folds every source (the active memtable and each on-disk segment) into
// the sorted, visible (userKey, value) view at snap over the half-open user-key range
// [lower, upper): for each user key the newest version <= snap.Version, tombstones
// removed, merges folded, range deletes applied. A nil lower starts at the first key,
// a nil upper runs to the last.
//
// It streams a k-way merge across the sources rather than gathering and sorting the
// whole keyspace: each source is seeked to lower and yields its cells in internal-key
// order, the merge picks the next-smallest, and the fold consumes the stream one user
// key's group at a time. The resolution is the shared format.Fold, the same the oracle
// and the B-tree core run, so the view is byte-for-byte what the old gather-and-sort
// produced and any divergence is a bug in this plumbing, never in resolution. Because
// the merge seeks to lower and stops past upper, a narrow scan touches only the pages
// its range covers instead of reading the whole database.
func (l *LSM) foldRange(snap engine.Snapshot, lower, upper []byte, keysOnly bool) ([]resolved, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// The deletes that may cover a key in the range live in the memtable's live set and
	// each segment's persisted list, the same union the point path folds against.
	rangeDels := l.liveRangeDels()

	segs := l.allSegmentsLocked()
	sources := make([]mergeSource, 0, 1+len(segs))
	sources = append(sources, &memSource{sl: l.mem.sl})
	for _, seg := range segs {
		sources = append(sources, &segSource{pgr: l.pgr, seg: seg})
	}
	var target []byte
	if lower != nil {
		// The smallest internal key for lower's user key seeks past nothing in the group.
		target = format.EncodeInternalKey(lower, format.MaxVersion, format.KindDelete)
	}
	mi, err := newMergeIter(sources, target)
	if err != nil {
		return nil, err
	}

	var out []resolved
	var ops []format.Op
	var groupKey []byte
	flush := func() {
		if groupKey == nil {
			return
		}
		rd := format.NewestCoveringRangeDel(rangeDels, groupKey, snap.Version)
		if val, ok := format.Fold(ops, snap.Version, rd, l.merge); ok {
			out = append(out, resolved{uk: groupKey, val: append([]byte(nil), val...)})
		}
		ops = ops[:0]
		groupKey = nil
	}
	for mi.valid() {
		ik := mi.key()
		uk := format.UserKey(ik)
		if upper != nil && bytes.Compare(uk, upper) >= 0 {
			break
		}
		if groupKey != nil && !bytes.Equal(uk, groupKey) {
			flush()
		}
		if groupKey == nil {
			groupKey = append([]byte(nil), uk...)
		}
		// A range marker resolves through rangeDels, not as an op; materializeOp rejects
		// it. materializeOp also owns the value bytes it returns (it copies the borrowed
		// source value, and a separated value comes back fresh from the vLog), so nothing
		// here aliases mi.value(), which dies on the next advance.
		op, ok, err := l.materializeOp(ik, mi.value(), snap.Now, keysOnly)
		if err != nil {
			return nil, err
		}
		if ok {
			ops = append(ops, op)
		}
		if err := mi.next(); err != nil {
			return nil, err
		}
	}
	flush()
	if mi.err != nil {
		return nil, mi.err
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
	for _, seg := range l.allSegmentsLocked() {
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
		// A point Get always wants the value, so keysOnly is false: a separated value is
		// dereferenced through the vLog here.
		op, ok, err := l.materializeOp(c.ik, c.val, r.snap.Now, false)
		if err != nil {
			return nil, err
		}
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
	for _, seg := range l.allSegmentsLocked() {
		dels = append(dels, seg.rangeDels...)
	}
	return dels
}

func (r *reader) NewIter(opts engine.IterOptions) (engine.Cursor, error) {
	lower, upper := opts.Lower, opts.Upper
	if len(opts.Prefix) > 0 {
		lower = opts.Prefix
		upper = format.PrefixSuccessor(opts.Prefix)
	}
	// foldRange resolves only the requested range, so the cursor over the result holds
	// just those keys; the bidirectional cursor protocol is served from that materialized
	// slice, which is now the size of the range rather than the whole keyspace.
	view, err := r.l.foldRange(r.snap, lower, upper, opts.KeysOnly)
	if err != nil {
		return nil, err
	}
	return &cursor{view: view, pos: -1, reverse: opts.Reverse}, nil
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
