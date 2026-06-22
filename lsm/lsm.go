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
// the engine hold a value larger than a page. When no segment compaction is due, Maintain
// instead runs the value log's garbage collector, which walks the chain, marks the pages
// any live pointer still references, and frees the dead ones back to the freelist. For
// scan-heavy workloads the optional REMIX range index folds each leveled level through one
// ordered cursor over its disjoint segments rather than one cursor per segment, shrinking
// the range merge's heap to one entry per level.
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
	// rangeIndex turns on the REMIX ordered index (spec 06 §6, spec 11 §5.3): when set,
	// a range scan folds each leveled level through one ordered levelSource over its
	// disjoint segments rather than one segSource per segment, so the heap-merge carries
	// one entry per level instead of one per segment. It is read from the options at Open
	// and is off by default. See remix.go.
	rangeIndex bool
	// filterKind selects the per-segment membership filter a flush and a compaction build
	// (spec 06 §5): filterBloom, the default, or filterRibbon, the opt-in Ribbon filter
	// that reaches the same false-positive rate in less space on the deep cold levels. It
	// is read from the options at Open. See ribbon.go.
	filterKind filterKind
	// compress turns on heat-tiered block compression of the segment data pages (spec 13):
	// a flush and a compaction pick a codec by the level they write to, cheap and fast on
	// the hot shallow levels, higher-ratio on the cold deep ones. It is read from the
	// options at Open and is off by default, so an unconfigured segment writes raw cell
	// pages byte-identical to before this knob existed. See codec.go and codecForLevel.
	compress bool
	// vlogHeadRecorded is the value-log head the MANIFEST last recorded, so a flush or GC
	// emits a manifestVLogHead edit only when the head actually moves: a flush moves it off
	// NoPage when the first value separates, the GC moves it forward when it frees the old
	// head. It is seeded from the MANIFEST at Open.
	vlogHeadRecorded format.PageNo

	// LSN tracking for the durable-mark seam. pendingLSN is the commit LSN the host
	// noted for the batch about to be applied; memMaxLSN is the largest LSN of any
	// batch in the active memtable; durableLSN is the largest LSN whose data a flush
	// has captured into an on-disk segment the MANIFEST records.
	pendingLSN uint64
	memMaxLSN  uint64
	durableLSN uint64

	// Background flush (flush.go, perf/03 W3). imm is the queue of sealed memtables
	// awaiting flush, oldest first: Apply seals the full active memtable into it and opens
	// a fresh one so a writer never waits for a segment write, and a reader folds these
	// between the active memtable and L0 since a sealed memtable is newer than any flushed
	// segment. flushCond, built over l.mu, coordinates the flusher, a backpressured Apply,
	// and the flushActive test waiter; closing and flusherDone drive shutdown; flushErr is
	// a sticky build failure surfaced to the next Apply; maxImm bounds the queue. flushMu
	// serializes a flush build (which appends separated values to the vLog) against the
	// value-log GC, the only other writer of the vLog chain, and is always taken before
	// l.mu when both are held.
	imm         []*immMem
	flushCond   *sync.Cond
	flusherUp   bool
	closing     bool
	flushErr    error
	flusherDone chan struct{}
	maxImm      int
	flushMu     sync.Mutex

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
	if env != nil && env.Options.RangeIndex {
		l.rangeIndex = true
	}
	if env != nil && env.Options.Filter == engine.FilterRibbon {
		l.filterKind = filterRibbon
	}
	if env != nil && env.Options.Compression {
		l.compress = true
	}
	l.durableLSN = l.pgr.CheckpointLSN()
	if err := l.loadManifestLocked(); err != nil {
		return err
	}
	// Start the background flusher last, once the segment set and durable mark are in
	// place, so the first seal it ever sees lands on a fully built engine.
	l.startFlusherLocked()
	return nil
}

// codecForLevel picks the block codec for a segment written to level, heat-tiering by
// depth (spec 13 §3.1): with compression off every level writes raw pages, and with it on
// the hot shallow levels take the fast codec, whose decompress cost is negligible against
// the I/O it saves on data that is read and rewritten often, while the cold deep levels
// take the higher-ratio codec, whose extra CPU is paid rarely because those pages are
// written once and read seldom. The boundary at level 2 keeps L0 and L1, where compaction
// churns, on the cheap codec.
func (l *LSM) codecForLevel(level int) codecID {
	if !l.compress {
		return codecNone
	}
	if level <= 1 {
		return codecFast
	}
	return codecHigh
}

// Close implements engine.Engine. It stops the background flusher and waits for any
// in-flight build to finish, so no goroutine outlives the engine and none touches the
// pager after the host closes it. It drops the in-memory state, including any still-sealed
// memtables; the host checkpoints before close, folding the segment and MANIFEST pages a
// flush wrote, so the next open rebuilds the segment set from the MANIFEST and replays only
// the WAL tail past the checkpoint into a fresh memtable.
func (l *LSM) Close() error {
	l.mu.Lock()
	if !l.flusherUp {
		l.mu.Unlock()
		return nil
	}
	l.closing = true
	l.flushCond.Broadcast()
	done := l.flusherDone
	l.flusherUp = false
	l.mu.Unlock()
	<-done
	return nil
}

// SetMergeFunc installs the merge resolver used during read-time version
// resolution, the same hook the B-tree core and the oracle expose.
func (l *LSM) SetMergeFunc(f func(existing, operand []byte) []byte) {
	l.mu.Lock()
	l.merge = f
	l.mu.Unlock()
}

// Apply implements engine.Engine: it inserts every entry into the active memtable.
// The batch is already durable in the WAL, so the insert is pure in-memory work.
// When the memtable grows past its cap the engine seals it into the flush queue and
// opens a fresh one, so the resident set stays bounded (spec 06 §2) without the writer
// waiting for the segment write: the background flusher (flush.go) drains the queue.
// The seal blocks only when the queue is already full, the backpressure that keeps a
// write burst that outruns the flusher from growing memory without bound.
func (l *LSM) Apply(batch *engine.WriteBatch, commitVersion uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.flushErr != nil {
		return l.flushErr
	}
	if l.mem.count() > 0 && l.mem.size() >= l.memtableCap {
		if err := l.sealForFlushLocked(); err != nil {
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

// buildSegmentFromMem serializes a sealed memtable to a new on-disk segment, separating
// large values into the value log as it goes, and syncs the vLog tail so every pointer the
// segment carries resolves to durable bytes the moment a reader can see it. It runs under
// flushMu but not l.mu (flush.go), so the foreground keeps inserting into the fresh active
// memtable while this serializes; it reads only the sealed memtable, which is write-frozen,
// and the open-time-immutable level and codec knobs. The returned segment is not yet
// visible: installSegmentLocked publishes it.
//
// A flush is the one place values separate. Each cell the memtable yields is passed through
// separateForFlush, which writes a large value into the vLog and rewrites the cell as a
// KindSetSep pointer; small values and non-set kinds pass through untouched. Separating only
// at flush, never at compaction, is the WiscKey win: once a value is in the vLog every later
// compaction of its key moves the pointer, not the bytes.
func (l *LSM) buildSegmentFromMem(mem *memtable) (*segment, error) {
	var sepErr error
	seg, err := writeSegment(l.pgr, bloomBitsForLevel(0, l.levelRatio), l.filterKind, l.codecForLevel(0), func(emit func(ik, val []byte) bool) {
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
		return nil, err
	}
	if sepErr != nil {
		return nil, sepErr
	}
	if err := l.vlog.sync(); err != nil {
		return nil, err
	}
	return seg, nil
}

// installSegmentLocked publishes a built segment: it records the value-log head in the
// MANIFEST if a separated value moved it, records the segment, adds it to L0, and advances
// the durable mark to the sealed memtable's largest LSN. The caller holds l.mu and flushMu.
// Recording in the MANIFEST before adding to the live set keeps a segment a reader can see
// always one the catalog will name after a restart.
//
// The durable advance is safe even though the segment pages are not yet on disk: the
// checkpoint folds them before the WAL resets, so a crash before the fold simply loses this
// in-memory advance and the kept WAL replays the batches again.
func (l *LSM) installSegmentLocked(seg *segment, sealedLSN uint64) error {
	if err := l.persistVLogHeadLocked(); err != nil {
		return err
	}
	if seg.numCells > 0 {
		if err := l.appendEditLocked(manifestAdd, 0, seg.footer); err != nil {
			return err
		}
		l.addSegmentLocked(0, seg)
	}
	if sealedLSN > l.durableLSN {
		l.durableLSN = sealedLSN
	}
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

// persistVLogHeadLocked records the current value-log head in the MANIFEST when it has
// moved since the last edit, the state Open needs to rebuild the append cursor and the GC
// needs to walk the chain. It is a no-op while the head is unchanged, so a steady stream
// of flushes that all append into the same chain emits the head edit just once. The
// caller holds l.mu.
func (l *LSM) persistVLogHeadLocked() error {
	if l.vlog.head == l.vlogHeadRecorded {
		return nil
	}
	if err := l.appendEditLocked(manifestVLogHead, 0, l.vlog.head); err != nil {
		return err
	}
	l.vlogHeadRecorded = l.vlog.head
	return nil
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
			return format.Op{}, false, errCorruptPointer
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
	// flushMu before l.mu, the same order the flusher takes them (flush.go): vLog GC and a
	// flush build are the two writers of the value-log chain, so they must not run at once,
	// and taking flushMu here serializes this maintenance pass against an in-flight flush.
	l.flushMu.Lock()
	defer l.flushMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	c, _ := l.pickCompactionLocked()
	switch c.kind {
	case compactPushDown:
		return l.runCompactionLocked(c.level, budget.Watermark, c.wholeLevel)
	case compactSelfMerge:
		return l.runSelfMergeLocked(c.level, budget.Watermark)
	default:
		// No segment compaction is due, so spend the budget reclaiming dead value-log
		// space instead, the vLog's analog of compaction (spec 06 §7).
		return l.runVLogGCLocked(budget)
	}
}

// Stats implements engine.Engine, reporting the memtable footprint plus the on-disk
// segment pages as the physical size. Live-key and amplification accounting sharpen
// once compaction and the watermark drop shadowed versions.
func (l *LSM) Stats() engine.EngineStats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	pageSize := int64(l.pgr.PageSize())
	physical := int64(l.mem.size())
	// Sealed memtables awaiting flush still hold their batches in memory, so count them
	// in the resident footprint until the flusher turns them into segment pages.
	for _, e := range l.imm {
		physical += int64(e.mem.size())
	}
	// Per-level shape for the compaction-backlog view (spec 19 §1.5): one entry per
	// level, youngest first, each carrying its segment count and on-disk bytes. The same
	// walk that sums physical bytes fills it, so the metric costs no extra pass.
	var levels []engine.LevelStats
	for i := range l.levels {
		var bytes int64
		for _, seg := range l.levels[i] {
			bytes += int64(seg.pages) * pageSize
		}
		physical += bytes
		levels = append(levels, engine.LevelStats{Segments: len(l.levels[i]), Bytes: bytes})
	}
	// The most-pending compaction's urgency, read without acting on it: a climbing
	// score is compaction losing to writes (spec 19 §1.5).
	_, score := l.pickCompactionLocked()
	return engine.EngineStats{
		PhysicalBytes:   physical,
		Amplification:   1,
		Levels:          levels,
		CompactionScore: score,
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
// key's group at a time. The sources come from rangeSourcesLocked, which with the REMIX
// index on folds each leveled level through one ordered levelSource instead of one source
// per segment, shrinking the heap without changing the merged order. The resolution is the
// shared format.Fold, the same the oracle and the B-tree core run, so the view is
// byte-for-byte what the old gather-and-sort produced and any divergence is a bug in this
// plumbing, never in resolution. Because the merge seeks to lower and stops past upper, a
// narrow scan touches only the pages its range covers instead of reading the whole
// database.
func (l *LSM) foldRange(snap engine.Snapshot, lower, upper []byte, keysOnly bool) ([]resolved, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// The deletes that may cover a key in the range live in the memtable's live set and
	// each segment's persisted list, the same union the point path folds against.
	rangeDels := l.liveRangeDels()

	sources := l.rangeSourcesLocked()
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

	// The deletes that may cover the key live in the memtable's live set and in each
	// segment's persisted range-delete list, all already in memory, so the covering
	// marker is known before any segment page is touched.
	rd := format.NewestCoveringRangeDel(l.liveRangeDels(), userKey, r.snap.Version)

	var ops []format.Op
	var matErr error
	collect := func(ik, val []byte) bool {
		// A point Get always wants the value, so keysOnly is false: a separated value is
		// dereferenced through the vLog here.
		op, ok, err := l.materializeOp(ik, val, r.snap.Now, false)
		if err != nil {
			matErr = err
			return false
		}
		if ok {
			ops = append(ops, op) // range markers resolve through rangeDels, not as ops
		}
		return true
	}

	// resolved reports whether the versions gathered so far already fix the value at the
	// snapshot. It mirrors Fold's base search: walking newest-first, the first visible set
	// or delete (or a covering range delete newer than the op under it) is the base, and
	// every older version is shadowed. Once a base is in hand, deeper (older) sources
	// cannot change the answer, so they need never be read.
	resolved := func() bool {
		sort.SliceStable(ops, func(i, j int) bool { return ops[i].Version > ops[j].Version })
		for _, op := range ops {
			if op.Version > r.snap.Version {
				continue // not visible at this snapshot
			}
			if rd > op.Version {
				return true // the covering range delete is the base
			}
			if op.Kind == format.KindMerge {
				continue // a merge needs an older base to fold over
			}
			return true // a set or delete fixes the result
		}
		return false
	}

	l.mem.getGroup(userKey, collect)
	if matErr != nil {
		return nil, matErr
	}
	// Fold the sealed memtables awaiting flush between the active memtable and the on-disk
	// levels: each is older than the active one but newer than any segment, and a sealed
	// memtable becomes a segment only in the one critical section that also pops it, so a
	// key is folded from exactly one of the two and a merge never applies its operand twice.
	// Newest-first means the most recently sealed (the tail of the queue) folds first.
	for i := len(l.imm) - 1; i >= 0; i-- {
		l.imm[i].mem.getGroup(userKey, collect)
		if matErr != nil {
			return nil, matErr
		}
	}
	if !resolved() {
		// Probe the on-disk levels shallowest first and stop at the first level that
		// supplies a base: a key's newest version always sits in the shallowest level
		// that holds it (a new write enters at the memtable and only ever migrates down),
		// so once a base is found a deeper level carries only shadowed versions. The whole
		// level is gathered before the check because a level may hold overlapping runs
		// (L0, and a tiered bottom kept in minKey order) whose relative age is not encoded
		// by their position, so a partial read of such a level could miss the newest
		// version; a level boundary is the coarsest unit at which newest-first holds.
		for lvl := 0; lvl < len(l.levels); lvl++ {
			touched := false
			for _, seg := range l.levels[lvl] {
				// The membership filter answers a miss definitively, so a segment it
				// rejects holds no version of the key and its block index need never be
				// touched. A segment without a filter has a nil one, so the read proceeds.
				if seg.filter != nil && !seg.filter.mayContain(userKey) {
					continue
				}
				if err := seg.getGroup(l.pgr, userKey, collect); err != nil {
					return nil, err
				}
				if matErr != nil {
					return nil, matErr
				}
				touched = true
			}
			if touched && resolved() {
				break
			}
		}
	}

	// ops is sorted newest-first by the final resolved() call, the order Fold expects;
	// the covering range delete folds in as a synthetic delete at rd.
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
	for _, e := range l.imm {
		dels = append(dels, e.mem.rangeDels...)
	}
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

// StreamForward reports that the LSM reader can serve the db layer's forward
// streaming scan (spec 04). The sources (memtable plus segments) keep every visible
// version in the merge order, so gathering one user-key group at a time off a fresh
// merge heap yields the same view foldRange would, without materializing the range.
func (r *reader) StreamForward() bool { return true }

// ScanForward returns the next visible user key strictly greater than after (or the
// first key >= lower when after is nil) within [lower, upper) at the reader's snapshot,
// or ok=false at end of range. It mirrors the B-tree reader's primitive: it holds no
// merge state across calls. Each call builds a fresh merge heap seeked to the start
// group and pulls forward only until the first visible group resolves, so the db layer
// can drop and retake l.mu between steps the way it does for a point Get. A flush or
// compaction between two calls is invisible: the next call re-seeks the new sources,
// and the fixed snapshot version keeps the sequence consistent.
//
// Re-seeking per call costs an O(log) heap build and source seek each step instead of
// O(1) off a held heap, but a bounded scan takes ScanLen steps, so the work is
// O(ScanLen log sources) rather than the O(keyspace) the materialized foldRange paid.
func (r *reader) ScanForward(after, lower, upper []byte, keysOnly bool) (uk, val []byte, ok bool, err error) {
	l := r.l
	l.mu.RLock()
	defer l.mu.RUnlock()

	rangeDels := l.liveRangeDels()
	sources := l.rangeSourcesLocked()

	// Seek the merge to the start of the group at after (which is then skipped, after is
	// exclusive) or at lower. MaxVersion gives the smallest internal key in the group, so
	// the seek lands at the group's first cell and skips none of it.
	var seekKey []byte
	switch {
	case after != nil:
		seekKey = after
	case lower != nil:
		seekKey = lower
	}
	var target []byte
	if seekKey != nil {
		target = format.EncodeInternalKey(seekKey, format.MaxVersion, format.KindDelete)
	}
	mi, err := newMergeIter(sources, target)
	if err != nil {
		return nil, nil, false, err
	}

	var ops []format.Op
	var groupKey []byte
	resolve := func() ([]byte, []byte, bool) {
		rd := format.NewestCoveringRangeDel(rangeDels, groupKey, r.snap.Version)
		v, vok := format.Fold(ops, r.snap.Version, rd, l.merge)
		if !vok {
			return nil, nil, false
		}
		out := append([]byte(nil), v...)
		if keysOnly {
			out = nil
		}
		return append([]byte(nil), groupKey...), out, true
	}
	for mi.valid() {
		ik := mi.key()
		guk := format.UserKey(ik)
		if upper != nil && bytes.Compare(guk, upper) >= 0 {
			break
		}
		// Skip the leading cells the seek over-reaches: after's own group (exclusive) and
		// anything below lower. Once past them, accumulate one group at a time.
		if after != nil && bytes.Compare(guk, after) <= 0 {
			if err := mi.next(); err != nil {
				return nil, nil, false, err
			}
			continue
		}
		if lower != nil && bytes.Compare(guk, lower) < 0 {
			if err := mi.next(); err != nil {
				return nil, nil, false, err
			}
			continue
		}
		if groupKey != nil && !bytes.Equal(guk, groupKey) {
			// A group boundary: the accumulated group is complete. Return it if visible,
			// otherwise drop it (a folded-absent tombstone) and accumulate the next.
			if ruk, rval, rok := resolve(); rok {
				return ruk, rval, true, nil
			}
			ops = ops[:0]
			groupKey = nil
		}
		if groupKey == nil {
			groupKey = append([]byte(nil), guk...)
		}
		// materializeOp owns the value bytes it returns (it copies the borrowed source value
		// and dereferences a separated value fresh from the vLog), so nothing kept here
		// aliases mi.value(), which dies on the next advance.
		op, opok, err := l.materializeOp(ik, mi.value(), r.snap.Now, keysOnly)
		if err != nil {
			return nil, nil, false, err
		}
		if opok {
			ops = append(ops, op)
		}
		if err := mi.next(); err != nil {
			return nil, nil, false, err
		}
	}
	if mi.err != nil {
		return nil, nil, false, mi.err
	}
	// The last group has no successor cell to trigger the boundary flush above.
	if groupKey != nil {
		if ruk, rval, rok := resolve(); rok {
			return ruk, rval, true, nil
		}
	}
	return nil, nil, false, nil
}

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
