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
	"runtime"
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
	// cur is the current immutable segment-tree snapshot and live the set of every version
	// still referenced (the current one plus any a slow reader holds). A read loads cur under
	// a brief l.mu, references it, and then probes its segments with no lock held; a flush or
	// compaction publishes a new cur by copy-on-write and frees a retired version's segments
	// only once the last reader of it lets go (perf/03 R3). See version.go.
	cur         *version
	live        []*version
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
	// compress selects which levels get heat-tiered block compression of their segment data
	// pages (spec 13): a flush and a compaction pick a codec by the level they write to. It
	// is read from the options at Open and is engine.CompressDefault by default, which with
	// no Compression bool set leaves every level raw, byte-identical to before this knob
	// existed. CompressHeatTiered compresses every level (fast on hot, high on cold);
	// CompressColdOnly leaves the hot levels raw and compresses only the cold deep ones, so
	// the bulk of the data shrinks without putting decompress CPU on the hot read path
	// (perf/05 F4d). See codec.go and codecForLevel.
	compress engine.CompressionMode
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
	// autoCompact lets the background flusher drive compaction automatically once a flush
	// pushes L0 past its trigger or a level past its size target (flush.go, perf/03 W4), so
	// read fan-out and space amp stay bounded under sustained writes without a host Maintain
	// call. It is on by default; a unit test that wants to observe the intermediate segment
	// shape its own flushes build, then drive one compaction by hand, turns it off before
	// Open so the flusher only ever drains the seal queue.
	autoCompact bool

	merge func(existing, operand []byte) []byte
	env   *engine.Env
}

// New returns an LSM core bound to a pager. The pager is the durable substrate
// flushes write segments to and that this core reads them back from.
func New(pgr *pager.Pager) *LSM {
	l := &LSM{
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
		autoCompact:    true,
	}
	// The engine starts on an empty current version, so a read before recovery loads a tree
	// it can fold (no segments) and recovery publishes the loaded tree over it.
	v0 := &version{}
	v0.refs.Store(1)
	l.cur = v0
	l.live = []*version{v0}
	return l
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
	if env != nil && env.Options.LevelSizeRatio > 0 {
		l.levelRatio = env.Options.LevelSizeRatio
	}
	if env != nil && env.Options.RangeIndex {
		l.rangeIndex = true
	}
	if env != nil && env.Options.Filter == engine.FilterRibbon {
		l.filterKind = filterRibbon
	}
	if env != nil {
		// CompressionMode, when set, wins; otherwise the legacy Compression bool maps
		// onto the equivalent mode so old call sites keep their heat-tiered behaviour.
		switch {
		case env.Options.CompressionMode != engine.CompressDefault:
			l.compress = env.Options.CompressionMode
		case env.Options.Compression:
			l.compress = engine.CompressHeatTiered
		default:
			l.compress = engine.CompressOff
		}
	}
	if env != nil && env.Options.DisableAutoCompaction {
		l.autoCompact = false
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
// depth (spec 13 §3.1). With compression off every level writes raw pages. Heat-tiered
// gives the hot shallow levels (L0, L1) the fast codec, whose decompress cost is negligible
// against the I/O it saves on data that is read and rewritten often, while the cold deep
// levels take the higher-ratio codec, whose extra CPU is paid rarely because those pages are
// written once and read seldom. Cold-only goes further: it leaves L0 and L1 raw so the hot
// read path pays no decompress at all, and compresses only the cold deep levels, where the
// bulk of the data settles, with the higher-ratio codec (perf/05 F4d). The boundary at
// level 2 keeps L0 and L1, where compaction churns, off the high codec in both modes.
func (l *LSM) codecForLevel(level int) codecID {
	switch l.compress {
	case engine.CompressHeatTiered:
		if level <= 1 {
			return codecFast
		}
		return codecHigh
	case engine.CompressColdOnly:
		if level <= 1 {
			return codecNone
		}
		return codecHigh
	default: // CompressOff / CompressDefault
		return codecNone
	}
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
	// The lock covers only the structural decisions: the seal check, capturing the active
	// memtable, and the LSN bump. The inserts then run with the lock released, so a concurrent
	// reader (which captures its snapshot under a brief l.mu.RLock and folds outside it) proceeds
	// in parallel (perf/03 W1). This is safe because the host serializes writers above the seam,
	// so only this goroutine inserts into the active memtable; the inserts race only readers,
	// which the lock-free skip list handles. The seal that would swap the active memtable out
	// happens here, under the lock, before the capture, so mem is the memtable the inserts belong
	// in for the whole call.
	l.mu.Lock()
	if l.flushErr != nil {
		l.mu.Unlock()
		return l.flushErr
	}
	if l.mem.count() > 0 && l.mem.size() >= l.memtableCap {
		if err := l.sealForFlushLocked(); err != nil {
			l.mu.Unlock()
			return err
		}
	}
	mem := l.mem
	// Track the largest WAL LSN now resident in the active memtable, so a later flush knows how
	// far the segment it writes lets the host reclaim the log. It is recorded before the inserts
	// rather than after, which is equivalent under the single-writer rule: no seal can snapshot
	// this memtable's mark between here and the inserts completing, since seal runs only on this
	// same goroutine.
	if l.pendingLSN > l.memMaxLSN {
		l.memMaxLSN = l.pendingLSN
	}
	l.mu.Unlock()

	for _, e := range batch.Entries() {
		mem.set(e.InternalKey, e.Value)
	}
	return nil
}

// parallelApplyMinEntries is the group size below which ApplyGroup inserts on the calling
// goroutine. Spreading the inserts costs a flatten, a few goroutines, and a join; below this
// many entries the serial insert is faster than paying for them. The number is small because
// the per-insert cost (a CompareInternal-heavy skip-list descent plus a key/value copy) is
// high, so even a few dozen entries amortize the fan-out.
const parallelApplyMinEntries = 64

// ApplyGroup installs every batch of a group-commit group into the active memtable, spreading
// the inserts across cores when the group is large enough to be worth it (perf/03 W1, perf/07).
// It is equivalent to Apply per batch in version order, with one difference the seam documents:
// the seal that rolls a full memtable is taken once, before the group, rather than between its
// batches, so a single group may push the memtable a little past its cap and seal at the next
// group boundary. The cap is a soft target, so a bounded overshoot of one group's bytes is fine.
//
// The lock covers only the structural prefix: the seal check, capturing the active memtable, and
// the LSN bump. The inserts, serial or fanned across cores, then run with the lock released, so a
// concurrent reader proceeds in parallel instead of waiting out the whole apply (perf/03 W1); the
// win compounds with the fan-out, since the apply both runs on several cores and no longer holds a
// reader off while it does. The host serializes writers above the seam, so only this call inserts
// into the active memtable, and the seal that would swap it out runs here under the lock before the
// capture, so mem is stable for the inserts. The workers insert through the lock-free skip list,
// which makes their concurrent inserts safe without any lock of their own; they share the active
// memtable only, and every entry across the group carries a distinct internal key (versions differ
// across batches), so no two workers ever insert the same key, and none races a reader's fold.
func (l *LSM) ApplyGroup(batches []*engine.WriteBatch, versions []uint64) error {
	l.mu.Lock()
	if l.flushErr != nil {
		l.mu.Unlock()
		return l.flushErr
	}
	if l.mem.count() > 0 && l.mem.size() >= l.memtableCap {
		if err := l.sealForFlushLocked(); err != nil {
			l.mu.Unlock()
			return err
		}
	}
	mem := l.mem
	if l.pendingLSN > l.memMaxLSN {
		l.memMaxLSN = l.pendingLSN
	}
	l.mu.Unlock()

	total := 0
	for _, b := range batches {
		total += len(b.Entries())
	}
	if total < parallelApplyMinEntries {
		for _, b := range batches {
			for _, e := range b.Entries() {
				mem.set(e.InternalKey, e.Value)
			}
		}
	} else {
		applyEntriesParallel(mem, batches, total)
	}
	return nil
}

// applyEntriesParallel inserts every batch's entries into mem across GOMAXPROCS workers. It
// flattens the group's entries into one slice so each worker takes a contiguous, equal share,
// then waits for all of them. The flatten is one allocation per group, amortized over many
// inserts; the inserts themselves, the CompareInternal-heavy skip-list descents that dominate
// apply CPU, are what spread across cores. The skip list is lock-free, so the workers need no
// lock to share mem.
func applyEntriesParallel(mem *memtable, batches []*engine.WriteBatch, total int) {
	flat := make([]engine.BatchEntry, 0, total)
	for _, b := range batches {
		flat = append(flat, b.Entries()...)
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > total {
		workers = total
	}
	if workers < 2 {
		for _, e := range flat {
			mem.set(e.InternalKey, e.Value)
		}
		return
	}
	chunk := (total + workers - 1) / workers
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo := w * chunk
		if lo >= total {
			break
		}
		hi := lo + chunk
		if hi > total {
			hi = total
		}
		wg.Add(1)
		go func(part []engine.BatchEntry) {
			defer wg.Done()
			for _, e := range part {
				mem.set(e.InternalKey, e.Value)
			}
		}(flat[lo:hi])
	}
	wg.Wait()
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
		l.publishVersionLocked(addSegment(l.cloneLevelsLocked(), 0, seg))
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

// compactionWatermark reports the version-GC horizon for a background compaction: the
// oldest version any live reader can still observe, below which a superseded version is
// collectible (spec 10 §6). The host supplies it through env.Clock; when no clock is wired
// (a bare engine test) it is zero, which keeps every version and lets a background
// compaction still bound read fan-out structurally without dropping any data. It is read
// with no lock held, so it must not be called while holding l.mu would invert a lock order;
// the clock's own lock is independent of l.mu.
func (l *LSM) compactionWatermark() uint64 {
	if l.env != nil && l.env.Clock != nil {
		return l.env.Clock.OldestReadable()
	}
	return 0
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
	cur := l.levelsLocked()
	for i := range cur {
		var bytes int64
		for _, seg := range cur[i] {
			bytes += int64(seg.pages) * pageSize
		}
		physical += bytes
		levels = append(levels, engine.LevelStats{Segments: len(cur[i]), Bytes: bytes})
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
	rangeDels := l.liveRangeDels(l.allSegmentsLocked())

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
	tc := snap.TTLClock()
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
		op, ok, err := l.materializeOp(ik, mi.value(), tc.For(format.KindOf(ik)), keysOnly)
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

	var ops []format.Op
	var matErr error
	var rd uint64
	tc := r.snap.TTLClock()
	collect := func(ik, val []byte) bool {
		// A point Get always wants the value, so keysOnly is false: a separated value is
		// dereferenced through the vLog here.
		op, ok, err := l.materializeOp(ik, val, tc.For(format.KindOf(ik)), false)
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

	// One brief l.mu.RLock captures a consistent snapshot: a reference on the current version,
	// the covering range-delete mark, the merge resolver, the active memtable pointer, and the
	// sealed queue as it stands. The lock is then released and every slow step (the memtable
	// folds and the on-disk segment probes) runs with no lock held, so a concurrent Apply, whose
	// inserts now run outside l.mu, proceeds in parallel with this read (perf/03 W1).
	//
	// The captured snapshot stays consistent without the lock because of how each piece behaves
	// once released. The active memtable (memSnap) is mutated only by Apply, lock-free through the
	// skip list, and only with cells newer than this snapshot's version, which the fold ignores;
	// the lock-free walk never returns a torn key. A sealed memtable is write-frozen the moment
	// seal moves it out of the active slot, so folding immSnap reads immutable skip lists. The
	// seal that moves a memtable from active to sealed, and the flush that turns a sealed memtable
	// into a segment and pops it, each happen entirely under l.mu, so the captured (memSnap,
	// immSnap, version) triple holds every memtable exactly once: a key is folded from the active
	// memtable, or one sealed memtable, or one segment, never two, so a merge never applies its
	// operand twice. The referenced version keeps its segments' pages off the freelist until the
	// deferred release, so the segment probes read only stable, still-allocated pages (perf/03 R3).
	l.mu.RLock()
	v := l.acquireVersionLocked()
	rd = format.NewestCoveringRangeDel(l.liveRangeDels(flattenSegments(v.levels)), userKey, r.snap.Version)
	mergeFn := l.merge
	memSnap := l.mem
	immSnap := append([]*immMem(nil), l.imm...)
	l.mu.RUnlock()
	defer l.releaseVersion(v)

	// Fold the active memtable, then the sealed memtables awaiting flush between it and the
	// on-disk levels: each sealed memtable is older than the active one but newer than any
	// segment. Newest-first means the most recently sealed (the tail of the queue) folds first.
	memSnap.getGroup(userKey, collect)
	for i := len(immSnap) - 1; matErr == nil && i >= 0; i-- {
		immSnap[i].mem.getGroup(userKey, collect)
	}
	if matErr != nil {
		return nil, matErr
	}

	// resolved() sorts ops newest-first, the order the final Fold expects, so it is evaluated
	// here whenever the memtables alone might already fix the key and let the segment probes be
	// skipped.
	if !resolved() {
		// Probe the on-disk levels shallowest first and stop at the first level that
		// supplies a base: a key's newest version always sits in the shallowest level
		// that holds it (a new write enters at the memtable and only ever migrates down),
		// so once a base is found a deeper level carries only shadowed versions. The whole
		// level is gathered before the check because a level may hold overlapping runs
		// (L0, and a tiered bottom kept in minKey order) whose relative age is not encoded
		// by their position, so a partial read of such a level could miss the newest
		// version; a level boundary is the coarsest unit at which newest-first holds. This
		// runs with l.mu released, reading only the referenced version's immutable segments
		// and the value log their separated pointers name.
		for lvl := 0; lvl < len(v.levels); lvl++ {
			touched := false
			for _, seg := range v.levels[lvl] {
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
	val, ok := format.Fold(ops, r.snap.Version, rd, mergeFn)
	if !ok {
		return nil, engine.ErrNotFound
	}
	return append([]byte(nil), val...), nil
}

// liveRangeDels gathers every range-delete interval the engine holds, from the
// memtable's live set, every sealed memtable, and the given segments' persisted lists, the
// set the fold filters to the markers that cover a key. The segments come from the caller's
// chosen version: a scan passes the current version's flat set under l.mu, while a point read
// passes the version it referenced so the deletes it folds match the segments it will probe.
// The caller holds l.mu when reading the memtables (the segment range-delete lists are loaded
// once at open and never change, so they are safe to read from a referenced version with the
// lock dropped).
func (l *LSM) liveRangeDels(segs []*segment) []format.RangeDel {
	active := l.mem.liveDels()
	dels := make([]format.RangeDel, 0, len(active))
	dels = append(dels, active...)
	for _, e := range l.imm {
		dels = append(dels, e.mem.liveDels()...)
	}
	for _, seg := range segs {
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

	rangeDels := l.liveRangeDels(l.allSegmentsLocked())
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
	tc := r.snap.TTLClock()
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
		op, opok, err := l.materializeOp(ik, mi.value(), tc.For(format.KindOf(ik)), keysOnly)
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
