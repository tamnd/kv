// Package db is the integration layer that wires the pager, the WAL, and a storage
// core into one durable, recoverable database, and runs crash recovery on open. It
// is the vertical seam the milestone roadmap calls "the first genuinely usable kv":
// a single embedded engine that commits through the WAL and comes back correct
// after any crash (spec 24 M1).
//
// It is deliberately thin and below the public library API (spec 15, M3): it assigns
// monotonic commit versions, enforces the write-ahead rule (log+commit durable
// before the engine mutates a page), and on open replays the committed WAL tail
// through the same engine.Apply path normal writes use, so redo and runtime cannot
// drift. The transaction API (View/Update, conflict retry), the merge registry, and
// the CLI/server surfaces layer on top of this without changing it.
package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tamnd/kv/btree"
	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/lsm"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// walSuffix is appended to the database path to name its write-ahead log sidecar,
// matching the SQLite-style "-wal" convention (spec 07).
const walSuffix = "-wal"

// Options configure a database at open. The zero value is usable: it selects the
// B-tree core, a 4 KiB page size, and SyncFull durability.
type Options struct {
	// PageSize is the page size for a freshly created file; ignored when opening an
	// existing file (its header's page size wins).
	PageSize int
	// CacheFrames is the buffer-pool capacity in frames; zero selects the pager
	// default.
	CacheFrames int
	// Engine selects the storage core for a fresh file; zero means the B-tree core.
	Engine format.EngineKind
	// Checksum selects the per-page checksum algorithm stamped into a fresh file
	// (spec 02 §3.2). Zero selects CRC32C, the spec default, so every database detects
	// torn writes and bit rot out of the box; set format.ChecksumXXH64 for the wider
	// digest. Ignored when opening an existing file (its header's choice wins).
	Checksum format.ChecksumAlgo
	// Sync is the WAL durability level (spec 07 §6). Zero is SyncFull, the safe
	// default: every acked commit survives a crash.
	Sync wal.Sync
	// Merge folds an existing value and a merge operand into a new value during read
	// resolution (spec 15). If nil, a merge operand behaves as a plain set.
	Merge func(existing, operand []byte) []byte
	// MaxRetries bounds how many times Update re-runs its closure on a write-write
	// conflict (spec 15 §2.1). Zero selects a small default.
	MaxRetries int
	// Isolation is the isolation level every transaction runs at (spec 10 §3, §4).
	// Zero is SnapshotIsolation, the default; Serializable adds read-set validation.
	Isolation Isolation
	// AutoCheckpoint is the WAL backlog, in frames, at which a background passive
	// checkpoint is triggered so the log stays bounded under sustained writes
	// (spec 09 §1.3). Zero selects a sensible default; a negative value disables
	// auto-checkpointing entirely, leaving the WAL to grow until an explicit
	// Checkpoint or a clean close.
	AutoCheckpoint int
	// Clock returns the current wall-clock time in Unix nanoseconds, the time base a
	// TTL set's absolute expiry is compared against during read resolution (spec 15
	// §6). Zero selects the real monotonic-corrected system clock; a test injects a
	// controllable clock here to drive expiry deterministically.
	Clock func() uint64
	// MemtableSize is the byte size at which the LSM core flushes its active memtable
	// to an on-disk segment (spec 06 §2). Zero takes the engine default; the B-tree
	// core ignores it. A smaller value flushes sooner, bounding memory and the WAL
	// backlog at the cost of more, smaller segments.
	MemtableSize int
	// RangeIndex turns on the LSM core's REMIX ordered index (spec 06 §6, spec 11
	// §5.3), which presents each leveled level's disjoint segments to a range scan as
	// one ordered cursor instead of one per segment, cutting the heap-merge's
	// comparisons and cursor switches. Off by default, since it helps only scan-heavy
	// workloads and the B-tree core, a single ordered source, never needs it.
	RangeIndex bool
	// Filter selects the LSM core's per-segment membership filter (spec 06 §5).
	// FilterBloom, the zero value, is the default double-hashing Bloom filter, fast to
	// probe on the hot levels. FilterRibbon is the opt-in Ribbon filter, which reaches the
	// same false-positive rate in meaningfully less space, attractive on the deep cold
	// levels where filters dominate the resident set, at some extra construction cost. The
	// B-tree core ignores it.
	Filter engine.FilterKind
	// BufferedInserts turns on the B-tree core's Bε buffered write path (spec 05 §4):
	// inserts park as messages in interior node buffers and flush one level down in
	// batches, trading a little read-path work and interior fan-out for sharply lower
	// per-key write amplification. Off by default, since the in-place tree's read
	// latency is the engine's headline; it helps write-heavier workloads that still
	// want the B-tree's tight space and ordered scans. The LSM core ignores it.
	BufferedInserts bool
	// Compression turns on the LSM core's heat-tiered block compression (spec 13): segment
	// data pages are compressed with a cheap fast codec on the hot shallow levels and a
	// higher-ratio codec on the cold deep ones, packing more cells per page so the file
	// shrinks and more data rides in each cached page, at the cost of decompress CPU on the
	// read miss path. Off by default, since it trades read CPU for space; it helps
	// space-bound or write-heavy workloads on storage slower than the CPU. The B-tree core
	// ignores it.
	Compression bool
}

func (o Options) maxRetries() int {
	if o.MaxRetries == 0 {
		return 10
	}
	return o.MaxRetries
}

func (o Options) pageSize() int {
	if o.PageSize == 0 {
		return 4096
	}
	return o.PageSize
}

func (o Options) engineKind() format.EngineKind {
	if o.Engine == 0 {
		return format.EngineBTree
	}
	return o.Engine
}

// checksum resolves the per-page checksum algorithm for a fresh file: the zero value
// selects CRC32C, the spec default (spec 02 §3.2), so the high-level API always
// creates an integrity-checked database. ChecksumNone shares the zero byte and so is
// not reachable here by design; integrity is not an opt-out at this layer.
func (o Options) checksum() format.ChecksumAlgo {
	if o.Checksum == 0 {
		return format.ChecksumCRC32C
	}
	return o.Checksum
}

// autoCheckpoint resolves the WAL backlog threshold in frames: zero takes the 1000
// frame default (the SQLite wal_autocheckpoint analog), a negative value disables
// auto-checkpointing and returns zero (spec 09 §1.3).
func (o Options) autoCheckpoint() int {
	if o.AutoCheckpoint == 0 {
		return 1000
	}
	if o.AutoCheckpoint < 0 {
		return 0
	}
	return o.AutoCheckpoint
}

// clock resolves the wall-clock source for TTL expiry: the injected clock if any,
// else the real system clock read as Unix nanoseconds (spec 15 §6).
func (o Options) clock() func() uint64 {
	if o.Clock != nil {
		return o.Clock
	}
	return func() uint64 { return uint64(time.Now().UnixNano()) }
}

func (o Options) sync() wal.Sync {
	// SyncFull is the iota-zero value of wal.Sync's predecessor SyncOff, so the zero
	// Options must map to SyncFull explicitly rather than relying on the zero value.
	if o.Sync == 0 {
		return wal.SyncFull
	}
	return o.Sync
}

// DB is an open database: a pager over the main file, a WAL sidecar, and a storage
// core, with a monotonic commit-version counter. It is safe for concurrent readers
// and serializes writers through its mutex (group commit and MVCC concurrency are
// later milestones).
type DB struct {
	fs   vfs.FS
	path string

	// mu serializes the single committing writer against itself and against the
	// engine reads (spec 10 §5.1): a commit takes it exclusively for log+apply, a
	// read takes it shared. The version state lives in the lock-light oracle, not
	// here, so it is consulted off this lock.
	mu  sync.RWMutex
	pgr *pager.Pager
	wal *wal.WAL
	eng engine.Engine
	orc *oracle

	merge           func(existing, operand []byte) []byte
	maxRetries      int
	syncMode        wal.Sync
	isolation       Isolation
	memtableSize    int
	rangeIndex      bool
	filter          engine.FilterKind
	bufferedInserts bool
	compression     bool

	// counters is the cumulative-since-open operation tally surfaced through Stats and
	// the Prometheus exposition (spec 19 §1.1). Reads bump it on the transaction path,
	// commits on applyCommitted; it is all atomics, so no lock guards it.
	counters opCounters

	// now is the wall-clock source, in Unix nanoseconds, that read resolution compares
	// a TTL set's absolute expiry against (spec 15 §6). It is the real system clock by
	// default and an injected clock under test, so expiry is deterministic in tests and
	// honest in production. Reads thread it into engine.Snapshot.Now; a zero Now there
	// disables expiry, which is what background GC and recovery want.
	now func() uint64

	// fatal fences the database after a WAL durability failure (spec 07 §6). It is
	// set under the write lock when a log append or commit sync fails and read at the
	// top of every commit path; once set, no further write is admitted until reopen.
	// Reads are unaffected: they continue to serve the last consistent state.
	fatal error

	// Auto-checkpointer (spec 09 §1.3). When ckptThreshold is positive a single
	// long-lived goroutine folds the WAL in the background: a commit that pushes the
	// backlog past the threshold signals ckptSig (non-blocking, coalesced through the
	// one-slot buffer), and the worker runs a passive Checkpoint off the commit path.
	// ckptStop closes on Close to retire the worker, ckptDone closes when it has; both
	// are nil when auto-checkpointing is disabled. The worker takes d.mu itself, so the
	// signal must be sent while holding it but the shutdown join must not.
	ckptThreshold int
	ckptSig       chan struct{}
	ckptStop      chan struct{}
	ckptDone      chan struct{}
	closeOnce     sync.Once

	ckptErrMu sync.Mutex
	ckptErr   error

	// Change-feed subscribers (spec 15 §7). publish enqueues each committed batch's
	// matching mutations onto every registered subscription's buffered channel under
	// subsMu; the Subscribe caller drains it. subClosed is closed on Close to wake any
	// blocked subscriber, and subsClosed fences Subscribe once the database is closing.
	// All three are guarded by subsMu, a separate lock taken under d.mu by publish.
	subsMu     sync.Mutex
	subs       map[*subscription]struct{}
	subClosed  chan struct{}
	subsClosed bool
}

// Open opens the database at path, creating it if it does not exist, and runs crash
// recovery: it replays every committed WAL batch past the last checkpoint through
// engine.Apply, so the returned DB reflects exactly the durable state of the last
// acked commit. A torn or uncommitted WAL tail is discarded (spec 08 §2-3).
func Open(fs vfs.FS, path string, opts Options) (*DB, error) {
	exists, err := fs.Exists(path)
	if err != nil {
		return nil, err
	}
	if exists {
		return openExisting(fs, path, opts)
	}
	return create(fs, path, opts)
}

// create initializes a fresh main file, a fresh WAL, and an empty engine root.
func create(fs vfs.FS, path string, opts Options) (*DB, error) {
	pgr, err := pager.Create(fs, path, pager.Options{
		PageSize:    opts.pageSize(),
		CacheFrames: opts.CacheFrames,
		Engine:      opts.engineKind(),
		Checksum:    opts.checksum(),
		Flags:       format.FlagWAL,
	})
	if err != nil {
		return nil, err
	}
	w, err := wal.Create(fs, path+walSuffix, wal.Options{PageSize: pgr.PageSize(), Sync: opts.sync()})
	if err != nil {
		pgr.Close()
		return nil, err
	}
	eng, err := newEngine(opts.engineKind(), pgr)
	if err != nil {
		w.Close()
		pgr.Close()
		return nil, err
	}
	d := &DB{fs: fs, path: path, pgr: pgr, wal: w, eng: eng, orc: newOracle(0),
		merge: opts.Merge, maxRetries: opts.maxRetries(), syncMode: opts.sync(), isolation: opts.Isolation, memtableSize: opts.MemtableSize, rangeIndex: opts.RangeIndex, filter: opts.Filter, bufferedInserts: opts.BufferedInserts, compression: opts.Compression, now: opts.clock()}
	if err := d.openEngine(opts.Merge); err != nil {
		w.Close()
		pgr.Close()
		return nil, err
	}
	d.startCheckpointer(opts.autoCheckpoint())
	return d, nil
}

// openExisting opens an existing main file, resumes or creates its WAL, and redoes
// the committed tail.
func openExisting(fs vfs.FS, path string, opts Options) (*DB, error) {
	pgr, err := pager.Open(fs, path, pager.Options{CacheFrames: opts.CacheFrames})
	if err != nil {
		return nil, err
	}
	eng, err := newEngine(pgr.Header().Engine, pgr)
	if err != nil {
		pgr.Close()
		return nil, err
	}
	d := &DB{fs: fs, path: path, pgr: pgr, eng: eng,
		merge: opts.Merge, maxRetries: opts.maxRetries(), syncMode: opts.sync(), isolation: opts.Isolation, memtableSize: opts.MemtableSize, rangeIndex: opts.RangeIndex, filter: opts.Filter, bufferedInserts: opts.BufferedInserts, compression: opts.Compression, now: opts.clock()}
	if err := d.openEngine(opts.Merge); err != nil {
		pgr.Close()
		return nil, err
	}

	// Resume the WAL if one exists, else start a fresh log for this generation.
	walPath := path + walSuffix
	walExists, err := fs.Exists(walPath)
	if err != nil {
		pgr.Close()
		return nil, err
	}
	var maxVer uint64
	if walExists {
		w, rec, err := wal.Open(fs, walPath, wal.Options{PageSize: pgr.PageSize(), Sync: opts.sync()})
		if err != nil {
			pgr.Close()
			return nil, err
		}
		d.wal = w
		if maxVer, err = d.redo(rec); err != nil {
			w.Close()
			pgr.Close()
			return nil, err
		}
	} else {
		w, err := wal.Create(fs, walPath, wal.Options{PageSize: pgr.PageSize(), Sync: opts.sync()})
		if err != nil {
			pgr.Close()
			return nil, err
		}
		d.wal = w
	}

	if err := d.eng.RecoverFinished(maxVer); err != nil {
		d.wal.Close()
		pgr.Close()
		return nil, err
	}
	// The version counter resumes from the larger of the durable header value and
	// any version redone from the WAL, so a fresh write never reissues a version
	// already on disk (spec 10 §1).
	last := pgr.Header().LastCommitVersion
	if maxVer > last {
		last = maxVer
	}
	d.orc = newOracle(last)
	d.startCheckpointer(opts.autoCheckpoint())
	return d, nil
}

// openEngine wires the engine to its substrate and installs the merge resolver.
func (d *DB) openEngine(merge func(existing, operand []byte) []byte) error {
	env := &engine.Env{
		Pager: d.pgr,
		Options: engine.EngineOptions{
			PageSize:        d.pgr.PageSize(),
			MemtableSize:    d.memtableSize,
			RangeIndex:      d.rangeIndex,
			Filter:          d.filter,
			BufferedInserts: d.bufferedInserts,
			Compression:     d.compression,
		},
	}
	if err := d.eng.Open(env); err != nil {
		return err
	}
	if ms, ok := d.eng.(interface {
		SetMergeFunc(func(existing, operand []byte) []byte)
	}); ok {
		ms.SetMergeFunc(merge)
	}
	return nil
}

// redo replays the committed batches past the pager's recorded checkpoint boundary
// through engine.Apply, reconstructing exactly the state a clean run produced. It
// returns the highest version it applied. Replaying the same committed batch twice
// is a no-op because every mutation is keyed by a unique versioned internal key, so
// redo is idempotent and restartable (spec 08 §3).
func (d *DB) redo(rec wal.RecoverResult) (uint64, error) {
	var maxVer uint64
	for _, cb := range rec.CommittedAfter(d.pgr.CheckpointLSN()) {
		b, err := engine.DecodeBatch(cb.Encoded)
		if err != nil {
			return 0, fmt.Errorf("kv: corrupt committed batch at LSN %d: %w", cb.LSN, err)
		}
		d.noteLSN(cb.LSN)
		if err := d.eng.Apply(b, cb.Version); err != nil {
			return 0, fmt.Errorf("kv: redo Apply at version %d: %w", cb.Version, err)
		}
		if cb.Version > maxVer {
			maxVer = cb.Version
		}
	}
	return maxVer, nil
}

// newEngine constructs the storage core for a kind.
func newEngine(kind format.EngineKind, pgr *pager.Pager) (engine.Engine, error) {
	switch kind {
	case format.EngineBTree:
		return btree.New(pgr), nil
	case format.EngineLSM:
		return lsm.New(pgr), nil
	default:
		return nil, fmt.Errorf("kv: unknown engine kind %d", kind)
	}
}

// Write builds a batch at the next commit version, lets fn populate it, logs and
// commits it to the WAL, and only then applies it to the engine -- the write-ahead
// rule (spec 07 §1). It returns the assigned commit version. At SyncFull the batch
// is durable before Write returns: a crash afterward will redo it.
func (d *DB) Write(fn func(b *engine.WriteBatch)) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.fatal != nil {
		return 0, d.fatal
	}
	// Under the single-writer lock the next version is stable between this peek and
	// the formal commit, so the batch can be built at it before it is reserved.
	v := d.orc.peekNext()
	b := engine.NewWriteBatch(v)
	fn(b)
	if b.Len() == 0 {
		// An empty write still consumes no version; report the last committed one.
		return v - 1, nil
	}

	got := d.orc.commit(batchKeys(b))
	if err := d.applyCommitted(b, got); err != nil {
		return 0, err
	}
	d.orc.applied(got)
	return got, nil
}

// Load bulk-populates the database from key/value pairs delivered in ascending key
// order. next returns each pair and true, or false at end of stream. It returns the
// commit version the loaded data is visible at.
//
// Load is the fast path for initial population (spec 15 §6): when the engine provides
// the bulk-load capability and the database has no commits yet, it builds the on-disk
// structure bottom-up, far faster than inserting key by key, and makes the result
// durable with a single checkpoint. A crash before that checkpoint leaves the database
// empty, so the fast path is atomic. The fast path requires keys to be strictly
// ascending and errors otherwise. On a database that already holds data, or an engine
// without the capability, Load falls back to a chunked sequence of ordinary commits,
// which is slower, accepts any order, and is durable per chunk like a WriteBatch.
func (d *DB) Load(next func() (key, value []byte, ok bool)) (uint64, error) {
	d.mu.Lock()
	if d.fatal != nil {
		d.mu.Unlock()
		return 0, d.fatal
	}
	bl, ok := d.eng.(engine.BulkLoader)
	if ok && d.orc.lastCommitted() == 0 {
		v, err := d.loadFast(bl, next)
		d.mu.Unlock()
		return v, err
	}
	d.mu.Unlock()
	return d.loadBatched(next)
}

// loadFast drives the engine bulk loader over an empty database and makes the build
// durable. The caller holds d.mu and has checked the database is empty, so the commit
// version is deterministically the next one (1 on a fresh file). The keys are stamped at
// that version as Set cells and fed in ascending internal-key order. The oracle is
// advanced only after the build succeeds, so a failed load leaves the database empty and
// the version counter untouched; the checkpoint then persists the version into the
// header and folds the freshly built pages into the main file.
func (d *DB) loadFast(bl engine.BulkLoader, next func() (key, value []byte, ok bool)) (uint64, error) {
	v := d.orc.peekNext()

	var prevKey []byte
	var streamErr error
	feed := func() (ik, value []byte, ok bool) {
		k, val, ok := next()
		if !ok {
			return nil, nil, false
		}
		if prevKey != nil && format.CompareUser(k, prevKey) <= 0 {
			streamErr = fmt.Errorf("kv: bulk load keys not strictly ascending at %q", k)
			return nil, nil, false
		}
		prevKey = append(prevKey[:0], k...)
		return format.EncodeInternalKey(k, v, format.KindSet), val, true
	}

	if err := bl.BulkLoad(feed); err != nil {
		return 0, err
	}
	if streamErr != nil {
		return 0, streamErr
	}

	// Advance the oracle before the checkpoint: checkpointLocked stamps the header with
	// the oracle's last committed version, so the version must be live first or the
	// freshly built pages would be folded under version 0 and read back invisible.
	d.orc.commit(nil)
	d.orc.applied(v)
	if err := d.checkpointLocked(); err != nil {
		return 0, err
	}
	// The fast path does not surface its keys on the change feed: it is initial
	// population of an empty database, before any serving or subscription, and
	// materializing the whole stream as one published batch would defeat the streaming
	// the bulk loader exists to provide. A subscriber reads the loaded state from its
	// opening snapshot like any other (spec 15 §7).
	return v, nil
}

// loadBatched is the order-agnostic fallback: it streams the pairs through the explicit
// WriteBatch builder, which commits in bounded chunks so a huge import never holds the
// whole stream in memory. It does not hold d.mu (the batch commits take it per chunk).
func (d *DB) loadBatched(next func() (key, value []byte, ok bool)) (uint64, error) {
	b := d.NewWriteBatch(0)
	for {
		k, val, ok := next()
		if !ok {
			break
		}
		if err := b.Set(k, val); err != nil {
			return 0, err
		}
	}
	if err := b.Close(); err != nil {
		return 0, err
	}
	return d.orc.lastCommitted(), nil
}

// commitTxn is the single-writer commit path for a transaction: it runs write-write
// conflict detection at the transaction's read snapshot, and on success logs,
// commits, and applies the buffered writes at the assigned version, then makes that
// version visible (spec 10 §3, §5.1). It returns the assigned commit version, or
// ErrConflict if the transaction lost a write-write race.
func (d *DB) commitTxn(readVersion uint64, ops []pendingOp, conflictKeys []string) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.fatal != nil {
		return 0, d.fatal
	}
	v, ok := d.orc.newCommitTs(readVersion, conflictKeys)
	if !ok {
		return 0, ErrConflict
	}
	return d.applyTxn(v, ops)
}

// commitTxnSerializable is the serializable-isolation commit path (spec 10 §4): it is
// commitTxn with the oracle's read-set validation in place of the plain write-write
// check. writeKeys is the resolved write set (first-committer-wins), readKeys and
// ranges are what the transaction read (rw-antidependency detection). It returns the
// assigned commit version, or ErrConflict if either check fails.
func (d *DB) commitTxnSerializable(readVersion uint64, ops []pendingOp, writeKeys, readKeys []string, ranges []keyRange) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.fatal != nil {
		return 0, d.fatal
	}
	v, ok := d.orc.newCommitTsSerializable(readVersion, writeKeys, readKeys, ranges)
	if !ok {
		return 0, ErrConflict
	}
	return d.applyTxn(v, ops)
}

// applyTxn builds the write batch for an admitted commit at version v, applies it
// through the write-ahead path, and makes the version visible. The caller holds d.mu
// and has already cleared conflict detection.
func (d *DB) applyTxn(v uint64, ops []pendingOp) (uint64, error) {
	b := engine.NewWriteBatch(v)
	for _, op := range ops {
		switch op.kind {
		case opSet:
			b.Set(op.key, op.value)
		case opSetTTL:
			b.SetWithTTL(op.key, op.value, op.expiry)
		case opDelete:
			b.Delete(op.key)
		case opMerge:
			b.Merge(op.key, op.value)
		case opRangeDelete:
			b.DeleteRange(op.key, op.value)
		}
	}
	if err := d.applyCommitted(b, v); err != nil {
		return 0, err
	}
	d.orc.applied(v)
	return v, nil
}

// applyCommitted enforces the write-ahead rule for an already-versioned batch: log
// and commit it durably, then apply it to the engine, then record the durable
// version in the header (persisted at the next checkpoint). The caller holds d.mu
// and calls oracle.applied after, in version order (spec 07 §1, spec 10 §2).
func (d *DB) applyCommitted(b *engine.WriteBatch, v uint64) error {
	encoded := b.Encode()
	// Time the durable-commit sequence (log append plus fsync) for the commit-latency
	// metric (spec 19 §1.1). time.Now is the right clock here, not d.now: this measures
	// real fsync cost, independent of the injectable TTL clock, and Go's monotonic
	// reading makes the delta immune to wall-clock steps.
	commitStart := time.Now()
	// A failed log append or commit sync is a fatal durability fault (fsyncgate, spec
	// 07 §6): the kernel may have dropped the un-synced bytes, so the commit must not
	// be acknowledged and the database is fenced until reopen. Apply runs only after a
	// durable commit, so a fault here leaves the engine untouched and this version
	// unapplied, exactly the state recovery reconstructs from the durable log.
	if err := d.wal.LogBatch(v, encoded); err != nil {
		d.fatal = fmt.Errorf("%w: %v", ErrFatalSync, err)
		return d.fatal
	}
	commitLSN, err := d.wal.Commit(v)
	if err != nil {
		d.fatal = fmt.Errorf("%w: %v", ErrFatalSync, err)
		return d.fatal
	}
	// The batch is durable: count it and fold in its latency. Only a successful, durable
	// commit is tallied, so the average latency is over acknowledged commits, never over
	// the faults that fenced the database above.
	d.counters.commits.Add(1)
	d.counters.commitNanos.Add(uint64(time.Since(commitStart)))
	// Tell an LSN-tracking engine (the LSM core) the batch's WAL position before
	// applying it, so its durable mark can later report how far a flush has reached.
	d.noteLSN(commitLSN)
	if err := d.eng.Apply(b, v); err != nil {
		return err
	}
	d.pgr.Header().LastCommitVersion = v
	d.maybeCheckpoint()
	// The commit is now durable and visible, so surface it to the change feed. publish
	// only enqueues onto subscriber channels and never calls user code, so holding the
	// write lock across it stays cheap (spec 15 §7).
	d.publish(b, v)
	return nil
}

// batchKeys returns the unique user keys a blind batch wrote, so a Write
// participates in conflict detection against concurrent transactions.
func batchKeys(b *engine.WriteBatch) []string {
	seen := make(map[string]struct{})
	var keys []string
	for _, e := range b.Entries() {
		k := string(format.UserKey(e.InternalKey))
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	return keys
}

// snapshotGet reads key at a fixed version through a short-lived engine reader,
// taking the shared read lock so it never observes a page mid-commit. It returns
// the value and whether the key is present at that snapshot (spec 10 §3).
func (d *DB) snapshotGet(version uint64, key []byte) ([]byte, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	rd, err := d.eng.NewReader(engine.Snapshot{Version: version, Now: d.now()})
	if err != nil {
		return nil, false, err
	}
	defer rd.Close()
	v, err := rd.Get(key)
	if err == engine.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

// Version reports the latest committed version, the snapshot a reader sees the
// newest data at. It is zero on a fresh database with no commits.
func (d *DB) Version() uint64 {
	return d.orc.lastCommitted()
}

// NewReader returns a consistent read view at version. Pass d.Version() for the
// latest committed snapshot. The returned reader holds engine resources for its
// lifetime; for snapshot-isolated reads prefer View/Begin, which manage the
// snapshot and its watermark registration.
func (d *DB) NewReader(version uint64) (engine.Reader, error) {
	return d.eng.NewReader(engine.Snapshot{Version: version, Now: d.now()})
}

// Get reads userKey at the latest committed snapshot, a convenience over a View
// transaction's Get.
func (d *DB) Get(userKey []byte) ([]byte, error) {
	v, ok, err := d.snapshotGet(d.Version(), userKey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, engine.ErrNotFound
	}
	return v, nil
}

// Maintain runs one round of engine-scheduled maintenance, currently version GC, up
// to a page budget, and reports what it reclaimed. The GC horizon is the oracle's
// read-mark: the oldest snapshot any in-flight reader still holds, or the latest
// committed version when none is live, so GC never reclaims a version a live snapshot
// can still read (spec 09, spec 10 §6). It takes the writer lock, so it is serialized
// against commits and checkpoints. A maxPages of zero means no page cap. Report.More
// is true when the budget ran out before the work was done and Maintain should be
// called again.
func (d *DB) Maintain(maxPages int) (engine.MaintReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	budget := engine.MaintBudget{MaxPages: maxPages, Watermark: d.orc.readMark(), Now: d.now()}
	return d.eng.Maintain(context.Background(), budget)
}

// Verify runs the engine's structural self-check and returns its report (spec 16 §4,
// spec 23 §3). It takes the writer lock so the walk sees a stable tree, not one mid
// commit or mid checkpoint. It returns ErrUnsupported when the engine has no verifier,
// so the CLI can say so plainly rather than reporting a silent pass.
func (d *DB) Verify() (*engine.VerifyReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.eng.(engine.Verifier)
	if !ok {
		return nil, ErrUnsupported
	}
	return v.Verify()
}

// Stats is a point-in-time space-and-durability snapshot of the database, aggregating
// the engine's space accounting (spec 09 §4) with the pager's page counts and the
// WAL's frame backlog. It is what the info/stats CLI surface and the observability
// layer read (spec 19). Every field is gathered cheaply under a read lock; counts that
// would need a full tree walk (live keys/bytes) are left to the engine and may be zero.
type Stats struct {
	// Engine is the storage core the file was created with (spec 02).
	Engine format.EngineKind
	// PageSize is the file's page size in bytes.
	PageSize int
	// PageCount is the file's high-water page count (its logical size in pages).
	PageCount uint32
	// FreePages is the number of pages on the freelist, reusable before the file grows.
	FreePages int64
	// PhysicalBytes is the engine's on-disk footprint including not-yet-reclaimed dead
	// versions.
	PhysicalBytes int64
	// LiveKeys and LiveBytes are the engine's live-data counts at the newest snapshot,
	// or zero when the engine does not compute them cheaply.
	LiveKeys  int64
	LiveBytes int64
	// Amplification is the engine's space-amplification estimate (physical / live).
	Amplification float64
	// Version is the latest committed commit version (spec 10 §1).
	Version uint64
	// WALFrames is the next WAL frame LSN: the count of frames the log has written.
	WALFrames uint64
	// WALBacklog is the number of WAL frames committed but not yet folded into the main
	// file by a checkpoint; it is the read-overhead and recovery-time signal (spec 09 §1.3).
	WALBacklog uint64
	// Syncs is how many fsyncs the WAL has performed since open (spec 19).
	Syncs uint64
	// PageReads and CacheHits are the buffer pool's cumulative traffic since open: physical
	// page reads against the main file, and Gets served from a resident frame. Their ratio
	// is the cache hit rate, and PageReads divided by a workload's logical read count is its
	// read amplification (spec 19, spec 21 §1).
	PageReads uint64
	CacheHits uint64
	// Ops is the cumulative-since-open per-operation tally and durable-commit latency
	// (spec 19 §1.1): the throughput counters a dashboard rates over time.
	Ops OpStats
	// Levels is the per-level segment-and-byte shape of an LSM engine, youngest first,
	// or nil for the B-tree (spec 19 §1.5).
	Levels []engine.LevelStats
	// CompactionScore is the urgency of the most-pending LSM compaction, 0 when nothing
	// is due or for the B-tree (spec 19 §1.5).
	CompactionScore float64
	// OldestSnapshotAgeNanos is the wall-clock age of the longest-held live read snapshot
	// in nanoseconds, 0 when no reader is live; a value that only climbs is a reader that
	// was never discarded (spec 19 §1.6).
	OldestSnapshotAgeNanos uint64
}

// Stats gathers a Stats snapshot under a read lock, so it is consistent against a
// concurrent commit without blocking one for long (spec 09 §4).
func (d *DB) Stats() Stats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	es := d.eng.Stats()
	// LSN is the next frame number to assign, so the last frame written is LSN-1, and a
	// checkpoint records that LSN as folded. Frames still unfolded are those numbered
	// above the checkpoint mark and at or below the last written frame.
	lsn := d.wal.LSN()
	var written uint64
	if lsn > 0 {
		written = lsn - 1
	}
	folded := d.pgr.CheckpointLSN()
	var backlog uint64
	if written > folded {
		backlog = written - folded
	}
	io := d.pgr.IOStats()
	return Stats{
		Engine:                 d.pgr.Header().Engine,
		PageSize:               d.pgr.PageSize(),
		PageCount:              d.pgr.DBSize(),
		FreePages:              es.FreePages,
		PhysicalBytes:          es.PhysicalBytes,
		LiveKeys:               es.LiveKeys,
		LiveBytes:              es.LiveBytes,
		Amplification:          es.Amplification,
		Version:                d.orc.lastCommitted(),
		WALFrames:              written,
		WALBacklog:             backlog,
		Syncs:                  d.wal.Syncs(),
		PageReads:              io.PageReads,
		CacheHits:              io.CacheHits,
		Ops:                    d.counters.snapshot(),
		Levels:                 es.Levels,
		CompactionScore:        es.CompactionScore,
		OldestSnapshotAgeNanos: d.oldestSnapshotAgeNanos(),
	}
}

// oldestSnapshotAgeNanos is the wall-clock age of the longest-held live read snapshot,
// or 0 when no reader is live or the clock has not advanced past the stamp. It turns the
// oracle's registration stamp into an age against the current clock for the leaked-reader
// gauge (spec 19 §1.6).
func (d *DB) oldestSnapshotAgeNanos() uint64 {
	since := d.orc.oldestReaderSince()
	if since == 0 {
		return 0
	}
	now := d.now()
	if now <= since {
		return 0
	}
	return now - since
}

// startCheckpointer launches the background passive-checkpoint worker when threshold
// is positive. It is called once at the end of open, after every field the worker
// reads is set, so a constructor that fails earlier never leaves a goroutine behind. A
// non-positive threshold leaves all of the worker channels nil, which maybeCheckpoint
// and Close both treat as "auto-checkpointing disabled" (spec 09 §1.3).
func (d *DB) startCheckpointer(threshold int) {
	if threshold <= 0 {
		return
	}
	d.ckptThreshold = threshold
	d.ckptSig = make(chan struct{}, 1)
	d.ckptStop = make(chan struct{})
	d.ckptDone = make(chan struct{})
	go d.checkpointLoop()
}

// checkpointLoop is the body of the auto-checkpoint worker: it folds the WAL whenever a
// commit signals a backlog past the threshold, and retires when Close stops it. The
// worker takes d.mu inside Checkpoint, so it must run on its own goroutine and never be
// joined while the caller holds the lock. A failed background checkpoint is remembered
// and surfaced from Close rather than crashing the writer that triggered it.
func (d *DB) checkpointLoop() {
	defer close(d.ckptDone)
	for {
		select {
		case <-d.ckptStop:
			return
		case <-d.ckptSig:
			if err := d.Checkpoint(); err != nil {
				d.recordCheckpointErr(err)
			}
		}
	}
}

// maybeCheckpoint signals the background worker when the unfolded WAL backlog has grown
// past the configured threshold. The caller holds d.mu, so the LSN and checkpoint mark
// it reads are stable; the send is non-blocking and the one-slot buffer coalesces a
// burst of commits into a single pending wakeup, so a hot writer never blocks on the
// checkpointer (spec 09 §1.3). It is a no-op when auto-checkpointing is disabled.
func (d *DB) maybeCheckpoint() {
	if d.ckptSig == nil {
		return
	}
	lsn := d.wal.LSN()
	if lsn == 0 {
		return
	}
	written := lsn - 1
	folded := d.pgr.CheckpointLSN()
	if written <= folded || written-folded < uint64(d.ckptThreshold) {
		return
	}
	// An engine that has not yet persisted past the folded point (an LSM core whose
	// memtable has not flushed) cannot reclaim any WAL, so a wakeup would fold nothing
	// and reset nothing. Skip it until a flush advances the durable LSN.
	if dl, tracked := d.engineDurableLSN(); tracked && dl <= folded {
		return
	}
	select {
	case d.ckptSig <- struct{}{}:
	default:
	}
}

// recordCheckpointErr keeps the first background-checkpoint failure so Close can report
// it; later failures are subsumed, since the first is the one that explains the backlog.
func (d *DB) recordCheckpointErr(err error) {
	d.ckptErrMu.Lock()
	if d.ckptErr == nil {
		d.ckptErr = err
	}
	d.ckptErrMu.Unlock()
}

// backgroundErr returns the first background-checkpoint failure, or nil.
func (d *DB) backgroundErr() error {
	d.ckptErrMu.Lock()
	defer d.ckptErrMu.Unlock()
	return d.ckptErr
}

// CheckpointMode selects how aggressively a checkpoint reclaims the WAL (spec 09 §1.2),
// mirroring SQLite's wal_checkpoint modes. This implementation logs commits as a logical
// redo stream and serves reads from the engine over the buffer pool, not by scanning WAL
// frames, so no reader ever pins a frame and a fold is never bounded short: PASSIVE, FULL,
// and RESTART all fold every committed frame and reset the log to its start. TRUNCATE adds
// the one behavioral difference the architecture leaves room for: it returns the WAL file's
// frame space to the operating system.
type CheckpointMode int

const (
	// CheckpointPassive folds every committed frame and resets the log without blocking;
	// the background autocheckpoint runs in this mode. It is the default.
	CheckpointPassive CheckpointMode = iota
	// CheckpointFull folds every committed frame and resets the log. Equivalent to PASSIVE
	// here, since a fold is never bounded by a reader.
	CheckpointFull
	// CheckpointRestart folds and resets so the next writer reuses the log from its start.
	// Equivalent to FULL here, since Checkpointed already restarts the log on every fold.
	CheckpointRestart
	// CheckpointTruncate folds, resets, and truncates the -wal file to its header, the
	// tightest mode, used on idle or close.
	CheckpointTruncate
)

// Checkpoint folds the WAL into the main file and resets the log, in the strict
// order that makes an interrupted checkpoint safe: fold dirty pages and fsync the
// main file (recording the folded LSN and the durable version in its header), then
// append the checkpoint frame, rotate the salt, and reset the WAL (spec 08 §5). A
// crash between the two steps re-folds harmlessly on the next open because redo is
// idempotent. It runs in PASSIVE mode; CheckpointMode selects a tighter one.
func (d *DB) Checkpoint() error {
	return d.CheckpointMode(CheckpointPassive)
}

// CheckpointMode folds the WAL and resets the log like Checkpoint, then applies the extra
// reclamation the mode asks for (spec 09 §1.2). Only TRUNCATE differs behaviorally in this
// architecture: it shrinks the -wal file to its header after the fold, returning the frame
// space to the operating system.
func (d *DB) CheckpointMode(m CheckpointMode) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkpointLocked(); err != nil {
		return err
	}
	if m == CheckpointTruncate {
		// Truncating returns the -wal frame space to the operating system, which is
		// safe only when checkpointLocked reset the log. When the engine lags (an
		// unflushed LSM memtable), checkpointLocked kept the tail and did not reset, so
		// the frames past the durable point are still live and must not be discarded.
		if dl, tracked := d.engineDurableLSN(); tracked && dl < d.wal.LSN()-1 {
			return nil
		}
		return d.wal.TruncateFile()
	}
	return nil
}

// checkpointLocked is the body of Checkpoint; the caller holds d.mu. It is factored
// out so other writer-lock operations that must fold the WAL first, such as Vacuum,
// can reuse it without releasing and reacquiring the lock.
func (d *DB) checkpointLocked() error {
	// Persist the durable commit version from the oracle, the single source of truth.
	// A live commit keeps the header's version current through applyCommitted, but a
	// version reconstructed by redo reaches the engine through eng.Apply directly and
	// never touches the header, so without this the checkpoint would fold the redone
	// pages under a stale version. The next open would then open a snapshot below those
	// commits and find the data invisible (spec 08 §5, spec 10 §1).
	d.pgr.Header().LastCommitVersion = d.orc.lastCommitted()

	foldedLSN := d.wal.LSN() - 1
	resetWAL := true
	if dl, tracked := d.engineDurableLSN(); tracked && dl < foldedLSN {
		// The engine has applied writes it has not yet persisted to the main file (an
		// unflushed LSM memtable, spec 06 §4). Fold only to its durable point and keep
		// the WAL frames past it: the WAL's Checkpointed resets the whole tail, which is
		// correct only when the engine has folded everything, so resetting here would
		// drop applied-but-unflushed data. The next open replays the kept frames into the
		// memtable. The header is still written and fsynced below, so the commit version
		// and any persistent setting remain durable.
		foldedLSN = dl
		resetWAL = false
	}
	if err := d.pgr.Checkpoint(foldedLSN); err != nil {
		return err
	}
	if resetWAL {
		return d.wal.Checkpointed(foldedLSN)
	}
	return nil
}

// engineDurableLSN reports the highest WAL LSN whose effects the engine has
// persisted to the main file, and whether the engine tracks a durable point at all.
// An engine that lands every applied batch in pages the checkpoint folds (the
// B-tree core) does not implement DurableLSN, so it returns tracked=false and the
// checkpoint folds the whole log. The LSM core reports how far its flushes have
// reached so the checkpoint never reclaims WAL past durable data (spec 06 §4).
func (d *DB) engineDurableLSN() (lsn uint64, tracked bool) {
	if e, ok := d.eng.(interface{ DurableLSN() uint64 }); ok {
		return e.DurableLSN(), true
	}
	return 0, false
}

// noteLSN passes a batch's WAL commit LSN to an engine that tracks a durable mark,
// the companion of engineDurableLSN. The host calls it just before Apply on both the
// live commit and redo paths; an engine that does not track LSNs (the B-tree core)
// does not implement it and the call is a no-op.
func (d *DB) noteLSN(lsn uint64) {
	if n, ok := d.eng.(interface{ NoteLSN(uint64) }); ok {
		n.NoteLSN(lsn)
	}
}

// Vacuum runs one round of incremental vacuum (spec 09 §3.1): it folds the WAL with a
// checkpoint so the freelist reflects every committed free, then hands trailing free
// pages back to the operating system by shrinking the file. budget caps how many pages
// it returns in this round, so a caller can bound the truncation work and the time the
// writer lock is held; a non-positive budget reclaims the whole trailing free run. It
// returns the number of pages freed.
//
// Only pages physically at the end of the file can be returned; free pages in the
// middle stay on the freelist for reallocation. Callers wanting steady reclamation run
// it after large deletes, or periodically with a small budget, the kv analog of
// SQLite's "PRAGMA incremental_vacuum(N)".
func (d *DB) Vacuum(budget int) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkpointLocked(); err != nil {
		return 0, err
	}
	return d.pgr.TruncateTail(budget)
}

// ApplicationID reports the application-defined file tag stored in the header (spec 22 §2).
// It is a free-form identifier an application stamps so a tool can recognize its own files.
func (d *DB) ApplicationID() uint32 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.pgr.Header().ApplicationID
}

// SetApplicationID records an application-defined file tag in the header and persists it
// durably (spec 22 §2). It is a persistent-runtime setting: the value survives reopen. The
// change is folded into the main file by a checkpoint, which writes a coherent image (header
// plus all committed data) and fsyncs, so the tag is durable even across a crash and the
// header never desyncs from the WAL.
func (d *DB) SetApplicationID(id uint32) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pgr.Header().ApplicationID = id
	return d.checkpointLocked()
}

// UserVersion reports the application-defined schema/version counter stored in the header
// (spec 22 §2), the kv analog of SQLite's user_version. kv never interprets it.
func (d *DB) UserVersion() uint32 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.pgr.Header().UserVersion
}

// SetUserVersion records the application-defined version counter in the header and persists
// it durably (spec 22 §2). Like SetApplicationID it is a persistent-runtime setting folded
// into the main file by a checkpoint.
func (d *DB) SetUserVersion(v uint32) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pgr.Header().UserVersion = v
	return d.checkpointLocked()
}

// Close releases the database without an implicit checkpoint: committed data is
// already durable in the WAL and recovers on the next open. For a clean shutdown
// that leaves an empty WAL, call Checkpoint first.
//
// It first retires the background checkpointer, joining the worker outside the lock so
// any in-flight passive checkpoint (which takes d.mu itself) finishes before the file
// is torn down, then closes the WAL and pager. A background-checkpoint failure that was
// otherwise silent surfaces here.
func (d *DB) Close() error {
	var bgErr error
	d.closeOnce.Do(func() {
		if d.ckptStop != nil {
			close(d.ckptStop)
			<-d.ckptDone
		}
		// Wake any blocked Subscribe and fence new ones, so a change feed returns
		// promptly when the database closes instead of hanging on a dead commit path.
		d.subsMu.Lock()
		d.subsClosed = true
		if d.subClosed != nil {
			close(d.subClosed)
		}
		d.subsMu.Unlock()
		bgErr = d.backgroundErr()
	})

	d.mu.Lock()
	defer d.mu.Unlock()
	var firstErr error
	if d.wal != nil {
		if err := d.wal.Close(); err != nil {
			firstErr = err
		}
	}
	if err := d.pgr.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr == nil {
		firstErr = bgErr
	}
	return firstErr
}
