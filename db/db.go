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
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/kv/btree"
	"github.com/tamnd/kv/crypto"
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
	// default: every acked commit survives a crash. SyncBarrier is the cheaper
	// middle ground, durable on a process or kernel crash but not guaranteed on
	// power loss, for callers that want most of SyncOff's throughput while staying
	// crash-safe (perf/06 F2).
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
	// FillFactor is the B-tree core's target leaf occupancy before it splits a node, in
	// the range (0, 1]. Zero takes the engine default (~0.7). A higher value packs more
	// keys per page (fewer pages, better read fan-out) at the cost of more splits on
	// random inserts. The LSM core ignores it (spec 05 §3).
	FillFactor float64
	// MaxInlineValue is the maximum value size the B-tree core stores inline on a leaf
	// page; larger values overflow to dedicated overflow pages. Zero takes the engine
	// default (¼ page). Increasing it avoids overflow page I/O for medium values at the
	// cost of sparser leaves; decreasing it keeps leaves dense for key-heavy workloads
	// (spec 05 §2.2). The LSM core ignores it.
	MaxInlineValue int
	// LevelRatio is the LSM core's size multiplier between adjacent levels: each level
	// is LevelRatio times larger than the one above it (spec 06 §4). Zero takes the
	// engine default (10). A larger ratio reduces write amplification by doing fewer,
	// bigger compactions at the cost of higher read fan-out on the cold levels. The
	// B-tree core ignores it.
	LevelRatio int
	// ValueSepThreshold, when positive, enables the LSM core's WiscKey value separation
	// (spec 06 §7): values larger than this many bytes are written to a separate vLog
	// file and only a pointer lives in the main segment, so large-value workloads keep
	// the tree small and cache-resident. Zero disables separation. The B-tree core
	// ignores it.
	ValueSepThreshold int
	// Compression turns on the LSM core's heat-tiered block compression (spec 13): segment
	// data pages are compressed with a cheap fast codec on the hot shallow levels and a
	// higher-ratio codec on the cold deep ones, packing more cells per page so the file
	// shrinks and more data rides in each cached page, at the cost of decompress CPU on the
	// read miss path. Off by default, since it trades read CPU for space; it helps
	// space-bound or write-heavy workloads on storage slower than the CPU. The B-tree core
	// ignores it.
	Compression bool
	// CompressionMode selects which levels the LSM core compresses, refining the on/off
	// Compression bool. The zero value defers to that bool (off, or heat-tiered when the
	// bool is set). engine.CompressColdOnly leaves the hot levels (L0, L1) raw and
	// compresses only the cold deep levels where the bulk of the data settles, so the file
	// shrinks toward sub-1.0x without putting decompress CPU on the hot read path (perf/05
	// F4d). When set it overrides Compression. The B-tree core ignores it.
	CompressionMode engine.CompressionMode
	// disableAutoCompaction turns off the LSM core's background compaction scheduler so
	// compaction runs only on an explicit Maintain. It is unexported: production always
	// self-schedules compaction, and only an in-package test that drives compaction by hand
	// to observe a precise segment shape or a deterministic crash window sets it.
	disableAutoCompaction bool
	// Logger is the structured-logging sink for database lifecycle, recovery,
	// checkpoint, maintenance, the fatal durability fault, and slow operations (spec
	// 19 §3). Nil, the default, disables logging entirely: no event is formatted and
	// no clock is read, so a build that does not set it pays nothing. A caller passes a
	// *slog.Logger to route these events into its own logging.
	Logger *slog.Logger
	// SlowOpThreshold arms the slow-op log: a commit or point read that runs at least
	// this long is logged at WARN with its key range and a phase breakdown (spec 19
	// §3). Zero, the default, disables it, and the read path skips reading the clock
	// when it is zero or Logger is nil. It has effect only when Logger is set.
	SlowOpThreshold time.Duration
	// Tracer arms the tracing surface: when set, kv starts a span around each operation
	// and its major phases (commit and its durable/apply split, checkpoint, compaction,
	// point read) so a slow request can be attributed to I/O versus engine versus
	// compaction (spec 19 §3). Nil, the default, disables tracing: no span is started and
	// every site is a single nil check, so a build that does not set it pays nothing. The
	// host adapts the Tracer to OpenTelemetry or any backend, so kv takes no tracing
	// dependency.
	Tracer Tracer
	// EncryptionKey, when non-empty, encrypts the main file's data pages at rest under
	// AES-256-GCM, the spec default cipher (spec 14). It must be exactly 32 bytes, a
	// key supplied directly (the KMS-managed path); the passphrase KDF that stretches a
	// human secret into this key arrives in a later slice. Page 1 (the header and the
	// cleartext key descriptor) and the freelist stay in the clear so the file stays
	// self-describing and a wrong key is a clean error rather than garbage.
	//
	// Slice-1 limitation: the write-ahead log sidecar is not yet encrypted, so data
	// committed since the last checkpoint can sit in the .wal file in the clear. The
	// public facade therefore does not expose this option yet; it becomes public once
	// WAL frame encryption lands.
	EncryptionKey []byte
	// ReadReplica opens the database as a read-only follower (spec 18 §4). User writes
	// are refused with ErrReadOnlyTxn; the only way state advances is ApplyWAL, which
	// replays committed frames shipped from a primary through the same redo path
	// recovery uses. A replica is always a consistent point-in-time copy of its primary,
	// slightly behind it (asynchronous replication); promote one to a writable primary
	// by reopening the file without this flag.
	ReadReplica bool
	// WALArchive, when set, receives each WAL generation as a self-describing delta just
	// before a checkpoint folds and resets it, so the committed history outlives the live
	// -wal file and supports point-in-time recovery (spec 18 §6). Each delta is the same
	// container ShipWAL produces, covering the commits since the previously archived
	// generation, so restoring a base backup and replaying the archived deltas in order
	// through ApplyWALUntil, stopped at a target version, reconstructs the exact committed
	// state at that version. The sink is called under the database write lock at checkpoint
	// time; an error it returns fails the checkpoint rather than reset the log, so a frame
	// is never lost to a failed archive. A generation with no new commits is not archived.
	WALArchive func(delta []byte) error
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
	// The zero value of wal.Sync is the reserved "unset" sentinel, not a real level, so
	// an unconfigured Options maps to SyncFull, the safe default. Any explicit choice,
	// including SyncOff, is non-zero and passes through untouched, which is what lets a
	// caller actually turn fsync off (perf/06 F1).
	if o.Sync == 0 {
		return wal.SyncFull
	}
	return o.Sync
}

// compressionMode resolves the on/off Compression bool and the finer CompressionMode into a
// single mode for the engine. An explicit CompressionMode wins; otherwise the bool maps to
// heat-tiered when set and off when not, so a caller that only flips Compression keeps the
// behaviour it had before this knob existed.
func (o Options) compressionMode() engine.CompressionMode {
	if o.CompressionMode != engine.CompressDefault {
		return o.CompressionMode
	}
	if o.Compression {
		return engine.CompressHeatTiered
	}
	return engine.CompressOff
}

// DB is an open database: a pager over the main file, a WAL sidecar, and a storage
// core, with a monotonic commit-version counter. It is safe for concurrent readers
// and serializes writers through its mutex (group commit and MVCC concurrency are
// later milestones).
type DB struct {
	fs   vfs.FS
	path string

	// rl serializes the single committing writer against itself and against the
	// engine reads (spec 10 §5.1): a commit takes it exclusively for log+apply, a
	// read takes it shared. The version state lives in the lock-light oracle, not
	// here, so it is consulted off this lock. It is a distributed RWMutex (rlatch),
	// so a read takes only its P's stripe and reads scale across cores instead of
	// serializing on one RWMutex reader count (perf/10 R1); the contract is the same
	// as a sync.RWMutex.
	rl  *rlatch
	pgr *pager.Pager
	wal *wal.WAL
	eng engine.Engine
	orc *oracle
	// orcPub publishes the oracle to the engine's background workers (the LSM
	// flusher's compaction watermark) race-free. orc itself is assigned once on the
	// Open goroutine, but a background compaction can read it concurrently during
	// recovery, before that assignment; the atomic load returns nil until then, which
	// the watermark adapter reads as "GC nothing yet."
	orcPub atomic.Pointer[oracle]

	// crypto is the encryption scheme this database seals pages and WAL frames under, nil
	// for an unencrypted file (spec 14). It is the handle a key rotation advances: RotateEncryptionKey
	// derives the next epoch from it and swaps the pager's and WAL's schemes.
	crypto *crypto.Scheme

	merge           func(existing, operand []byte) []byte
	maxRetries      int
	isolation       Isolation
	memtableSize    int
	rangeIndex      bool
	filter          engine.FilterKind
	bufferedInserts bool
	compression     engine.CompressionMode
	noAutoCompact   bool
	fillFactor      float64
	maxInlineValue  int
	levelRatio      int
	valueSepThresh  int

	// readReplica makes this a read-only follower (spec 18 §4): user writes are refused
	// and state advances only through ApplyWAL replaying shipped frames. replicaHigh is
	// the primary's commit version as of the last applied ship, so ReplicaLag (the gap to
	// the follower's applied version) is observable in Stats. Both are guarded by d.mu:
	// ApplyWAL writes them under the write lock, Stats reads them under the read lock.
	readReplica bool
	replicaHigh uint64

	// archive, when set, receives each WAL generation as a replication delta just before a
	// checkpoint resets the log, so the committed history survives for point-in-time
	// recovery (spec 18 §6). It is called under d.mu at checkpoint time.
	archive func(delta []byte) error

	// counters is the cumulative-since-open operation tally surfaced through Stats and
	// the Prometheus exposition (spec 19 §1.1). Reads bump it on the transaction path,
	// commits on applyCommitted; it is all atomics, so no lock guards it.
	counters opCounters

	// logger is the structured-logging sink (spec 19 §3), nil when logging is off. Every
	// emitter in logging.go guards on nil, so a disabled build formats no events. slowOp
	// is the slow-op threshold from Options.SlowOpThreshold; the read path times an
	// operation only when both a logger is set and slowOp is positive.
	logger *slog.Logger
	slowOp time.Duration
	// tracer is the span hook from Options.Tracer (spec 19 §3), nil when tracing is off.
	// Every site goes through startSpan, which guards on nil, so a disabled build starts
	// no spans.
	tracer Tracer

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

	// Group commit (spec 06 F3). Committers queue their request under cmu and the first
	// to find no leader becomes one: it drains the queue, appends every batch's WAL
	// frames, issues one shared fsync for the whole group, and applies them in version
	// order under d.mu. So N concurrent committers pay one fsync, not N in series, and
	// only the leader contends for d.mu while the rest wait on ccond. cleader is true
	// while a leader is processing a group.
	cmu     sync.Mutex
	ccond   *sync.Cond
	cqueue  []*commitReq
	cleader bool

	// Auto-checkpointer (spec 09 §1.3). When ckptThreshold is positive a single
	// long-lived goroutine folds the WAL in the background: a commit that pushes the
	// backlog past the threshold signals ckptSig (non-blocking, coalesced through the
	// one-slot buffer), and the worker runs a passive Checkpoint off the commit path.
	// ckptStop closes on Close to retire the worker, ckptDone closes when it has; both
	// are nil when auto-checkpointing is disabled. The worker takes d.mu itself, so the
	// signal must be sent while holding it but the shutdown join must not.
	//
	// After each successful checkpoint the worker also drains dead B-tree MVCC versions
	// with bounded Maintain calls (perf/05 F3c). gcPagesPerCheckpoint is the per-call
	// page budget; the worker loops until More is false. Zero disables the GC step.
	ckptThreshold        int
	ckptSig              chan struct{}
	ckptStop             chan struct{}
	ckptDone             chan struct{}
	gcPagesPerCheckpoint int
	closeOnce            sync.Once

	ckptErrMu sync.Mutex
	ckptErr   error

	// ckptMu serializes whole checkpoint folds so two CheckpointMode calls (the
	// background worker and an explicit Checkpoint, say) never run at once. Without it
	// one fold's lock-free page-image logging (off d.mu since slice 95) would race the
	// other's prepare/finalize reads of the WAL tail, and two folds resetting the same
	// WAL generation is wasteful besides. It wraps the whole CheckpointMode body and is
	// always taken before d.mu; checkpointLocked does not take it (it holds d.mu
	// throughout, which already excludes a concurrent CheckpointMode).
	ckptMu sync.Mutex

	// fullPageWrites controls whether the checkpoint path logs a full pre-image of each
	// dirty page to the WAL before overwriting it on disk (spec 07 §5). When true (the
	// default), recovery can restore partially-written pages after an interrupted
	// checkpoint. Set to false only on storage that guarantees atomic 4 KiB writes.
	// Reads and writes under d.mu; persisted in the file header's FullPageWritesOff field.
	fullPageWrites bool

	// autoVacuumMode controls automatic space reclamation (spec 09 §3.3). 0=NONE (off),
	// 1=INCREMENTAL (truncate trailing free pages after each checkpoint),
	// 2=FULL (same as INCREMENTAL for now; pointer-map optimization is future work).
	// Reads and writes under d.mu; persisted in the file header's AutoVacuumMode field.
	autoVacuumMode uint8

	// commitLingerUs is the maximum microseconds the group-commit leader waits for
	// additional committers to join the group before flushing (spec 07 §4). 0=no explicit
	// delay (adaptive, the existing behaviour). Writes to lingerUs are atomic; the zero
	// value is a correct default even on first open before the header is loaded.
	lingerUs atomic.Uint32

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
	enc, descBytes, err := newEncryptionForCreate(opts)
	if err != nil {
		return nil, err
	}
	pgr, err := pager.Create(fs, path, pager.Options{
		PageSize:    opts.pageSize(),
		CacheFrames: opts.CacheFrames,
		Engine:      opts.engineKind(),
		Checksum:    opts.checksum(),
		Flags:       format.FlagWAL,
		Encryption:  enc,
		Descriptor:  descBytes,
	})
	if err != nil {
		return nil, err
	}
	w, err := wal.Create(fs, path+walSuffix, wal.Options{PageSize: pgr.PageSize(), Sync: opts.sync(), Encryption: enc})
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
	chdr := pgr.Header()
	d := &DB{rl: newRlatch(), fs: fs, path: path, pgr: pgr, wal: w, eng: eng, orc: newOracle(0), crypto: enc,
		merge: opts.Merge, maxRetries: opts.maxRetries(), isolation: opts.Isolation, memtableSize: opts.MemtableSize, rangeIndex: opts.RangeIndex, filter: opts.Filter, bufferedInserts: opts.BufferedInserts, compression: opts.compressionMode(), noAutoCompact: opts.disableAutoCompaction, fillFactor: opts.FillFactor, maxInlineValue: opts.MaxInlineValue, levelRatio: opts.LevelRatio, valueSepThresh: opts.ValueSepThreshold, now: opts.clock(), logger: opts.Logger, slowOp: opts.SlowOpThreshold, tracer: opts.Tracer, readReplica: opts.ReadReplica, archive: opts.WALArchive,
		fullPageWrites: chdr.FullPageWritesOff == 0,
		autoVacuumMode: chdr.AutoVacuumMode,
	}
	d.ccond = sync.NewCond(&d.cmu)
	d.lingerUs.Store(chdr.CommitLingerUs)
	d.syncPageImageLogger()
	if err := d.openEngine(opts.Merge); err != nil {
		w.Close()
		pgr.Close()
		return nil, err
	}
	d.startCheckpointer(opts.autoCheckpoint(), defaultGCPagesPerCheckpoint)
	d.logOpened(0)
	return d, nil
}

// openExisting opens an existing main file, resumes or creates its WAL, and redoes
// the committed tail.
func openExisting(fs vfs.FS, path string, opts Options) (*DB, error) {
	enc, err := openEncryptionForExisting(fs, path, opts)
	if err != nil {
		return nil, err
	}
	pgr, err := pager.Open(fs, path, pager.Options{CacheFrames: opts.CacheFrames, Encryption: enc})
	if err != nil {
		return nil, err
	}
	eng, err := newEngine(pgr.Header().Engine, pgr)
	if err != nil {
		pgr.Close()
		return nil, err
	}
	hdr := pgr.Header()
	d := &DB{rl: newRlatch(), fs: fs, path: path, pgr: pgr, eng: eng, crypto: enc,
		merge: opts.Merge, maxRetries: opts.maxRetries(), isolation: opts.Isolation, memtableSize: opts.MemtableSize, rangeIndex: opts.RangeIndex, filter: opts.Filter, bufferedInserts: opts.BufferedInserts, compression: opts.compressionMode(), noAutoCompact: opts.disableAutoCompaction, fillFactor: opts.FillFactor, maxInlineValue: opts.MaxInlineValue, levelRatio: opts.LevelRatio, valueSepThresh: opts.ValueSepThreshold, now: opts.clock(), logger: opts.Logger, slowOp: opts.SlowOpThreshold, tracer: opts.Tracer, readReplica: opts.ReadReplica, archive: opts.WALArchive,
		fullPageWrites: hdr.FullPageWritesOff == 0,
		autoVacuumMode: hdr.AutoVacuumMode,
	}
	d.ccond = sync.NewCond(&d.cmu)
	d.lingerUs.Store(hdr.CommitLingerUs)
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
		w, rec, err := wal.Open(fs, walPath, wal.Options{PageSize: pgr.PageSize(), Sync: opts.sync(), Encryption: enc})
		if err != nil {
			pgr.Close()
			return nil, err
		}
		d.wal = w
		// A checkpoint that emptied the log leaves the next generation with no frames, so
		// the durable-tail scan positions the writer at LSN 1 even though the pager's
		// checkpoint marker sits at the folded LSN. Resume past that marker so the next
		// commit lands above it and redo on a later open keeps it (spec 08 §5).
		d.wal.ResumeFrom(d.pgr.CheckpointLSN() + 1)
		// If the WAL scan recovered any page pre-images, restore corrupt pages from them
		// before replaying kv batches. This heals an interrupted checkpoint: pages the
		// checkpoint started writing but did not finish are restored to their pre-write
		// state, so redo can traverse the B-tree correctly (spec 07 §5).
		if len(rec.PageImages) > 0 {
			if err := d.pgr.RestorePageImages(rec.PageImages); err != nil {
				w.Close()
				pgr.Close()
				return nil, err
			}
		}
		// Count the committed batches recovery is about to replay before redo consumes
		// them, so the recovery log can report exactly what was replayed and whether a
		// torn tail was discarded.
		replayed := len(rec.CommittedAfter(d.pgr.CheckpointLSN()))
		if maxVer, err = d.redo(rec); err != nil {
			w.Close()
			pgr.Close()
			return nil, err
		}
		d.logRecovery(replayed, maxVer, rec.TornTail)
	} else {
		w, err := wal.Create(fs, walPath, wal.Options{PageSize: pgr.PageSize(), Sync: opts.sync(), Encryption: enc})
		if err != nil {
			pgr.Close()
			return nil, err
		}
		d.wal = w
	}

	// Wire the full-page-write logger now that both d.wal and d.pgr are ready.
	d.syncPageImageLogger()

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
	d.orcPub.Store(d.orc)
	d.startCheckpointer(opts.autoCheckpoint(), defaultGCPagesPerCheckpoint)
	d.logOpened(last)
	return d, nil
}

// engineWatermark feeds a core's background work the version-GC horizon (spec 10 §6):
// the oldest version any live reader can still observe, below which a superseded version
// is collectible. The LSM core reads it when its background compaction merges away dead
// versions. The oracle is built after the engine opens, so this loads it atomically and
// reports 0 (collect nothing) until it exists, which is only during recovery before the
// database is live and no reader can yet hold a version back.
type engineWatermark struct{ d *DB }

func (w engineWatermark) OldestReadable() uint64 {
	o := w.d.orcPub.Load()
	if o == nil {
		return 0
	}
	return o.readMark()
}

// openEngine wires the engine to its substrate and installs the merge resolver.
func (d *DB) openEngine(merge func(existing, operand []byte) []byte) error {
	env := &engine.Env{
		Pager: d.pgr,
		Clock: engineWatermark{d},
		Options: engine.EngineOptions{
			PageSize:          d.pgr.PageSize(),
			MemtableSize:      d.memtableSize,
			RangeIndex:        d.rangeIndex,
			Filter:            d.filter,
			BufferedInserts:   d.bufferedInserts,
			CompressionMode:   d.compression,
			FillFactor:        d.fillFactor,
			MaxInlineValue:    d.maxInlineValue,
			LevelSizeRatio:    d.levelRatio,
			ValueSepThreshold: d.valueSepThresh,

			DisableAutoCompaction: d.noAutoCompact,
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
//
// The commit joins a group: concurrent Writes queue together and share one fsync rather
// than each paying its own (spec 06 F3). The batch is built inside the leader's prepare
// step so it is stamped at the version the commit is actually assigned.
func (d *DB) Write(fn func(b *engine.WriteBatch)) (uint64, error) {
	return d.submitCommit(&commitReq{
		prepare: func() (*engine.WriteBatch, uint64, bool, error) {
			if d.readReplica {
				return nil, 0, false, ErrReadOnlyTxn
			}
			// Under the leader's write lock the next version is stable between this peek
			// and the formal commit, so the batch is built at it before it is reserved.
			v := d.orc.peekNext()
			b := engine.NewWriteBatch(v)
			fn(b)
			if b.Len() == 0 {
				// An empty write consumes no version; report the last committed one.
				return nil, v - 1, true, nil
			}
			got := d.orc.commit(batchKeys(b))
			return b, got, false, nil
		},
	})
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
	d.rl.Lock()
	if d.fatal != nil {
		d.rl.Unlock()
		return 0, d.fatal
	}
	bl, ok := d.eng.(engine.BulkLoader)
	if ok && d.orc.lastCommitted() == 0 {
		v, err := d.loadFast(bl, next)
		d.rl.Unlock()
		return v, err
	}
	d.rl.Unlock()
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
	return d.submitCommit(&commitReq{
		prepare: func() (*engine.WriteBatch, uint64, bool, error) {
			if d.readReplica {
				return nil, 0, false, ErrReadOnlyTxn
			}
			v, ok := d.orc.newCommitTs(readVersion, conflictKeys)
			if !ok {
				return nil, 0, false, ErrConflict
			}
			return d.buildOpsBatch(v, ops), v, false, nil
		},
	})
}

// commitTxnSerializable is the serializable-isolation commit path (spec 10 §4): it is
// commitTxn with the oracle's read-set validation in place of the plain write-write
// check. writeKeys is the resolved write set (first-committer-wins), readKeys and
// ranges are what the transaction read (rw-antidependency detection). It returns the
// assigned commit version, or ErrConflict if either check fails.
func (d *DB) commitTxnSerializable(readVersion uint64, ops []pendingOp, writeKeys, readKeys []string, ranges []keyRange) (uint64, error) {
	return d.submitCommit(&commitReq{
		prepare: func() (*engine.WriteBatch, uint64, bool, error) {
			if d.readReplica {
				return nil, 0, false, ErrReadOnlyTxn
			}
			v, ok := d.orc.newCommitTsSerializable(readVersion, writeKeys, readKeys, ranges)
			if !ok {
				return nil, 0, false, ErrConflict
			}
			return d.buildOpsBatch(v, ops), v, false, nil
		},
	})
}

// buildOpsBatch assembles the engine write batch for an admitted transaction at its
// assigned version v. It runs inside the leader's prepare closure (under d.mu) so the
// version baked into each internal key is the one the oracle actually handed out, the
// same build the old single-writer applyTxn did before group commit took over the log
// and apply phases.
func (d *DB) buildOpsBatch(v uint64, ops []pendingOp) *engine.WriteBatch {
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
	return b
}

// batchKeys returns the unique user keys a blind batch wrote, so a Write
// participates in conflict detection against concurrent transactions.
//
// A blind write is usually a handful of keys, so for a small batch the dedup is a linear scan
// of the result, which keeps the whole call to one slice allocation; the map, which costs an
// allocation of its own on every commit, is reserved for the rare large batch where the
// quadratic scan would actually matter (spec 07, commit-side cost).
func batchKeys(b *engine.WriteBatch) []string {
	entries := b.Entries()
	keys := make([]string, 0, len(entries))
	if len(entries) <= batchKeysLinearMax {
		for _, e := range entries {
			k := string(format.UserKey(e.InternalKey))
			dup := false
			for _, existing := range keys {
				if existing == k {
					dup = true
					break
				}
			}
			if !dup {
				keys = append(keys, k)
			}
		}
		return keys
	}
	seen := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		k := string(format.UserKey(e.InternalKey))
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	return keys
}

// batchKeysLinearMax is the batch size below which batchKeys dedups by linear scan instead of a
// map. Set where the quadratic scan is still cheaper than allocating and populating a map.
const batchKeysLinearMax = 16

// snapshotGet reads key at a fixed version through a short-lived engine reader,
// taking the shared read lock so it never observes a page mid-commit. It returns
// the value and whether the key is present at that snapshot (spec 10 §3).
func (d *DB) snapshotGet(version uint64, key []byte) ([]byte, bool, error) {
	_, span := d.startSpan(context.Background(), "kv.get")
	defer endSpan(span)
	sh := d.rl.RLock()
	defer d.rl.RUnlock(sh)
	// Readerless fast path: an engine that resolves a point read off its shared immutable
	// state needs no per-call reader (perf/10 R3). The NewReader path below heap-allocates the
	// reader and, in the B-tree, folds through the streaming resolver that allocates a result
	// slice for one key; GetAt does neither. Fall back to NewReader when the engine declines.
	if pr, ok := d.eng.(engine.PointReader); ok {
		v, err := pr.GetAt(engine.Snapshot{Version: version, Clock: d.now}, key)
		if err == engine.ErrNotFound {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		return v, true, nil
	}
	rd, err := d.eng.NewReader(engine.Snapshot{Version: version, Clock: d.now})
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

// snapshotGetZeroCopy is snapshotGet for the zero-copy read: it serves the value through
// the engine's ZeroCopyReader when the reader implements it, and falls back to the copying
// Get otherwise. The capability's contract is that the value stays valid after the reader is
// closed (it is backed by an immutable internal node, not the reader's transient state), so
// returning it past rd.Close and the read lock is sound; an engine that cannot promise that
// simply does not implement the capability and takes the copying path here.
func (d *DB) snapshotGetZeroCopy(version uint64, key []byte) ([]byte, bool, error) {
	_, span := d.startSpan(context.Background(), "kv.get")
	defer endSpan(span)
	sh := d.rl.RLock()
	defer d.rl.RUnlock(sh)
	rd, err := d.eng.NewReader(engine.Snapshot{Version: version, Clock: d.now})
	if err != nil {
		return nil, false, err
	}
	defer rd.Close()
	var v []byte
	if zc, ok := rd.(engine.ZeroCopyReader); ok {
		v, err = zc.GetZeroCopy(key)
	} else {
		v, err = rd.Get(key)
	}
	if err == engine.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

// GetZeroCopy reads userKey at the latest committed snapshot and returns the value aliased
// to immutable engine storage instead of a fresh copy, for read paths hot enough that the
// per-read copy and allocation matter. The returned value is READ-ONLY: it may be shared
// with the engine's cache and with other concurrent readers, so a caller that modifies it
// corrupts that shared copy. It stays valid for reading after this returns. A caller that
// needs an owned, mutable, or long-retained value should use Get, which copies. When the
// engine does not support zero-copy reads this transparently falls back to Get's copy, so it
// is always safe to call; it just may not save the copy.
func (d *DB) GetZeroCopy(userKey []byte) ([]byte, error) {
	v, ok, err := d.snapshotGetZeroCopy(d.Version(), userKey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, engine.ErrNotFound
	}
	return v, nil
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
	return d.eng.NewReader(engine.Snapshot{Version: version, Clock: d.now})
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
	d.rl.Lock()
	defer d.rl.Unlock()
	budget := engine.MaintBudget{MaxPages: maxPages, Watermark: d.orc.readMark(), Now: d.now()}
	// Trace the maintenance round (compaction, version GC) so a span backend can see how
	// much of a request's latency is background work serialized behind the writer lock
	// (spec 19 §3). The span passes its context to the engine, so a tracer-aware engine
	// could nest its own phases, and is a no-op when no tracer is set.
	ctx, span := d.startSpan(context.Background(), "kv.compaction")
	rep, err := d.eng.Maintain(ctx, budget)
	endSpan(span)
	if err == nil {
		d.logMaintain(rep)
	}
	return rep, err
}

// Verify runs the engine's structural self-check and returns its report (spec 16 §4,
// spec 23 §3). It takes the writer lock so the walk sees a stable tree, not one mid
// commit or mid checkpoint. It returns ErrUnsupported when the engine has no verifier,
// so the CLI can say so plainly rather than reporting a silent pass.
func (d *DB) Verify() (*engine.VerifyReport, error) {
	d.rl.Lock()
	defer d.rl.Unlock()
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
	// ReadReplica reports whether the database was opened as a read-only follower
	// (spec 18 §4).
	ReadReplica bool
	// ReplicaLag is the follower's distance behind its primary in commit versions: the
	// primary's commit version as of the last applied ship minus the follower's applied
	// version. It is 0 on a primary and on a fully caught-up replica; a value that climbs
	// means ships are arriving slower than the primary commits (spec 18 §4).
	ReplicaLag uint64
}

// Stats gathers a Stats snapshot under a read lock, so it is consistent against a
// concurrent commit without blocking one for long (spec 09 §4).
func (d *DB) Stats() Stats {
	sh := d.rl.RLock()
	defer d.rl.RUnlock(sh)

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
	var lag uint64
	if applied := d.orc.lastCommitted(); d.replicaHigh > applied {
		lag = d.replicaHigh - applied
	}
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
		ReadReplica:            d.readReplica,
		ReplicaLag:             lag,
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

// defaultGCPagesPerCheckpoint is the per-Maintain call page budget the auto-GC step
// uses after each background checkpoint. 512 pages = 2 MiB at the default 4 KiB page
// size, a single GC batch that leaves the writer lock free between passes.
const defaultGCPagesPerCheckpoint = 512

// startCheckpointer launches the background passive-checkpoint worker when threshold
// is positive. It is called once at the end of open, after every field the worker
// reads is set, so a constructor that fails earlier never leaves a goroutine behind. A
// non-positive threshold leaves all of the worker channels nil, which maybeCheckpoint
// and Close both treat as "auto-checkpointing disabled" (spec 09 §1.3). When the
// worker starts, it also arms the post-checkpoint auto-GC step unless gcPages is zero.
// SyncMode returns the WAL's current sync level (spec 22 §3).
func (d *DB) SyncMode() wal.Sync { return d.wal.SyncMode() }

// SetSyncMode changes the WAL sync level, taking effect on the next commit (spec 22 §3).
func (d *DB) SetSyncMode(s wal.Sync) { d.wal.SetSync(s) }

// AutoCheckpointFrames returns the WAL backlog threshold at which the background
// checkpointer fires, 0 when auto-checkpointing is disabled (spec 22 §3).
func (d *DB) AutoCheckpointFrames() int {
	sh := d.rl.RLock()
	t := d.ckptThreshold
	d.rl.RUnlock(sh)
	return t
}

// SetAutoCheckpointFrames changes the background-checkpoint trigger threshold in WAL
// frames. Zero or negative disables auto-checkpointing (spec 22 §3). The change takes
// effect on the next commit; the background worker goroutine (if any) continues running.
func (d *DB) SetAutoCheckpointFrames(n int) {
	d.rl.Lock()
	d.ckptThreshold = n
	d.rl.Unlock()
}

// CacheFrames returns the buffer pool capacity in frames (pages). Multiply by PageSize
// for bytes (spec 22 §5).
func (d *DB) CacheFrames() int { return d.pgr.CacheFrames() }

func (d *DB) startCheckpointer(threshold, gcPages int) {
	if threshold <= 0 {
		return
	}
	d.ckptThreshold = threshold
	d.gcPagesPerCheckpoint = gcPages
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
				continue
			}
			// A successful checkpoint advances the GC horizon (the oracle read-mark),
			// making dead B-tree versions collectible. Run bounded Maintain passes until
			// the engine reports no more work or the worker is asked to stop, so dead
			// versions do not accumulate between explicit Maintain calls (perf/05 F3c).
			if d.gcPagesPerCheckpoint > 0 {
				d.drainGC()
			}
		}
	}
}

// drainGC runs Maintain in a loop until no more GC work is ready or Close signals the
// worker to stop. Each call holds d.mu only for one bounded batch, so writers are not
// locked out for longer than a single GC pass.
func (d *DB) drainGC() {
	for {
		select {
		case <-d.ckptStop:
			return
		default:
		}
		rep, err := d.Maintain(d.gcPagesPerCheckpoint)
		if err != nil || !rep.More {
			return
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

// CheckpointMode folds the WAL and resets the log like Checkpoint, then applies the
// extra reclamation the mode asks for (spec 09 §1.2). Only TRUNCATE differs
// behaviorally: it shrinks the -wal file to its header after the fold, returning the
// frame space to the operating system.
//
// The page-writeback and fsync (the slow I/O part) run without d.mu so foreground
// writers can commit concurrently. d.mu is held only for the brief prepare step
// (capture the fold LSN, stamp the header version) and the finalize step (WAL reset,
// archive). This removes the periodic write stall the checkpoint caused before
// (perf/02 F5).
func (d *DB) CheckpointMode(m CheckpointMode) error {
	_, span := d.startSpan(context.Background(), "kv.checkpoint")
	defer endSpan(span)

	// Serialize whole folds: a second checkpoint must not enter prepare/finalize while
	// this one's lock-free page-image logging is appending to the WAL tail off d.mu.
	d.ckptMu.Lock()
	defer d.ckptMu.Unlock()

	d.rl.Lock()
	foldedLSN, lastCommitVersion, resetWAL := d.prepareCheckpointLocked()
	d.rl.Unlock()

	// Page writeback and fsync run without d.mu. The pager's own shard locks and
	// ckptGate provide the necessary mutual exclusion for the I/O, so foreground
	// writers can commit into the WAL while the checkpoint is writing pages from the
	// buffer pool to the main file. lastCommitVersion is passed in rather than read
	// from p.header inside pgr.Checkpoint to avoid a data race with the commit path.
	if err := d.pgr.Checkpoint(foldedLSN, lastCommitVersion); err != nil {
		return err
	}

	d.rl.Lock()
	defer d.rl.Unlock()
	if err := d.finalizeCheckpointLocked(foldedLSN, resetWAL); err != nil {
		return err
	}
	d.runAutoVacuumLocked()
	if m == CheckpointTruncate {
		// Truncating returns the -wal frame space to the OS, which is safe only when
		// finalizeCheckpointLocked reset the log. When the engine lags (an unflushed
		// LSM memtable), the WAL tail was kept and must not be discarded.
		if dl, tracked := d.engineDurableLSN(); tracked && dl < d.wal.LSN()-1 {
			return nil
		}
		return d.wal.TruncateFile()
	}
	return nil
}

// prepareCheckpointLocked captures the fold LSN, the WAL-reset flag, and the oracle's
// commit version at this instant. The caller holds d.mu. No I/O is performed and the
// pager header is not touched here — lastCommitVersion is passed to pgr.Checkpoint,
// which stamps the header atomically under its own locks, avoiding a data race with the
// commit path that also writes LastCommitVersion under d.mu (a lock independent of the
// pager's shard + metaMu locks). The version reconstructed by redo replay reaches the
// engine through eng.Apply and never goes through publishApplied, so capturing it here
// from the oracle is the correct single source of truth (spec 08 §5, spec 10 §1).
func (d *DB) prepareCheckpointLocked() (foldedLSN uint64, lastCommitVersion uint64, resetWAL bool) {
	lastCommitVersion = d.orc.lastCommitted()
	foldedLSN = d.wal.LSN() - 1
	resetWAL = true
	if dl, tracked := d.engineDurableLSN(); tracked && dl < foldedLSN {
		// The engine has applied writes it has not yet persisted to the main file (an
		// unflushed LSM memtable, spec 06 §4). Fold only to its durable point and keep
		// the WAL frames past it: resetting here would drop applied-but-unflushed data.
		// The next open replays the kept frames into the memtable.
		foldedLSN = dl
		resetWAL = false
	}
	return foldedLSN, lastCommitVersion, resetWAL
}

// finalizeCheckpointLocked logs the checkpoint and resets/archives the WAL when the
// fold was complete. The caller holds d.mu. pgr.Checkpoint must have already succeeded.
func (d *DB) finalizeCheckpointLocked(foldedLSN uint64, resetWAL bool) error {
	if !resetWAL {
		d.logCheckpoint(foldedLSN, d.orc.lastCommitted(), false)
		return nil
	}
	// Commits that ran during the lock-free I/O phase (between d.mu release in
	// CheckpointMode and re-acquire here) wrote WAL frames at LSNs > foldedLSN and
	// then were blocked by pgr.Checkpoint's shard locks from applying their pages to
	// the engine. Those pages were dirtied after pgr.Checkpoint's frame-flush loop, so
	// they are not on disk yet. Their WAL frames are in the old generation: if we call
	// wal.Checkpointed(foldedLSN) now, those frames become inaccessible (old generation,
	// overwritten by the next write), and their page changes would be silently rolled back
	// — a durability violation. Fix: run a second pgr.Checkpoint at the current WAL
	// high-water mark. It flushes the remaining dirty pages and updates the on-disk
	// header with the actual fold LSN and version, then wal.Checkpointed resets at the
	// correct boundary. For checkpointLocked (which holds d.mu throughout), no concurrent
	// commits can slip in and actualFoldedLSN == foldedLSN, so the extra call is skipped.
	actualFoldedLSN := d.wal.LSN() - 1
	actualVersion := d.orc.lastCommitted()
	if actualFoldedLSN > foldedLSN {
		if err := d.pgr.Checkpoint(actualFoldedLSN, actualVersion); err != nil {
			return err
		}
		foldedLSN = actualFoldedLSN
	}
	d.logCheckpoint(foldedLSN, actualVersion, true)
	// Archive the generation about to be folded before the reset discards it, so the
	// committed history outlives the live -wal file for point-in-time recovery (spec 18
	// §6). A failed archive fails the checkpoint, so the frames are never lost.
	if err := d.archiveGeneration(); err != nil {
		return err
	}
	return d.wal.Checkpointed(foldedLSN)
}

// checkpointLocked folds the WAL under d.mu for the duration, including the I/O.
// It is used by callers that need full mutual exclusion (loadFast, Vacuum,
// SetApplicationID, SetUserVersion, RotateEncryptionKey). The background and public
// checkpoint paths use CheckpointMode, which releases d.mu for the I/O phase.
func (d *DB) checkpointLocked() error {
	// Trace the checkpoint (fold the WAL into the main file, fsync, reset the log) so a
	// span backend can see checkpoint stalls in a request's timeline (spec 19 §3). It is
	// a no-op when no tracer is set.
	_, span := d.startSpan(context.Background(), "kv.checkpoint")
	defer endSpan(span)
	foldedLSN, lastCommitVersion, resetWAL := d.prepareCheckpointLocked()
	if err := d.pgr.Checkpoint(foldedLSN, lastCommitVersion); err != nil {
		return err
	}
	if err := d.finalizeCheckpointLocked(foldedLSN, resetWAL); err != nil {
		return err
	}
	d.runAutoVacuumLocked()
	return nil
}

// archiveGeneration hands the current WAL generation's committed image to the archive
// sink before a checkpoint resets the log, wrapped in the same container ShipWAL produces
// so the PITR replay path reads it with ApplyWAL/ApplyWALUntil. It skips a generation with
// no commits since the last checkpoint, whose durable image is a header-only log with
// nothing to replay. The caller holds d.mu.
func (d *DB) archiveGeneration() error {
	if d.archive == nil {
		return nil
	}
	if d.wal.DurableSize() <= int64(wal.HeaderSize) {
		return nil // empty generation, nothing committed to archive
	}
	img, err := d.wal.DurableImage()
	if err != nil {
		return err
	}
	hdr := shipHeader{pageSize: uint32(d.pgr.PageSize()), highVersion: d.orc.lastCommitted(), walLen: uint64(len(img))}
	return d.archive(append(hdr.encode(), img...))
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
	d.rl.Lock()
	defer d.rl.Unlock()
	if err := d.checkpointLocked(); err != nil {
		return 0, err
	}
	return d.pgr.TruncateTail(budget)
}

// ApplicationID reports the application-defined file tag stored in the header (spec 22 §2).
// It is a free-form identifier an application stamps so a tool can recognize its own files.
func (d *DB) ApplicationID() uint32 {
	d.rl.Lock()
	defer d.rl.Unlock()
	return d.pgr.Header().ApplicationID
}

func (d *DB) FullPageWrites() bool {
	d.rl.Lock()
	defer d.rl.Unlock()
	return d.fullPageWrites
}

func (d *DB) AutoVacuumMode() uint8 {
	d.rl.Lock()
	defer d.rl.Unlock()
	return d.autoVacuumMode
}

func (d *DB) CommitLingerUs() uint32 { return d.lingerUs.Load() }

// SetApplicationID records an application-defined file tag in the header and persists it
// durably (spec 22 §2). It is a persistent-runtime setting: the value survives reopen. The
// change is folded into the main file by a checkpoint, which writes a coherent image (header
// plus all committed data) and fsyncs, so the tag is durable even across a crash and the
// header never desyncs from the WAL.
func (d *DB) SetApplicationID(id uint32) error {
	d.rl.Lock()
	defer d.rl.Unlock()
	d.pgr.Header().ApplicationID = id
	return d.checkpointLocked()
}

// UserVersion reports the application-defined schema/version counter stored in the header
// (spec 22 §2), the kv analog of SQLite's user_version. kv never interprets it.
func (d *DB) UserVersion() uint32 {
	d.rl.Lock()
	defer d.rl.Unlock()
	return d.pgr.Header().UserVersion
}

// SetUserVersion records the application-defined version counter in the header and persists
// it durably (spec 22 §2). Like SetApplicationID it is a persistent-runtime setting folded
// into the main file by a checkpoint.
func (d *DB) SetUserVersion(v uint32) error {
	d.rl.Lock()
	defer d.rl.Unlock()
	d.pgr.Header().UserVersion = v
	return d.checkpointLocked()
}

// syncPageImageLogger installs or clears the page-image callback on the pager
// based on the current d.fullPageWrites value. Called at open time and whenever
// SetFullPageWrites changes the setting. The caller must ensure d.wal is valid.
func (d *DB) syncPageImageLogger() {
	if d.fullPageWrites {
		// AppendLock/AppendUnlock bracket the page-image run so it cannot interleave
		// with a foreground commit appending to the same WAL tail off d.mu (the
		// CheckpointMode lock-free path released d.mu before logging images).
		d.pgr.SetPageImageLogger(d.wal.LogPageImage, d.wal.Flush, d.wal.AppendLock, d.wal.AppendUnlock)
	} else {
		d.pgr.SetPageImageLogger(nil, nil, nil, nil)
	}
}

// runAutoVacuumLocked reclaims trailing free pages after a checkpoint when
// auto_vacuum is enabled (spec 09 §3.3). It is a best-effort call: errors are
// logged but do not fail the checkpoint that triggered it. The caller holds d.mu.
func (d *DB) runAutoVacuumLocked() {
	if d.autoVacuumMode == 0 {
		return
	}
	if _, err := d.pgr.TruncateTail(0); err != nil && d.logger != nil {
		d.logger.Info("auto_vacuum truncate failed", "err", err)
	}
}

// SetFullPageWrites enables or disables pre-image logging during checkpoints
// (spec 07 §5). When on (the default), the checkpoint logs each page's on-disk
// pre-image to the WAL before overwriting it; recovery uses these images to
// restore any page left corrupt by an interrupted checkpoint. Disabling trades
// that safety for lower checkpoint write amplification.
//
// The change takes effect immediately on the next checkpoint; it is persisted in
// the file header so it survives re-open.
func (d *DB) SetFullPageWrites(on bool) error {
	d.rl.Lock()
	defer d.rl.Unlock()
	d.fullPageWrites = on
	if on {
		d.pgr.Header().FullPageWritesOff = 0
	} else {
		d.pgr.Header().FullPageWritesOff = 1
	}
	d.syncPageImageLogger()
	return d.checkpointLocked()
}

// SetAutoVacuumMode sets the automatic space-reclamation policy (spec 09 §3.3).
// 0 = NONE (off), 1 = INCREMENTAL, 2 = FULL. Both non-zero modes call
// TruncateTail(0) after every checkpoint. The mode is persisted in the file
// header so it survives re-open.
func (d *DB) SetAutoVacuumMode(mode uint8) error {
	d.rl.Lock()
	defer d.rl.Unlock()
	if mode > 2 {
		return fmt.Errorf("kv: invalid auto_vacuum mode %d", mode)
	}
	d.autoVacuumMode = mode
	d.pgr.Header().AutoVacuumMode = mode
	return d.checkpointLocked()
}

// SetCommitLingerUs sets the maximum microsecond delay a group-commit leader
// waits for additional writers before flushing the batch (spec 07 §4). A value
// of 0 disables the linger window (the current default). The change is
// immediately visible to the commit path and is persisted in the file header.
func (d *DB) SetCommitLingerUs(us uint32) error {
	d.lingerUs.Store(us)
	d.rl.Lock()
	defer d.rl.Unlock()
	d.pgr.Header().CommitLingerUs = us
	return d.checkpointLocked()
}

// RotateEncryptionKey performs a lazy DEK rotation (spec 14 §5): it bumps the key epoch,
// derives a new data-encryption key from the same master key, and from then on seals new and
// rewritten pages and WAL frames under the new epoch, while pages already on disk keep the
// epoch their envelopes record and stay readable. It is the cheap, incremental rotation the
// envelope-encryption model gives: the master key is unchanged, so it does not re-derive from
// a passphrase, and it does not re-encrypt the whole file. A full re-encryption that leaves no
// page under a superseded epoch is available separately by compacting the database, which
// rebuilds it from scratch and reseals every page.
//
// It first folds the WAL into the main file with a checkpoint so the log starts empty in the
// new epoch and the rotation has a clean boundary, then persists the new descriptor durably on
// page 1 before swapping the live schemes, so a crash at any point leaves a file that opens.
// The database must have been created with an encryption key; otherwise it returns
// ErrNotEncrypted.
func (d *DB) RotateEncryptionKey() error {
	d.rl.Lock()
	defer d.rl.Unlock()
	if d.crypto == nil {
		return ErrNotEncrypted
	}
	// Fold the WAL so every committed page lands in the main file under the current epoch and
	// the log is empty before the new epoch begins.
	if err := d.checkpointLocked(); err != nil {
		return err
	}
	next := d.crypto.Rotate(d.crypto.Epoch() + 1)
	desc, err := crypto.NewDescriptor(next, crypto.KDFRaw, nil, 0, 0, 0)
	if err != nil {
		return err
	}
	if err := d.pgr.Rekey(next, desc.Encode()); err != nil {
		return err
	}
	d.wal.SetScheme(next)
	d.crypto = next
	return nil
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
		d.logClosed()
	})

	d.rl.Lock()
	defer d.rl.Unlock()
	var firstErr error
	if d.wal != nil {
		if err := d.wal.Close(); err != nil {
			firstErr = err
		}
	}
	// Close the engine before the pager: an engine may run background workers (the LSM
	// core's flusher) that touch pager pages, so the worker must be joined while the pager
	// is still live.
	if err := d.eng.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := d.pgr.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr == nil {
		firstErr = bgErr
	}
	return firstErr
}
