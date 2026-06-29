// Package kv is the public, embeddable key/value database API: open a file, get a
// handle, run transactions (spec 15). It is the contract the CLI (spec 16) and server
// (spec 17) build on. The storage core is the f2 engine (notes/Spec/2070), a sharded
// hash index over a self-durable hybrid log; a program written against kv never names
// it and works the same whichever release it links (spec 15 §10).
//
// The shape is familiar to anyone who has used bbolt or Badger, with SQLite's
// operational feel. A *DB holds one file and is safe for concurrent use by many
// goroutines; it is the long-lived shared handle, and there is no connection pool to
// manage. Reads and writes happen inside transactions (View/Update or Begin/Commit),
// which carry the snapshot-isolation semantics of spec 10.
//
// This package is a thin facade over the integration layer in the internal db package:
// it presents the public kv.* types, the functional-option surface, and the typed
// error set, and hides the engine and db packages so the surface stays small and
// stable (spec 15 §10).
package kv

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/tamnd/kv/db"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// EngineKind names the storage core recorded in a file's header. kv runs one core, f2,
// so this exists mainly so Stats can report which core a file uses (spec 02, spec 04 §5).
type EngineKind = format.EngineKind

// Version is the current release of the kv library. It follows semantic versioning.
// The 0.x series is the pre-1.0 line: the API is broadly stable and the on-disk
// format is fixed, but the surface may still change before the 1.0 commitment.
const Version = "0.2.0"

// Sync is the WAL durability level (spec 07 §6). SyncFull, the default, makes every
// acked commit survive a crash.
type Sync = wal.Sync

const (
	// SyncOff never fsyncs the WAL; fastest, loses recent commits on power loss.
	SyncOff = wal.SyncOff
	// SyncNormal fdatasyncs at checkpoint and periodically, not every commit.
	SyncNormal = wal.SyncNormal
	// SyncBarrier issues a write-ordering barrier on every commit (F_BARRIERFSYNC on
	// macOS, fdatasync on Linux); crash-durable but not power-loss-durable.
	SyncBarrier = wal.SyncBarrier
	// SyncFull fdatasyncs on every commit (group-batched); the safe default.
	SyncFull = wal.SyncFull
	// SyncExtra is SyncFull plus a directory sync on file growth.
	SyncExtra = wal.SyncExtra
)

// Isolation selects a transaction's isolation level (spec 10 §3, §4). SnapshotIsolation
// is the default; Serializable adds commit-time read-set validation.
type Isolation = db.Isolation

const (
	// SnapshotIsolation is the high-performance default; its one anomaly is write skew.
	SnapshotIsolation = db.SnapshotIsolation
	// Serializable closes write skew and every other SI anomaly via read-set
	// validation, at a higher abort rate under contention.
	Serializable = db.Serializable
)

// Tracer is the optional span hook kv calls around each operation and its major phases
// (spec 19 §3). It is the seam a host wires to OpenTelemetry, or any tracer, without kv
// taking a dependency on it: an implementation returns a Span whose End closes the host's
// own span. It is off by default and enabled with WithTracer. See Tracer in the db package
// for the phase names kv emits.
type Tracer = db.Tracer

// Span is one started trace span; End closes it, called exactly once.
type Span = db.Span

// Option is a functional configuration knob passed to Open. Options split into
// create-time (persisted in the header, immutable for the file) and open-time
// (runtime, changeable across opens), per spec 15 §8.
type Option func(*config)

// config accumulates the functional options before they are resolved into the db
// layer's Options at Open.
type config struct {
	opts       db.Options
	cacheBytes int
	mergeName  string
}

// WithPageSize sets the page size for a fresh file (create-time); ignored when opening
// an existing file, whose header page size wins.
func WithPageSize(bytes int) Option {
	return func(c *config) { c.opts.PageSize = bytes }
}

// WithCacheSize sets the buffer-pool capacity in bytes (open-time). It is converted to
// a frame count against the effective page size at Open.
func WithCacheSize(bytes int) Option {
	return func(c *config) { c.cacheBytes = bytes }
}

// WithSynchronous sets the WAL durability level (open-time, spec 07 §6).
func WithSynchronous(s Sync) Option {
	return func(c *config) { c.opts.Sync = s }
}

// WithMaxRetries bounds how many times Update re-runs its closure on a conflict
// (open-time). Zero selects a small default.
func WithMaxRetries(n int) Option {
	return func(c *config) { c.opts.MaxRetries = n }
}

// WithIsolation sets the isolation level every transaction runs at (open-time,
// spec 10 §3-4). The default is SnapshotIsolation.
func WithIsolation(level Isolation) Option {
	return func(c *config) { c.opts.Isolation = level }
}

// WithAutoCheckpoint sets the WAL backlog, in frames, at which a background passive
// checkpoint folds the log into the main file so it stays bounded under sustained writes
// (open-time, spec 09 §1.3). Zero keeps the default; a negative value disables
// auto-checkpointing, leaving the WAL to grow until an explicit Checkpoint or clean close.
func WithAutoCheckpoint(frames int) Option {
	return func(c *config) { c.opts.AutoCheckpoint = frames }
}

// WithLogger routes the database's structured operational log into logger (open-time,
// spec 19 §3): lifecycle, crash recovery, checkpoint, maintenance, the fatal durability
// fault, and, when WithSlowOpThreshold is also set, slow operations. The default is no
// logger, which disables logging entirely and costs nothing. Pass a *slog.Logger to fold
// these events into the application's own logging.
func WithLogger(logger *slog.Logger) Option {
	return func(c *config) { c.opts.Logger = logger }
}

// WithSlowOpThreshold arms the slow-op log (open-time, spec 19 §3): a commit or point
// read that runs at least d is logged at WARN with its key range and, for a commit, a
// durable/apply phase split. Zero, the default, disables it. It has effect only when
// WithLogger is also set, and the read path reads no clock when it is off.
func WithSlowOpThreshold(d time.Duration) Option {
	return func(c *config) { c.opts.SlowOpThreshold = d }
}

// WithTracer arms the tracing surface (open-time, spec 19 §3): kv starts a span around
// each commit (split into its durable and apply phases), checkpoint, compaction round, and
// point read, so a slow request can be attributed to I/O versus engine versus compaction.
// Tracing is off by default; the host passes a Tracer that adapts these calls to its own
// span backend, so kv takes no tracing dependency. The disabled path is a single nil check
// per site, so a build that does not set it pays nothing.
func WithTracer(t Tracer) Option {
	return func(c *config) { c.opts.Tracer = t }
}

// WithEncryptionKey encrypts the database at rest under a 32-byte master key (spec 14).
// Every data page and every write-ahead-log batch payload is sealed with AES-256-GCM and
// authenticated before it reaches the disk, so a wrong key or a tampered file fails cleanly
// with ErrWrongKey rather than serving garbage. Encryption is fixed at create time: a file
// created with a key is encrypted for its lifetime and must be opened with the same key,
// opening an encrypted file without the key is refused, and offering a key to a file that
// was created unencrypted is refused too. The key must be exactly 32 bytes; a passphrase
// KDF arrives in a later slice, so for now derive the 32 bytes yourself, from a KMS or your
// own stretching, and pass them here.
func WithEncryptionKey(key []byte) Option {
	return func(c *config) { c.opts.EncryptionKey = key }
}

// WithMergeOperator registers the associative merge operator Txn.Merge folds operands
// through (spec 15 §5). The name identifies the operator's semantics; operator-name
// persistence in the header is a later slice, so today the function must be re-supplied
// at every Open, as with Badger.
func WithMergeOperator(name string, fn func(existing, operand []byte) []byte) Option {
	return func(c *config) {
		c.mergeName = name
		c.opts.Merge = fn
	}
}

// WithReadReplica opens the database as a read-only follower (spec 18 §4). Update, the
// WriteBatch path, and any other write are refused with ErrReadOnly; the only way the
// follower advances is ApplyWAL, replaying frames shipped from a primary. Promote a
// follower to a primary by reopening its file without this option. Reads always work and
// serve the follower's last applied version.
func WithReadReplica() Option {
	return func(c *config) { c.opts.ReadReplica = true }
}

// WithWALArchive registers a sink that receives each WAL generation as a replication delta
// just before a checkpoint folds and resets it, so the committed history survives for
// point-in-time recovery (spec 18 §6). Each delta is the same container ShipWAL produces;
// persist them in order (to files, an object store, anywhere) and a restored base backup
// rolled forward through them with ApplyWALUntil reconstructs any committed version in
// between. The sink runs under the database write lock at checkpoint time, so it should be
// quick and must not call back into the database; an error it returns fails the checkpoint
// rather than lose a frame. A generation with no new commits is not handed to the sink.
func WithWALArchive(sink func(delta []byte) error) Option {
	return func(c *config) { c.opts.WALArchive = sink }
}

// DB is an open database over one file, safe for concurrent use by many goroutines
// (spec 15 §1). It is obtained from Open and must be Closed.
type DB struct {
	d *db.DB
}

// Open opens the database at path, creating it with defaults if it does not exist, and
// runs crash recovery before returning a usable handle (spec 08). Create-time options
// take effect only on a fresh file; on an existing file the header's values win.
func Open(path string, opts ...Option) (*DB, error) {
	c := &config{}
	for _, o := range opts {
		o(c)
	}
	c.resolveCache()
	d, err := db.Open(vfs.NewOS(), path, c.opts)
	if err != nil {
		return nil, wrap(err)
	}
	return &DB{d: d}, nil
}

// RestoreBackup reconstructs a database at path from a stream produced by DB.Backup, writing
// the main file and its -wal sidecar so a subsequent Open reads the restored database (spec
// 18 §2). It refuses to overwrite an existing file at either path: restore creates, it never
// clobbers. The restored database is byte-faithful to the source at the backup version, same
// engine and format; an encrypted backup restores to an encrypted database that needs the
// original key supplied at Open.
func RestoreBackup(path string, r io.Reader) error {
	return wrap(db.RestoreBackup(vfs.NewOS(), path, r))
}

// resolveCache converts a byte cache budget into the frame count the pager wants,
// using the configured page size or the 4 KiB default when none was set.
func (c *config) resolveCache() {
	if c.cacheBytes <= 0 {
		return
	}
	ps := c.opts.PageSize
	if ps <= 0 {
		ps = 4096
	}
	c.opts.CacheFrames = c.cacheBytes / ps
}

// View runs fn in a read-only transaction at a fresh snapshot. The snapshot never
// blocks and never conflicts and is released when View returns (spec 15 §2.1).
func (kdb *DB) View(fn func(txn *Txn) error) error {
	return kdb.d.View(func(t *db.Txn) error { return fn(&Txn{t: t}) })
}

// Get returns an owned copy of the newest committed value of key, or ErrNotFound if
// the key is absent or tombstoned. It reads at the latest committed snapshot through
// the engine's point-read fast path, with no transaction to begin and discard and no
// snapshot watermark to register, so it is the lightest way to read a single key and
// the one to reach for when a read does not need to see a consistent snapshot across
// several keys. For that, open a View or a Snapshot and read through it instead; a
// sequence of Get calls can each observe a different commit.
func (kdb *DB) Get(key []byte) ([]byte, error) {
	v, err := kdb.d.Get(key)
	return v, wrap(err)
}

// Update runs fn in a writable transaction, committing on a nil return and discarding
// on an error. It retries fn against a fresh snapshot on a write-write or SSI conflict,
// up to the configured bound, so fn must be re-runnable (spec 15 §2.1).
func (kdb *DB) Update(fn func(txn *Txn) error) error {
	// wrap maps a commit-time sentinel onto the public surface, in particular the
	// ErrReadOnlyTxn a read replica raises when a write reaches commit (spec 18 §4). A user
	// error returned from fn passes through wrap unchanged.
	return wrap(kdb.d.Update(func(t *db.Txn) error { return fn(&Txn{t: t}) }))
}

// UpdateVersion is Update that also returns the commit version the transaction was assigned
// (spec 17 §3.1). It is zero for a transaction that committed no writes; a caller wanting a
// monotonic marker in that case reads Stats().Version. The server uses it to return a
// write's version so a client can resume a watch from just after its own write.
func (kdb *DB) UpdateVersion(fn func(txn *Txn) error) (uint64, error) {
	v, err := kdb.d.UpdateVersion(func(t *db.Txn) error { return fn(&Txn{t: t}) })
	return v, wrap(err)
}

// Begin starts an explicit transaction at a fresh snapshot (spec 15 §2.2). The caller
// must Discard it (deferred) to release the snapshot, and Commit a writable one to
// apply its writes.
func (kdb *DB) Begin(writable bool) *Txn {
	return &Txn{t: kdb.d.Begin(writable)}
}

// Snapshot is a long-lived read snapshot: a single pinned committed version reusable
// across many reads, for consistent multi-step reads or an online backup (spec 15 §7).
// It holds the garbage-collection watermark back for its whole life, so a caller must
// Close it. Open one with DB.Snapshot.
type Snapshot struct {
	s *db.Snapshot
}

// Snapshot pins the latest committed version and returns a snapshot reading at it. Every
// read through the snapshot sees exactly that state regardless of later writes. The
// returned snapshot must be Closed to release the version it pins.
func (kdb *DB) Snapshot() *Snapshot {
	return &Snapshot{s: kdb.d.Snapshot()}
}

// Version reports the committed version the snapshot reads at.
func (s *Snapshot) Version() uint64 { return s.s.Version() }

// View runs fn in a read-only transaction pinned at the snapshot's version. Reusing one
// snapshot across many View calls is what makes a multi-step read consistent: each call
// observes the identical committed state. Using a closed snapshot returns an error.
func (s *Snapshot) View(fn func(txn *Txn) error) error {
	return wrap(s.s.View(func(t *db.Txn) error { return fn(&Txn{t: t}) }))
}

// Close releases the version the snapshot pinned so it can again be garbage collected. It
// is idempotent; further View calls then return an error.
func (s *Snapshot) Close() error { return wrap(s.s.Close()) }

// ChangeKind classifies a mutation on the change feed (spec 15 §7). It distinguishes a
// point upsert, a point delete, a merge operand, and a range delete, so a feed consumer
// sees the faithful committed operation rather than a lossy point-only projection.
type ChangeKind = db.ChangeKind

// The change kinds delivered by Subscribe.
const (
	ChangeSet         = db.ChangeSet
	ChangeDelete      = db.ChangeDelete
	ChangeMerge       = db.ChangeMerge
	ChangeRangeDelete = db.ChangeRangeDelete
)

// Change is one committed mutation delivered to a Subscribe callback (spec 15 §7). For a
// range delete, Key is the inclusive lower bound and Value the exclusive upper bound; for
// a delete, Value is nil; for a merge, Value is the operand. The slices are copies the
// callback may retain, and Version is the commit version every Change in one batch shares.
type Change = db.Change

// Subscribe delivers a change feed of committed mutations whose key has prefix, invoking
// fn once per committed batch in commit order (spec 15 §7). It blocks until ctx is
// cancelled, fn returns an error, or the consumer falls too far behind (ErrSubscriberLagged),
// returning the cause. A nil prefix matches every key. fn runs on the calling goroutine, so
// a slow callback slows only its own feed and never the database's writers, and only
// durable, committed mutations are ever delivered. It is the foundation for the server's
// watch endpoints and replication (spec 17, spec 18).
func (kdb *DB) Subscribe(ctx context.Context, prefix []byte, fn func([]Change) error) error {
	return wrap(kdb.d.Subscribe(ctx, prefix, fn))
}

// WriteBatch is an explicit, memory-bounded builder for very large writes (spec 15 §6). It
// buffers Set and Delete operations and flushes them in bounded chunks, so loading millions
// of keys never holds them all in memory. It is the bulk-load path, not a transaction: it
// spans many commits, one per chunk, so it is not atomic across chunks. Within a chunk the
// last write for a key wins. Open one with DB.NewWriteBatch and Close it to flush the tail.
type WriteBatch struct {
	b *db.WriteBatch
}

// NewWriteBatch returns a batch that flushes every maxOps operations; a non-positive maxOps
// selects a default. The batch must be Closed to flush its final partial chunk.
func (kdb *DB) NewWriteBatch(maxOps int) *WriteBatch {
	return &WriteBatch{b: kdb.d.NewWriteBatch(maxOps)}
}

// Set buffers an upsert of key to value, auto-flushing the chunk when it fills.
func (w *WriteBatch) Set(key, value []byte) error { return wrap(w.b.Set(key, value)) }

// Delete buffers a tombstone for key, auto-flushing the chunk when it fills.
func (w *WriteBatch) Delete(key []byte) error { return wrap(w.b.Delete(key)) }

// Flush commits the buffered chunk now. The first failing flush fences the batch, so a
// partial load never silently continues past an I/O fault.
func (w *WriteBatch) Flush() error { return wrap(w.b.Flush()) }

// Count reports how many operations the caller has recorded over the batch's life.
func (w *WriteBatch) Count() int { return w.b.Count() }

// Pending reports how many operations are buffered but not yet flushed.
func (w *WriteBatch) Pending() int { return w.b.Pending() }

// Close flushes the final partial chunk and marks the batch done. It is idempotent; further
// operations return ErrBatchClosed.
func (w *WriteBatch) Close() error { return wrap(w.b.Close()) }

// Load bulk-populates the database from key/value pairs delivered in ascending key
// order, the fast path for initial population (spec 15 §6). next returns each pair and
// true, or false at end of stream. On a fresh database it builds the tree bottom-up and
// makes it durable with one checkpoint, far faster than inserting key by key; the keys
// must be strictly ascending. On a database that already holds data it falls back to
// chunked commits, which accept any order. It returns the commit version the loaded data
// is visible at.
func (kdb *DB) Load(next func() (key, value []byte, ok bool)) (uint64, error) {
	v, err := kdb.d.Load(next)
	return v, wrap(err)
}

// Stats is a point-in-time space-and-durability snapshot of an open database: page
// counts, freelist depth, the engine's physical footprint and amplification, the latest
// commit version, and the WAL frame backlog (spec 09 §4, spec 19). It is what the
// info/stats CLI prints and what an operator watches to decide whether to checkpoint or
// vacuum.
type Stats struct {
	// Engine is the storage core the file was created with.
	Engine EngineKind
	// PageSize is the file's page size in bytes.
	PageSize int
	// PageCount is the file's size in pages (high-water mark).
	PageCount uint32
	// FreePages is the freelist depth: pages reusable before the file grows.
	FreePages int64
	// PhysicalBytes is the on-disk footprint, dead versions included.
	PhysicalBytes int64
	// LiveKeys and LiveBytes are live-data counts at the newest snapshot, zero when the
	// engine does not compute them cheaply.
	LiveKeys  int64
	LiveBytes int64
	// Amplification is the space-amplification estimate (physical / live).
	Amplification float64
	// Version is the latest committed commit version.
	Version uint64
	// WALFrames is how many frames the WAL has written.
	WALFrames uint64
	// WALBacklog is the frames committed but not yet folded by a checkpoint, the
	// read-overhead and recovery-time signal.
	WALBacklog uint64
	// Syncs is how many fsyncs the WAL has performed since open.
	Syncs uint64
	// PageReads and CacheHits are the buffer pool's cumulative traffic since open: physical
	// page reads against the main file, and Gets served from a resident frame. Their ratio
	// is the cache hit rate, and PageReads over a workload's logical read count is its read
	// amplification (spec 19, spec 21 §1).
	PageReads uint64
	CacheHits uint64
	// Gets, Sets, Deletes, Merges, and Scans are the cumulative-since-open counts of each
	// logical operation the database has served (spec 19 §1.1). A get counts a point read
	// or existence check; a set counts a plain or TTL upsert; a delete counts a single-key
	// or range delete; a scan counts an iterator opened. They count operations issued, so a
	// fresh process starts at zero and a long-lived one accumulates.
	Gets    uint64
	Sets    uint64
	Deletes uint64
	Merges  uint64
	Scans   uint64
	// Commits is the number of durable commits acknowledged and CommitNanos their summed
	// latency; CommitNanos over Commits is the average durable-commit cost. Only a
	// successful, fsynced commit is counted, so the average is over acknowledged commits.
	Commits     uint64
	CommitNanos uint64
	// CompactionScore is the urgency of the most-pending compaction, normalized so 1.0
	// is at-trigger; 0 when nothing is due (spec 19 §1.5).
	CompactionScore float64
	// OldestSnapshotAgeNanos is the wall-clock age in nanoseconds of the longest-held live
	// read snapshot, 0 when none is live. A value that only climbs is a snapshot or
	// iterator that was never closed, the leaked-reader signal (spec 19 §1.6).
	OldestSnapshotAgeNanos uint64
	// ReadReplica reports whether the database was opened as a read-only follower with
	// WithReadReplica (spec 18 §4).
	ReadReplica bool
	// ReplicaLag is a follower's distance behind its primary in commit versions, 0 on a
	// primary or a caught-up replica. A climbing value means ApplyWAL is not keeping pace
	// with the primary's commits (spec 18 §4).
	ReplicaLag uint64
}

// Stats returns a current space-and-durability snapshot of the database (spec 09 §4).
// It is cheap and lock-light, safe to poll.
func (kdb *DB) Stats() Stats {
	s := kdb.d.Stats()
	return Stats{
		Engine:                 s.Engine,
		PageSize:               s.PageSize,
		PageCount:              s.PageCount,
		FreePages:              s.FreePages,
		PhysicalBytes:          s.PhysicalBytes,
		LiveKeys:               s.LiveKeys,
		LiveBytes:              s.LiveBytes,
		Amplification:          s.Amplification,
		Version:                s.Version,
		WALFrames:              s.WALFrames,
		WALBacklog:             s.WALBacklog,
		Syncs:                  s.Syncs,
		PageReads:              s.PageReads,
		CacheHits:              s.CacheHits,
		Gets:                   s.Ops.Gets,
		Sets:                   s.Ops.Sets,
		Deletes:                s.Ops.Deletes,
		Merges:                 s.Ops.Merges,
		Scans:                  s.Ops.Scans,
		Commits:                s.Ops.Commits,
		CommitNanos:            s.Ops.CommitNanos,
		CompactionScore:        s.CompactionScore,
		OldestSnapshotAgeNanos: s.OldestSnapshotAgeNanos,
		ReadReplica:            s.ReadReplica,
		ReplicaLag:             s.ReplicaLag,
	}
}

// CheckpointMode selects how aggressively a checkpoint reclaims the WAL (spec 09 §1.2),
// the kv analog of SQLite's wal_checkpoint modes.
type CheckpointMode = db.CheckpointMode

const (
	// CheckpointPassive folds every committed frame and resets the log without blocking.
	CheckpointPassive = db.CheckpointPassive
	// CheckpointFull folds every committed frame and resets the log.
	CheckpointFull = db.CheckpointFull
	// CheckpointRestart folds and resets so the next writer reuses the log from its start.
	CheckpointRestart = db.CheckpointRestart
	// CheckpointTruncate folds, resets, and truncates the -wal file to its header.
	CheckpointTruncate = db.CheckpointTruncate
)

// Checkpoint folds the WAL into the main file and resets the log (spec 09), in PASSIVE
// mode. Use CheckpointMode for a tighter mode such as TRUNCATE.
func (kdb *DB) Checkpoint() error {
	return wrap(kdb.d.Checkpoint())
}

// CheckpointMode folds the WAL and resets the log, then applies the reclamation the mode
// asks for (spec 09 §1.2). TRUNCATE additionally shrinks the -wal file to its header.
func (kdb *DB) CheckpointMode(m CheckpointMode) error {
	return wrap(kdb.d.CheckpointMode(m))
}

// Vacuum performs an incremental vacuum, returning trailing free pages to the operating
// system so the file shrinks after large deletes (spec 09 §3.1). It first folds the WAL
// with a checkpoint, then truncates the maximal run of free pages at the end of the file.
// budget caps the pages reclaimed this round so the caller can bound the work and the
// writer-lock hold; a non-positive budget reclaims the whole trailing run. It returns the
// number of pages freed. Free pages buried in the middle of the file stay on the freelist
// for reallocation rather than being returned to the OS; this is the kv analog of
// SQLite's incremental_vacuum.
func (kdb *DB) Vacuum(budget int) (int, error) {
	freed, err := kdb.d.Vacuum(budget)
	return freed, wrap(err)
}

// Backup streams a consistent physical image of the database to w and returns the commit
// version it captured (spec 18 §2). The image folds the WAL into the main file first, so it
// is self-contained, and restores with RestoreBackup into a database that opens directly and
// passes Check. Backup runs under the write lock, so it serializes with the single writer:
// commits wait while the image is copied, the simple always-correct physical snapshot. If
// the database is encrypted the image is ciphertext, so the backup is encrypted at rest and
// a restore needs the same key (spec 18 §7); back the key up separately, or the backup is
// unrecoverable.
func (kdb *DB) Backup(w io.Writer) (uint64, error) {
	v, err := kdb.d.Backup(w)
	return v, wrap(err)
}

// ShipWAL streams the current WAL generation to w as a replication delta and returns the
// commit version it captured (spec 18 §4). It is the primary half of WAL shipping: a
// follower's ApplyWAL replays the frames this writes, advancing it to the same version.
// Ship repeatedly to keep a follower current. Shipping does not checkpoint, so it captures
// the committed tail the follower still needs; an encrypted database ships ciphertext, so
// the stream is encrypted at rest and the follower needs the same key (spec 18 §7).
func (kdb *DB) ShipWAL(w io.Writer) (uint64, error) {
	v, err := kdb.d.ShipWAL(w)
	return v, wrap(err)
}

// ApplyWAL replays a shipped WAL generation from r onto a follower and returns the
// follower's new applied version (spec 18 §4). It is the follower half of WAL shipping:
// the frames apply through the same redo path recovery uses, and reads advance to the new
// version once it returns. It is idempotent over already-applied versions and refuses with
// ErrReplicaGap if the stream begins past the applied version, meaning the primary
// checkpointed away the frames in between and the follower must re-seed from a full Backup.
// Call it on a database opened WithReadReplica.
func (kdb *DB) ApplyWAL(r io.Reader) (uint64, error) {
	v, err := kdb.d.ApplyWAL(r)
	return v, wrap(err)
}

// ApplyWALUntil replays a shipped or archived delta from r but stops after the target
// version, leaving later commits in the delta unapplied (spec 18 §6). It is the
// point-in-time-recovery primitive: restore a base backup into a fresh file opened
// WithReadReplica, then feed the archived generations in order through ApplyWALUntil with
// the same target, and the database rolls forward to exactly the committed state at that
// version. A target at or above the delta's last version applies it whole, like ApplyWAL.
func (kdb *DB) ApplyWALUntil(r io.Reader, target uint64) (uint64, error) {
	v, err := kdb.d.ApplyWALUntil(r, target)
	return v, wrap(err)
}

// Synchronous returns the WAL sync level in effect (spec 22 §3).
func (kdb *DB) Synchronous() Sync { return Sync(kdb.d.SyncMode()) }

// SetSynchronous changes the WAL sync level, taking effect on the next commit (spec 22 §3).
// The change does not persist across reopen; set WithSynchronous at Open to make it sticky.
func (kdb *DB) SetSynchronous(s Sync) error {
	kdb.d.SetSyncMode(wal.Sync(s))
	return nil
}

// AutoCheckpointFrames returns the WAL backlog threshold at which the background
// checkpointer fires, 0 when auto-checkpointing is disabled (spec 22 §3).
func (kdb *DB) AutoCheckpointFrames() int { return kdb.d.AutoCheckpointFrames() }

// SetAutoCheckpointFrames changes the background-checkpoint trigger threshold in WAL
// frames (spec 22 §3). Zero or negative disables auto-checkpointing.
func (kdb *DB) SetAutoCheckpointFrames(n int) error {
	kdb.d.SetAutoCheckpointFrames(n)
	return nil
}

// CacheFrames returns the buffer pool capacity in frames (pages); multiply by
// Stats().PageSize for the byte capacity (spec 22 §5).
func (kdb *DB) CacheFrames() int { return kdb.d.CacheFrames() }

// ApplicationID returns the application-defined file tag stored in the header (spec 22 §2),
// the value an application stamps so a tool can recognize its own files. kv never interprets
// it.
func (kdb *DB) ApplicationID() uint32 { return kdb.d.ApplicationID() }

// SetApplicationID stamps the application-defined file tag and persists it durably (spec 22
// §2). It is a persistent-runtime setting: the value survives reopen and a crash.
func (kdb *DB) SetApplicationID(id uint32) error { return wrap(kdb.d.SetApplicationID(id)) }

// UserVersion returns the application-defined version counter stored in the header (spec 22
// §2), the kv analog of SQLite's user_version.
func (kdb *DB) UserVersion() uint32 { return kdb.d.UserVersion() }

// SetUserVersion records the application-defined version counter and persists it durably
// (spec 22 §2). Like the application id it is persistent-runtime and fsynced before return.
func (kdb *DB) SetUserVersion(v uint32) error { return wrap(kdb.d.SetUserVersion(v)) }

// FullPageWrites reports whether checkpoint pre-image logging is enabled (spec 07 §5).
func (kdb *DB) FullPageWrites() bool { return kdb.d.FullPageWrites() }

// AutoVacuumMode returns the current auto-vacuum policy: 0=NONE, 1=INCREMENTAL, 2=FULL.
func (kdb *DB) AutoVacuumMode() uint8 { return kdb.d.AutoVacuumMode() }

// CommitLingerUs returns the current group-commit linger window in microseconds.
func (kdb *DB) CommitLingerUs() uint32 { return kdb.d.CommitLingerUs() }

// SetFullPageWrites enables or disables pre-image logging during checkpoints (spec 07 §5).
// When on (the default), the checkpoint logs the on-disk pre-image of each dirty page to
// the WAL before overwriting it; recovery uses these images to restore any page that an
// interrupted checkpoint left corrupt. Disabling trades that safety for lower checkpoint
// write amplification. The setting is persisted in the file header.
func (kdb *DB) SetFullPageWrites(on bool) error { return wrap(kdb.d.SetFullPageWrites(on)) }

// SetAutoVacuumMode sets the automatic space-reclamation policy (spec 09 §3.3).
// 0 = NONE (off, the default for existing files), 1 = INCREMENTAL, 2 = FULL.
// Both non-zero modes call TruncateTail after every checkpoint. The setting is
// persisted in the file header.
func (kdb *DB) SetAutoVacuumMode(mode uint8) error {
	return wrap(kdb.d.SetAutoVacuumMode(mode))
}

// SetCommitLingerUs sets the group-commit linger window in microseconds (spec 07 §4).
// The commit leader waits up to this long for additional writers to join the batch
// before flushing. Zero (the default) disables the window. The setting is persisted
// in the file header and takes effect immediately.
func (kdb *DB) SetCommitLingerUs(us uint32) error { return wrap(kdb.d.SetCommitLingerUs(us)) }

// CheckProblem is one structural violation found by Check: a corruption class, the page
// it was found on (zero for a file-wide problem), and a human-readable description
// (spec 16 §4, spec 23 §3).
type CheckProblem struct {
	Class  string
	Page   uint32
	Detail string
}

// CheckReport is the outcome of a structural integrity check: what was inspected and
// every problem found. OK reports whether the file is sound.
type CheckReport struct {
	// PagesVisited is how many pages the walk reached from the engine root.
	PagesVisited int
	// Keys is how many live key cells the walk saw.
	Keys int64
	// FreePages is the freelist depth at the time of the check.
	FreePages int
	// PageCount is the file's high-water page count.
	PageCount uint32
	// Problems is every violation found; empty means the file is structurally sound.
	Problems []CheckProblem
}

// OK reports whether the check found no problems.
func (r *CheckReport) OK() bool { return len(r.Problems) == 0 }

// Check runs a structural integrity check over the open database and returns a report of
// everything it inspected and every problem it found (spec 16 §4, spec 23 §3). It walks
// the engine's on-disk structure under the writer lock, verifying page types, key
// ordering, subtree bounds, and that the reachable pages, the freelist, and the file size
// all reconcile. It is what `kv check` and a CI/cron soundness gate call; the report's OK
// method is false on any violation.
func (kdb *DB) Check() (*CheckReport, error) {
	r, err := kdb.d.Verify()
	if err != nil {
		return nil, wrap(err)
	}
	out := &CheckReport{
		PagesVisited: r.PagesVisited,
		Keys:         r.Keys,
		FreePages:    r.FreePages,
		PageCount:    r.PageCount,
	}
	for _, p := range r.Problems {
		out.Problems = append(out.Problems, CheckProblem{Class: p.Class, Page: p.Page, Detail: p.Detail})
	}
	return out, nil
}

// RotateEncryptionKey rotates the database's data-encryption key in place (spec 14 §5). It
// advances the key epoch and seals every new and rewritten page and WAL frame under a fresh
// key derived from the same master key, while the pages already on disk keep the epoch they
// were sealed under and stay readable: a lazy, incremental rotation that does not re-encrypt
// the whole file and does not change the master key supplied through WithEncryptionKey. It
// folds the WAL and persists the new key epoch durably before returning. Pages already on
// disk under an older epoch are resealed lazily as later writes rewrite them, so over time
// the old epoch fades out without a whole-file pass. It returns ErrNotEncrypted if the
// database was not created with an encryption key.
func (kdb *DB) RotateEncryptionKey() error {
	return wrap(kdb.d.RotateEncryptionKey())
}

// Close flushes, runs a final checkpoint, and releases the file (spec 15 §1).
func (kdb *DB) Close() error {
	return wrap(kdb.d.Close())
}
