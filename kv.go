// Package kv is the public, embeddable key/value database API: open a file, get a
// handle, run transactions (spec 15). It is the contract the CLI (spec 16) and server
// (spec 17) build on, and it is engine-agnostic: nothing here names the B-tree or LSM
// core except the create-time WithEngine selector, so a program written against kv
// works unchanged whichever engine its file uses (spec 04 §6, spec 15 §10).
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

	"github.com/tamnd/kv/db"
	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// EngineKind selects the storage core at create time. It is persisted in the file
// header, so it is fixed for the life of a file (spec 02, spec 04 §5).
type EngineKind = format.EngineKind

const (
	// BTree is the default B-tree core: read-optimized, in-place, a single ordered
	// structure (spec 05).
	BTree = format.EngineBTree
	// LSM is the write-optimized log-structured core (spec 06). It is a later
	// milestone; selecting it on a fresh file errors until it lands.
	LSM = format.EngineLSM
)

// Sync is the WAL durability level (spec 07 §6). SyncFull, the default, makes every
// acked commit survive a crash.
type Sync = wal.Sync

const (
	// SyncOff never fsyncs the WAL; fastest, loses recent commits on power loss.
	SyncOff = wal.SyncOff
	// SyncNormal fdatasyncs at checkpoint and periodically, not every commit.
	SyncNormal = wal.SyncNormal
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

// IterOptions controls a range scan: bounds, prefix, reverse, key-only (spec 11 §1).
// It is the same shape the iterator layer consumes, exposed here so callers construct
// it as kv.IterOptions.
type IterOptions = engine.IterOptions

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

// WithEngine selects the storage core for a fresh file (create-time). On an existing
// file the header's engine wins and a conflicting selection is an error (spec 04 §5).
func WithEngine(e EngineKind) Option {
	return func(c *config) { c.opts.Engine = e }
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

// Compact runs a full vacuum on the database at path (spec 09 §3.2): it rebuilds the file
// from scratch into a fresh, maximally compact copy holding only the live data and swaps it
// in atomically, reclaiming all the space that obsolete versions, tombstones, and freelist
// holes were holding. It is an offline operation: path must not be open elsewhere while it
// runs, and it needs room on disk for a second copy of the live data. Open the database
// again afterward to use it.
func Compact(path string, opts ...Option) error {
	c := &config{}
	for _, o := range opts {
		o(c)
	}
	c.resolveCache()
	return wrap(db.Compact(vfs.NewOS(), path, c.opts))
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

// Update runs fn in a writable transaction, committing on a nil return and discarding
// on an error. It retries fn against a fresh snapshot on a write-write or SSI conflict,
// up to the configured bound, so fn must be re-runnable (spec 15 §2.1).
func (kdb *DB) Update(fn func(txn *Txn) error) error {
	return kdb.d.Update(func(t *db.Txn) error { return fn(&Txn{t: t}) })
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
}

// Stats returns a current space-and-durability snapshot of the database (spec 09 §4).
// It is cheap and lock-light, safe to poll.
func (kdb *DB) Stats() Stats {
	s := kdb.d.Stats()
	return Stats{
		Engine:        s.Engine,
		PageSize:      s.PageSize,
		PageCount:     s.PageCount,
		FreePages:     s.FreePages,
		PhysicalBytes: s.PhysicalBytes,
		LiveKeys:      s.LiveKeys,
		LiveBytes:     s.LiveBytes,
		Amplification: s.Amplification,
		Version:       s.Version,
		WALFrames:     s.WALFrames,
		WALBacklog:    s.WALBacklog,
		Syncs:         s.Syncs,
		PageReads:     s.PageReads,
		CacheHits:     s.CacheHits,
		Gets:          s.Ops.Gets,
		Sets:          s.Ops.Sets,
		Deletes:       s.Ops.Deletes,
		Merges:        s.Ops.Merges,
		Scans:         s.Ops.Scans,
		Commits:       s.Ops.Commits,
		CommitNanos:   s.Ops.CommitNanos,
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

// Close flushes, runs a final checkpoint, and releases the file (spec 15 §1).
func (kdb *DB) Close() error {
	return wrap(kdb.d.Close())
}
