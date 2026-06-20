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
	"errors"
	"fmt"
	"sync"

	"github.com/tamnd/kv/btree"
	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
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
	// Sync is the WAL durability level (spec 07 §6). Zero is SyncFull, the safe
	// default: every acked commit survives a crash.
	Sync wal.Sync
	// Merge folds an existing value and a merge operand into a new value during read
	// resolution (spec 15). If nil, a merge operand behaves as a plain set.
	Merge func(existing, operand []byte) []byte
	// MaxRetries bounds how many times Update re-runs its closure on a write-write
	// conflict (spec 15 §2.1). Zero selects a small default.
	MaxRetries int
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

	merge      func(existing, operand []byte) []byte
	maxRetries int
	syncMode   wal.Sync
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
		merge: opts.Merge, maxRetries: opts.maxRetries(), syncMode: opts.sync()}
	if err := d.openEngine(opts.Merge); err != nil {
		w.Close()
		pgr.Close()
		return nil, err
	}
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
		merge: opts.Merge, maxRetries: opts.maxRetries(), syncMode: opts.sync()}
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
	return d, nil
}

// openEngine wires the engine to its substrate and installs the merge resolver.
func (d *DB) openEngine(merge func(existing, operand []byte) []byte) error {
	env := &engine.Env{
		Pager:   d.pgr,
		Options: engine.EngineOptions{PageSize: d.pgr.PageSize()},
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
		return nil, errors.New("kv: LSM core is not implemented yet (roadmap M4)")
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

// commitTxn is the single-writer commit path for a transaction: it runs write-write
// conflict detection at the transaction's read snapshot, and on success logs,
// commits, and applies the buffered writes at the assigned version, then makes that
// version visible (spec 10 §3, §5.1). It returns ErrConflict if the transaction
// lost a write-write race.
func (d *DB) commitTxn(readVersion uint64, ops []pendingOp, conflictKeys []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	v, ok := d.orc.newCommitTs(readVersion, conflictKeys)
	if !ok {
		return ErrConflict
	}
	b := engine.NewWriteBatch(v)
	for _, op := range ops {
		switch op.kind {
		case opSet:
			b.Set(op.key, op.value)
		case opDelete:
			b.Delete(op.key)
		case opMerge:
			b.Merge(op.key, op.value)
		}
	}
	if err := d.applyCommitted(b, v); err != nil {
		return err
	}
	d.orc.applied(v)
	return nil
}

// applyCommitted enforces the write-ahead rule for an already-versioned batch: log
// and commit it durably, then apply it to the engine, then record the durable
// version in the header (persisted at the next checkpoint). The caller holds d.mu
// and calls oracle.applied after, in version order (spec 07 §1, spec 10 §2).
func (d *DB) applyCommitted(b *engine.WriteBatch, v uint64) error {
	encoded := b.Encode()
	if err := d.wal.LogBatch(v, encoded); err != nil {
		return err
	}
	if _, err := d.wal.Commit(v); err != nil {
		return err
	}
	if err := d.eng.Apply(b, v); err != nil {
		return err
	}
	d.pgr.Header().LastCommitVersion = v
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
	rd, err := d.eng.NewReader(engine.Snapshot{Version: version})
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
	return d.eng.NewReader(engine.Snapshot{Version: version})
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

// Checkpoint folds the WAL into the main file and resets the log, in the strict
// order that makes an interrupted checkpoint safe: fold dirty pages and fsync the
// main file (recording the folded LSN and the durable version in its header), then
// append the checkpoint frame, rotate the salt, and reset the WAL (spec 08 §5). A
// crash between the two steps re-folds harmlessly on the next open because redo is
// idempotent.
func (d *DB) Checkpoint() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	foldedLSN := d.wal.LSN() - 1
	if err := d.pgr.Checkpoint(foldedLSN); err != nil {
		return err
	}
	return d.wal.Checkpointed(foldedLSN)
}

// Close releases the database without an implicit checkpoint: committed data is
// already durable in the WAL and recovers on the next open. For a clean shutdown
// that leaves an empty WAL, call Checkpoint first.
func (d *DB) Close() error {
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
	return firstErr
}
