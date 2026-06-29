// Package f2engine adapts an f2.Store to the engine.Engine SPI so the kv database can
// run on the f2 core: a sharded hash index over a self-durable hybrid log (spec 2070).
//
// f2 stores one record per user key. The record's value is that key's MVCC version
// group: the set of committed (version, kind, value) cells, newest-first (see group.go).
// A point read decodes the group and folds it at the read snapshot with format.Fold, the
// one resolver the other cores and the conformance oracle share, so f2 resolves get, set,
// delete, TTL, and merge at any snapshot exactly as a B-tree or LSM core would.
//
// What f2 cannot do is anything that needs key order. A hash index has no ordering, so a
// range scan (NewIter) and a range delete return ErrUnsupported rather than a wrong or
// O(n)-per-call answer. The host treats those as unsupported on this engine.
//
// Durability is f2's own. The store recovers itself from its log and index snapshot on
// open, and the host drives a checkpoint by calling Checkpoint, so the engine owns its
// persistence rather than going through the host pager.
package f2engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tamnd/kv/crypto"
	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/f2"
	"github.com/tamnd/kv/format"
)

// ErrUnsupported is returned for the range operations an unordered hash index cannot
// serve: a range scan (NewIter) and a range delete. f2 has no key order, so these have no
// meaning on it. Point reads and writes are fully supported.
var ErrUnsupported = fmt.Errorf("kv: operation not supported by the f2 engine (no key order)")

// Config configures the f2 engine. An empty Path is the memory-only mode the tests use;
// a Path is the single-file, self-durable mode the database opens. Zero fields take f2's
// defaults.
type Config struct {
	Path                  string
	PageSize              int
	Shards                int
	ResidentPagesPerShard int
	Durability            f2.Durability
	CheckpointBytes       int64
	// Crypto, when set, seals the records region of every data, snapshot, and superblock
	// page f2 writes with the database key, and opens it on read. A nil Crypto leaves the
	// file in plaintext and keeps f2's record-granular fast paths. The host passes the same
	// scheme it derived for the main pager so f2 needs no second key descriptor.
	Crypto *crypto.Scheme
}

// Engine adapts an f2.Store to engine.Engine.
type Engine struct {
	s   *f2.Store
	env *engine.Env
	// mu serializes the read-modify-write of a key's version group. The host already
	// serializes Apply through its commit pipeline, so this only guards Apply against a
	// concurrent maintenance pass that rewrites the same group.
	mu    sync.Mutex
	merge func(existing, operand []byte) []byte

	// pendingLSN is the WAL LSN the host announced for the next batch through NoteLSN.
	// The host calls NoteLSN then Apply on the same goroutine, so Apply reads it without
	// a lock and promotes it to appliedLSN once the batch's writes have landed.
	pendingLSN uint64
	// appliedLSN is the highest WAL LSN whose batch has fully landed in the store. Apply
	// stores it under mu after the writes succeed, so a reader that takes mu sees only
	// batches whose writes are complete. DurableLSN and Flush read it.
	appliedLSN atomic.Uint64
	// durableLSN is the highest WAL LSN whose effects a Checkpoint has fsynced into the
	// f2 file. The host folds the WAL no further than this and replays the tail past it on
	// the next open, so f2 owns its persistence while the host keeps the unflushed tail.
	durableLSN atomic.Uint64
}

// New opens an f2 engine with the given configuration.
func New(cfg Config) (*Engine, error) {
	shards := cfg.Shards
	if shards == 0 {
		shards = 64
	}
	pageSize := cfg.PageSize
	if pageSize == 0 {
		pageSize = 1 << 20
	}
	s, err := f2.New(f2.Tunables{
		Shards:                shards,
		PageSize:              pageSize,
		ResidentPagesPerShard: cfg.ResidentPagesPerShard,
		Path:                  cfg.Path,
		Durability:            cfg.Durability,
		CheckpointBytes:       cfg.CheckpointBytes,
		Crypto:                cfg.Crypto,
	})
	if err != nil {
		return nil, err
	}
	return &Engine{s: s}, nil
}

// Kind implements engine.Engine.
func (e *Engine) Kind() engine.Kind { return engine.F2 }

// SetMergeFunc installs the merge resolver the fold uses, matching the other cores so a
// merge folds identically. The conformance suite calls it before driving the engine.
func (e *Engine) SetMergeFunc(f func(existing, operand []byte) []byte) { e.merge = f }

// Open implements engine.Engine. The store is already open from New; Open only records the
// host environment. f2 does not use the host pager or WAL: it persists itself.
func (e *Engine) Open(env *engine.Env) error {
	e.env = env
	return nil
}

// Close implements engine.Engine.
func (e *Engine) Close() error { return e.s.Close() }

// Apply implements engine.Engine. It installs each entry into the f2 store by reading the
// key's current version group, adding the entry's cell newest-first, and writing the group
// back. The batch is already durable in the host WAL, so a crash mid-Apply re-derives the
// same calls on the post-checkpoint tail.
func (e *Engine) Apply(batch *engine.WriteBatch, commitVersion uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var scratch []byte
	for _, ent := range batch.Entries() {
		ik := ent.InternalKey
		kind := format.KindOf(ik)
		if kind == format.KindRangeBegin {
			// A range delete needs the key order f2 does not have, so it cannot be applied.
			return fmt.Errorf("%w: range delete", ErrUnsupported)
		}
		uk := format.UserKey(ik)
		cells, err := e.loadCells(uk)
		if err != nil {
			return err
		}
		cells = upsertCell(cells, cell{version: format.Version(ik), kind: kind, value: ent.Value})
		scratch = encodeGroup(scratch[:0], cells)
		if err := e.s.Set(uk, scratch); err != nil {
			return err
		}
	}
	// All writes for this batch have landed, so the LSN the host announced for it is now
	// applied. Storing it under mu means Flush, which also takes mu, never reads an LSN
	// whose writes are still in flight.
	e.appliedLSN.Store(e.pendingLSN)
	return nil
}

// loadCells returns the current version group of uk, or nil if the key is absent. The
// returned cells alias the store's page and are only read before the next store call, so
// they are not copied.
func (e *Engine) loadCells(uk []byte) ([]cell, error) {
	v, ok, err := e.s.Get(uk)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	cells, ok := decodeGroup(v)
	if !ok {
		return nil, fmt.Errorf("f2engine: corrupt version group for key %q", uk)
	}
	return cells, nil
}

// upsertCell inserts nc into cells keeping them newest-first (version descending). Commit
// versions increase, so a new cell is normally the newest and lands at the front; a redo
// replays in the same order. A cell whose version already exists is a re-applied commit, so
// it replaces the existing one rather than duplicating it.
func upsertCell(cells []cell, nc cell) []cell {
	for i := range cells {
		if cells[i].version == nc.version {
			cells[i] = nc
			return cells
		}
		if cells[i].version < nc.version {
			cells = append(cells, cell{})
			copy(cells[i+1:], cells[i:])
			cells[i] = nc
			return cells
		}
	}
	return append(cells, nc)
}

// NewReader implements engine.Engine.
func (e *Engine) NewReader(snap engine.Snapshot) (engine.Reader, error) {
	return &reader{e: e, snap: snap}, nil
}

// GetAt implements engine.PointReader: a point read at a snapshot with no per-read reader
// allocation. It is the same resolution NewReader().Get performs.
func (e *Engine) GetAt(snap engine.Snapshot, userKey []byte) ([]byte, error) {
	return e.resolve(snap, userKey)
}

// resolve reads userKey's version group and folds it at snap.
func (e *Engine) resolve(snap engine.Snapshot, userKey []byte) ([]byte, error) {
	v, ok, err := e.s.Get(userKey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, engine.ErrNotFound
	}
	cells, ok := decodeGroup(v)
	if !ok {
		return nil, fmt.Errorf("f2engine: corrupt version group for key %q", userKey)
	}
	tc := snap.TTLClock()
	ops := make([]format.Op, 0, len(cells))
	for _, c := range cells {
		op, ok := format.OpFromParts(c.version, c.kind, c.value, tc.For(c.kind))
		if !ok {
			continue // a range marker would resolve through a range-delete set f2 has none of
		}
		ops = append(ops, op)
	}
	// f2 has no range deletes (Apply rejects them), so the covering range-delete version is 0.
	val, ok := format.Fold(ops, snap.Version, 0, e.merge)
	if !ok {
		return nil, engine.ErrNotFound
	}
	return append([]byte(nil), val...), nil
}

// Maintain implements engine.Engine. f2 schedules its own log compaction, so the engine
// has no host-driven maintenance to do yet; version-group GC lands separately.
func (e *Engine) Maintain(ctx context.Context, budget engine.MaintBudget) (engine.MaintReport, error) {
	return engine.MaintReport{}, nil
}

// Stats implements engine.Engine, mapping f2's accounting onto the shared shape.
func (e *Engine) Stats() engine.EngineStats {
	st := e.s.Stats()
	return engine.EngineStats{
		LiveKeys:      st.Keys,
		LiveBytes:     st.LiveBytes,
		PhysicalBytes: st.LogBytes,
		Amplification: st.SpaceAmplification,
	}
}

// Reclaim implements engine.Engine. f2 reclaims stranded bytes through its own compaction
// rather than a page freelist, so there is nothing for the host vacuum to drive here.
func (e *Engine) Reclaim(budget int) (int, error) { return 0, nil }

// RecoverFinished implements engine.Engine. f2 recovers itself on open, so there is no
// in-memory index for the host to ask it to rebuild after WAL replay.
func (e *Engine) RecoverFinished(lastVersion uint64) error { return nil }

// Checkpoint flushes the f2 store to its durable layout and advances the durable LSN to
// the last applied batch. The host drives durability through Flush in its checkpoint path;
// this method is the same fold for a direct caller or a test. It is a no-op in memory-only
// mode, where there is nothing to fsync.
func (e *Engine) Checkpoint() error { return e.checkpoint(nil) }

// CheckpointContext is Checkpoint threaded with a context so a large checkpoint can be
// bounded or cancelled.
func (e *Engine) CheckpointContext(ctx context.Context) error { return e.checkpoint(ctx) }

// checkpoint folds the store and records how far the fold reached. It reads the applied
// mark under mu first, so the recorded durable LSN never runs ahead of a batch whose writes
// are still in flight; the store fold that follows persists at least every batch up to that
// mark, so the mark is a safe lower bound on what is now durable. A batch that commits
// during the fold lands at a higher LSN and is simply kept in the host WAL for the next
// open, never lost.
func (e *Engine) checkpoint(ctx context.Context) error {
	e.mu.Lock()
	reached := e.appliedLSN.Load()
	e.mu.Unlock()
	var err error
	if ctx == nil {
		err = e.s.Checkpoint()
	} else {
		err = e.s.CheckpointContext(ctx)
	}
	if err != nil {
		return err
	}
	e.durableLSN.Store(reached)
	return nil
}

// Flush implements the host's self-durable seam: it persists every applied batch into the
// f2 file and advances the durable LSN. The host calls it at the start of a checkpoint,
// before it folds the WAL, so the WAL is reclaimed no further than f2's durable point.
func (e *Engine) Flush() error { return e.checkpoint(nil) }

// NoteLSN records the WAL LSN the host assigned to the next batch. The host calls it on the
// same goroutine just before Apply, on both the live commit and the redo path, so Apply can
// promote it to the applied mark once the batch lands.
func (e *Engine) NoteLSN(lsn uint64) { e.pendingLSN = lsn }

// DurableLSN reports the highest WAL LSN whose effects are fsynced into the f2 file. The
// host folds the WAL no further than this and replays the tail past it on the next open, so
// f2's own recovery and the host's WAL replay meet exactly at this point with no gap or
// double count.
func (e *Engine) DurableLSN() uint64 { return e.durableLSN.Load() }

// reader is a point-read view at a fixed snapshot. f2 has no key order, so it serves point
// reads and rejects range iteration.
type reader struct {
	e    *Engine
	snap engine.Snapshot
}

// Get implements engine.Reader.
func (r *reader) Get(userKey []byte) ([]byte, error) { return r.e.resolve(r.snap, userKey) }

// NewIter implements engine.Reader by reporting that f2 cannot serve an ordered scan.
func (r *reader) NewIter(opts engine.IterOptions) (engine.Cursor, error) {
	return nil, ErrUnsupported
}

// Close implements engine.Reader.
func (r *reader) Close() error { return nil }

// Compile-time checks that the engine and reader satisfy the SPI, including the optional
// point-read fast path.
var (
	_ engine.Engine      = (*Engine)(nil)
	_ engine.PointReader = (*Engine)(nil)
	_ engine.Reader      = (*reader)(nil)
)
