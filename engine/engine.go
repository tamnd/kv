// Package engine defines the storage-engine SPI: the seam that the two cores —
// B-tree (spec 05) and LSM (spec 06) — implement, and the contract that lets
// every layer above it (transactions, iterators, cache, API, CLI, server) be
// written once, engine-agnostic (spec 04).
//
// The design rule for the seam is to push everything that can be shared above it
// and confine to it only the physics that genuinely differ between an in-place
// tree and a stack of sorted runs: how keys are physically laid out, how a point
// read and a range scan are served, how a batch of writes is applied, how space
// is reclaimed, and how the engine recovers. Everything else — the file
// container, the pager, the WAL, MVCC versioning, the iterator protocol,
// durability, the API — lives above the seam and is identical for both cores.
//
// The host injects its shared substrate into a core through Env. Spec 04 sketches
// those dependencies as concrete pointers (*Pager, *WAL, ...); this
// implementation declares them as interfaces local to this package so the cores
// can be built and tested (against the model engine) before the concrete pager
// and WAL exist, and so engine never imports those lower packages. The concrete
// types in the pager and wal packages satisfy these interfaces.
package engine

import (
	"context"

	"github.com/tamnd/kv/format"
)

// Kind names a storage core. It reuses the on-disk engine selector from the file
// header (spec 02 offset 21) so the value an Engine reports is the same byte
// recorded in the file.
type Kind = format.EngineKind

const (
	BTree = format.EngineBTree
	LSM   = format.EngineLSM
)

// Engine is the top-level handle for an opened core (spec 04 §2.1).
type Engine interface {
	// Kind reports which core this is.
	Kind() Kind

	// Open binds the core to its durable substrate. It is called once, after the
	// pager is up and recovery has replayed the WAL.
	Open(env *Env) error
	// Close releases the core. It does not flush; the host checkpoints first.
	Close() error

	// NewReader returns a consistent read view at a snapshot version.
	NewReader(snap Snapshot) (Reader, error)

	// Apply installs a committed batch of internal-key mutations into the
	// engine's in-memory and on-disk structures. The batch is ALREADY durable in
	// the WAL by the time Apply is called, so a crash mid-Apply is harmless:
	// recovery re-derives the same Apply from the WAL. commitVersion is the
	// version stamped on every entry's internal key.
	Apply(batch *WriteBatch, commitVersion uint64) error

	// Maintain performs engine-scheduled background work (LSM compaction, B-tree
	// rebalance, vLog GC) up to a budget. The host calls it opportunistically and
	// on a timer; the engine decides what, if anything, to do.
	Maintain(ctx context.Context, budget MaintBudget) (MaintReport, error)

	// Stats reports space accounting and the data the checkpoint/vacuum driver
	// needs (spec 09).
	Stats() EngineStats

	// Reclaim returns pages the engine no longer needs to the freelist, up to the
	// given budget. Used by vacuum (spec 09).
	Reclaim(budget int) (freed int, err error)

	// RecoverFinished is called after the WAL has been replayed into Apply calls,
	// so the engine can reconstruct any in-memory index it needs (the LSM core
	// rebuilds its level structure from the MANIFEST; the B-tree core does
	// nothing, since all its state lives in pages). See spec 08.
	RecoverFinished(lastVersion uint64) error
}

// Reader is a point/range read interface at a fixed snapshot (spec 04 §2.2). The
// host's transaction layer holds a Reader for the life of a read transaction.
type Reader interface {
	// Get returns the value for userKey visible at the reader's snapshot, or
	// ErrNotFound. The engine resolves MVCC versions using the shared
	// internal-key ordering, returning the newest committed version <= snapshot
	// and skipping tombstones.
	Get(userKey []byte) (value []byte, err error)

	// NewIter returns a cursor over a key range at the snapshot. The returned
	// Cursor implements the shared protocol (spec 11).
	NewIter(opts IterOptions) (Cursor, error)

	Close() error
}

// ZeroCopyReader is an optional Reader capability: a point read that returns the value
// aliased to immutable engine-internal storage rather than a fresh caller-owned copy. The
// ordinary Get copies the resolved value out so the caller owns bytes it may keep and
// mutate; for a read that only inspects the value and discards it, that copy is pure
// overhead on the hottest path. An engine implements this only when it can hand back a value
// that stays valid and never mutates under the caller.
//
// The contract on the returned value is narrower than Get's, and a caller opts into it
// knowingly:
//
//   - It is READ-ONLY. The bytes may be shared with the engine's internal cache and with
//     other concurrent readers, so the caller must not modify them. Modifying them corrupts
//     the shared copy for every other reader.
//   - It stays valid for reading after the reader is closed. The value is backed by an
//     immutable, separately allocated node the engine never mutates in place (a writer
//     replaces such a node wholesale rather than editing it), so a reference keeps exactly
//     the read bytes alive. A caller that needs to keep the value past a read it might mutate
//     must copy it itself.
//
// The host (db.GetZeroCopy) uses this when the engine's reader implements it and falls back
// to Get otherwise, so it is purely an optimization an engine may decline.
type ZeroCopyReader interface {
	// GetZeroCopy returns the value for userKey visible at the reader's snapshot, aliased
	// to immutable internal storage, or ErrNotFound. It resolves MVCC versions exactly as
	// Get does; the only difference is the returned value is not copied and is read-only.
	GetZeroCopy(userKey []byte) (value []byte, err error)
}

// PointReader is an optional Engine capability: a point read at a snapshot that does not
// allocate a per-read Reader. The ordinary point read goes NewReader -> Reader.Get -> Close;
// the reader escapes through the Reader interface and so heap-allocates, and an engine that
// folds point and range reads through one streaming resolver also allocates that resolver's
// result slice even though a point read resolves exactly one key. For a host whose hot path is
// nothing but Get, both are pure per-call garbage. An engine implements this when it can resolve
// a point read straight off its shared, immutable internal state with no per-read object.
//
// The host (db.snapshotGet) uses it when the engine implements it and falls back to the
// NewReader path otherwise, so it is purely an optimization an engine may decline.
type PointReader interface {
	// GetAt returns the value for userKey visible at snap, or ErrNotFound. The value is
	// copied and caller-owned, exactly as Reader.Get's: GetAt is equivalent to
	// NewReader(snap).Get(userKey) then Close, without allocating the reader.
	GetAt(snap Snapshot, userKey []byte) (value []byte, err error)
}

// BulkLoader is an optional engine capability: population of an empty engine from a
// stream of cells already in ascending internal-key order, building the on-disk
// structure bottom-up instead of inserting one cell at a time (spec 15 §6). The host
// (db.Load) uses it when the engine implements it and the database has no commits yet,
// and falls back to ordinary batched commits otherwise.
//
// The cells are not logged per entry: BulkLoad writes pages straight through the pager,
// so the host makes the build durable with a single checkpoint after it returns. A
// crash before that checkpoint leaves the database empty, which makes the load atomic
// at the checkpoint boundary. Because it rebuilds the root from scratch it is only
// valid on a freshly opened, empty engine.
type BulkLoader interface {
	// BulkLoad consumes cells from next, which returns each (internalKey, value) in
	// ascending CompareInternal order and false at end of stream, and installs a tree
	// built bottom-up over them as the engine's contents.
	BulkLoad(next func() (internalKey, value []byte, ok bool)) error
}

// GroupApplier is an optional engine capability: applying a whole group-commit group's
// batches in one call so the engine can spread the inserts across cores instead of
// taking them one batch at a time on the leader's goroutine (perf/03 W1, perf/07). The
// host (db group commit) uses it when the engine implements it, and falls back to a Apply
// per batch otherwise. Every batch is already durable in the WAL by the time this is
// called, the same precondition as Apply.
//
// The batches are independent: each carries a distinct commit version, so no two entries
// across the group share an internal key, and insert order does not affect the result.
// versions[i] is the commit version of batches[i], the same value Apply would receive. An
// engine that tracks a durable mark folds it through the usual NoteLSN call the host makes
// once before this, with the group's largest commit LSN.
type GroupApplier interface {
	// ApplyGroup installs every batch's mutations, concurrently when the group is large
	// enough to be worth it. It is equivalent to calling Apply on each batch in version
	// order, except an engine that seals or rolls a structure on a size threshold may take
	// that boundary once for the whole group rather than between its batches.
	ApplyGroup(batches []*WriteBatch, versions []uint64) error
}

// Cursor is the low-level iterator primitive both cores expose (spec 04 §2.3).
// The rich caller-facing iterator (spec 11) is built on top of it above the
// seam.
type Cursor interface {
	SeekGE(userKey []byte) bool // position at first key >= userKey
	SeekLT(userKey []byte) bool // position at last key < userKey
	First() bool
	Last() bool
	Next() bool
	Prev() bool
	Valid() bool

	// Key returns the user key at the cursor, with the version suffix stripped.
	Key() []byte
	// InternalKey returns the full internal key, used by the merge layer above
	// the seam to resolve versions across sources.
	InternalKey() []byte
	// Value returns the value, possibly a lazy pointer that defers fetching a
	// separated value (vLog/overflow) until the caller reads it, so a key-only
	// scan never touches the value store.
	Value() (LazyValue, error)

	Error() error
	Close() error
}
