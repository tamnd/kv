// Package lsm implements the log-structured merge engine (spec 06), the core a
// workload opts into when write throughput dominates. It satisfies the same
// engine.Engine seam as the B-tree core (spec 04), so every layer above the seam
// (transactions, iterators, cache, API, CLI, server) drives it unchanged.
//
// The engine is built in vertical slices. This first slice is the write path and
// the in-memory read path: an arena-backed skip-list memtable that Apply inserts
// into and that NewReader folds for point and range reads, matching the shared
// MVCC resolution the oracle and the B-tree core use. Durability already holds,
// because the host logs every batch to the WAL before calling Apply and replays
// the WAL into Apply on open, so a memtable-only engine is crash-safe even before
// on-disk segments exist. What this slice does not yet do is bound memory: there is
// no sealing or flush, so the memtable grows until close. Sealing, L0 flush, the
// MANIFEST, compaction, and filters arrive in the segment slices that follow, each
// extending this seam-conformant core without disturbing it.
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

// LSM is the log-structured merge engine handle (spec 06 §9).
type LSM struct {
	pgr *pager.Pager

	mu  sync.RWMutex
	mem *memtable

	merge func(existing, operand []byte) []byte
	env   *engine.Env
}

// New returns an LSM core bound to a pager. The pager is the durable substrate the
// later segment slices write to; this slice keeps all data in the memtable and uses
// the pager only for its page geometry.
func New(pgr *pager.Pager) *LSM {
	return &LSM{pgr: pgr, mem: newMemtable(defaultArenaCap)}
}

// Kind implements engine.Engine.
func (l *LSM) Kind() engine.Kind { return engine.LSM }

// Open implements engine.Engine.
func (l *LSM) Open(env *engine.Env) error {
	l.env = env
	return nil
}

// Close implements engine.Engine. It drops the in-memory state; the host
// checkpoints before close, and on the next open the WAL replay rebuilds the
// memtable (this slice has no on-disk segments to flush).
func (l *LSM) Close() error { return nil }

// SetMergeFunc installs the merge resolver used during read-time version
// resolution, the same hook the B-tree core and the oracle expose.
func (l *LSM) SetMergeFunc(f func(existing, operand []byte) []byte) {
	l.mu.Lock()
	l.merge = f
	l.mu.Unlock()
}

// Apply implements engine.Engine: it inserts every entry into the active memtable.
// The batch is already durable in the WAL, so this is pure in-memory work.
func (l *LSM) Apply(batch *engine.WriteBatch, commitVersion uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range batch.Entries() {
		l.mem.set(e.InternalKey, e.Value)
	}
	return nil
}

// Maintain implements engine.Engine. Compaction and flush land in later slices; for
// now the memtable-only core has no background work.
func (l *LSM) Maintain(ctx context.Context, budget engine.MaintBudget) (engine.MaintReport, error) {
	return engine.MaintReport{}, nil
}

// Stats implements engine.Engine, reporting the memtable's footprint as the
// physical size. On-disk segment accounting joins this once flush exists.
func (l *LSM) Stats() engine.EngineStats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return engine.EngineStats{
		PhysicalBytes: int64(l.mem.size()),
		Amplification: 1,
	}
}

// Reclaim implements engine.Engine: nothing on disk to reclaim yet.
func (l *LSM) Reclaim(budget int) (int, error) { return 0, nil }

// DurableLSN reports the highest WAL LSN whose effects the engine has persisted to
// the main file, the mark past which the host must not fold and reset the WAL
// (spec 06 §4). The LSM core's applied writes live in the memtable until a flush
// turns it into an on-disk segment; this slice has no flush, so nothing is durable
// in the file and the engine reports 0. The host therefore keeps the whole WAL and
// replays it into the memtable on every open, the bring-up tradeoff flush removes:
// once a memtable is flushed, this advances to that batch's LSN and the WAL past it
// becomes reclaimable. The B-tree core, whose every Apply lands in pages the
// checkpoint folds, does not implement this method and the host folds the entire
// log.
func (l *LSM) DurableLSN() uint64 { return 0 }

// RecoverFinished implements engine.Engine. The B-tree core does nothing here; the
// LSM core will, in a later slice, replay the MANIFEST to rebuild its level
// structure. With no segments yet, the WAL replay into Apply has already restored
// the full memtable, so there is nothing more to do.
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

// snapshot folds the memtable into the sorted, visible (userKey, value) view at
// snap: for each user key the newest version <= snap.Version, tombstones removed,
// merges folded, range deletes applied. It uses the shared format.Fold so any
// divergence from the oracle or the B-tree core is a bug here, never in resolution.
func (l *LSM) snapshot(snap engine.Snapshot) []resolved {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Gather every cell in internal-key order: user ascending, version descending,
	// kind ascending. Equal-user-key cells are therefore already grouped newest-first.
	type cell struct {
		ik  []byte
		val []byte
	}
	var cells []cell
	l.mem.scan(func(ik, val []byte) bool {
		cells = append(cells, cell{
			ik:  append([]byte(nil), ik...),
			val: append([]byte(nil), val...),
		})
		return true
	})

	var out []resolved
	var i int
	for i < len(cells) {
		uk := format.UserKey(cells[i].ik)
		var ops []format.Op
		j := i
		for j < len(cells) && bytes.Equal(format.UserKey(cells[j].ik), uk) {
			op, ok := format.OpFromCell(cells[j].ik, cells[j].val, snap.Now)
			j++
			if !ok {
				continue // range markers resolve through rangeDels, not as ops
			}
			ops = append(ops, op)
		}
		i = j

		rd := format.NewestCoveringRangeDel(l.mem.rangeDels, uk, snap.Version)
		val, ok := format.Fold(ops, snap.Version, rd, l.merge)
		if !ok {
			continue
		}
		out = append(out, resolved{
			uk:  append([]byte(nil), uk...),
			val: append([]byte(nil), val...),
		})
	}
	return out
}

// reader is a point/range read view at a fixed snapshot. It resolves the memtable
// once and serves reads from the materialized view, the same shape as the model
// engine's reader; the segment slices replace this with a streaming merge across
// memtable and on-disk levels.
type reader struct {
	l    *LSM
	snap engine.Snapshot
}

func (r *reader) Get(userKey []byte) ([]byte, error) {
	view := r.l.snapshot(r.snap)
	idx := sort.Search(len(view), func(i int) bool {
		return bytes.Compare(view[i].uk, userKey) >= 0
	})
	if idx < len(view) && bytes.Equal(view[idx].uk, userKey) {
		return append([]byte(nil), view[idx].val...), nil
	}
	return nil, engine.ErrNotFound
}

func (r *reader) NewIter(opts engine.IterOptions) (engine.Cursor, error) {
	view := r.l.snapshot(r.snap)
	lower, upper := opts.Lower, opts.Upper
	if len(opts.Prefix) > 0 {
		lower = opts.Prefix
		upper = format.PrefixSuccessor(opts.Prefix)
	}
	var filtered []resolved
	for _, e := range view {
		if lower != nil && bytes.Compare(e.uk, lower) < 0 {
			continue
		}
		if upper != nil && bytes.Compare(e.uk, upper) >= 0 {
			continue
		}
		filtered = append(filtered, e)
	}
	return &cursor{view: filtered, pos: -1, reverse: opts.Reverse}, nil
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
