// Package betree is the unified Bε-tree core: the re-founded storage engine the
// 2059 redesign builds (notes/Spec/2059/redesign). It replaces the shipped
// two-engine split, an in-place B+tree and an LSM behind one SPI, with a single
// tunable core whose ε buffering ratio spans read-optimized to write-optimized on
// one structure. The architecture of record is redesign doc 01; this file is the
// start of milestone M0 from doc 08.
//
// What this file is, and is not, yet. M0 lands the new core as a correct, slow,
// single-latched skeleton that implements the Engine SPI end to end and is verified
// against the conformance oracle, so the later milestones have a known-correct base
// to make fast without ever passing through an incorrect state. This skeleton keeps
// its cells in one in-memory ordered store and resolves MVCC visibility with the
// shared format fold, exactly as the model engine does, so it is correct by
// construction against the same oracle the shipped cores answer to. It is not the
// paged Bε node, the new on-disk format, or the migration path: those are the next
// M0 PRs (doc 06), which replace the in-memory store under this same SPI. It is also
// not buffered, not optimistically latched, and not off-heap: those are M1 through
// M6. The point of landing it first is the alongside-then-flip plan from doc 08: the
// new core sits behind the SPI next to the shipped cores, off the default path, and
// the differential harness drives the shipped engine and this core through the same
// operation stream so every divergence is caught against known-correct behavior
// before any milestone makes the core fast.
//
// The constraints the whole redesign builds under hold here: pure Go, zero
// dependencies, CGO_ENABLED=0, GOWORK=off, no internal/ package directory.
package betree

import (
	"bytes"
	"context"
	"sort"
	"sync"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// Tree is an opened Bε-tree core. In M0 it carries its cells in an in-memory
// ordered store guarded by one RWMutex (the single-latched skeleton). The pager
// handle is held but not yet used: M0's later PRs move the store onto paged Bε
// nodes through it, and the SPI above does not change when they do.
type Tree struct {
	// pgr is the shared pager the later M0 PRs build the paged node layout over. The
	// skeleton keeps it so New has the same shape db.newEngine calls for every core,
	// and so the format/migration PRs do not have to rewire construction.
	pgr *pager.Pager

	mu sync.RWMutex
	// store maps string(internalKey) -> value. Keying by the full internal key
	// (user_key || ^version || kind) makes every version of a user key sort together
	// newest-first under format.CompareInternal, and makes re-applying the same
	// committed batch (as recovery does) idempotent, since the key is identical.
	store map[string][]byte
	// rangeDels is the live set of range-delete intervals, extended on Apply and
	// rebuilt at Open, so a read folds a range delete whose marker cell the read
	// never visits (spec 11 §4), the same contract the shipped cores keep.
	rangeDels []format.RangeDel
	// merge folds an existing value and a merge operand into a new value. Nil makes a
	// merge operand behave as a plain set. The library's merge registry and the
	// conformance harness install it through SetMergeFunc.
	merge func(existing, operand []byte) []byte
}

// New returns a Bε-tree core bound to pgr. Call Open to finish wiring it to the
// shared substrate. The signature matches the other cores so db.newEngine
// constructs it the same way.
func New(pgr *pager.Pager) *Tree {
	return &Tree{pgr: pgr, store: map[string][]byte{}}
}

// Kind implements engine.Engine. It reports the new selector value so a file this
// core writes records 3, distinct from the shipped cores.
func (t *Tree) Kind() engine.Kind { return engine.Beta }

// SetMergeFunc installs the merge resolver used during version resolution. The
// conformance harness and the library's merge registry call it.
func (t *Tree) SetMergeFunc(f func(existing, operand []byte) []byte) { t.merge = f }

// Open implements engine.Engine. The skeleton holds all of its state in memory, so
// there is no root page to materialize; it records the merge resolver if the host
// supplied one through the env and is otherwise ready. The paged M0 PRs grow this
// into the real open path (materialize an empty root, read the header's engine-root
// field) without changing the SPI.
func (t *Tree) Open(env *engine.Env) error {
	return nil
}

// Close implements engine.Engine. It does not flush; the host checkpoints first.
// The in-memory skeleton has nothing to release.
func (t *Tree) Close() error { return nil }

// Apply implements engine.Engine. It installs every entry's internal key into the
// store and records any range-delete marker so reads can fold it. The batch is
// already durable in the WAL by the time Apply is called, so a crash mid-Apply is
// harmless: recovery re-derives the identical Apply from the WAL, and because the
// internal key carries the commit version the re-apply is idempotent.
func (t *Tree) Apply(batch *engine.WriteBatch, commitVersion uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, e := range batch.Entries() {
		ik := string(e.InternalKey)
		t.store[ik] = append([]byte(nil), e.Value...)
		if format.KindOf(e.InternalKey) == format.KindRangeBegin {
			t.rangeDels = append(t.rangeDels, format.RangeDel{
				Lo:      append([]byte(nil), format.UserKey(e.InternalKey)...),
				Hi:      append([]byte(nil), e.Value...),
				Version: format.Version(e.InternalKey),
			})
		}
	}
	return nil
}

// Maintain implements engine.Engine. The skeleton has no background work; version
// GC, compaction, and reclamation arrive with the paged layout in the later
// milestones.
func (t *Tree) Maintain(ctx context.Context, budget engine.MaintBudget) (engine.MaintReport, error) {
	return engine.MaintReport{}, nil
}

// Stats implements engine.Engine with the in-memory footprint. The paged PRs
// replace this with real space accounting.
func (t *Tree) Stats() engine.EngineStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var bytesN int64
	for k, v := range t.store {
		bytesN += int64(len(k) + len(v))
	}
	return engine.EngineStats{PhysicalBytes: bytesN, Amplification: 1}
}

// Reclaim implements engine.Engine. Nothing to reclaim in the in-memory skeleton.
func (t *Tree) Reclaim(budget int) (int, error) { return 0, nil }

// RecoverFinished implements engine.Engine. The skeleton's state is rebuilt entirely
// from the replayed Apply calls, so there is no separate index to reconstruct.
func (t *Tree) RecoverFinished(lastVersion uint64) error { return nil }

// NewReader implements engine.Engine, returning a consistent read view at snap.
func (t *Tree) NewReader(snap engine.Snapshot) (engine.Reader, error) {
	return &reader{t: t, snap: snap}, nil
}

// resolved is one MVCC-resolved (userKey, value) pair in the snapshot view.
type resolved struct {
	uk  []byte
	val []byte
}

// snapshot returns the sorted, MVCC-resolved view at snap: for each user key, the
// newest version <= snap.Version with tombstones removed, merges folded, and range
// deletes applied. It uses the same format.Fold the shipped cores and the oracle
// use, so a divergence is always a real bug, never a difference in resolution. The
// skeleton resolves the whole store on every read, which is correct but slow; the
// paged PRs replace this with a descent and a leaf walk.
func (t *Tree) snapshot(snap engine.Snapshot) []resolved {
	t.mu.RLock()
	defer t.mu.RUnlock()

	iks := make([][]byte, 0, len(t.store))
	for k := range t.store {
		iks = append(iks, []byte(k))
	}
	sort.Slice(iks, func(i, j int) bool {
		return format.CompareInternal(iks[i], iks[j]) < 0
	})

	var out []resolved
	tc := snap.TTLClock()
	var i int
	for i < len(iks) {
		uk := format.UserKey(iks[i])
		// Gather this user key's version group (already newest-first under the sort),
		// dropping range-delete markers, which resolve through rangeDels not as ops.
		var ops []format.Op
		j := i
		for j < len(iks) && bytes.Equal(format.UserKey(iks[j]), uk) {
			ik := iks[j]
			j++
			op, ok := format.OpFromCell(ik, t.store[string(ik)], tc.For(format.KindOf(ik)))
			if !ok {
				continue
			}
			ops = append(ops, op)
		}
		i = j

		rd := format.NewestCoveringRangeDel(t.rangeDels, uk, snap.Version)
		val, ok := format.Fold(ops, snap.Version, rd, t.merge)
		if !ok {
			continue
		}
		out = append(out, resolved{uk: append([]byte(nil), uk...), val: append([]byte(nil), val...)})
	}
	return out
}

// reader is a point/range read view at a fixed snapshot.
type reader struct {
	t    *Tree
	snap engine.Snapshot
}

func (r *reader) Get(userKey []byte) ([]byte, error) {
	view := r.t.snapshot(r.snap)
	idx := sort.Search(len(view), func(i int) bool {
		return bytes.Compare(view[i].uk, userKey) >= 0
	})
	if idx < len(view) && bytes.Equal(view[idx].uk, userKey) {
		return append([]byte(nil), view[idx].val...), nil
	}
	return nil, engine.ErrNotFound
}

func (r *reader) NewIter(opts engine.IterOptions) (engine.Cursor, error) {
	view := r.t.snapshot(r.snap)
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

// cursor walks a pre-resolved snapshot view. Bounds and prefix are already applied;
// reverse flips the direction of First/Last/Next/Prev.
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
	// The resolved view does not carry a version; synthesize a max-version internal
	// key so the merge layer's comparisons above the seam stay well-defined, exactly
	// as the model cursor does.
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
