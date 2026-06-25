// Package betree is the unified Bε-tree core: the re-founded storage engine the
// 2059 redesign builds (notes/Spec/2059/redesign). It replaces the shipped
// two-engine split, an in-place B+tree and an LSM behind one SPI, with a single
// tunable core whose ε buffering ratio spans read-optimized to write-optimized on
// one structure. The architecture of record is redesign doc 01; this file is the
// start of milestone M0 from doc 08.
//
// What this file is, and is not, yet. M0 lands the new core as a correct, slow,
// single-latched engine that implements the Engine SPI end to end and is verified
// against the conformance oracle, so the later milestones have a known-correct base
// to make fast without ever passing through an incorrect state. The cells now live
// in a real chain of generation-2 leaf pages on the pager (paged.go and node.go);
// reads decode the run and resolve MVCC visibility with the shared format fold,
// exactly as the model engine does, so the core is correct by construction against
// the same oracle the shipped cores answer to. It is not yet the interior-routed
// logarithmic descent, the in-place leaf insert, or the migration path: those are
// the next M0 slices (doc 06), which replace the whole-run rewrite under this same
// SPI. It is also not buffered, not optimistically latched, and not off-heap: those
// are M1 through M6. The point of landing the slow base first is the
// alongside-then-flip plan from doc 08: the new core sits behind the SPI next to the
// shipped cores, off the default path, and the differential harness drives the
// shipped engine and this core through the same operation stream so every divergence
// is caught against known-correct behavior before any milestone makes the core fast.
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

// Tree is an opened Bε-tree core. In M0 its cells live in a sorted run of
// generation-2 leaf pages on the pager (paged.go), guarded by one RWMutex (the
// single-latched base): writers take the write lock and rewrite the run, readers
// take the read lock and decode it. The pager handle is the substrate the run is
// built over, the same way the shipped btree builds over it.
type Tree struct {
	// pgr is the shared pager the leaf run lives on. New holds it so db.newEngine
	// constructs this core the same way it constructs the others.
	pgr *pager.Pager

	// mu guards the run. Apply rewrites it under the write lock; reads decode it under
	// the read lock. The single latch is the M0 base; M2 replaces it with optimistic
	// lock coupling and epoch reclamation.
	mu sync.RWMutex

	// merge folds an existing value and a merge operand into a new value. Nil makes a
	// merge operand behave as a plain set. The library's merge registry and the
	// conformance harness install it through SetMergeFunc.
	merge func(existing, operand []byte) []byte

	// tail is the mutable hot tail (tail.go): an in-memory ordered region keyed by
	// internal key that absorbs writes before any becomes a tree message, overwriting a
	// hot key's slot in place rather than appending a message. tailBytes tracks its live
	// size against the rollover budget. Both are guarded by mu, the same write latch
	// Apply and Flush take. A nil map is the empty tail; tailPut lazily allocates it.
	tail      map[string]message
	tailBytes int

	// curLSN is the WAL commit LSN of the batch currently being applied, set by NoteLSN
	// just before Apply; tailMinLSN is the low-water LSN of the current tail epoch (the
	// LSN when the tail last went from empty to non-empty); durableLSN is the highest
	// LSN whose effects have rolled over onto pages the checkpoint folds. The host reads
	// durableLSN through DurableLSN to keep the WAL above the un-rolled tail, exactly as
	// it does for the LSM memtable. All three are guarded by mu.
	curLSN     uint64
	tailMinLSN uint64
	durableLSN uint64
}

// New returns a Bε-tree core bound to pgr. Call Open to materialize the root and
// finish wiring it to the shared substrate. The signature matches the other cores so
// db.newEngine constructs it the same way.
func New(pgr *pager.Pager) *Tree {
	return &Tree{pgr: pgr}
}

// Kind implements engine.Engine. It reports the new selector value so a file this
// core writes records 3, distinct from the shipped cores.
func (t *Tree) Kind() engine.Kind { return engine.Beta }

// SetMergeFunc installs the merge resolver used during version resolution. The
// conformance harness and the library's merge registry call it.
func (t *Tree) SetMergeFunc(f func(existing, operand []byte) []byte) { t.merge = f }

// Open implements engine.Engine. On a fresh database the engine root is the null
// page, so Open materializes an empty root leaf and records it in the header; on an
// existing database the root already names the run and reads load it lazily, so Open
// has nothing to rebuild. It runs once at construction before any concurrent use, so
// it does not take the latch.
func (t *Tree) Open(env *engine.Env) error {
	if t.pgr.Header().EngineRoot != format.NoPage {
		return nil
	}
	return t.emptyRoot()
}

// Close implements engine.Engine. It does not flush; the host checkpoints first, and
// once the tail is drained (Flush, run before the checkpoint that must stand alone)
// the run already lives on the pager. There is nothing to release.
func (t *Tree) Close() error { return nil }

// NoteLSN records the WAL commit LSN of the batch about to be applied. The host calls
// it just before Apply on both the live commit and the redo path, the companion of
// DurableLSN, exactly as it feeds the LSM core. It stamps the tail's view of "how far
// the WAL has reached" so a rollover can advance the durable mark to the right point.
// An engine with no logical-WAL host (the betree reopen tests drive the pager
// directly) never sees a NoteLSN, leaving curLSN zero, and reaches durability through
// Flush instead.
func (t *Tree) NoteLSN(lsn uint64) {
	t.mu.Lock()
	t.curLSN = lsn
	t.mu.Unlock()
}

// DurableLSN reports the highest WAL LSN whose effects have rolled over onto pages the
// checkpoint folds, so the host never reclaims WAL that still backs an un-rolled tail
// slot. It mirrors the LSM core's durable mark: when the tail is empty every applied
// batch is on pages and the mark is the last rollover's LSN; when the tail holds
// un-rolled writes the mark is held one below the tail epoch's low-water, since those
// writes live only in the heap and the WAL until the next rollover. The host keeps the
// WAL above this point and recovery replays it back through Apply, rebuilding the tail.
func (t *Tree) DurableLSN() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.tail) == 0 || t.tailMinLSN == 0 {
		return t.durableLSN
	}
	if lim := t.tailMinLSN - 1; lim < t.durableLSN {
		return lim
	}
	return t.durableLSN
}

// Flush drains the mutable hot tail into the tree, leaving every visible write on a
// page the checkpoint can fold. The host calls it before a checkpoint that must stand
// alone without its WAL sidecar (migration writes a self-contained file; the reopen
// tests drive the pager directly with no logical WAL to replay), so the tail is never
// the only home of a committed write across that boundary. It is a no-op on an empty
// tail.
func (t *Tree) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rollover()
}

// Apply implements engine.Engine. Each batch entry lands first in the mutable hot
// tail (tail.go), the in-memory region that absorbs writes before any becomes a tree
// message: a hot key rewritten many times overwrites one slot rather than appending a
// message per write, and the tail rolls over into the buffered tree in one batched
// descent when it fills (M1.2, doc 02 section 3). Under the tail, a rollover pushes a
// sealed run into the highest owning node's buffer, where messages rest until a flush
// carries them down, instead of descending per cell (M1.1, doc 02 sections 1 and 2).
// The batch is already durable in the WAL by the time Apply is called, so a crash
// mid-Apply, or with a populated tail, is harmless: recovery replays the WAL through
// Apply and rebuilds both the tail and the tree, and because the internal key carries
// the commit version each message overwrites the identical cell rather than
// duplicating it, so the replay is idempotent in content. Range-delete markers flow
// through as ordinary messages (kind range-begin, value = the interval end) and reads
// rebuild the interval set from them. A tail slot, a buffered message, and the leaf
// record they become all fold to the same answer, so the read path resolves the tail
// plus the buffered tree identically to the unbuffered one it replaces (paged.go
// gathers the tail, the buffers, and the leaf records into one fold).
func (t *Tree) Apply(batch *engine.WriteBatch, commitVersion uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, e := range batch.Entries() {
		if err := t.tailPut(e.InternalKey, e.Value); err != nil {
			return err
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

// Stats implements engine.Engine with the run's physical footprint: the number of
// leaf pages times the page size. A run-walk error reports a zero footprint rather
// than failing, since Stats has no error channel.
func (t *Tree) Stats() engine.EngineStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, pages, err := t.loadRun()
	if err != nil {
		return engine.EngineStats{}
	}
	return engine.EngineStats{
		PhysicalBytes: int64(len(pages)) * int64(t.pgr.PageSize()),
		Amplification: 1,
	}
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

// reader is a point/range read view at a fixed snapshot.
type reader struct {
	t    *Tree
	snap engine.Snapshot
}

func (r *reader) Get(userKey []byte) ([]byte, error) {
	view, err := r.t.snapshot(r.snap)
	if err != nil {
		return nil, err
	}
	idx := sort.Search(len(view), func(i int) bool {
		return bytes.Compare(view[i].uk, userKey) >= 0
	})
	if idx < len(view) && bytes.Equal(view[idx].uk, userKey) {
		return append([]byte(nil), view[idx].val...), nil
	}
	return nil, engine.ErrNotFound
}

func (r *reader) NewIter(opts engine.IterOptions) (engine.Cursor, error) {
	view, err := r.t.snapshot(r.snap)
	if err != nil {
		return nil, err
	}
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
