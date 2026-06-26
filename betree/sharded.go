package betree

// This file is the fourth integration slice of milestone M7, and the one the previous three were
// built for: the sharded core itself, an engine.Engine that mounts one Bε-tree sub-tree per shard in
// a single file and routes every write and read across them. The substance landed and was proven in
// isolation across the earlier slices: partition.go routes a key to one shard, merge.go reassembles a
// global scan order from the per-shard views, shard.go proved the cross-shard commit coordination, and
// the two M8 integration slices made the routing durable (sharddir.go) and let a sub-tree root at a
// directory slot instead of the single header field (root.go). This slice composes them into a working
// core: the directory names the shards, each shard is a real sub-tree on the shared pager, and the
// wrapper fans a write out to the owning shards and folds the per-shard reads back into one view.
//
// What the wrapper is, on this substrate. It is the live read-write path the M7 design draws, built on
// the real pager-backed sub-trees rather than shard.go's in-memory record log: each shard is a *Tree
// with its own tail, its own optimistic read protocol, and its own root in its directory slot, so two
// writers landing in different shards share no tail and no writer lock and proceed in parallel, the
// whole sharding payoff. The cross-shard commit coordinator shard.go proved (the shared committed flag,
// the contiguous read frontier) is the next thing to wire on, once a single committed batch needs to
// span shards atomically through the logical WAL; on this slice a host batch is already a committed
// unit, so the wrapper fans its entries to the shards and applies each shard's subset under that
// shard's own writer lock, and the merge of the disjoint per-shard views is what makes the read whole.
//
// The two routing rules the fan-out follows, and why they are correct.
//
// A point write routes to exactly one shard. A set, a delete, or a merge for a user key goes to the one
// shard the partition function names for that key, and because the same key always routes to the same
// shard, every version of a key and every merge operand on it land in the same sub-tree, so that
// sub-tree folds the key's whole version history by itself with no cross-shard coordination on the
// read. The sub-trees are disjoint by user key, which is what lets the read merge them by plain
// interleave rather than by resolving the same key from two sources.
//
// A range delete replicates to every shard. A DeleteRange marker covers a contiguous key interval, but
// under hash partitioning the keys in that interval are scattered across all the shards, so no single
// shard holds all the keys the marker must shadow. The marker is therefore copied into every shard's
// sub-tree, where each shard folds it against its own keys: a key in the interval is shadowed by the
// copy resident in whichever shard owns it, and a shard that happens to hold no covered key just carries
// a harmless marker. Replicating to all shards is the correct superset for both partitioners (a range
// partitioner could route the marker more narrowly, but the all-shards copy is never wrong and keeps the
// fan-out independent of the partitioner kind), and because the marker cell is keyed and idempotent the
// duplicate copies never multiply into the user-visible keyspace.
//
// What this slice deliberately leaves. The cross-shard commit coordinator is not wired yet, because a
// host batch is already atomic above the engine here; it lands when a logical-WAL host drives the
// sharded core and a single transaction's writes must publish across shards in one step. The streaming
// cross-shard cursor (a heap over N live shard cursors rather than over N materialized views) waits on
// the same arena-backed zero-copy cursor the single-domain core is still waiting on, so the merge here
// is the slice merge merge.go proved, fed by each shard's resolved view. The per-shard WAL slot is the
// logical-WAL integration. None of those change the routing or the merge this slice proves; they are
// the live-path wiring the flip carries.

import (
	"bytes"
	"context"
	"sync"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// Sharded is a sharded Bε-tree core: N sub-trees in one file, a partition function routing each key to
// one of them, and a shard directory recording the partitioning durably. It implements engine.Engine
// by fanning a write out to the owning shards and merging the per-shard reads into one snapshot view.
// It is off the default path, selected by the host behind a sharding option; the default core is the
// single-shard Tree, untouched.
type Sharded struct {
	// pgr is the shared pager every sub-tree and the directory page live on.
	pgr *pager.Pager

	// part is the partition function. On a fresh database it is the one the constructor was given,
	// which establishes the sharding; on a reopen it is rebuilt from the directory, which is the durable
	// source of truth for routing, so a key always finds the shard it was written into.
	part partitioner

	// dir is the in-memory shard directory: the partition kind, the per-shard sub-tree roots, and a
	// range partitioner's split keys. The sub-trees record their roots into its slots as they grow, and
	// persistDir writes it back to its page so the roots and the partitioning survive a reopen.
	dir *shardDir

	// dirPgno is the durable home of the directory page, NoPage until the first persist allocates it (or
	// the reopen path that read it). Holding it lets a later flush rewrite the directory in place rather
	// than orphaning a page per flush.
	dirPgno format.PageNo

	// subs are the per-shard sub-trees in shard order, each rooted at its directory slot. subs[i] owns
	// every key the partition function routes to shard i.
	subs []*Tree

	// merge is the resolver installed through SetMergeFunc, fanned out to every sub-tree so each folds
	// merges the way the host and the conformance oracle do.
	merge func(existing, operand []byte) []byte
}

// newSharded returns a sharded core over pgr partitioned by part. Call Open to mount the sub-trees:
// on a fresh database Open establishes the sharding from part, on a reopen it rebuilds it from the
// directory. The constructor is unexported because the sharded core is off the default path and proven
// in-package, the same alongside-then-flip discipline the M6 substrate and the earlier M7 slices kept;
// the host wiring that selects it behind a PRAGMA is the flip's integration.
func newSharded(pgr *pager.Pager, part partitioner) *Sharded {
	return &Sharded{pgr: pgr, part: part, dirPgno: format.NoPage}
}

// Kind implements engine.Engine. A sharded file is still the Bε-tree core on disk, so it reports the
// same selector as the single-shard Tree; the shard directory at the engine root is what distinguishes
// the two layouts, not the engine kind.
func (s *Sharded) Kind() engine.Kind { return engine.Beta }

// SetMergeFunc installs the merge resolver and fans it out to every mounted sub-tree. It may be called
// before Open (no sub-trees yet, so the resolver is only recorded and Open installs it) or after (the
// sub-trees exist, so it reaches them here), so the order the host and the conformance harness call it
// in does not matter.
func (s *Sharded) SetMergeFunc(f func(existing, operand []byte) []byte) {
	s.merge = f
	for _, sub := range s.subs {
		sub.SetMergeFunc(f)
	}
}

// Open implements engine.Engine. On a fresh database the engine root is the null page, so Open
// establishes the sharding from the constructor partitioner: a directory of NoPage roots, one sub-tree
// per shard, each sub-tree's Open materializing an empty root into its slot. On a reopen the engine root
// names the directory page, so Open reads it, rebuilds the partition function from it (the durable
// routing rule, not the constructor's), and remounts each sub-tree at its recorded root. It runs once at
// construction before any concurrent use, so it takes no latch.
func (s *Sharded) Open(env *engine.Env) error {
	if root := s.pgr.Header().EngineRoot; root != format.NoPage {
		dir, err := readShardDir(s.pgr, root)
		if err != nil {
			return err
		}
		s.dir = dir
		s.part = dir.partitioner()
		s.dirPgno = root
	} else {
		// A fresh database: the directory's roots are all NoPage, so each sub-tree's Open builds an empty
		// root and records its page in its slot through the directory store.
		s.dir = newShardDir(s.part, make([]format.PageNo, s.part.shards()))
	}

	s.subs = make([]*Tree, len(s.dir.roots))
	for i := range s.subs {
		sub := newSubTree(s.pgr, s.dir, i)
		if s.merge != nil {
			sub.SetMergeFunc(s.merge)
		}
		if err := sub.Open(env); err != nil {
			return err
		}
		s.subs[i] = sub
	}
	return nil
}

// Close implements engine.Engine. It does not flush; the host checkpoints first, and once every
// sub-tree's tail is drained the runs live on the pager. Each sub-tree's Close is a no-op today, so this
// only releases the slice.
func (s *Sharded) Close() error {
	for _, sub := range s.subs {
		if err := sub.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Apply implements engine.Engine. It fans the batch's entries out to the shards that own them, then
// applies each shard's subset to its sub-tree under that sub-tree's own writer lock. A point mutation
// routes to the one shard its user key belongs to; a range-delete marker replicates to every shard so
// each shadows its own covered keys (see the file header for why). A shard that receives no entry is not
// touched, so a batch confined to one shard wakes only that sub-tree.
func (s *Sharded) Apply(batch *engine.WriteBatch, commitVersion uint64) error {
	n := len(s.subs)
	var perShard []*engine.WriteBatch
	at := func(i int) *engine.WriteBatch {
		if perShard == nil {
			perShard = make([]*engine.WriteBatch, n)
		}
		if perShard[i] == nil {
			perShard[i] = engine.NewWriteBatch(commitVersion)
		}
		return perShard[i]
	}

	for _, e := range batch.Entries() {
		if format.KindOf(e.InternalKey) == format.KindRangeBegin {
			for i := 0; i < n; i++ {
				at(i).Add(e.InternalKey, e.Value)
			}
			continue
		}
		at(s.part.route(format.UserKey(e.InternalKey))).Add(e.InternalKey, e.Value)
	}

	for i, b := range perShard {
		if b == nil {
			continue
		}
		if err := s.subs[i].Apply(b, commitVersion); err != nil {
			return err
		}
	}
	return nil
}

// NoteLSN fans the WAL commit LSN out to every sub-tree, so each shard's durable mark advances against
// the same logical clock the host feeds. The companion of DurableLSN; an off-WAL test driver never calls
// it, leaving every shard's curLSN zero and reaching durability through Flush.
func (s *Sharded) NoteLSN(lsn uint64) {
	for _, sub := range s.subs {
		sub.NoteLSN(lsn)
	}
}

// DurableLSN reports the highest WAL LSN every sub-tree has made durable: the minimum of the shards'
// durable marks, since the host may reclaim WAL only below the least-advanced shard. A shard with an
// un-rolled tail holds the whole core's durable mark down exactly as it holds its own.
func (s *Sharded) DurableLSN() uint64 {
	if len(s.subs) == 0 {
		return 0
	}
	min := s.subs[0].DurableLSN()
	for _, sub := range s.subs[1:] {
		if d := sub.DurableLSN(); d < min {
			min = d
		}
	}
	return min
}

// Flush drains every sub-tree's hot tail onto pages and then persists the directory, so a checkpoint
// that must stand alone without its WAL sidecar finds every committed write on a page and the
// partitioning on the directory page the engine root names. The host calls it before such a checkpoint;
// the reopen tests call it to make a clean file the pager can be reopened over.
func (s *Sharded) Flush() error {
	for _, sub := range s.subs {
		if err := sub.Flush(); err != nil {
			return err
		}
	}
	return s.persistDir()
}

// persistDir writes the directory, with the sub-trees' current roots in its slots, to its durable page
// and points the engine root at it. The first persist allocates the page; later persists rewrite it in
// place, so repeated flushes do not orphan a directory page each time (the sub-tree roots change across
// flushes, so the rewrite carries the new roots). The trees are quiescent under the caller, off the
// latch-free read path, so the rewrite copies into the frame directly.
func (s *Sharded) persistDir() error {
	if s.dirPgno == format.NoPage {
		pgno, err := writeShardDir(s.pgr, s.dir)
		if err != nil {
			return err
		}
		s.dirPgno = pgno
	} else {
		fr, err := s.pgr.Get(s.dirPgno, pager.Write)
		if err != nil {
			return err
		}
		dst := make([]byte, s.pgr.UsablePageSize())
		if _, encErr := encodeShardDir(dst, s.dir); encErr != nil {
			s.pgr.Unpin(fr, false)
			return encErr
		}
		copy(fr.Data(), dst)
		s.pgr.Unpin(fr, true)
	}
	s.pgr.Header().EngineRoot = s.dirPgno
	return nil
}

// Maintain implements engine.Engine, fanning the background-work budget out to every sub-tree. Each
// sub-tree's Maintain is a no-op today, so this reports an empty report; it is wired so the per-shard
// maintenance the later milestones add reaches every shard.
func (s *Sharded) Maintain(ctx context.Context, budget engine.MaintBudget) (engine.MaintReport, error) {
	for _, sub := range s.subs {
		if _, err := sub.Maintain(ctx, budget); err != nil {
			return engine.MaintReport{}, err
		}
	}
	return engine.MaintReport{}, nil
}

// Stats implements engine.Engine with the whole core's footprint: the sum of the per-shard runs'
// physical bytes. Amplification is one, the same single-copy accounting each sub-tree reports.
func (s *Sharded) Stats() engine.EngineStats {
	var bytes int64
	for _, sub := range s.subs {
		bytes += sub.Stats().PhysicalBytes
	}
	return engine.EngineStats{PhysicalBytes: bytes, Amplification: 1}
}

// Reclaim implements engine.Engine. Nothing to reclaim in the sub-trees yet.
func (s *Sharded) Reclaim(budget int) (int, error) { return 0, nil }

// RecoverFinished implements engine.Engine, fanning the recovery-complete signal out to every sub-tree.
// Each rebuilds whatever in-memory index it needs from its replayed Apply calls (nothing today, since
// the sub-trees' state lives in pages), so this is a no-op fan-out wired for the milestones that add one.
func (s *Sharded) RecoverFinished(lastVersion uint64) error {
	for _, sub := range s.subs {
		if err := sub.RecoverFinished(lastVersion); err != nil {
			return err
		}
	}
	return nil
}

// NewReader implements engine.Engine, returning a read view across every shard at snap. It registers one
// epoch guard per sub-tree, held for the reader's life, so a page retired mid-read in any shard is not
// freed under the reader.
func (s *Sharded) NewReader(snap engine.Snapshot) (engine.Reader, error) {
	guards := make([]*guard, len(s.subs))
	for i := range guards {
		guards[i] = s.subs[i].recl.register()
	}
	return &shardedReader{s: s, snap: snap, guards: guards}, nil
}

// shardedReader is a read view across all shards at a fixed snapshot. It holds one epoch guard per
// sub-tree, registered for its life and pinned across each gather, so the registration cost is paid once
// per reader rather than once per Get.
type shardedReader struct {
	s      *Sharded
	snap   engine.Snapshot
	guards []*guard
}

// Get routes the point read to the one shard that owns userKey and resolves it there, the same
// single-key bounded gather the single-shard reader runs, on the routed sub-tree. Because every version
// of a key lives in one shard, no cross-shard work is needed: the owning sub-tree folds the whole
// version group, including any replicated range-delete marker that shadows the key.
func (r *shardedReader) Get(userKey []byte) ([]byte, error) {
	sh := r.s.part.route(userKey)
	upper := append(append([]byte(nil), userKey...), 0x00)
	view, err := r.s.subs[sh].snapshotRange(r.snap, r.guards[sh], userKey, upper)
	if err != nil {
		return nil, err
	}
	if len(view) > 0 && bytes.Equal(view[0].uk, userKey) {
		return append([]byte(nil), view[0].val...), nil
	}
	return nil, engine.ErrNotFound
}

// NewIter gathers each shard's resolved view of the range and merges them into one globally sorted view,
// which the existing index-walking cursor walks in either direction. The shard views are disjoint by
// user key, so the merge is a plain interleave (merge.go), and the reverse flag only flips the walk over
// the merged view. A prefix narrows the range the same way the single-shard reader does.
func (r *shardedReader) NewIter(opts engine.IterOptions) (engine.Cursor, error) {
	lower, upper := opts.Lower, opts.Upper
	if len(opts.Prefix) > 0 {
		lower = opts.Prefix
		upper = format.PrefixSuccessor(opts.Prefix)
	}
	views, err := r.gather(lower, upper)
	if err != nil {
		return nil, err
	}
	return newViewCursor(mergeShardViews(views), opts.Reverse), nil
}

// gather materializes every shard's resolved view of [lower, upper) at the reader's snapshot. The
// per-shard gathers are independent reads of disjoint sub-trees: each walks its own index and tail under
// its own already-registered epoch guard, and the only state they share is the pager, whose read path is
// built for concurrent access (the distributed read latch). So a wide scan, the OLAP path the sharding
// exists for, walks its N shard indexes in parallel across cores rather than one after another, and the
// merge still runs once over the materialized views. A reader gathering its own sub-trees concurrently
// races nothing two concurrent readers do not already race, which the optimistic read protocol handles.
//
// A single-shard core (the degenerate one-slot directory) stays inline: there is nothing to overlap, so
// it pays no goroutine and no WaitGroup, exactly the no-cost path the default core deserves. The errors
// land in a per-slot array so the goroutines never share an error word; the first non-nil is returned,
// and a failed gather drops its whole scan, matching the sequential path's fail-fast.
func (r *shardedReader) gather(lower, upper []byte) ([][]resolved, error) {
	n := len(r.s.subs)
	views := make([][]resolved, n)
	if n == 1 {
		v, err := r.s.subs[0].snapshotRange(r.snap, r.guards[0], lower, upper)
		if err != nil {
			return nil, err
		}
		views[0] = v
		return views, nil
	}

	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range r.s.subs {
		go func(i int) {
			defer wg.Done()
			views[i], errs[i] = r.s.subs[i].snapshotRange(r.snap, r.guards[i], lower, upper)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return views, nil
}

// Close unregisters every per-shard epoch guard.
func (r *shardedReader) Close() error {
	for i, g := range r.guards {
		r.s.subs[i].recl.unregister(g)
	}
	return nil
}

// ensure Sharded satisfies the engine contract at compile time, alongside the merge-resolver capability
// the conformance harness installs through.
var (
	_ engine.Engine    = (*Sharded)(nil)
	_ mergeSetterIface = (*Sharded)(nil)
)

// mergeSetterIface mirrors the unexported merge-setter capability the engine package's conformance
// harness type-asserts for, so a compile-time check here catches a signature drift before a test does.
type mergeSetterIface interface {
	SetMergeFunc(func(existing, operand []byte) []byte)
}
