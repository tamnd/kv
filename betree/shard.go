package betree

// This file is the third slice of milestone M7, the per-shard write machinery and the cross-shard
// commit (doc 05 section 6, decision D9). The previous slices built the two read-side halves of
// sharding: the partition function that routes a key to one shard, and the merge that reassembles a
// global scan order from the per-shard views. This slice builds the write side, which is where the
// sharding payoff actually lives: N independent shards, each its own lock domain, so two transactions
// on different shards commit in parallel sharing no mutex, and a transaction spanning shards commits
// atomically across them with a lightweight two-phase protocol.
//
// What the shard coordinator is, in this slice. It is the commit-visibility coordinator, built and
// proven in isolation against a serial oracle, the same alongside-then-flip discipline the M6 substrate
// followed. It holds N shard stores, routes a write to its shard by the partition function, stages a
// transaction's writes into the participating shards under their own locks, and publishes the whole
// transaction visible in one atomic step so a reader sees all of a cross-shard transaction or none of
// it. What it deliberately does not do yet is bind to the pager: a real sharded core needs N tree roots
// in one file (a shard directory the pager points at), which is a format change, so the durable home of
// the shards is the M8 flip's integration, exactly as the off-heap arena's wiring was deferred. The
// store here is an in-memory versioned record log per shard, which is enough to prove the commit
// coordination correct, the novel risk this milestone introduces, against a serial oracle and under the
// race detector.
//
// The two correctness properties the gate holds it to.
//
// Cross-shard atomicity. A transaction that writes keys in more than one shard cannot commit inside one
// shard's machinery; its writes land in several shards' record logs and have to become visible together
// or not at all. The coordinator gives every transaction a shared txnState carrying its commit version
// and a single committed flag. It stages the writes into every participating shard (under each shard's
// lock, the shards locked in ascending index order so two concurrent cross-shard commits cannot
// deadlock), and only then flips the one committed flag. Because every write of the transaction points
// at that one flag, the flip publishes them all at once: a reader either sees the flag false and none
// of the writes, or true and every write, never a torn middle. This is the two-phase commit doc 05
// describes, phase one stages in each shard, phase two publishes, with the publish reduced to a single
// atomic store because the in-memory store has no separate durability step to coordinate (the durable
// version arrives with the pager integration).
//
// Snapshot consistency. A reader has to see a consistent cut of the commit history, every transaction
// up to some point and no fragment beyond it. The coordinator maintains a read frontier: the largest
// version V such that every transaction with version <= V has committed. Versions come from one
// monotonic counter, so they are a total order, and the frontier advances contiguously, never stepping
// over a still-uncommitted version, so a reader snapshotting at the frontier sees exactly the
// transactions with version <= frontier, all of them whole (the frontier only passes a version once its
// transaction's committed flag is set, which is once all its shards are staged). The frontier is the
// one piece of global coordination left, and it is deliberately cheap: assigning a version is one
// atomic add and advancing the frontier is a short critical section over a small pending set, while the
// actual write work, staging records into the shards, runs under the per-shard locks in parallel. That
// is the honest shape of logical sharding, the per-shard parallelism on the write work with only the
// version assignment and the frontier advance coordinated, which is the decentralized-commit frontier
// from doc 05 section 3 reused here for the cross-shard cut.

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/tamnd/kv/format"
)

// shardWrite is one staged write in a transaction: a user key, its value, and whether it is a delete.
// A delete carries no value and resolves the key absent at and after its version.
type shardWrite struct {
	key []byte
	val []byte
	del bool
}

// txnState is the shared commit state of one transaction, pointed at by every record the transaction
// stages in every shard. version is the commit version assigned from the monotonic counter; committed
// is the single atomic flag whose flip publishes the whole transaction. Sharing one flag across all the
// transaction's records in all its shards is what makes the publish atomic: there is exactly one bit to
// flip and it governs every write at once.
type txnState struct {
	version   uint64
	committed atomic.Bool
}

// shardRecord is one versioned write resting in a shard's log. ik is the full internal key (user key
// plus inverted version plus kind) so resolution orders and folds exactly as the rest of the engine
// does; val is the value (nil for a delete); txn is the shared commit state the record's visibility
// gates on.
type shardRecord struct {
	ik  []byte
	val []byte
	txn *txnState
}

// shardStore is one shard: an independent lock domain over a versioned record log. mu guards recs; it
// is the shard's own lock, contended only by the committers and readers that touch this shard, which is
// the whole point of sharding, two shards' committers share no lock. The log is append-only within a
// session (version GC that compacts superseded records is the single-domain engine's concern and
// arrives for the sharded core with the pager integration); reads resolve the newest visible version
// per key over it.
type shardStore struct {
	mu   sync.RWMutex
	recs []shardRecord
}

// shardCoord is the cross-shard commit coordinator: the partition function, the N shard stores, the
// monotonic version counter, and the read frontier. It is the in-memory proof of the commit
// coordination, off the pager and off the live path, that the M8 flip wires onto a durable sharded
// layout.
type shardCoord struct {
	part   partitioner
	shards []*shardStore

	// version is the monotonic commit-version counter. Each commit takes the next value with one
	// atomic add, so versions are a total order with no lock.
	version atomic.Uint64

	// frontierMu guards the contiguous advance of the read frontier and its pending set. It is a short
	// critical section entered once per commit after the shard work is done, not held across the
	// staging, so it does not serialize the per-shard write work.
	frontierMu sync.Mutex
	frontier   atomic.Uint64
	// pending holds versions that have committed but cannot yet advance the frontier because an earlier
	// version is still in flight. The frontier drains them in order as the gap fills.
	pending map[uint64]bool
}

// newShardCoord builds a coordinator over the given partitioner, one shard store per partition.
func newShardCoord(part partitioner) *shardCoord {
	n := part.shards()
	shards := make([]*shardStore, n)
	for i := range shards {
		shards[i] = &shardStore{}
	}
	return &shardCoord{part: part, shards: shards, pending: make(map[uint64]bool)}
}

// shardCount reports the number of shards.
func (c *shardCoord) shardCount() int { return len(c.shards) }

// Commit stages the writes of one transaction into their shards and publishes them atomically,
// returning the assigned commit version. Writes are grouped by shard, the participating shards are
// locked in ascending index order (so two concurrent cross-shard commits acquire shared shards in the
// same order and cannot deadlock), every write is appended at the commit version under its shard's
// lock, and then the transaction's single committed flag is flipped, which publishes all the writes at
// once. After releasing the shard locks the commit advances the read frontier past its version once the
// version is contiguous. An empty batch is a no-op that still consumes a version, so the frontier stays
// dense.
func (c *shardCoord) Commit(writes []shardWrite) uint64 {
	v := c.version.Add(1)
	txn := &txnState{version: v}

	// Collapse to at most one write per key, last write winning, before staging. The txn layer above a
	// real engine already guarantees one write per key in a batch, and the coordinator mirrors that
	// precondition rather than trusting it: two writes to one key at the same commit version would
	// encode the identical internal key and leave resolution order ambiguous, so the later write has to
	// win deterministically, exactly as a serial apply of the batch would resolve it.
	seen := make(map[string]int, len(writes))
	collapsed := make([]shardWrite, 0, len(writes))
	for _, w := range writes {
		if idx, ok := seen[string(w.key)]; ok {
			collapsed[idx] = w
			continue
		}
		seen[string(w.key)] = len(collapsed)
		collapsed = append(collapsed, w)
	}

	// Group writes by shard and collect the participating shard indices in ascending order.
	byShard := make(map[int][]shardWrite)
	for _, w := range collapsed {
		s := c.part.route(w.key)
		byShard[s] = append(byShard[s], w)
	}
	parts := make([]int, 0, len(byShard))
	for s := range byShard {
		parts = append(parts, s)
	}
	sort.Ints(parts)

	// Phase one: lock the participating shards in ascending order and stage every write at version v.
	for _, s := range parts {
		c.shards[s].mu.Lock()
	}
	for _, s := range parts {
		st := c.shards[s]
		for _, w := range byShard[s] {
			kind := format.KindSet
			if w.del {
				kind = format.KindDelete
			}
			st.recs = append(st.recs, shardRecord{
				ik:  format.EncodeInternalKey(w.key, v, kind),
				val: append([]byte(nil), w.val...),
				txn: txn,
			})
		}
	}
	// Phase two: one atomic store publishes every staged write of this transaction at once.
	txn.committed.Store(true)
	// Release in descending order, the mirror of the acquire order.
	for i := len(parts) - 1; i >= 0; i-- {
		c.shards[parts[i]].mu.Unlock()
	}

	c.advanceFrontier(v)
	return v
}

// advanceFrontier records that version v has committed and advances the read frontier across every
// now-contiguous committed version. The frontier never steps over a version whose transaction has not
// committed, so a reader snapshotting at the frontier always sees a consistent cut.
func (c *shardCoord) advanceFrontier(v uint64) {
	c.frontierMu.Lock()
	defer c.frontierMu.Unlock()
	c.pending[v] = true
	for {
		next := c.frontier.Load() + 1
		if !c.pending[next] {
			break
		}
		delete(c.pending, next)
		c.frontier.Store(next)
	}
}

// ReadFrontier returns the current read frontier, the snapshot version a consistent read should use:
// every transaction with version at or below it has committed.
func (c *shardCoord) ReadFrontier() uint64 { return c.frontier.Load() }

// Read returns the MVCC-resolved view of [lower, upper) at the snapshot version, gathered across all
// shards and merged into one globally sorted order. Each shard resolves its own portion under its own
// read lock (so two shards' reads never contend), and the per-shard views are merged with the
// cross-shard heap merge from the previous slice. A nil bound is unbounded on that side.
func (c *shardCoord) Read(snapshot uint64, lower, upper []byte) []resolved {
	views := make([][]resolved, len(c.shards))
	for i, sh := range c.shards {
		views[i] = sh.resolve(snapshot, lower, upper)
	}
	return mergeShardViews(views)
}

// resolve returns this shard's resolved view of [lower, upper) at the snapshot: the newest committed
// version at or below the snapshot for each user key, deletes resolving the key absent. It copies the
// candidate records out under the read lock and resolves outside it, so a writer staging into the shard
// blocks a reader only for the brief copy, not the resolution.
func (s *shardStore) resolve(snapshot uint64, lower, upper []byte) []resolved {
	s.mu.RLock()
	cands := make([]shardRecord, 0, len(s.recs))
	for _, r := range s.recs {
		// Visible means the transaction is committed and its version is at or below the snapshot.
		if !r.txn.committed.Load() || r.txn.version > snapshot {
			continue
		}
		uk := format.UserKey(r.ik)
		if lower != nil && compareBytes(uk, lower) < 0 {
			continue
		}
		if upper != nil && compareBytes(uk, upper) >= 0 {
			continue
		}
		cands = append(cands, r)
	}
	s.mu.RUnlock()

	// Sort by user key ascending, then version descending, so the newest visible version of each key is
	// first in its group, the order the per-key fold expects.
	sort.Slice(cands, func(i, j int) bool {
		ci := format.CompareInternal(cands[i].ik, cands[j].ik)
		return ci < 0
	})

	out := make([]resolved, 0, len(cands))
	var i int
	for i < len(cands) {
		uk := format.UserKey(cands[i].ik)
		// The first record in the group is the newest version (internal-key order inverts the version),
		// so it wins. A delete wins the key absent; a set emits the value.
		winner := cands[i]
		// Skip the rest of this key's versions.
		j := i + 1
		for j < len(cands) && bytesEqual(format.UserKey(cands[j].ik), uk) {
			j++
		}
		i = j
		if format.KindOf(winner.ik) == format.KindDelete {
			continue
		}
		out = append(out, resolved{
			uk:  append([]byte(nil), uk...),
			val: append([]byte(nil), winner.val...),
		})
	}
	return out
}
