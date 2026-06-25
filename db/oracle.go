package db

import (
	"sync"
	"sync/atomic"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// keyRange is a half-open scan predicate [lo, hi) a serializable transaction read, in
// user-key order. A nil bound is open: nil lo is unbounded below, nil hi unbounded
// above. The oracle tests a concurrent write key against it for rw-antidependencies.
type keyRange struct {
	lo, hi []byte
}

// covers reports whether key falls in [lo, hi).
func (r keyRange) covers(key []byte) bool {
	if r.lo != nil && format.CompareUser(key, r.lo) < 0 {
		return false
	}
	if r.hi != nil && format.CompareUser(key, r.hi) >= 0 {
		return false
	}
	return true
}

// oracle assigns commit versions and tracks which read snapshots are still live,
// the Badger-style watermark oracle of spec 10 §2. It is the one place version
// state lives once transactions exist: the monotonic commit-version source, the
// set of in-flight readers (whose minimum is the GC watermark), and the recent
// write sets used for snapshot-isolation conflict detection.
//
// It is not on the data path. It is consulted at transaction begin (readTs) and
// commit (newCommitTs/applied), not per key, so its single mutex is never a read
// or scan bottleneck.
type oracle struct {
	mu sync.Mutex

	// nextVersion is the next commit version to hand out. appliedVersion is the
	// highest version whose Apply has completed and is durable -- the snapshot a
	// fresh reader sees. They differ only briefly, while a just-assigned commit is
	// being logged and applied; keeping readTs on appliedVersion is what stops a
	// reader from observing a version before its page changes land (spec 10 §2).
	nextVersion    uint64
	appliedVersion uint64

	// Padding separates appliedPub onto its own cache line. nextVersion and
	// appliedVersion are written under mu by the group-commit leader; appliedPub
	// is read lock-free by every non-transactional Get. Without the pad, every
	// reader load triggers a cache-line bounce from the committing core.
	_ [64]byte

	// appliedPub mirrors appliedVersion for the lock-free read path. It is published
	// (stored) under mu every time appliedVersion advances, and read with a plain atomic
	// load by lastCommitted, so a non-transactional Get takes its read snapshot without
	// touching the oracle mutex (spec 07 §MVCC: non-transactional reads skip the lock and
	// the read-set registry entirely). It only ever moves forward, in lockstep with
	// appliedVersion, so a lock-free reader sees a monotonic, already-durable version.
	appliedPub atomic.Uint64

	// readers counts live read snapshots by version. The minimum live version is
	// the readMark: no reader can ever again observe anything older, so older
	// versions and tombstones are reclaimable (spec 10 §6) and stale commit records
	// are trimmable.
	readers map[uint64]int

	// readerSince stamps the wall-clock time, in Unix nanoseconds, a version cohort
	// first became live: set when readers[v] goes from absent to present, cleared when
	// it empties. The oldest stamp over the live cohorts is the age of the
	// longest-held snapshot, the leaked-reader signal of spec 19 §1.6. It tracks the
	// cohort, not the individual reader, so continuous read traffic that keeps churning
	// the newest version keeps its stamps fresh while a genuinely abandoned old snapshot
	// keeps a stamp that only ages, which is the bias a leak detector wants.
	readerSince map[uint64]uint64

	// commits holds recent write sets in ascending version order, for write-write
	// conflict detection (spec 10 §3). A record is trimmed once its version is at or
	// below the readMark, because no live or future transaction reads from before
	// the readMark, so none can still conflict with it.
	commits []commitRecord

	// freeSets recycles the conflict-tracking maps that trimLocked retires. A commit
	// allocates one map for its write set; when no live reader pins an older version the
	// record is trimmed on the very next commit, so without recycling every commit churns a
	// map. Returning the trimmed map (cleared, buckets intact) here and reusing it on the next
	// commit turns that per-commit allocation into a steady-state reuse (spec 07, commit-side
	// cost). The pool is bounded so a burst of commits under a held-open reader cannot pin an
	// unbounded number of empty maps. Touched only under mu, by takeWriteSet/retireWriteSet.
	freeSets []map[string]struct{}
}

// maxFreeSets bounds the recycled-map pool. A handful covers the steady state (add then trim
// one record per commit); beyond that the maps are let go so a long-held reader, which keeps
// many records live and untrimmed, does not also hoard empty maps.
const maxFreeSets = 8

// commitRecord is the set of user keys a committed transaction wrote, tagged with
// its commit version. Conflict detection intersects a committing transaction's
// write set against the records committed since its read snapshot.
type commitRecord struct {
	version uint64
	writes  map[string]struct{}
}

// newOracle starts the oracle past the last durable commit: a fresh write gets
// lastCommitted+1, and a fresh reader sees lastCommitted (spec 10 §1).
func newOracle(lastCommitted uint64) *oracle {
	o := &oracle{
		nextVersion:    lastCommitted + 1,
		appliedVersion: lastCommitted,
		readers:        make(map[uint64]int),
		readerSince:    make(map[uint64]uint64),
	}
	o.appliedPub.Store(lastCommitted)
	return o
}

// readTs registers a new read snapshot at the latest applied version and returns
// it. nowNanos is the wall-clock registration time, used only to stamp a freshly
// opened version cohort for the oldest-snapshot-age metric. The caller must pair it
// with doneRead (via Txn.Discard) so the version stops pinning the watermark.
func (o *oracle) readTs(nowNanos uint64) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	v := o.appliedVersion
	if o.readers[v] == 0 {
		o.readerSince[v] = nowNanos
	}
	o.readers[v]++
	return v
}

// doneRead releases a read snapshot taken by readTs.
func (o *oracle) doneRead(v uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.readers[v] <= 1 {
		delete(o.readers, v)
		delete(o.readerSince, v)
	} else {
		o.readers[v]--
	}
}

// oldestReaderSince reports the Unix-nanosecond stamp of the longest-held live read
// snapshot, or 0 when no reader is live. The caller turns it into an age against the
// current clock; a value that only grows is a reader that was never discarded
// (spec 19 §1.6).
func (o *oracle) oldestReaderSince() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	var oldest uint64
	first := true
	for _, since := range o.readerSince {
		if first || since < oldest {
			oldest, first = since, false
		}
	}
	return oldest
}

// lastCommitted reports the newest applied version, the default read snapshot. It is
// the lock-free read fast path: it loads the published applied version with a single
// atomic load and never takes the oracle mutex, so concurrent non-transactional reads do
// not serialize on it (spec 07 §MVCC). The value is monotonic and only ever names an
// already-durable version, since appliedPub is stored under mu in lockstep with
// appliedVersion right after engine.Apply.
func (o *oracle) lastCommitted() uint64 {
	return o.appliedPub.Load()
}

// peekNext reports the version the next commit will receive without reserving it.
// The single committing writer holds db.mu across the peek and the commit, so the
// value is stable in between (it is what lets a blind Write build its batch at the
// version before formally reserving it).
func (o *oracle) peekNext() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.nextVersion
}

// commit reserves the next version for a blind batch (a Write with no read
// snapshot) and records its write set, skipping conflict detection. It still
// records the write set so a concurrent transaction that read before this commit
// conflicts against it.
func (o *oracle) commit(writes []string) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.recordCommitLocked(writes)
}

// commitWriteSet is commit for a blind batch that records its write set straight from
// the batch entries instead of a pre-deduped []string. It builds the (pooled) write-set
// map by inserting each entry's user key, which dedups inherently, so the recorded set
// has the same membership a deduped slice would and the same conflict semantics, without
// the throwaway slice and its quadratic dedup scan the slice form paid on every commit.
// Like commit it runs no conflict detection (a blind Write has no read snapshot) but
// still records the set so a concurrent transaction that read before this commit
// conflicts against it.
func (o *oracle) commitWriteSet(entries []engine.BatchEntry) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	v := o.nextVersion
	o.nextVersion++
	set := o.takeWriteSet(len(entries))
	for i := range entries {
		set[string(format.UserKey(entries[i].InternalKey))] = struct{}{}
	}
	o.commits = append(o.commits, commitRecord{version: v, writes: set})
	o.trimLocked()
	return v
}

// recordCommitLocked assigns the next version, stores the write set for future
// conflict checks, trims stale records, and returns the version. The caller holds
// o.mu.
func (o *oracle) recordCommitLocked(writes []string) uint64 {
	v := o.nextVersion
	o.nextVersion++
	set := o.takeWriteSet(len(writes))
	for _, k := range writes {
		set[k] = struct{}{}
	}
	o.commits = append(o.commits, commitRecord{version: v, writes: set})
	o.trimLocked()
	return v
}

// takeWriteSet returns an empty map for a new commit record's write set, reusing one the pool
// holds when available and allocating a hint-sized map otherwise. A pooled map was cleared on
// retirement, so it is ready to fill. The caller holds o.mu.
func (o *oracle) takeWriteSet(hint int) map[string]struct{} {
	if n := len(o.freeSets); n > 0 {
		m := o.freeSets[n-1]
		o.freeSets[n-1] = nil
		o.freeSets = o.freeSets[:n-1]
		return m
	}
	return make(map[string]struct{}, hint)
}

// retireWriteSet clears a trimmed record's write set and returns it to the recycle pool, up to
// the pool bound. clear empties the map but keeps its allocated buckets, which is exactly the
// reuse the next takeWriteSet exploits. The caller holds o.mu, and the map is internal to the
// oracle (no commitRecord escapes), so the recycled map can never alias a live reference.
func (o *oracle) retireWriteSet(m map[string]struct{}) {
	if len(o.freeSets) >= maxFreeSets {
		return
	}
	clear(m)
	o.freeSets = append(o.freeSets, m)
}

// newCommitTs runs write-write conflict detection for a transaction that read at
// readVersion and wrote the keys in writes, and on success assigns and returns its
// commit version. It reports ok=false if any of those keys was committed by another
// transaction after readVersion (first-committer-wins, spec 10 §3); the caller must
// not proceed to log or apply in that case.
//
// The commit version is assigned here, under the same lock that checks conflicts,
// so the version order is the conflict-serialization order. The single committing
// writer (spec 10 §5.1) then logs and applies in that order and calls applied.
func (o *oracle) newCommitTs(readVersion uint64, writes []string) (uint64, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for i := range o.commits {
		c := &o.commits[i]
		if c.version <= readVersion {
			continue
		}
		for _, k := range writes {
			if _, hit := c.writes[k]; hit {
				return 0, false
			}
		}
	}
	return o.recordCommitLocked(writes), true
}

// newCommitTsSerializable is the serializable-isolation commit path (spec 10 §4). It
// runs the same first-committer-wins write-write check as newCommitTs, and in addition
// validates the transaction's read set: if any transaction that committed after
// readVersion wrote a key the committing transaction read (as a point read or inside a
// scanned range), that is a read-write antidependency that can produce a
// non-serializable schedule, so the commit aborts. With the single committing writer
// (spec 10 §5.1) this read validation makes the commit-version order a serializable
// order: every committed transaction's reads are still valid as of its commit, so it
// could have run entirely at that point. It closes write skew and every other SI
// anomaly, conservatively (it may abort some schedules a precise pivot detector would
// allow, the standard optimistic-validation trade).
func (o *oracle) newCommitTsSerializable(readVersion uint64, writes, reads []string, ranges []keyRange) (uint64, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	readSet := make(map[string]struct{}, len(reads))
	for _, k := range reads {
		readSet[k] = struct{}{}
	}
	for i := range o.commits {
		c := &o.commits[i]
		if c.version <= readVersion {
			continue
		}
		// Write-write: a key we are writing was written since our snapshot.
		for _, k := range writes {
			if _, hit := c.writes[k]; hit {
				return 0, false
			}
		}
		// Read-write antidependency: a key we read was written since our snapshot.
		for wk := range c.writes {
			if _, hit := readSet[wk]; hit {
				return 0, false
			}
			for _, rg := range ranges {
				if rg.covers([]byte(wk)) {
					return 0, false
				}
			}
		}
	}
	return o.recordCommitLocked(writes), true
}

// applied marks a commit version as durable and visible, advancing the snapshot
// fresh readers receive. The single writer calls it after engine.Apply, so commits
// become visible in version order.
func (o *oracle) applied(v uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if v > o.appliedVersion {
		o.appliedVersion = v
		o.appliedPub.Store(v)
	}
}

// advanceTo forces the version counters up to an externally assigned commit version,
// the replica-apply path of spec 18 §4. A follower does not allocate its own versions;
// it replays versions a primary already assigned, so applying a shipped batch at version
// v moves nextVersion past it and marks it applied and visible. It never moves a counter
// backward, so re-applying an already-seen ship is a no-op. The caller holds the database
// write lock, so no local committer races this.
func (o *oracle) advanceTo(v uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if v >= o.nextVersion {
		o.nextVersion = v + 1
	}
	if v > o.appliedVersion {
		o.appliedVersion = v
		o.appliedPub.Store(v)
	}
}

// readMark is the version-GC horizon: the oldest version any live reader can still
// observe, or the newest applied version when none is live. The maintenance driver
// passes it to the engine so GC never reclaims a version a live snapshot needs
// (spec 10 §6).
func (o *oracle) readMark() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.readMarkLocked()
}

// readMarkLocked is the oldest version any live reader can still observe, or the
// newest applied version when no reader is live. Older versions and commit records
// can never again matter to anyone.
func (o *oracle) readMarkLocked() uint64 {
	mark := o.appliedVersion
	first := true
	for v := range o.readers {
		if first || v < mark {
			mark, first = v, false
		}
	}
	return mark
}

// trimLocked drops commit records at or below the readMark; no transaction reads
// from before it, so they can no longer cause a conflict.
func (o *oracle) trimLocked() {
	mark := o.readMarkLocked()
	keep := o.commits[:0]
	for _, c := range o.commits {
		if c.version > mark {
			keep = append(keep, c)
		} else {
			o.retireWriteSet(c.writes)
		}
	}
	o.commits = keep
}
