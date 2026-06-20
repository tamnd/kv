package db

import "sync"

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

	// readers counts live read snapshots by version. The minimum live version is
	// the readMark: no reader can ever again observe anything older, so older
	// versions and tombstones are reclaimable (spec 10 §6) and stale commit records
	// are trimmable.
	readers map[uint64]int

	// commits holds recent write sets in ascending version order, for write-write
	// conflict detection (spec 10 §3). A record is trimmed once its version is at or
	// below the readMark, because no live or future transaction reads from before
	// the readMark, so none can still conflict with it.
	commits []commitRecord
}

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
	return &oracle{
		nextVersion:    lastCommitted + 1,
		appliedVersion: lastCommitted,
		readers:        make(map[uint64]int),
	}
}

// readTs registers a new read snapshot at the latest applied version and returns
// it. The caller must pair it with doneRead (via Txn.Discard) so the version stops
// pinning the watermark.
func (o *oracle) readTs() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	v := o.appliedVersion
	o.readers[v]++
	return v
}

// doneRead releases a read snapshot taken by readTs.
func (o *oracle) doneRead(v uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.readers[v] <= 1 {
		delete(o.readers, v)
	} else {
		o.readers[v]--
	}
}

// lastCommitted reports the newest applied version, the default read snapshot.
func (o *oracle) lastCommitted() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.appliedVersion
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

// recordCommitLocked assigns the next version, stores the write set for future
// conflict checks, trims stale records, and returns the version. The caller holds
// o.mu.
func (o *oracle) recordCommitLocked(writes []string) uint64 {
	v := o.nextVersion
	o.nextVersion++
	set := make(map[string]struct{}, len(writes))
	for _, k := range writes {
		set[k] = struct{}{}
	}
	o.commits = append(o.commits, commitRecord{version: v, writes: set})
	o.trimLocked()
	return v
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

// applied marks a commit version as durable and visible, advancing the snapshot
// fresh readers receive. The single writer calls it after engine.Apply, so commits
// become visible in version order.
func (o *oracle) applied(v uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if v > o.appliedVersion {
		o.appliedVersion = v
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
		}
	}
	o.commits = keep
}
