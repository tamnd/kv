package db

import "sync/atomic"

// opCounters is the database's cumulative-since-open operation tally (spec 19 §1.1). Every
// field is an atomic counter bumped on the hot path with a single relaxed add, cheap enough
// that instrumenting every operation does not move the latency it measures. The commit path
// also accumulates the durable-commit latency so an operator can read the average fsync cost
// without a full histogram: total nanoseconds over the commit count is the mean.
//
// The counters live on the DB, not on a transaction, because they are a property of the whole
// database since it was opened, and a single long-lived process is where they are meaningful:
// a fresh open starts them at zero, which is why the CLI (one process per invocation) reads
// them low and the embedded or served process reads them accumulating.
type opCounters struct {
	get    atomic.Uint64
	set    atomic.Uint64
	delete atomic.Uint64
	merge  atomic.Uint64
	scan   atomic.Uint64

	// commits is the number of durable commits acknowledged; commitNanos is their summed
	// latency. A blind Write and a transaction commit both land here, since both pass through
	// the one durable-commit path.
	commits     atomic.Uint64
	commitNanos atomic.Uint64
}

// OpStats is the point-in-time read of the operation counters, folded into a Stats snapshot.
// It carries the per-op totals and the commit latency as a sum-and-count pair, the same shape
// a Prometheus summary exposes, so the renderer can publish an average without inventing
// buckets.
type OpStats struct {
	Gets        uint64
	Sets        uint64
	Deletes     uint64
	Merges      uint64
	Scans       uint64
	Commits     uint64
	CommitNanos uint64
}

// countWrite bumps the counter for a buffered write by its kind. It counts the operation
// the user issued and a transaction accepted, set/delete/merge as the user sees them, so a
// TTL set lands as a set and a range delete as a delete. Counting at the buffering point,
// not at apply, means an aborted transaction's buffered writes are still counted as work the
// process did; the commit counter, bumped only on a durable commit, is the one to read for
// successful writes.
func (c *opCounters) countWrite(kind opKind) {
	switch kind {
	case opSet, opSetTTL:
		c.set.Add(1)
	case opDelete, opRangeDelete:
		c.delete.Add(1)
	case opMerge:
		c.merge.Add(1)
	}
}

// snapshot reads every counter once. The reads are independent atomics, so the snapshot is
// not a single consistent instant across all fields, but each field is individually current
// and monotonic, which is all a metrics scrape needs.
func (c *opCounters) snapshot() OpStats {
	return OpStats{
		Gets:        c.get.Load(),
		Sets:        c.set.Load(),
		Deletes:     c.delete.Load(),
		Merges:      c.merge.Load(),
		Scans:       c.scan.Load(),
		Commits:     c.commits.Load(),
		CommitNanos: c.commitNanos.Load(),
	}
}
