package db

import (
	"context"
	"time"

	"github.com/tamnd/kv/engine"
)

// commitReq is one buffered commit waiting to join a group. prepare runs under the
// leader's write lock (d.mu): it assigns the commit version, runs conflict detection for
// a transaction, and builds the engine batch at that version. It reports skip=true for a
// no-op (an empty blind write, which consumes no version) and an error for a lost conflict
// or a read-only replica. The leader logs every prepared batch, issues one shared fsync for
// the whole group, then applies them in version order.
type commitReq struct {
	prepare func() (b *engine.WriteBatch, v uint64, skip bool, err error)

	// Set by the leader while it processes the group. b/v/commitLSN describe an appended
	// batch waiting to be applied; appended marks that its WAL frames are on disk.
	b         *engine.WriteBatch
	v         uint64
	commitLSN uint64
	appended  bool

	// The outcome the submitter returns. ready is set once the leader has finished with
	// this request, success or failure.
	result uint64
	err    error
	ready  bool
}

// submitCommit enqueues req and drives it to completion through group commit. The first
// submitter to find no leader becomes the leader and processes the whole queued group; the
// rest wait on ccond until their request is marked ready. Every submitter blocks until its
// own request is done, so the call is synchronous from the caller's view even though the
// durable fsync was shared with the rest of the group.
func (d *DB) submitCommit(req *commitReq) (uint64, error) {
	d.cmu.Lock()
	d.cqueue = append(d.cqueue, req)
	for {
		if req.ready {
			r, e := req.result, req.err
			d.cmu.Unlock()
			return r, e
		}
		if d.cleader {
			// Another goroutine is leading a group; wait for it to finish, which either
			// completes our request (it was in that group) or frees the leadership for us.
			d.ccond.Wait()
			continue
		}
		// Become the leader. Release cmu immediately so late arrivals can enqueue
		// during the linger window before we drain and process the batch.
		d.cleader = true
		d.cmu.Unlock()

		if us := d.lingerUs.Load(); us > 0 {
			time.Sleep(time.Duration(us) * time.Microsecond)
		}

		// Drain everything that queued (original submitters + linger-window arrivals).
		d.cmu.Lock()
		group := d.cqueue
		d.cqueue = nil
		d.cmu.Unlock()

		d.rl.Lock()
		d.processGroup(group)
		d.rl.Unlock()

		d.cmu.Lock()
		// Publish every outcome under cmu. processGroup filled each request's result/err off
		// the lock; setting ready here, under the same mutex the waiters poll it with, gives
		// the happens-before that makes those off-lock writes safe to read (spec 06 F3).
		for _, r := range group {
			r.ready = true
		}
		d.cleader = false
		// Wake every waiter: those in the group we just finished return their result, and
		// one of the rest takes leadership for the batch that queued while we ran.
		d.ccond.Broadcast()
	}
}

// processGroup logs, durably commits, and applies a queued group of commits as one unit.
// The caller holds d.mu. It runs the three phases of group commit: order-and-append (assign
// versions and write every batch's WAL frames, no fsync), one shared fsync, then apply in
// version order. A WAL append or fsync failure is a fatal durability fault that fences the
// database and fails every appended request in the group (spec 06 F3, spec 07 §6).
func (d *DB) processGroup(group []*commitReq) {
	if d.fatal != nil {
		for _, r := range group {
			r.fail(d.fatal)
		}
		return
	}

	// Phases 1 and 2 (append every batch's frames, then one shared fsync) run under the
	// WAL append lock so they cannot interleave with a background checkpoint's
	// full_page_writes page-image logging, which appends to the same single-writer WAL
	// tail off d.mu since slice 95 moved page writeback into the pager. The lock spans the
	// whole append-plus-sync run, not each frame, so a checkpoint sees either none of a
	// group's frames or all of them, never a torn interleave. fenceGroup is terminal but
	// still returns through the deferred unlock so a racing checkpoint never deadlocks.
	durableStart := time.Now()
	appended, ok := d.appendGroupLocked(group)
	if !ok {
		return
	}
	durable := time.Since(durableStart)

	// Phase 3: apply each durable batch to the engine, then make it visible. The shared fsync
	// above covers every batch, so a reader that sees the newest applied version sees only
	// durable state. An engine that can apply a whole group at once (the LSM core) spreads the
	// inserts across cores; the bookkeeping that makes each batch visible still runs serially in
	// version order. An engine without that capability (the B-tree core) takes one Apply per
	// batch.
	if ga, ok := d.eng.(engine.GroupApplier); ok && len(appended) > 0 {
		d.applyGroupDurable(ga, appended, durable)
	} else {
		for _, r := range appended {
			d.applyDurable(r, durable)
		}
	}
	d.maybeCheckpoint()
}

// appendGroupLocked runs phases 1 and 2 of group commit under the WAL append lock: it assigns
// a version to each request, appends its batch and commit frames, and issues the one shared
// fsync that makes the whole run durable. It returns the requests that reached the log and
// ok=true on success. Conflicts and empty writes resolve here (fail/succeed) and never reach
// the log. A WAL append or fsync error fences the database and returns ok=false; the caller
// stops, since every appended request has already been failed. The append lock is released by
// defer on every path, so a checkpoint waiting to log page images never blocks on a fenced
// group.
func (d *DB) appendGroupLocked(group []*commitReq) (appended []*commitReq, ok bool) {
	d.wal.AppendLock()
	defer d.wal.AppendUnlock()

	appended = make([]*commitReq, 0, len(group))
	for i, r := range group {
		b, v, skip, err := r.prepare()
		if err != nil {
			r.fail(err)
			continue
		}
		if skip {
			r.succeed(v)
			continue
		}
		// Serialize the batch straight into the WAL frame buffer rather than into a
		// throwaway buffer that LogBatch would copy in again (perf/02 Finding 4).
		if err := d.wal.LogBatchAppend(v, b.AppendEncoded); err != nil {
			d.fenceGroup(append(appended, group[i:]...), err)
			return appended, false
		}
		commitLSN, err := d.wal.AppendCommit(v)
		if err != nil {
			d.fenceGroup(append(appended, group[i:]...), err)
			return appended, false
		}
		r.b, r.v, r.commitLSN, r.appended = b, v, commitLSN, true
		appended = append(appended, r)
	}

	// Phase 2: one fsync makes every appended batch durable. At SyncOff/SyncNormal this is
	// a no-op and durability is finalized at the next checkpoint.
	if err := d.wal.Sync(); err != nil {
		d.fenceGroup(appended, err)
		return appended, false
	}
	return appended, true
}

// applyGroupDurable applies an already-durable group to a group-capable engine in one parallel
// call, then publishes each batch's version in order. The engine insert is the parallel part;
// the visibility bookkeeping (counters, header, subscriptions, the oracle's applied mark, the
// caller's result) stays serial and in version order so a reader never sees version N+1 before N.
// The group's apply latency is the shared engine cost, attributed to each batch the way the shared
// fsync latency already is. An engine apply failure fails the whole group: on the LSM core it is a
// sticky flush fault that would fail every batch in turn anyway.
func (d *DB) applyGroupDurable(ga engine.GroupApplier, appended []*commitReq, durable time.Duration) {
	batches := make([]*engine.WriteBatch, len(appended))
	versions := make([]uint64, len(appended))
	var maxLSN uint64
	for i, r := range appended {
		batches[i], versions[i] = r.b, r.v
		if r.commitLSN > maxLSN {
			maxLSN = r.commitLSN
		}
	}
	// Fold the group's largest commit LSN into the engine's durable mark once, the group-apply
	// analog of the per-batch noteLSN the serial path makes before each Apply.
	d.noteLSN(maxLSN)

	applyStart := time.Now()
	if err := ga.ApplyGroup(batches, versions); err != nil {
		for _, r := range appended {
			r.fail(err)
		}
		return
	}
	apply := time.Since(applyStart)
	for _, r := range appended {
		d.publishApplied(r, durable, apply)
	}
}

// applyDurable applies one already-durable batch to the engine and makes its version
// visible, the apply phase of group commit. The caller holds d.mu and calls it in version
// order. durable is the group's shared fsync latency, folded into the commit-latency metric
// and the per-batch tracing span so the I/O cost stays attributed even though it was shared.
func (d *DB) applyDurable(r *commitReq, durable time.Duration) {
	// Per-commit spans: the durable span carries the (shared) fsync cost, the apply span the
	// engine cost, the same I/O-versus-engine split the slow-op log reports (spec 19 §3).
	ctx, commitSpan := d.startSpan(context.Background(), "kv.commit")
	_, durableSpan := d.startSpan(ctx, "kv.commit.durable")
	endSpan(durableSpan)

	d.counters.commits.Add(1)
	d.counters.commitNanos.Add(uint64(durable))
	d.noteLSN(r.commitLSN)

	applyStart := time.Now()
	_, applySpan := d.startSpan(ctx, "kv.commit.apply")
	if err := d.eng.Apply(r.b, r.v); err != nil {
		endSpan(applySpan)
		endSpan(commitSpan)
		r.fail(err)
		return
	}
	endSpan(applySpan)
	endSpan(commitSpan)

	if d.slowOpEnabled() {
		apply := time.Since(applyStart)
		if total := durable + apply; total >= d.slowOp {
			d.logSlowCommit(r.b, r.v, durable, apply, total)
		}
	}
	d.publish(r.b, r.v)
	d.orc.applied(r.v)
	r.succeed(r.v)
}

// publishApplied makes a batch visible after the engine has already applied it as part of a
// parallel group apply: it records the commit, spans, and slow-op log, fans the batch to
// subscribers, marks its version applied in the oracle, and returns the result. The caller runs
// it serially in version order so the oracle's applied mark and the subscription feed advance
// monotonically even though the inserts ran out of order.
// durable is the group's shared fsync latency and apply is the group's shared engine latency,
// both attributed to each batch since both costs were genuinely shared across the group.
func (d *DB) publishApplied(r *commitReq, durable, apply time.Duration) {
	ctx, commitSpan := d.startSpan(context.Background(), "kv.commit")
	_, durableSpan := d.startSpan(ctx, "kv.commit.durable")
	endSpan(durableSpan)
	_, applySpan := d.startSpan(ctx, "kv.commit.apply")
	endSpan(applySpan)
	endSpan(commitSpan)

	d.counters.commits.Add(1)
	d.counters.commitNanos.Add(uint64(durable))
	// The durable mark was already folded once for the whole group, with its largest commit
	// LSN, before the parallel apply ran, so there is no per-batch noteLSN here.

	if d.slowOpEnabled() {
		if total := durable + apply; total >= d.slowOp {
			d.logSlowCommit(r.b, r.v, durable, apply, total)
		}
	}
	// NOTE: header.LastCommitVersion is NOT updated here. It is set once at checkpoint time
	// by pgr.Checkpoint (under the pager's own locks), which captures the oracle's committed
	// version via prepareCheckpointLocked (under d.mu). Updating it here would race with the
	// lock-free checkpoint I/O path (perf/02 F5).
	d.publish(r.b, r.v)
	d.orc.applied(r.v)
	r.succeed(r.v)
}

// fenceGroup records a fatal durability fault and fails every request that had reached the
// log in this group. Apply never ran for them, so the engine is untouched and recovery
// rebuilds exactly the durable state from the WAL; the database stays fenced until reopen.
func (d *DB) fenceGroup(reqs []*commitReq, cause error) {
	d.fatal = wrapFatalSync(cause)
	d.logFatal(d.fatal)
	for _, r := range reqs {
		r.fail(d.fatal)
	}
}

// succeed and fail record a request's outcome. They run inside processGroup with cmu
// released; submitCommit publishes the ready flag for the whole group under cmu once
// processing returns, so these writes are read only after that lock-mediated barrier.
func (r *commitReq) succeed(v uint64) { r.result = v }
func (r *commitReq) fail(err error)   { r.err = err }
