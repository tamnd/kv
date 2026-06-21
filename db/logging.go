package db

import (
	"bytes"
	"context"
	"log/slog"
	"time"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// This file is the database's structured-logging surface (spec 19 §3): a handful of
// slog events at the moments an operator cares about (lifecycle, recovery, checkpoint,
// maintenance, the fatal durability fault) plus a slow-op log that names any commit or
// read that ran past a configured threshold. Logging is off unless Options.Logger is
// set, so the default build emits nothing and pays nothing; every emitter below is a
// no-op when the logger is nil. The events live here, at the db layer, rather than
// inside the engine packages, so the lsm and btree cores stay free of a logging
// dependency and the host owns the one timing window it already measures for metrics.

// slowOpEnabled reports whether the slow-op log is armed: a logger is set and a positive
// threshold is configured. The read path checks this before reading the clock, so a
// database with no logger or a zero threshold times nothing.
func (d *DB) slowOpEnabled() bool { return d.logger != nil && d.slowOp > 0 }

// logOpened records a database open at INFO with the engine, the path, and the commit
// version the file came back at, the one line that says "the database is up and at
// version N". It is emitted by both the create and open-existing paths.
func (d *DB) logOpened(version uint64) {
	if d.logger == nil {
		return
	}
	d.logger.Info("kv: database opened",
		"path", d.path,
		"engine", d.pgr.Header().Engine.String(),
		"version", version,
	)
}

// logClosed records a clean database close at INFO, the bookend to logOpened. It is
// emitted once, from inside Close's closeOnce, so a double Close logs a single event.
func (d *DB) logClosed() {
	if d.logger == nil {
		return
	}
	d.logger.Info("kv: database closed", "path", d.path)
}

// logRecovery records what crash recovery replayed and what it discarded (spec 08
// §2-3): the count of committed batches redone past the last checkpoint, the highest
// version reached, and whether a torn or stale tail was dropped. A clean log that
// replayed nothing logs nothing, so the event appears only when recovery actually did
// work; a torn tail raises it to WARN because a discarded tail, while expected after a
// crash mid-commit, is the kind of thing an operator wants to see in the log.
func (d *DB) logRecovery(replayed int, highestVersion uint64, tornTail bool) {
	if d.logger == nil || (replayed == 0 && !tornTail) {
		return
	}
	level := slog.LevelInfo
	if tornTail {
		level = slog.LevelWarn
	}
	d.logger.Log(context.Background(), level, "kv: wal recovery",
		"replayed_batches", replayed,
		"highest_version", highestVersion,
		"discarded_torn_tail", tornTail,
	)
}

// logCheckpoint records a completed checkpoint fold at DEBUG: the LSN folded into the
// main file, the durable commit version stamped into the header, and whether the WAL was
// reset or kept (kept when an LSM memtable still lags the fold, spec 06 §4). It is DEBUG
// because a busy database checkpoints often and the event is routine; an operator turns
// it up when chasing a checkpoint-backlog question.
func (d *DB) logCheckpoint(foldedLSN, version uint64, walReset bool) {
	if d.logger == nil {
		return
	}
	d.logger.Debug("kv: checkpoint folded wal",
		"folded_lsn", foldedLSN,
		"version", version,
		"wal_reset", walReset,
	)
}

// logMaintain records a maintenance round that did work at DEBUG: pages compacted, bytes
// written and reclaimed, expired entries swept, and whether more work remains. A round
// that touched nothing logs nothing, so the event marks real compaction and GC activity
// rather than every idle poll.
func (d *DB) logMaintain(rep engine.MaintReport) {
	if d.logger == nil {
		return
	}
	if rep.PagesCompacted == 0 && rep.BytesReclaimed == 0 && rep.ExpiredSwept == 0 {
		return
	}
	d.logger.Debug("kv: maintenance ran",
		"pages_compacted", rep.PagesCompacted,
		"bytes_written", rep.BytesWritten,
		"bytes_reclaimed", rep.BytesReclaimed,
		"expired_swept", rep.ExpiredSwept,
		"more", rep.More,
	)
}

// logFatal records the fatal durability fault loudly at ERROR (fsyncgate, spec 07 §6):
// a failed log append or commit sync that fenced the database. This is the one event
// that always wants to be seen, so it is ERROR and carries the wrapped I/O cause.
func (d *DB) logFatal(err error) {
	if d.logger == nil {
		return
	}
	d.logger.Error("kv: fatal durability fault; database fenced until reopen", "error", err)
}

// logSlowCommit records a durable commit that ran past the slow-op threshold at WARN,
// with a phase split (durable log+fsync versus engine apply), the commit version, the
// number of keys, and the key range it touched. The phase split tells an operator
// whether the time went to the disk (fsync) or to the engine (apply), the first question
// a slow commit raises. Bounds are the user keys, lo and hi inclusive.
func (d *DB) logSlowCommit(b *engine.WriteBatch, version uint64, durable, apply, total time.Duration) {
	if d.logger == nil {
		return
	}
	lo, hi, n := batchKeyRange(b)
	d.logger.Warn("kv: slow commit",
		"version", version,
		"keys", n,
		"key_lo", string(lo),
		"key_hi", string(hi),
		"total", total,
		"durable", durable,
		"apply", apply,
	)
}

// logSlowRead records a point read that ran past the slow-op threshold at WARN, with the
// operation name and the key. A slow read is almost always a cold cache or a deep LSM
// probe, and the key is what an operator needs to reproduce it.
func (d *DB) logSlowRead(op string, key []byte, took time.Duration) {
	if d.logger == nil || d.slowOp <= 0 || took < d.slowOp {
		return
	}
	d.logger.Warn("kv: slow read", "op", op, "key", string(key), "took", took)
}

// batchKeyRange returns the lowest and highest user keys a batch wrote and how many
// distinct internal entries it carries, the span the slow-commit log reports. It scans
// the entries once rather than building the full key set, since only the extremes are
// needed.
func batchKeyRange(b *engine.WriteBatch) (lo, hi []byte, n int) {
	for _, e := range b.Entries() {
		k := format.UserKey(e.InternalKey)
		n++
		if lo == nil || bytes.Compare(k, lo) < 0 {
			lo = k
		}
		if hi == nil || bytes.Compare(k, hi) > 0 {
			hi = k
		}
	}
	return lo, hi, n
}
