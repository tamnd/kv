package db

import (
	"errors"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// ErrConflict is returned by a write transaction whose commit lost a write-write
// race: a key it wrote was committed by another transaction after its read snapshot
// (first-committer-wins, spec 10 §3). Update retries on it automatically; explicit
// callers handle it themselves. The public kv package re-exports it (spec 15 §2.2).
var ErrConflict = errors.New("kv: transaction conflict")

// ErrReadOnlyTxn is returned when a mutation is attempted on a read-only (View)
// transaction.
var ErrReadOnlyTxn = errors.New("kv: write on a read-only transaction")

// ErrTxnDone is returned when a transaction is used after Commit or Discard.
var ErrTxnDone = errors.New("kv: transaction already finished")

// ErrUnsupported is returned when an operation is asked of an engine that does not
// implement the optional capability it needs, such as Verify on a core with no
// structural verifier (spec 23 §3).
var ErrUnsupported = errors.New("kv: operation not supported by this engine")

// Isolation selects a transaction's isolation level (spec 10 §3, §4). The zero value
// is SnapshotIsolation, the high-performance default; Serializable adds read-set
// tracking and rw-antidependency detection at commit to give full serializability.
type Isolation uint8

const (
	// SnapshotIsolation gives every read a stable snapshot and serializes conflicting
	// writers (first-committer-wins). Its one anomaly is write skew (spec 10 §3).
	SnapshotIsolation Isolation = iota
	// Serializable is snapshot isolation plus commit-time read-set validation: a
	// transaction aborts if any key or range it read was written by a transaction that
	// committed in its snapshot-to-commit window. This closes write skew and every
	// other SI anomaly, at a higher abort rate under contention (spec 10 §4). Reads
	// still never block; the check is optimistic, not lock-based.
	Serializable
)

// opKind tags a buffered mutation so read-your-writes resolution and batch
// construction can replay it.
type opKind uint8

const (
	opSet opKind = iota
	opDelete
	opMerge
	opRangeDelete
)

// pendingOp is one buffered mutation in a write transaction, held privately until
// commit so reads in other transactions never see it (spec 10 §3). For a range
// delete, key is the inclusive low bound and value is the exclusive high bound.
type pendingOp struct {
	kind  opKind
	key   []byte
	value []byte // the set value or merge operand; nil for a delete; high bound for a range delete
}

// rangeCovers reports whether the half-open interval [lo, hi) contains key.
func rangeCovers(lo, hi, key []byte) bool {
	return format.CompareUser(key, lo) >= 0 && format.CompareUser(key, hi) < 0
}

// Txn is a transaction: a fixed read snapshot plus, for a writable transaction, a
// private buffer of mutations applied atomically at commit. It carries the
// snapshot-isolation semantics of spec 10 and the API shape of spec 15 §2. A Txn is
// not safe for concurrent use by multiple goroutines; a *DB is.
type Txn struct {
	db          *DB
	writable    bool
	readVersion uint64

	// ops are the buffered mutations in chronological order, replayed over the
	// snapshot for read-your-writes and turned into one WriteBatch at commit.
	ops []pendingOp

	// commitTs is the version this transaction was assigned at commit, set once a
	// writable transaction commits successfully and zero otherwise. It is the
	// serial-order position the oracle gave the commit, used by the linearizability
	// harness (spec 23 §2) to reconstruct the commit-version order.
	commitTs uint64

	// isolation is the level this transaction runs at, copied from the database
	// default at Begin. Under Serializable the transaction also tracks its reads.
	isolation Isolation

	// reads is the set of user keys this writable serializable transaction has read,
	// and readRanges the scan predicates it has iterated. The oracle validates them at
	// commit (spec 10 §4): a concurrent write to any of them is a rw-antidependency
	// that aborts the commit. Both stay nil under snapshot isolation and for read-only
	// transactions, which never validate.
	reads      map[string]struct{}
	readRanges []keyRange

	done bool
}

// View runs fn in a read-only transaction at a fresh snapshot. The snapshot never
// blocks and never conflicts; it is released when View returns (spec 15 §2.1).
func (d *DB) View(fn func(txn *Txn) error) error {
	txn := d.Begin(false)
	defer txn.Discard()
	return fn(txn)
}

// Update runs fn in a writable transaction, committing on a nil return and
// discarding on an error. On a write-write conflict it retries fn against a fresh
// snapshot, up to the configured bound (spec 15 §2.1), so fn must be re-runnable.
func (d *DB) Update(fn func(txn *Txn) error) error {
	var lastErr error
	for attempt := 0; attempt <= d.maxRetries; attempt++ {
		txn := d.Begin(true)
		err := fn(txn)
		if err != nil {
			txn.Discard()
			return err
		}
		if err := txn.Commit(); err != nil {
			if errors.Is(err, ErrConflict) {
				lastErr = err
				continue // re-run the closure against a newer snapshot
			}
			return err
		}
		return nil
	}
	return lastErr
}

// Begin starts an explicit transaction at a fresh snapshot. The caller must call
// Discard (deferred) to release the snapshot, and Commit to durably apply a
// writable transaction's buffered writes (spec 15 §2.2).
func (d *DB) Begin(writable bool) *Txn {
	return &Txn{
		db:          d,
		writable:    writable,
		readVersion: d.orc.readTs(),
		isolation:   d.isolation,
	}
}

// Get returns the newest value of key visible to the transaction: its own buffered
// writes overlaid on its read snapshot (read-your-writes, spec 10 §7), or
// engine.ErrNotFound if the key is absent or tombstoned. The returned bytes are
// valid until the next mutation on the same key; callers that keep them must copy.
func (t *Txn) Get(key []byte) ([]byte, error) {
	if t.done {
		return nil, ErrTxnDone
	}
	t.trackRead(key)
	val, ok, err := t.resolve(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, engine.ErrNotFound
	}
	return val, nil
}

// Exists reports whether key has a visible value, without returning it.
func (t *Txn) Exists(key []byte) (bool, error) {
	if t.done {
		return false, ErrTxnDone
	}
	t.trackRead(key)
	_, ok, err := t.resolve(key)
	return ok, err
}

// trackRead records a point read for commit-time serializability validation. It runs
// only for a writable serializable transaction, since a read-only transaction never
// commits writes and so never validates, and snapshot isolation does not track reads.
// A read of an absent key is recorded too: its absence is part of what the transaction
// depends on, so a concurrent insert is a rw-antidependency. The key is copied because
// callers may reuse the slice.
func (t *Txn) trackRead(key []byte) {
	if t.isolation != Serializable || !t.writable {
		return
	}
	if t.reads == nil {
		t.reads = make(map[string]struct{})
	}
	t.reads[string(key)] = struct{}{}
}

// trackRange records a scan predicate [lo, hi) for commit-time serializability
// validation, so a concurrent write that lands inside the scanned interval, including
// an insert of a key the scan would have seen (a phantom), aborts the commit. It runs
// only for a writable serializable transaction. Bounds are copied.
func (t *Txn) trackRange(lo, hi []byte) {
	if t.isolation != Serializable || !t.writable {
		return
	}
	t.readRanges = append(t.readRanges, keyRange{
		lo: cloneOrNil(lo),
		hi: cloneOrNil(hi),
	})
}

// resolve folds the transaction's buffered ops for key over the snapshot value,
// chronologically, matching the engine's version fold (format.Fold): a set
// replaces, a delete clears, a merge folds through the registered operator (or
// replaces when no operator is registered), and a range delete covering key clears
// it. It walks the whole op stream so an interleaved range delete is applied in the
// right order relative to the point ops on key. It returns the value and whether
// the key is present.
func (t *Txn) resolve(key []byte) ([]byte, bool, error) {
	// Snapshot base: the engine value visible at the read version.
	val, exists, err := t.db.snapshotGet(t.readVersion, key)
	if err != nil {
		return nil, false, err
	}
	ks := string(key)
	for _, op := range t.ops {
		switch op.kind {
		case opSet:
			if string(op.key) == ks {
				val, exists = op.value, true
			}
		case opDelete:
			if string(op.key) == ks {
				val, exists = nil, false
			}
		case opMerge:
			if string(op.key) == ks {
				if t.db.merge != nil {
					val = t.db.merge(val, op.value)
				} else {
					val = op.value
				}
				exists = true
			}
		case opRangeDelete:
			if rangeCovers(op.key, op.value, key) {
				val, exists = nil, false
			}
		}
	}
	return val, exists, nil
}

// Set buffers an upsert of key to value, applied at commit.
func (t *Txn) Set(key, value []byte) error { return t.record(opSet, key, value) }

// Delete buffers a tombstone for key, applied at commit.
func (t *Txn) Delete(key []byte) error { return t.record(opDelete, key, nil) }

// Merge buffers a merge operand for key, folded through the registered operator at
// read and commit time (spec 15 §5).
func (t *Txn) Merge(key, operand []byte) error { return t.record(opMerge, key, operand) }

// DeleteRange buffers a deletion of the half-open interval [lo, hi), applied at
// commit as a single range-delete marker (spec 11 §4). Every key in [lo, hi) older
// than the commit version reads as absent, including in this transaction's own
// reads and scans before commit. A range delete is blind for conflict detection:
// its write set is an interval, not a key set, so it does not abort on a concurrent
// write to a covered key (commit-version order resolves the overlap).
func (t *Txn) DeleteRange(lo, hi []byte) error {
	if t.done {
		return ErrTxnDone
	}
	if !t.writable {
		return ErrReadOnlyTxn
	}
	t.ops = append(t.ops, pendingOp{
		kind:  opRangeDelete,
		key:   append([]byte(nil), lo...),
		value: append([]byte(nil), hi...),
	})
	return nil
}

// bufferedRangeCovers reports whether any buffered range delete covers key.
func (t *Txn) bufferedRangeCovers(key []byte) bool {
	for _, op := range t.ops {
		if op.kind == opRangeDelete && rangeCovers(op.key, op.value, key) {
			return true
		}
	}
	return false
}

func (t *Txn) record(kind opKind, key, value []byte) error {
	if t.done {
		return ErrTxnDone
	}
	if !t.writable {
		return ErrReadOnlyTxn
	}
	k := append([]byte(nil), key...)
	var v []byte
	if value != nil {
		v = append([]byte(nil), value...)
	}
	t.ops = append(t.ops, pendingOp{kind: kind, key: k, value: v})
	return nil
}

// finalize collapses the buffered ops into one mutation per key, the form the
// commit batch needs. Every op in a transaction shares one commit version, so the
// engine's (user_key, version, kind) keying cannot order two mutations of the same
// key; collapsing sidesteps that entirely.
//
// A key touched by a single merge stays a blind merge operand: it is not resolved
// against the snapshot and not added to the conflict set, preserving the
// merge-as-blind-append concurrency win (spec 15 §5). Any other key -- one with a
// set or delete, or more than one op -- is resolved to its net value against the
// snapshot and emitted as a single Set (present) or Delete (absent), and is added
// to the conflict set because its outcome depends on what it read.
//
// It reads the snapshot, so it runs before the writer lock is taken (no reentrancy
// on db.mu); a base that shifts between here and commit is caught by the conflict
// check on the resolved keys.
// A buffered range delete is emitted as its own marker, in order, and is blind: it
// is not collapsed and not conflict-detected. A point-written key that a buffered
// range delete also covers is still resolved to its net value (resolve folds the
// range delete in), so the marker and the per-key op agree.
func (t *Txn) finalize() (ops []pendingOp, conflictKeys []string, err error) {
	seen := make(map[string]struct{}, len(t.ops))
	for _, op := range t.ops {
		if op.kind == opRangeDelete {
			ops = append(ops, op) // blind range marker
			continue
		}
		ks := string(op.key)
		if _, ok := seen[ks]; ok {
			continue
		}
		seen[ks] = struct{}{}

		count, onlyMerge := 0, true
		for _, o := range t.ops {
			if o.kind != opRangeDelete && string(o.key) == ks {
				count++
				if o.kind != opMerge {
					onlyMerge = false
				}
			}
		}
		// A lone merge stays a blind operand only when no buffered range delete
		// covers it; a covering range delete changes the key's net value, so it must
		// be resolved instead.
		if count == 1 && onlyMerge && !t.bufferedRangeCovers(op.key) {
			ops = append(ops, op)
			continue
		}
		val, exists, rerr := t.resolve(op.key)
		if rerr != nil {
			return nil, nil, rerr
		}
		if exists {
			ops = append(ops, pendingOp{kind: opSet, key: op.key, value: val})
		} else {
			ops = append(ops, pendingOp{kind: opDelete, key: op.key})
		}
		conflictKeys = append(conflictKeys, ks)
	}
	return ops, conflictKeys, nil
}

// Commit durably applies a writable transaction's buffered writes, or returns
// ErrConflict if it lost a write-write race. A read-only transaction, or a writable
// one with no buffered writes, commits trivially. After Commit the transaction is
// finished; calling it again returns ErrTxnDone (spec 15 §2.2).
func (t *Txn) Commit() error {
	if t.done {
		return ErrTxnDone
	}
	if !t.writable || len(t.ops) == 0 {
		t.finish()
		return nil
	}
	ops, conflictKeys, err := t.finalize()
	if err != nil {
		t.finish()
		return err
	}
	var v uint64
	if t.isolation == Serializable {
		v, err = t.db.commitTxnSerializable(t.readVersion, ops, conflictKeys, t.readKeys(), t.readRanges)
	} else {
		v, err = t.db.commitTxn(t.readVersion, ops, conflictKeys)
	}
	if err == nil {
		t.commitTs = v
	}
	t.finish()
	return err
}

// readKeys returns the tracked read set as a slice, for the serializable commit check.
func (t *Txn) readKeys() []string {
	if len(t.reads) == 0 {
		return nil
	}
	out := make([]string, 0, len(t.reads))
	for k := range t.reads {
		out = append(out, k)
	}
	return out
}

// cloneOrNil copies a bound, preserving a nil (open) bound as nil.
func cloneOrNil(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

// Discard releases the transaction's snapshot without applying its writes. It is
// safe to call after Commit (it is then a no-op) and must always be called to free
// the readMark registration the snapshot holds (spec 15 §2.2).
func (t *Txn) Discard() {
	if t.done {
		return
	}
	t.finish()
}

// finish releases the snapshot exactly once.
func (t *Txn) finish() {
	if t.done {
		return
	}
	t.done = true
	t.db.orc.doneRead(t.readVersion)
}
