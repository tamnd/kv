package kv

import (
	"time"

	"github.com/tamnd/kv/db"
)

// Txn is a transaction: a fixed read snapshot plus, for a writable transaction, a
// private buffer of mutations applied atomically at commit (spec 15 §2). A Txn is not
// safe for concurrent use by multiple goroutines; a *DB is.
type Txn struct {
	t *db.Txn
}

// Get returns the newest value of key visible to the transaction, overlaying its own
// buffered writes on the snapshot (read-your-writes), or ErrNotFound if absent or
// tombstoned. The bytes are valid only until the transaction ends and may alias engine
// memory; use GetCopy to retain them (spec 15 §3).
func (t *Txn) Get(key []byte) ([]byte, error) {
	v, err := t.t.Get(key)
	return v, wrap(err)
}

// GetCopy is Get returning an owned copy of the value, safe to keep past the
// transaction (spec 15 §3).
func (t *Txn) GetCopy(key []byte) ([]byte, error) {
	v, err := t.t.Get(key)
	if err != nil {
		return nil, wrap(err)
	}
	return append([]byte(nil), v...), nil
}

// Exists reports whether key has a visible value without fetching it (spec 15 §3).
func (t *Txn) Exists(key []byte) (bool, error) {
	ok, err := t.t.Exists(key)
	return ok, wrap(err)
}

// Set buffers an upsert of key to value, applied at commit.
func (t *Txn) Set(key, value []byte) error { return wrap(t.t.Set(key, value)) }

// SetWithTTL buffers an upsert of key to value that expires after ttl (spec 15 §6).
// The deadline is computed once, here, from the database clock, and stored as an
// absolute time, so it is honored consistently across reads and survives reopen rather
// than restarting on recovery. A reader past the deadline sees the key as absent before
// any background sweep reclaims it. A non-positive ttl means the key never expires,
// equivalent to a plain Set.
func (t *Txn) SetWithTTL(key, value []byte, ttl time.Duration) error {
	var expiry uint64
	if ttl > 0 {
		expiry = t.t.Now() + uint64(ttl.Nanoseconds())
	}
	return wrap(t.t.SetWithTTL(key, value, expiry))
}

// Delete buffers a tombstone for key, applied at commit.
func (t *Txn) Delete(key []byte) error { return wrap(t.t.Delete(key)) }

// DeleteRange buffers a deletion of the half-open interval [lo, hi), applied at commit
// as a single range-delete marker (spec 11 §4).
func (t *Txn) DeleteRange(lo, hi []byte) error { return wrap(t.t.DeleteRange(lo, hi)) }

// Merge buffers a merge operand for key, folded through the registered operator at read
// and commit time (spec 15 §5). It needs no read, so a contended counter becomes blind
// appends the engine collapses.
func (t *Txn) Merge(key, operand []byte) error { return wrap(t.t.Merge(key, operand)) }

// NewIterator returns a snapshot-consistent iterator over the transaction's snapshot,
// overlaid with its own buffered writes (spec 11). The caller must Close it.
func (t *Txn) NewIterator(opts IterOptions) (*Iterator, error) {
	it, err := t.t.NewIterator(opts)
	if err != nil {
		return nil, wrap(err)
	}
	return &Iterator{it: it}, nil
}

// NewScanCursor returns a forward-only zero-copy range scan over the transaction's
// snapshot (spec 11). When the transaction has no buffered writes and the scan is
// forward, it takes the zero-copy batch path; otherwise it falls back to an Iterator so
// read-your-writes and reverse iteration still hold. The caller must Close it.
func (t *Txn) NewScanCursor(opts IterOptions) (*ScanCursor, error) {
	sc, err := t.t.NewScanCursor(opts)
	if err != nil {
		return nil, wrap(err)
	}
	return &ScanCursor{sc: sc}, nil
}

// Commit durably applies a writable transaction's buffered writes, or returns
// ErrConflict if it lost a write-write or SSI race (spec 15 §2.2).
func (t *Txn) Commit() error { return wrap(t.t.Commit()) }

// CommitVersion returns the version this transaction committed at, valid only after a
// successful Commit. It is zero before commit and for a read-only or empty transaction.
func (t *Txn) CommitVersion() uint64 { return t.t.CommitVersion() }

// Discard releases the transaction's snapshot without applying its writes. It is a
// no-op after Commit and must always be called (deferred) to free the snapshot's
// readMark registration (spec 15 §2.2).
func (t *Txn) Discard() { t.t.Discard() }
