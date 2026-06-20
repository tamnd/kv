package kv

import (
	"errors"

	"github.com/tamnd/kv/db"
	"github.com/tamnd/kv/engine"
)

// The typed error set callers branch on with errors.Is (spec 15 §9). They are the
// loud-failure contract: kv reports a sentinel callers can match while still carrying
// context in the message.
var (
	// ErrNotFound means the key is absent or tombstoned at the snapshot.
	ErrNotFound = engine.ErrNotFound
	// ErrConflict means a write-write or SSI conflict; retry the transaction.
	ErrConflict = db.ErrConflict
	// ErrReadOnly means a write was attempted on a read-only transaction or database.
	ErrReadOnly = errors.New("kv: read-only")
	// ErrClosed means an operation was attempted on a closed database or a finished
	// transaction.
	ErrClosed = errors.New("kv: closed")
	// ErrTxnTooBig means a single transaction exceeded the configured size bound; use a
	// batch instead.
	ErrTxnTooBig = errors.New("kv: transaction too big")
	// ErrCorrupt means a checksum or AEAD failure; the database needs recovery or
	// restore.
	ErrCorrupt = errors.New("kv: corrupt")
	// ErrNeedsRecovery means a prior fatal fsync error fenced the database; reopen to
	// recover (spec 07 §6).
	ErrNeedsRecovery = errors.New("kv: needs recovery")
)

// wrap maps the internal db/engine sentinels onto the public ones so callers match the
// kv.Err* surface, while preserving the original message and any wrapped context. An
// already-public or unrecognized error passes through unchanged.
func wrap(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, db.ErrReadOnlyTxn):
		return ErrReadOnly
	case errors.Is(err, db.ErrTxnDone):
		return ErrClosed
	default:
		return err
	}
}
