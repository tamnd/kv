package kv

import "github.com/tamnd/kv/db"

// ScanCursor is a forward-only zero-copy range scan over a read transaction's snapshot
// (spec 11). It is the fast path the general Iterator cannot be: Key and Value return
// views aliased into kv's decoded leaves, valid only until the next Next call, so a
// consumer that needs a key or value past the next advance must copy it. Inside a write
// transaction, or for a reverse scan, it falls back to the Iterator so read-your-writes
// still holds. It must be Closed.
type ScanCursor struct {
	sc *db.ScanCursor
}

// Next advances to the next key in range and reports whether one is present. The first
// call positions at the lower bound. After it returns false, check Error.
func (s *ScanCursor) Next() bool { return s.sc.Next() }

// Key returns the current key as a transient view, valid until the next Next call.
func (s *ScanCursor) Key() []byte { return s.sc.Key() }

// Value returns the current value as a transient view, valid until the next Next call.
func (s *ScanCursor) Value() []byte { return s.sc.Value() }

// Error returns the first error the cursor hit, or nil. Call it after Next returns false.
func (s *ScanCursor) Error() error { return wrap(s.sc.Error()) }

// Close releases the cursor. It does not close the transaction.
func (s *ScanCursor) Close() error { return wrap(s.sc.Close()) }
