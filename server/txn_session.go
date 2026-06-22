package server

import (
	"errors"
	"sync"
	"time"

	"github.com/tamnd/kv"
)

// This file holds the interactive transaction registry (spec 17 §3.1). A single-shot Txn carries
// its whole operation set in one request and never pins server state, which is the recommended
// path. An interactive transaction is the rare case a single shot cannot express: a
// read-modify-write whose write depends on a read the client must see first, across more than one
// round trip. The server holds a real library *Txn open between those round trips, keyed by an id
// the client passes back.
//
// Holding a transaction open is the thing the embedded model is careful about: the open txn pins
// a read snapshot (the readMark, spec 10 §6), which holds the garbage-collection watermark back,
// and a dead or slow client must not pin it forever. So the registry bounds interactive
// transactions two ways. A cap limits how many can be open at once, so a flood of begins cannot
// exhaust memory or pin the watermark across the whole keyspace. An idle timeout force-discards a
// transaction no operation has touched recently, so a client that began a transaction and then
// died releases its snapshot on its own. Single-shot remains the default; this exists for when a
// caller genuinely needs to hold a read and decide a write from it.

// Default bounds for interactive transactions, deliberately conservative: most workloads use
// single-shot, so a small cap and a short idle window are plenty and keep a leaked transaction
// from holding the watermark for long.
const (
	defaultMaxOpenTxns = 256
	defaultTxnIdleTTL  = 30 * time.Second
)

// ErrNoSuchTxn is returned when an operation references a transaction id the registry does not
// hold: it was never opened, already committed or discarded, or force-discarded after going
// idle. The adapters map it to a bad-request class, since it is the client referencing a token
// that is gone.
var ErrNoSuchTxn = errors.New("kv: no such transaction")

// ErrTooManyTxns is returned by BeginTxn when the open-transaction cap is reached. The adapters
// map it to the unavailable class, a back-pressure signal: the client should retry later or fall
// back to single-shot, the same way it would treat a busy server.
var ErrTooManyTxns = errors.New("kv: too many open interactive transactions")

// txnSession is one open interactive transaction. Its mutex serializes the operations on the
// held *kv.Txn, which is not safe for concurrent use, so two requests that race on the same id
// take turns rather than corrupting the transaction's buffered write set. done guards against
// using a transaction the reaper or a commit/discard already ended.
type txnSession struct {
	mu       sync.Mutex
	id       uint64
	txn      *kv.Txn
	writable bool
	lastUsed time.Time
	done     bool
}

// end discards the transaction once, idempotently. The reaper, a discard, and a failed commit
// all funnel through it, so the readMark is released exactly once no matter which path ends the
// session.
func (s *txnSession) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return
	}
	s.done = true
	s.txn.Discard()
}

// expireIfIdle ends the session if no operation has touched it within ttl, returning whether it
// is now ended (already-done counts as ended). It checks idleness under the session lock so it
// cannot race an in-flight operation: an operation updates lastUsed under the same lock, so a
// transaction used a moment ago is never reaped out from under its user.
func (s *txnSession) expireIfIdle(ttl time.Duration, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return true
	}
	if now.Sub(s.lastUsed) <= ttl {
		return false
	}
	s.done = true
	s.txn.Discard()
	return true
}

// txnRegistry holds the open interactive transactions and the reaper that force-discards idle
// ones. Its mutex guards the map and the id counter; each session's own mutex guards that
// session's transaction.
type txnRegistry struct {
	mu      sync.Mutex
	txns    map[uint64]*txnSession
	nextID  uint64
	maxOpen int
	idleTTL time.Duration
	stop    chan struct{}
	stopped bool
}

// newTxnRegistry builds a registry with the given bounds and starts its reaper. The bounds are
// fixed at construction so the running reaper never reads a field a caller is mutating; a
// non-positive value falls back to the default. NewService passes the defaults; tests pass tight
// bounds to exercise the cap and the reaper without a post-construction poke that would race the
// reaper goroutine.
func newTxnRegistry(maxOpen int, idleTTL time.Duration) *txnRegistry {
	if maxOpen <= 0 {
		maxOpen = defaultMaxOpenTxns
	}
	if idleTTL <= 0 {
		idleTTL = defaultTxnIdleTTL
	}
	r := &txnRegistry{
		txns:    make(map[uint64]*txnSession),
		maxOpen: maxOpen,
		idleTTL: idleTTL,
		stop:    make(chan struct{}),
	}
	go r.reap()
	return r
}

// begin opens a transaction on db and registers it under a fresh id, refusing once the cap is
// reached so a client cannot pin the watermark without bound. The id is monotonic and never
// reused within a process, so a stale id from an ended transaction can never collide with a live
// one.
func (r *txnRegistry) begin(db *kv.DB, writable bool) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return 0, kv.ErrClosed
	}
	if len(r.txns) >= r.maxOpen {
		return 0, ErrTooManyTxns
	}
	r.nextID++
	id := r.nextID
	r.txns[id] = &txnSession{
		id:       id,
		txn:      db.Begin(writable),
		writable: writable,
		lastUsed: time.Now(),
	}
	return id, nil
}

// lookup returns the session for an id, or nil if the registry does not hold it.
func (r *txnRegistry) lookup(id uint64) *txnSession {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.txns[id]
}

// remove drops an id from the map, returning the session it held or nil. commit and discard call
// it to take ownership of ending the session.
func (r *txnRegistry) remove(id uint64) *txnSession {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.txns[id]
	delete(r.txns, id)
	return s
}

// with runs fn against the held transaction under the session lock, refreshing the idle clock.
// It is the single path every interactive operation takes, so the lock discipline and the
// liveness check live in one place. A reference to a vanished or ended transaction is
// ErrNoSuchTxn.
func (r *txnRegistry) with(id uint64, fn func(*kv.Txn) error) error {
	s := r.lookup(id)
	if s == nil {
		return ErrNoSuchTxn
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return ErrNoSuchTxn
	}
	s.lastUsed = time.Now()
	return fn(s.txn)
}

// reap force-discards idle transactions on a ticker until the registry is closed. The tick is
// half the idle window, so a transaction is reaped within about one and a half idle windows of
// its last use, which is prompt enough to release a leaked snapshot without waking constantly.
func (r *txnRegistry) reap() {
	tick := r.idleTTL / 2
	if tick <= 0 {
		tick = time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
			r.reapIdle(time.Now())
		}
	}
}

// reapIdle force-discards every session idle past the timeout. It snapshots the session pointers
// under the registry lock, then checks and ends each under its own lock, so a long discard never
// holds the registry lock and blocks begins.
func (r *txnRegistry) reapIdle(now time.Time) {
	r.mu.Lock()
	sessions := make([]*txnSession, 0, len(r.txns))
	for _, s := range r.txns {
		sessions = append(sessions, s)
	}
	r.mu.Unlock()
	for _, s := range sessions {
		if s.expireIfIdle(r.idleTTL, now) {
			r.mu.Lock()
			// Re-check identity: only delete if the map still holds this exact session, never a
			// newer one that reused nothing (ids are monotonic, so this is belt and suspenders).
			if r.txns[s.id] == s {
				delete(r.txns, s.id)
			}
			r.mu.Unlock()
		}
	}
}

// close stops the reaper and force-discards every still-open transaction, releasing their
// snapshots. It is idempotent. The Service calls it during the server's shutdown, which is the
// spec's "force-discard interactive txns" step (spec 17 §6).
func (r *txnRegistry) close() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	close(r.stop)
	sessions := make([]*txnSession, 0, len(r.txns))
	for id, s := range r.txns {
		sessions = append(sessions, s)
		delete(r.txns, id)
	}
	r.mu.Unlock()
	for _, s := range sessions {
		s.end()
	}
}

// BeginTxn opens an interactive transaction and returns its id (spec 17 §3.1). A writable
// transaction can read and write; a read-only one reads at a fixed snapshot. The caller ends it
// with CommitTxn or DiscardTxn; if it does neither, the idle reaper discards it.
func (s *Service) BeginTxn(writable bool) (uint64, error) {
	return s.txns.begin(s.db, writable)
}

// TxnGet reads a key inside an open transaction, seeing the transaction's own uncommitted writes
// over the snapshot beneath them.
func (s *Service) TxnGet(id uint64, key []byte) (value []byte, found bool, err error) {
	err = s.txns.with(id, func(txn *kv.Txn) error {
		v, e := txn.GetCopy(key)
		if errors.Is(e, kv.ErrNotFound) {
			return nil
		}
		if e != nil {
			return e
		}
		value, found = v, true
		return nil
	})
	return value, found, err
}

// TxnExists reports whether a key is present inside an open transaction.
func (s *Service) TxnExists(id uint64, key []byte) (bool, error) {
	var found bool
	err := s.txns.with(id, func(txn *kv.Txn) error {
		ok, e := txn.Exists(key)
		found = ok
		return e
	})
	return found, err
}

// TxnSet buffers a set inside an open transaction. The write is not durable until CommitTxn.
func (s *Service) TxnSet(id uint64, key, value []byte, ttl time.Duration) error {
	return s.txns.with(id, func(txn *kv.Txn) error {
		if ttl > 0 {
			return txn.SetWithTTL(key, value, ttl)
		}
		return txn.Set(key, value)
	})
}

// TxnDelete buffers a delete inside an open transaction.
func (s *Service) TxnDelete(id uint64, key []byte) error {
	return s.txns.with(id, func(txn *kv.Txn) error { return txn.Delete(key) })
}

// TxnDeleteRange buffers a range delete inside an open transaction.
func (s *Service) TxnDeleteRange(id uint64, lo, hi []byte) error {
	return s.txns.with(id, func(txn *kv.Txn) error { return txn.DeleteRange(lo, hi) })
}

// TxnMerge buffers a merge inside an open transaction.
func (s *Service) TxnMerge(id uint64, key, operand []byte) error {
	return s.txns.with(id, func(txn *kv.Txn) error { return txn.Merge(key, operand) })
}

// CommitTxn commits an open transaction and returns its commit version, removing it from the
// registry whether it succeeds or fails (a failed commit, conflict or otherwise, ends the
// transaction; the client begins a fresh one to retry). A read-only or empty transaction commits
// nothing and reports a zero version.
func (s *Service) CommitTxn(id uint64) (uint64, error) {
	sess := s.txns.remove(id)
	if sess == nil {
		return 0, ErrNoSuchTxn
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.done {
		return 0, ErrNoSuchTxn
	}
	sess.done = true
	if err := sess.txn.Commit(); err != nil {
		sess.txn.Discard()
		return 0, err
	}
	return sess.txn.CommitVersion(), nil
}

// DiscardTxn ends an open transaction without applying its writes, releasing its snapshot. It is
// the explicit rollback; referencing an unknown id is ErrNoSuchTxn.
func (s *Service) DiscardTxn(id uint64) error {
	sess := s.txns.remove(id)
	if sess == nil {
		return ErrNoSuchTxn
	}
	sess.end()
	return nil
}
