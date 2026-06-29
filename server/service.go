// Package server is the networked projection of the embedded library (spec 17): one kv
// server process opens a .kv file for writing and fans out to many client connections,
// exposing the same operations, isolation, and durability the library has, with transport
// and limits layered on. It is the third surface over one engine, after the library and
// the CLI, and is optional: kv is embedded-first, and the server exists for when a
// database must be shared across processes or hosts.
//
// The package is built in two layers. Service, in this file, is the transport-agnostic
// core: each method maps one operation onto a kv.DB library call and returns plain Go
// values, so it has no knowledge of HTTP or any wire format. The protocol adapters (the
// HTTP/JSON surface in http.go, the pure-Go binary surface in later slices) are thin shells
// that decode a request, call Service, and encode the result. Keeping the mapping in one
// place means both protocols expose exactly the same operation set with the same semantics,
// the spec's "two protocols, same surface" stance, and a new protocol is just another shell
// over this core.
//
// Per the zero-dependency rule the server takes no third-party framework: the HTTP surface
// is net/http and encoding/json from the standard library, and the binary protocol is
// hand-rolled length-prefixed framing. There is no gRPC, which would pull in protobuf and
// google.golang.org/grpc; the binary protocol fills the efficient-streaming role gRPC plays
// in the spec without the dependency.
package server

import (
	"bytes"
	"errors"
	"time"

	"github.com/tamnd/kv"
)

// OpKind names an operation in a single-shot transaction or batch (spec 17 §3). The wire
// adapters decode a request into a slice of these and Service replays them in order inside
// one library transaction, so a multi-op request is atomic exactly as the library's Update
// is.
type OpKind string

const (
	OpGet    OpKind = "get"
	OpExists OpKind = "exists"
	OpSet    OpKind = "set"
	OpDelete OpKind = "delete"
	OpMerge  OpKind = "merge"
)

// Op is one operation in a transaction or batch request. Key/Value carry a point op's operands;
// TTL, when positive, makes a set a SetWithTTL. The fields a kind does not use are ignored.
type Op struct {
	Kind  OpKind
	Key   []byte
	Value []byte
	TTL   time.Duration
}

// Assert is a read-set condition checked at the start of a single-shot transaction (spec 17
// §3.1): the operation set applies only if every assert holds, so a caller expresses a
// compare-and-set or compare-and-delete in one round trip without holding a transaction open
// across the network. ExpectAbsent asserts the key is missing; otherwise the key's current
// value must equal ExpectValue.
type Assert struct {
	Key          []byte
	ExpectValue  []byte
	ExpectAbsent bool
}

// ReadResult is the outcome of a get or exists op in a transaction, in the order the read
// ops appeared in the request. Found is whether the key was present; Value is its value for a
// get (nil for an exists or a miss).
type ReadResult struct {
	Found bool
	Value []byte
}

// TxnRequest is a single-shot transaction: a set of asserts that must all hold and a set of
// ops applied atomically if they do (spec 17 §3.1). It runs as one library Update, so it
// retries on conflict and commits or aborts as a unit, and no server-side transaction state
// spans requests.
type TxnRequest struct {
	Asserts []Assert
	Ops     []Op
}

// TxnResult is a single-shot transaction's outcome: the reads it performed, in order, and
// the commit version (spec 17 §3.1). Version is the version the writes committed at, or the
// snapshot version for a read-only request.
type TxnResult struct {
	Reads   []ReadResult
	Version uint64
}

// ErrAssertFailed is returned by Txn when a read-set assertion did not hold, so the
// transaction applied nothing. The HTTP adapter maps it to 409 Conflict, the same status a
// write-write conflict returns, since both mean "the precondition the caller assumed no
// longer holds".
var ErrAssertFailed = errors.New("kv: transaction assertion failed")

// Service is the transport-agnostic operation surface over a single database. It holds the
// open *kv.DB and the registry of open interactive transactions; every method is a thin,
// concurrency-safe mapping onto a library call, so many adapter goroutines call one Service in
// parallel exactly as many callers share one *kv.DB.
type Service struct {
	db     *kv.DB
	txns   *txnRegistry
	limits Limits
}

// NewService wraps an open database in a Service with the default request limits. The caller owns
// the database's lifetime; the Service never closes it. The interactive transaction registry it
// starts must be stopped with Close so its reaper goroutine does not outlive the Service.
func NewService(db *kv.DB) *Service {
	return &Service{db: db, txns: newTxnRegistry(defaultMaxOpenTxns, defaultTxnIdleTTL), limits: DefaultLimits()}
}

// SetLimits replaces the request limits the Service enforces. It is meant to be called once at
// startup before the Service serves any request, the way Options.Limits flows through New; it
// is not safe to call concurrently with in-flight requests.
func (s *Service) SetLimits(l Limits) { s.limits = l }

// newServiceWithTxnBounds builds a Service with explicit interactive-transaction bounds and the
// default request limits. It exists for tests that need a tiny cap or a short idle window without
// poking the registry's fields after its reaper has started, which would race the reaper's reads.
func newServiceWithTxnBounds(db *kv.DB, maxOpen int, idleTTL time.Duration) *Service {
	return &Service{db: db, txns: newTxnRegistry(maxOpen, idleTTL), limits: DefaultLimits()}
}

// Close stops the interactive transaction registry, force-discarding any still-open
// transactions and ending the reaper goroutine. It does not close the database, which the
// caller owns. The Server calls it during Shutdown.
func (s *Service) Close() { s.txns.close() }

// DB returns the underlying database, for adapters that need a surface Service does not wrap
// directly (the streaming Scan and Watch, the ops endpoints).
func (s *Service) DB() *kv.DB { return s.db }

// Get reads a key at the latest committed snapshot and reports whether it was present.
func (s *Service) Get(key []byte) (value []byte, found bool, err error) {
	err = s.db.View(func(txn *kv.Txn) error {
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

// Exists reports whether a key is present without materializing its value.
func (s *Service) Exists(key []byte) (bool, error) {
	var found bool
	err := s.db.View(func(txn *kv.Txn) error {
		ok, e := txn.Exists(key)
		found = ok
		return e
	})
	return found, err
}

// Set upserts a key, with an optional TTL, and returns the commit version. An over-limit key or
// value is refused before the transaction opens.
func (s *Service) Set(key, value []byte, ttl time.Duration) (uint64, error) {
	if err := s.limits.checkKey(key); err != nil {
		return 0, err
	}
	if err := s.limits.checkValue(value); err != nil {
		return 0, err
	}
	return s.db.UpdateVersion(func(txn *kv.Txn) error {
		if ttl > 0 {
			return txn.SetWithTTL(key, value, ttl)
		}
		return txn.Set(key, value)
	})
}

// Delete removes one key and returns the commit version.
func (s *Service) Delete(key []byte) (uint64, error) {
	if err := s.limits.checkKey(key); err != nil {
		return 0, err
	}
	return s.db.UpdateVersion(func(txn *kv.Txn) error { return txn.Delete(key) })
}

// Merge applies the registered merge operator to a key and returns the commit version.
func (s *Service) Merge(key, operand []byte) (uint64, error) {
	if err := s.limits.checkKey(key); err != nil {
		return 0, err
	}
	if err := s.limits.checkValue(operand); err != nil {
		return 0, err
	}
	return s.db.UpdateVersion(func(txn *kv.Txn) error { return txn.Merge(key, operand) })
}

// Txn runs a single-shot transaction: it checks every assertion, performs the read ops
// collecting their results in order, applies the write ops, and commits, all as one library
// Update (spec 17 §3.1). A failed assertion aborts with ErrAssertFailed before any write. The
// whole thing retries on conflict like any Update, so the asserts are re-checked against the
// fresh snapshot each attempt, giving a correct compare-and-swap over the wire.
func (s *Service) Txn(req TxnRequest) (TxnResult, error) {
	if err := s.limits.checkBatch(len(req.Ops)); err != nil {
		return TxnResult{}, err
	}
	for _, a := range req.Asserts {
		if err := s.limits.checkAssert(a); err != nil {
			return TxnResult{}, err
		}
	}
	for _, op := range req.Ops {
		if err := s.limits.checkOp(op); err != nil {
			return TxnResult{}, err
		}
	}
	var reads []ReadResult
	version, err := s.db.UpdateVersion(func(txn *kv.Txn) error {
		reads = reads[:0]
		for _, a := range req.Asserts {
			v, e := txn.GetCopy(a.Key)
			present := true
			if errors.Is(e, kv.ErrNotFound) {
				present, e = false, nil
			}
			if e != nil {
				return e
			}
			if a.ExpectAbsent {
				if present {
					return ErrAssertFailed
				}
				continue
			}
			if !present || !bytes.Equal(v, a.ExpectValue) {
				return ErrAssertFailed
			}
		}
		for _, op := range req.Ops {
			if e := applyOp(txn, op, &reads); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		return TxnResult{}, err
	}
	if version == 0 {
		version = s.db.Stats().Version
	}
	return TxnResult{Reads: reads, Version: version}, nil
}

// Batch applies a set of write ops atomically and returns the commit version (spec 17 §3). It
// is Txn without reads or asserts, the bulk-write path: a single Update over the ops.
func (s *Service) Batch(ops []Op) (uint64, error) {
	if err := s.limits.checkBatch(len(ops)); err != nil {
		return 0, err
	}
	for _, op := range ops {
		if err := s.limits.checkOp(op); err != nil {
			return 0, err
		}
	}
	version, err := s.db.UpdateVersion(func(txn *kv.Txn) error {
		for _, op := range ops {
			if e := applyOp(txn, op, nil); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if version == 0 {
		version = s.db.Stats().Version
	}
	return version, nil
}

// applyOp performs one op against an open transaction, appending a ReadResult for a read op
// when reads is non-nil. A batch passes a nil reads since it carries no reads; a read op in a
// batch is treated as a no-op write-wise but still recorded if reads is provided.
func applyOp(txn *kv.Txn, op Op, reads *[]ReadResult) error {
	switch op.Kind {
	case OpGet:
		v, e := txn.GetCopy(op.Key)
		if errors.Is(e, kv.ErrNotFound) {
			if reads != nil {
				*reads = append(*reads, ReadResult{})
			}
			return nil
		}
		if e != nil {
			return e
		}
		if reads != nil {
			*reads = append(*reads, ReadResult{Found: true, Value: v})
		}
		return nil
	case OpExists:
		ok, e := txn.Exists(op.Key)
		if e != nil {
			return e
		}
		if reads != nil {
			*reads = append(*reads, ReadResult{Found: ok})
		}
		return nil
	case OpSet:
		if op.TTL > 0 {
			return txn.SetWithTTL(op.Key, op.Value, op.TTL)
		}
		return txn.Set(op.Key, op.Value)
	case OpDelete:
		return txn.Delete(op.Key)
	case OpMerge:
		return txn.Merge(op.Key, op.Value)
	default:
		return errInvalidOp(op.Kind)
	}
}

// Stats returns the database's current space-and-durability snapshot, the same numbers the
// CLI's stats command and the library's Stats expose.
func (s *Service) Stats() kv.Stats { return s.db.Stats() }

// Checkpoint folds the WAL into the main file, the ops-surface checkpoint (spec 17 §3).
func (s *Service) Checkpoint() error { return s.db.Checkpoint() }

// errInvalidOp reports an unknown op kind in a request, mapped to 400 by the HTTP adapter.
func errInvalidOp(k OpKind) error {
	return errors.New("kv: invalid operation kind " + string(k))
}
