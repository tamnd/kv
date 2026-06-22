package server

import (
	"errors"
	"fmt"
)

// This file defines the request limits the server enforces (spec 17 §6). The server fans many
// clients onto one process and one database, so an unbounded request from one client is a way to
// hurt every other client and the process itself: a gigabyte value, a batch of millions of ops,
// or a scan asked to return the whole keyspace each turn one request into a memory or latency
// spike the single writer cannot absorb. Limits are the guardrails that keep one process serving
// many clients safely.
//
// The limits live on the Service, the transport-agnostic core, not on either protocol adapter, so
// the HTTP face and the binary face enforce exactly the same bounds. That is the "two protocols,
// same surface" stance applied to safety: a value too large to set is too large on either wire,
// and a client cannot dodge a limit by switching protocols. A size limit is checked before the
// transaction opens, so an over-limit request is refused without touching the engine.

// ErrLimitExceeded is returned when a request exceeds a configured size limit: a key or value too
// large, or a batch with too many operations. It is a client error, the request is malformed for
// this server's configuration, so the HTTP adapter maps it to 413 and the binary adapter to the
// bad-request status, both of which tell the client the fault is in the request, not the server.
var ErrLimitExceeded = errors.New("kv: request exceeds a configured limit")

// Limits bounds the size of a request the server accepts. A zero in any field disables that one
// limit, so a field-by-field opt-out is possible; the zero value of the whole struct is therefore
// "no limits", which is why an unset Options.Limits falls back to DefaultLimits rather than the
// zero value. The scan limit is a ceiling the server clamps to rather than an error, because a
// scan that returns fewer rows than asked is a normal paginated result the client resumes from,
// whereas a value too large to store has no smaller correct answer.
type Limits struct {
	MaxKeySize   int // largest key in bytes; 0 disables
	MaxValueSize int // largest value or merge operand in bytes; 0 disables
	MaxBatchOps  int // most operations in one batch or single-shot transaction; 0 disables
	MaxScanLimit int // most pairs one scan returns; 0 disables, else clamps the request's limit
}

// DefaultLimits returns the limits a Service uses when none are configured. They are generous
// enough that an ordinary request never notices them and tight enough that a pathological one is
// refused: a 64 KiB key, a 16 MiB value, a hundred thousand ops per batch, and a million pairs per
// scan. A deployment with different needs sets its own through Options.Limits.
func DefaultLimits() Limits {
	return Limits{
		MaxKeySize:   64 << 10, // 64 KiB
		MaxValueSize: 16 << 20, // 16 MiB
		MaxBatchOps:  100_000,  // 100k ops
		MaxScanLimit: 1 << 20,  // ~1M pairs
	}
}

// checkKey rejects an over-limit key.
func (l Limits) checkKey(key []byte) error {
	if l.MaxKeySize > 0 && len(key) > l.MaxKeySize {
		return fmt.Errorf("%w: key of %d bytes exceeds the %d-byte limit", ErrLimitExceeded, len(key), l.MaxKeySize)
	}
	return nil
}

// checkValue rejects an over-limit value or merge operand.
func (l Limits) checkValue(value []byte) error {
	if l.MaxValueSize > 0 && len(value) > l.MaxValueSize {
		return fmt.Errorf("%w: value of %d bytes exceeds the %d-byte limit", ErrLimitExceeded, len(value), l.MaxValueSize)
	}
	return nil
}

// checkBatch rejects a batch or single-shot transaction with too many operations.
func (l Limits) checkBatch(n int) error {
	if l.MaxBatchOps > 0 && n > l.MaxBatchOps {
		return fmt.Errorf("%w: %d operations exceed the %d-operation batch limit", ErrLimitExceeded, n, l.MaxBatchOps)
	}
	return nil
}

// checkOp validates one operation's operands against the size limits. A range delete bounds both
// ends; a write bounds the value too. The kinds that carry no value skip the value check.
func (l Limits) checkOp(op Op) error {
	switch op.Kind {
	case OpDeleteRange:
		if err := l.checkKey(op.Lo); err != nil {
			return err
		}
		return l.checkKey(op.Hi)
	case OpSet, OpMerge:
		if err := l.checkKey(op.Key); err != nil {
			return err
		}
		return l.checkValue(op.Value)
	default:
		return l.checkKey(op.Key)
	}
}

// checkAssert validates a single-shot transaction assert: its key against the key limit and its
// expected value against the value limit, since both ride the request.
func (l Limits) checkAssert(a Assert) error {
	if err := l.checkKey(a.Key); err != nil {
		return err
	}
	return l.checkValue(a.ExpectValue)
}

// clampScan applies the scan ceiling: an unbounded request (limit zero) is capped to the maximum,
// and a request above the maximum is lowered to it. A request at or below the maximum is returned
// unchanged. With the limit disabled the request passes through as-is.
func (l Limits) clampScan(limit int) int {
	if l.MaxScanLimit <= 0 {
		return limit
	}
	if limit <= 0 || limit > l.MaxScanLimit {
		return l.MaxScanLimit
	}
	return limit
}
