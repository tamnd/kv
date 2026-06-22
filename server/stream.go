package server

import (
	"context"

	"github.com/tamnd/kv"
)

// This file adds the streaming half of the operation surface to Service: a range scan that
// yields key/value pairs one at a time, and a change watch that yields committed mutations as
// they happen. Both are pull-shaped at this layer: Service drives a caller-supplied callback
// per item and never buffers the whole result, so a scan over a billion keys or a watch that
// runs for a day costs the server one item of memory at a time. The HTTP adapter turns each
// callback into a written line (NDJSON for scan, SSE for watch); the binary adapter will turn
// the same callbacks into framed messages. Keeping the iteration here, above the wire, means
// both protocols stream with identical bounds, ordering, and stop semantics.

// ScanOptions bounds a streaming scan (spec 17 §2.2): an inclusive Lower and exclusive Upper,
// or a Prefix that the iterator layer turns into those bounds, a Reverse direction, a
// KeysOnly mode that skips value materialization, and a Limit that caps the number of pairs
// yielded (zero means no cap). It mirrors the library's IterOptions plus the wire-only Limit.
type ScanOptions struct {
	Lower    []byte
	Upper    []byte
	Prefix   []byte
	Reverse  bool
	KeysOnly bool
	Limit    int
}

// Scan iterates the keyspace under opts at the latest committed snapshot, calling yield once
// per pair in key order (reverse order when opts.Reverse) until the range is exhausted, the
// limit is reached, or yield returns an error, which Scan returns. The whole scan runs inside
// one library View so it sees a single consistent snapshot, and the iterator is closed before
// Scan returns even on an early stop. value is nil in KeysOnly mode. The yield callback must
// not retain key or value past the call: they are the iterator's own buffers, valid only for
// the duration of the call, which is what lets a long scan stay at one item of memory.
func (s *Service) Scan(opts ScanOptions, yield func(key, value []byte) error) error {
	// Clamp the requested limit to the scan ceiling so an unbounded or oversized scan returns at
	// most the configured maximum, the same guardrail on either protocol. A client that wanted
	// more resumes from the last key it saw, the normal paginated-scan pattern.
	opts.Limit = s.limits.clampScan(opts.Limit)
	return s.db.View(func(txn *kv.Txn) error {
		it, err := txn.NewIterator(kv.IterOptions{
			Lower:    opts.Lower,
			Upper:    opts.Upper,
			Prefix:   opts.Prefix,
			Reverse:  opts.Reverse,
			KeysOnly: opts.KeysOnly,
		})
		if err != nil {
			return err
		}
		defer it.Close()

		n := 0
		for ok := it.First(); ok; ok = it.Next() {
			if opts.Limit > 0 && n >= opts.Limit {
				break
			}
			var value []byte
			if !opts.KeysOnly {
				v, e := it.Value()
				if e != nil {
					return e
				}
				value = v
			}
			if e := yield(it.Key(), value); e != nil {
				return e
			}
			n++
		}
		return it.Error()
	})
}

// Watch streams committed mutations whose key has the given prefix, calling yield once per
// committed batch in commit order until ctx is cancelled, yield returns an error, or the
// consumer falls too far behind (kv.ErrSubscriberLagged), returning the cause (spec 17
// §2.2). A nil prefix matches every key. It is a thin pass-through to the library's
// Subscribe, which already delivers only durable, committed changes and runs the callback on
// the subscribing goroutine, so a slow client slows only its own feed. The since cursor (only
// deliver changes after a version) is applied by the adapter, since the library feed starts
// at the moment of subscription and carries no backlog.
func (s *Service) Watch(ctx context.Context, prefix []byte, yield func([]kv.Change) error) error {
	return s.db.Subscribe(ctx, prefix, yield)
}
