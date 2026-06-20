package db

import (
	"errors"

	"github.com/tamnd/kv/engine"
)

// ErrBatchClosed is returned when a WriteBatch is used after Close (spec 15 §6).
var ErrBatchClosed = errors.New("kv: write batch closed")

// defaultBatchOps is the chunk size a WriteBatch uses when the caller asks for none.
const defaultBatchOps = 1000

// WriteBatch is an explicit, memory-bounded builder for very large writes (spec 15 §6).
// It buffers Set and Delete operations and flushes them to the database in bounded chunks,
// so populating a database with millions of keys never holds them all in memory at once.
//
// It is deliberately NOT one atomic unit. A WriteBatch spans many commits -- one per
// flushed chunk -- which is exactly the point: it is the bulk-load path, not a transaction.
// A crash partway through leaves the chunks that already committed durable and the rest
// unwritten, the same as if the caller had run a sequence of small Update calls. Code that
// needs all-or-nothing must use Update.
//
// Within one chunk the last operation written for a key wins, so a Set followed by a Delete
// of the same key in the same chunk resolves to the Delete, matching the obvious reading of
// the call sequence. A WriteBatch is not safe for concurrent use.
type WriteBatch struct {
	db     *DB
	maxOps int

	ops    []pendingOp
	count  int // operations recorded across the batch's life, buffered plus flushed
	closed bool
	err    error // sticky: the first flush error fences the batch
}

// NewWriteBatch returns a builder that flushes every maxOps operations. A maxOps of zero or
// less selects a sensible default. The batch must be Closed to flush the final partial chunk.
func (d *DB) NewWriteBatch(maxOps int) *WriteBatch {
	if maxOps <= 0 {
		maxOps = defaultBatchOps
	}
	return &WriteBatch{db: d, maxOps: maxOps, ops: make([]pendingOp, 0, maxOps)}
}

// Set buffers an upsert of key to value, flushing the buffered chunk first if it is full.
func (b *WriteBatch) Set(key, value []byte) error {
	return b.add(pendingOp{kind: opSet, key: cloneBytes(key), value: cloneBytes(value)})
}

// Delete buffers a tombstone for key, flushing the buffered chunk first if it is full.
func (b *WriteBatch) Delete(key []byte) error {
	return b.add(pendingOp{kind: opDelete, key: cloneBytes(key)})
}

// add appends one operation and auto-flushes when the chunk fills. It clones the caller's
// slices (like Txn.record) so reusing the input buffer between calls is safe.
func (b *WriteBatch) add(op pendingOp) error {
	if b.err != nil {
		return b.err
	}
	if b.closed {
		return ErrBatchClosed
	}
	b.ops = append(b.ops, op)
	b.count++
	if len(b.ops) >= b.maxOps {
		return b.Flush()
	}
	return nil
}

// Flush commits the buffered chunk as one blind batch and clears the buffer. An empty
// buffer flushes nothing. The first flush that fails fences the batch: its error is sticky
// and every later call returns it, so a partial load never silently continues past an I/O
// fault.
func (b *WriteBatch) Flush() error {
	if b.err != nil {
		return b.err
	}
	if b.closed {
		return ErrBatchClosed
	}
	if len(b.ops) == 0 {
		return nil
	}

	// Collapse to the net last op per key, in first-seen order. A blind batch keys every
	// entry at one commit version, so two entries for the same key cannot be ordered by
	// version; collapsing here makes last-write-wins hold within the chunk and sidesteps
	// the kind-ordering ambiguity (spec 10 §3) the transaction path solves with finalize.
	last := make(map[string]pendingOp, len(b.ops))
	order := make([]string, 0, len(b.ops))
	for _, op := range b.ops {
		k := string(op.key)
		if _, seen := last[k]; !seen {
			order = append(order, k)
		}
		last[k] = op
	}

	_, err := b.db.Write(func(wb *engine.WriteBatch) {
		for _, k := range order {
			op := last[k]
			if op.kind == opDelete {
				wb.Delete(op.key)
			} else {
				wb.Set(op.key, op.value)
			}
		}
	})
	if err != nil {
		b.err = err
		return err
	}
	b.ops = b.ops[:0]
	return nil
}

// Count reports the number of operations recorded over the batch's life, buffered and
// flushed together. After collapsing, fewer entries may reach the engine when a key is
// written more than once, but Count tracks what the caller issued.
func (b *WriteBatch) Count() int { return b.count }

// Pending reports the number of operations buffered but not yet flushed.
func (b *WriteBatch) Pending() int { return len(b.ops) }

// Close flushes the final partial chunk and marks the batch done. It is idempotent. Once
// closed, every operation returns ErrBatchClosed.
func (b *WriteBatch) Close() error {
	if b.closed {
		return b.err
	}
	err := b.Flush()
	b.closed = true
	return err
}

// cloneBytes returns an owned copy of b, or nil for a nil input, matching how the
// transaction path defensively copies caller slices before buffering them.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
