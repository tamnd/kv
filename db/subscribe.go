package db

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// ErrSubscriberLagged is returned by Subscribe when its callback fell too far behind
// the commit stream and the subscription was dropped. The change feed never stalls a
// writer to wait for a slow consumer, so a consumer that cannot keep up is cut loose
// rather than allowed to block commits (spec 15 §7). The caller may resubscribe, but
// must treat the gap as a coverage hole and reconcile from a snapshot if it needs every
// change.
var ErrSubscriberLagged = errors.New("kv: subscriber lagged behind the change feed")

// ChangeKind classifies a mutation delivered to a Subscribe callback. It mirrors the
// internal key kinds (spec 02 §4) so the feed is a faithful log of committed operations,
// not a lossy point-only projection: a replication or watch consumer downstream can
// replay exactly what was committed.
type ChangeKind uint8

const (
	// ChangeSet is an upsert; Value holds the new value.
	ChangeSet ChangeKind = iota
	// ChangeDelete is a point tombstone; Value is nil.
	ChangeDelete
	// ChangeMerge is a merge operand; Value holds the operand, not the resolved value.
	ChangeMerge
	// ChangeRangeDelete is a range tombstone; Key is the inclusive lower bound and Value
	// is the exclusive upper bound.
	ChangeRangeDelete
)

// Change is one committed mutation surfaced on the change feed. The Key and Value slices
// are freshly copied for the callback and are safe to retain. Version is the commit
// version the mutation became visible at, shared by every Change in one delivered batch.
type Change struct {
	Kind    ChangeKind
	Key     []byte
	Value   []byte
	Version uint64
}

// subscribeBuffer bounds how many committed batches a subscription can fall behind before
// it is dropped as lagged. It is generous enough that a callback doing ordinary work keeps
// up, while still capping the memory a stuck consumer can pin.
const subscribeBuffer = 1024

// subscription is one live Subscribe call. publish sends matching batches on ch under the
// database's subsMu; the Subscribe goroutine drains ch and runs the callback. done is
// closed exactly once, by lag, when a non-blocking send finds ch full.
type subscription struct {
	prefix []byte
	ch     chan []Change
	done   chan struct{}
	once   sync.Once
}

func (s *subscription) lag() { s.once.Do(func() { close(s.done) }) }

// Subscribe delivers a change feed of committed mutations whose key has the given prefix
// (spec 15 §7). It blocks the calling goroutine, invoking fn once per committed batch that
// contains a matching mutation, in commit order, until one of three things happens: ctx is
// cancelled (returns ctx.Err()), fn returns an error (returns that error), or the consumer
// falls too far behind and is dropped (returns ErrSubscriberLagged). A nil prefix matches
// every key.
//
// fn runs synchronously on this goroutine, so a slow fn slows only this feed, never the
// writers: a backlog past the buffer drops the subscription rather than stalling commits.
// The Change slices passed to fn are owned by the callback and safe to retain. Only fully
// committed, durable mutations are delivered, so the feed never shows a write that a crash
// would have rolled back.
func (d *DB) Subscribe(ctx context.Context, prefix []byte, fn func([]Change) error) error {
	sub := &subscription{
		prefix: append([]byte(nil), prefix...),
		ch:     make(chan []Change, subscribeBuffer),
		done:   make(chan struct{}),
	}

	d.subsMu.Lock()
	if d.subsClosed {
		d.subsMu.Unlock()
		return ErrClosed
	}
	if d.subs == nil {
		d.subs = make(map[*subscription]struct{})
	}
	if d.subClosed == nil {
		d.subClosed = make(chan struct{})
	}
	d.subs[sub] = struct{}{}
	closed := d.subClosed
	d.subsMu.Unlock()

	defer func() {
		d.subsMu.Lock()
		delete(d.subs, sub)
		d.subsMu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-closed:
			return ErrClosed
		case <-sub.done:
			return ErrSubscriberLagged
		case batch := <-sub.ch:
			if err := fn(batch); err != nil {
				return err
			}
		}
	}
}

// publish fans a committed batch out to every subscription whose prefix it matches. It
// runs under d.mu at the tail of applyCommitted and takes subsMu, so the lock order is
// always d.mu then subsMu. It builds each subscriber's view by copying only the matching
// entries, then does a non-blocking send: a subscription whose buffer is full is marked
// lagged and removed, so one stuck consumer can neither block the writer nor be silently
// starved (spec 15 §7). Deleting the current key while ranging the map is allowed in Go.
func (d *DB) publish(b *engine.WriteBatch, v uint64) {
	d.subsMu.Lock()
	defer d.subsMu.Unlock()
	if len(d.subs) == 0 {
		return
	}
	entries := b.Entries()
	for sub := range d.subs {
		batch := changesFor(entries, sub.prefix, v)
		if len(batch) == 0 {
			continue
		}
		select {
		case sub.ch <- batch:
		default:
			sub.lag()
			delete(d.subs, sub)
		}
	}
}

// changesFor projects the batch's entries into the Changes a prefix subscriber should see,
// copying the key and value bytes so the callback may retain them. Range deletes match on
// their lower bound. The paired range-end marker carries no new information for the feed,
// so it is skipped; the range-begin entry already carries the whole interval.
func changesFor(entries []engine.BatchEntry, prefix []byte, v uint64) []Change {
	var out []Change
	for _, e := range entries {
		ik := e.InternalKey
		uk := format.UserKey(ik)
		if !bytes.HasPrefix(uk, prefix) {
			continue
		}
		switch format.KindOf(ik) {
		case format.KindSet:
			out = append(out, Change{Kind: ChangeSet, Key: clone(uk), Value: clone(e.Value), Version: v})
		case format.KindDelete:
			out = append(out, Change{Kind: ChangeDelete, Key: clone(uk), Version: v})
		case format.KindMerge:
			out = append(out, Change{Kind: ChangeMerge, Key: clone(uk), Value: clone(e.Value), Version: v})
		case format.KindRangeBegin:
			out = append(out, Change{Kind: ChangeRangeDelete, Key: clone(uk), Value: clone(e.Value), Version: v})
		}
	}
	return out
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
