package server

import (
	"context"

	"github.com/tamnd/kv"
)

// This file adds the watch half of the operation surface to Service: a change watch that
// yields committed mutations as they happen. It is pull-shaped at this layer: Service drives a
// caller-supplied callback per batch and never buffers the whole feed, so a watch that runs for
// a day costs the server one batch of memory at a time. The HTTP adapter turns each callback
// into an SSE event; the binary adapter turns the same callback into framed messages. Keeping
// the iteration here, above the wire, means both protocols stream with identical ordering and
// stop semantics.

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
