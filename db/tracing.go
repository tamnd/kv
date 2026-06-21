package db

import "context"

// This file is the database's optional tracing surface (spec 19 §3). A Tracer is the seam
// an operator wires to OpenTelemetry, or any span backend, without kv taking a dependency
// on it: kv calls StartSpan around each operation and its major phases, and the host's
// implementation adapts those calls to real spans. Tracing is off unless Options.Tracer
// is set, so the default build starts no spans and pays only a nil check at each site,
// the same discipline the logging surface follows. The spans live here at the db layer,
// above the engine seam, so the lsm and btree cores stay free of a tracing dependency;
// the phases traced are exactly the ones the host already brackets for metrics, so the
// I/O-vs-engine-vs-compaction attribution the spec asks for falls out of timing windows
// that already exist.

// Tracer is the hook kv calls to start a span around an operation or an internal phase.
// It is off by default and enabled with WithTracer. Implementations must be safe for
// concurrent use: commits, reads, and maintenance on different goroutines start spans at
// the same time. An implementation adapts these calls to its own tracer, for example
// returning a Span whose End closes an OpenTelemetry span, so kv never imports the tracer.
type Tracer interface {
	// StartSpan begins a span named name as a child of whatever span the context carries,
	// and returns a context to thread into nested calls plus the Span to End when the phase
	// finishes. name is a stable dotted identifier ("kv.commit", "kv.commit.durable",
	// "kv.checkpoint", "kv.compaction", "kv.get") so a backend can aggregate one phase
	// across many operations.
	StartSpan(ctx context.Context, name string) (context.Context, Span)
}

// Span is one started span. End closes it and is called exactly once, normally through a
// deferred endSpan. A Span need not be safe for concurrent End.
type Span interface {
	End()
}

// startSpan begins a span when a tracer is configured and is a no-op otherwise: it returns
// the context unchanged and a nil Span when tracing is off, so every call site is a single
// nil check on the disabled path. The returned context carries the new span for nested
// startSpan calls to parent under.
func (d *DB) startSpan(ctx context.Context, name string) (context.Context, Span) {
	if d.tracer == nil {
		return ctx, nil
	}
	return d.tracer.StartSpan(ctx, name)
}

// endSpan closes a span, tolerating the nil a disabled tracer returns so callers can defer
// it unconditionally.
func endSpan(s Span) {
	if s != nil {
		s.End()
	}
}
