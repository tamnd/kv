package db

import (
	"context"
	"sync"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// recordingTracer captures the name of every span started, in order, and tracks parent
// nesting through the context so a test can assert which phases nested under which. It is
// the stand-in for a real OpenTelemetry adapter: kv calls StartSpan, the tracer records,
// and End is a no-op beyond marking the span closed.
type recordingTracer struct {
	mu      sync.Mutex
	names   []string
	parents map[string]string // child name -> parent name, by the most recent of each
	open    int
	maxOpen int
}

type ctxKey struct{}

type recordingSpan struct {
	t      *recordingTracer
	name   string
	closed bool
}

func (s *recordingSpan) End() {
	s.t.mu.Lock()
	defer s.t.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.t.open--
	}
}

func (t *recordingTracer) StartSpan(ctx context.Context, name string) (context.Context, Span) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.names = append(t.names, name)
	if t.parents == nil {
		t.parents = map[string]string{}
	}
	if parent, ok := ctx.Value(ctxKey{}).(string); ok {
		t.parents[name] = parent
	}
	t.open++
	if t.open > t.maxOpen {
		t.maxOpen = t.open
	}
	return context.WithValue(ctx, ctxKey{}, name), &recordingSpan{t: t, name: name}
}

func (t *recordingTracer) saw(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, n := range t.names {
		if n == name {
			return true
		}
	}
	return false
}

func openTraced(t *testing.T, tr Tracer) *DB {
	t.Helper()
	d, err := Open(vfs.NewOS(), t.TempDir()+"/traced.kv", Options{Tracer: tr})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return d
}

// TestTraceCommitPhases confirms a commit starts a kv.commit span with kv.commit.durable
// and kv.commit.apply nested under it, the I/O-versus-engine split the spec asks for.
func TestTraceCommitPhases(t *testing.T) {
	tr := &recordingTracer{}
	d := openTraced(t, tr)
	defer d.Close()

	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}

	for _, name := range []string{"kv.commit", "kv.commit.durable", "kv.commit.apply"} {
		if !tr.saw(name) {
			t.Fatalf("missing span %q; saw %v", name, tr.names)
		}
	}
	if got := tr.parents["kv.commit.durable"]; got != "kv.commit" {
		t.Fatalf("durable parent = %q, want kv.commit", got)
	}
	if got := tr.parents["kv.commit.apply"]; got != "kv.commit" {
		t.Fatalf("apply parent = %q, want kv.commit", got)
	}
}

// TestTraceCheckpointAndCompaction confirms an explicit checkpoint and a maintenance round
// each start their own span.
func TestTraceCheckpointAndCompaction(t *testing.T) {
	tr := &recordingTracer{}
	d := openTraced(t, tr)
	defer d.Close()

	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if _, err := d.Maintain(0); err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if !tr.saw("kv.checkpoint") {
		t.Fatalf("missing kv.checkpoint span; saw %v", tr.names)
	}
	if !tr.saw("kv.compaction") {
		t.Fatalf("missing kv.compaction span; saw %v", tr.names)
	}
}

// TestTraceReadSpan confirms a point read starts a kv.get span.
func TestTraceReadSpan(t *testing.T) {
	tr := &recordingTracer{}
	d := openTraced(t, tr)
	defer d.Close()

	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := d.Get([]byte("k")); err != nil {
		t.Fatalf("get: %v", err)
	}
	if !tr.saw("kv.get") {
		t.Fatalf("missing kv.get span; saw %v", tr.names)
	}
}

// TestTraceEverySpanClosed confirms every started span is ended: after a commit, checkpoint
// and read the tracer has no span left open, so a defer never leaks a span.
func TestTraceEverySpanClosed(t *testing.T) {
	tr := &recordingTracer{}
	d := openTraced(t, tr)
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if _, err := d.Get([]byte("k")); err != nil {
		t.Fatalf("get: %v", err)
	}
	d.Close()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.open != 0 {
		t.Fatalf("%d spans left open, want 0", tr.open)
	}
	if tr.maxOpen < 2 {
		t.Fatalf("max concurrent open spans = %d, want >= 2 (nested commit phases)", tr.maxOpen)
	}
}

// TestNoTracerNoPanic confirms the default build with no tracer runs the same operations
// without starting spans and without panicking on the nil tracer.
func TestNoTracerNoPanic(t *testing.T) {
	d, err := Open(vfs.NewOS(), t.TempDir()+"/untraced.kv", Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()
	if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if _, err := d.Get([]byte("k")); err != nil {
		t.Fatalf("get: %v", err)
	}
}
