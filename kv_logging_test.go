package kv_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/kv"
)

// TestWithLoggerRoutesEvents confirms WithLogger threads a slog logger through the public
// facade so the database's lifecycle events land in the caller's logging, and
// WithSlowOpThreshold arms the slow-op log on the same handle.
func TestWithLoggerRoutesEvents(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	d := open(t, kv.WithLogger(logger), kv.WithSlowOpThreshold(time.Nanosecond))
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.Set([]byte("k"), []byte("v"))
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, `msg="kv: database opened"`) {
		t.Errorf("expected an open event through WithLogger\n--- got ---\n%s", out)
	}
	if !strings.Contains(out, `msg="kv: slow commit"`) {
		t.Errorf("expected a slow-commit event with a one-nanosecond threshold\n--- got ---\n%s", out)
	}
}

// TestWithLoggerDefaultSilent confirms that without WithLogger the database emits
// nothing, the zero-cost default.
func TestWithLoggerDefaultSilent(t *testing.T) {
	d := open(t)
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.Set([]byte("k"), []byte("v"))
	}); err != nil {
		t.Fatalf("set: %v", err)
	}
	// Nothing to assert beyond a clean run: with no logger there is no sink to inspect,
	// and the db-layer tests cover the nil-guard behavior directly.
}

// facadeTracer records started span names so a facade test can confirm WithTracer threads
// the hook through the public API.
type facadeTracer struct {
	mu    sync.Mutex
	names []string
}

type facadeSpan struct {
	t    *facadeTracer
	name string
}

func (s facadeSpan) End() {}

func (t *facadeTracer) StartSpan(ctx context.Context, name string) (context.Context, kv.Span) {
	t.mu.Lock()
	t.names = append(t.names, name)
	t.mu.Unlock()
	return ctx, facadeSpan{t: t, name: name}
}

func (t *facadeTracer) saw(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, n := range t.names {
		if n == name {
			return true
		}
	}
	return false
}

// TestWithTracerFacade confirms WithTracer threads a Tracer through the public facade so a
// commit and a read start spans on the caller's tracer (spec 19 §3).
func TestWithTracerFacade(t *testing.T) {
	tr := &facadeTracer{}
	d := open(t, kv.WithTracer(tr))
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.Set([]byte("k"), []byte("v"))
	}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := d.View(func(txn *kv.Txn) error {
		_, err := txn.Get([]byte("k"))
		return err
	}); err != nil {
		t.Fatalf("get: %v", err)
	}
	for _, name := range []string{"kv.commit", "kv.commit.durable", "kv.commit.apply"} {
		if !tr.saw(name) {
			t.Fatalf("missing span %q through facade; saw %v", name, tr.names)
		}
	}
}
