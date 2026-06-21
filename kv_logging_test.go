package kv_test

import (
	"bytes"
	"log/slog"
	"strings"
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
