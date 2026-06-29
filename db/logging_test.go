package db

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/kv/vfs"
)

// debugLogger returns a slog logger that writes text records into buf at DEBUG level,
// so a test can capture the routine checkpoint events as well as the louder lifecycle
// and slow-op ones.
func debugLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestLoggingLifecycleEvents drives a database through its lifecycle with a logger
// attached and checks each milestone event lands: the open, an explicit checkpoint fold,
// and the close. These are the events an operator reads to confirm the database came up,
// folded its log, and shut down cleanly.
func TestLoggingLifecycleEvents(t *testing.T) {
	var buf bytes.Buffer
	d := openMem(t, Options{Logger: debugLogger(&buf)})
	if err := d.Update(func(txn *Txn) error { return txn.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		`msg="kv: database opened"`,
		`engine=f2`,
		`msg="kv: checkpoint folded wal"`,
		`msg="kv: database closed"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("lifecycle log missing %q\n--- got ---\n%s", want, out)
		}
	}
}

// TestLoggingRecoveryReplay closes a database with committed but un-checkpointed writes,
// then reopens it against the same file system with a logger, so crash recovery must
// replay the WAL tail. The recovery event should report the batches it replayed.
func TestLoggingRecoveryReplay(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "recover.kv", Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := range 3 {
		k := []byte{byte(i)}
		if err := d.Update(func(txn *Txn) error { return txn.Set(k, []byte("v")) }); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	// Close without a checkpoint, so the committed batches stay in the WAL for the next
	// open to replay rather than being folded into the main file.
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var buf bytes.Buffer
	d2, err := Open(fs, "recover.kv", Options{Logger: debugLogger(&buf)})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { d2.Close() })

	out := buf.String()
	if !strings.Contains(out, `msg="kv: wal recovery"`) {
		t.Errorf("recovery log missing the wal recovery event\n--- got ---\n%s", out)
	}
	if !strings.Contains(out, "replayed_batches=3") {
		t.Errorf("recovery log should report 3 replayed batches\n--- got ---\n%s", out)
	}
}

// TestSlowOpLogsCommitAndRead arms the slow-op log with a one-nanosecond threshold so
// every commit and read trips it, then checks both events render with their identifying
// fields: the commit's version and key range, the read's operation and key.
func TestSlowOpLogsCommitAndRead(t *testing.T) {
	var buf bytes.Buffer
	d := openMem(t, Options{Logger: debugLogger(&buf), SlowOpThreshold: time.Nanosecond})
	if err := d.Update(func(txn *Txn) error { return txn.Set([]byte("alpha"), []byte("v")) }); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := d.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("alpha"))
		return err
	}); err != nil {
		t.Fatalf("get: %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		`msg="kv: slow commit"`,
		"key_lo=alpha",
		"key_hi=alpha",
		`msg="kv: slow read"`,
		"op=get",
		"key=alpha",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("slow-op log missing %q\n--- got ---\n%s", want, out)
		}
	}
}

// TestSlowOpSilentByDefault confirms that a logger without a slow-op threshold emits the
// lifecycle events but never a slow-op line, since the threshold is the only thing that
// arms slow-op timing and the default is off.
func TestSlowOpSilentByDefault(t *testing.T) {
	var buf bytes.Buffer
	d := openMem(t, Options{Logger: debugLogger(&buf)})
	if err := d.Update(func(txn *Txn) error { return txn.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := d.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("k"))
		return err
	}); err != nil {
		t.Fatalf("get: %v", err)
	}

	out := buf.String()
	if strings.Contains(out, "slow commit") || strings.Contains(out, "slow read") {
		t.Errorf("slow-op log should be silent with no threshold set\n--- got ---\n%s", out)
	}
}

// TestLoggingDisabledIsSilent checks the default no-logger build emits nothing: a nil
// logger is the off switch, and every emitter guards on it.
func TestLoggingDisabledIsSilent(t *testing.T) {
	d := openMem(t, Options{SlowOpThreshold: time.Nanosecond})
	if d.slowOpEnabled() {
		t.Error("slow-op timing should stay off when no logger is set, even with a threshold")
	}
	if err := d.Update(func(txn *Txn) error { return txn.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("set: %v", err)
	}
}
