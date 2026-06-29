package hashlog

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

// TestNewContextCancelledRecovery asserts that opening a durable file whose recovery has
// work to do honours a cancelled context: NewContext returns the cancellation error
// instead of replaying the log to completion.
func TestNewContextCancelledRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctx.hlog")
	// Build a file that has something to recover: write keys and close, which commits a
	// checkpoint and leaves a durable log the reopen would replay.
	s, err := New(ckptTunables(path, DurabilityNormal))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if err := s.Set(key(i), value4(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s2, err := NewContext(ctx, ckptTunables(path, DurabilityNormal))
	if !errors.Is(err, context.Canceled) {
		if s2 != nil {
			s2.Close()
		}
		t.Fatalf("NewContext on a cancelled context returned err=%v, want context.Canceled", err)
	}
	if s2 != nil {
		t.Fatal("NewContext returned a store despite cancellation")
	}

	// A live context must still open the same file cleanly, so the cancellation path did
	// not corrupt anything and the recovery itself works.
	s3, err := NewContext(context.Background(), ckptTunables(path, DurabilityNormal))
	if err != nil {
		t.Fatalf("NewContext with a live context failed to recover: %v", err)
	}
	defer s3.Close()
	for i := 0; i < 500; i++ {
		got, ok, err := s3.Get(key(i))
		if err != nil {
			t.Fatal(err)
		}
		if !ok || string(got) != string(value4(i)) {
			t.Fatalf("key %d after recovery: ok=%v got=%q", i, ok, got)
		}
	}
}

// TestCheckpointContextCancelled asserts that a cancelled context stops a checkpoint
// before it commits: it returns the cancellation error, the generation does not advance,
// and a later checkpoint with a live context still commits normally.
func TestCheckpointContextCancelled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckptctx.hlog")
	s, err := New(ckptTunables(path, DurabilityNormal))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 500; i++ {
		if err := s.Set(key(i), value4(i)); err != nil {
			t.Fatal(err)
		}
	}

	genBefore := s.df.sb.generation
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.CheckpointContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("CheckpointContext on a cancelled context returned err=%v, want context.Canceled", err)
	}
	if got := s.df.sb.generation; got != genBefore {
		t.Fatalf("generation advanced from %d to %d on a cancelled checkpoint", genBefore, got)
	}

	// A live context commits: the cancelled attempt left the store ready, not wedged.
	if err := s.CheckpointContext(context.Background()); err != nil {
		t.Fatalf("CheckpointContext with a live context failed: %v", err)
	}
	if got := s.df.sb.generation; got != genBefore+1 {
		t.Fatalf("generation %d after a committed checkpoint, want %d", got, genBefore+1)
	}
}
