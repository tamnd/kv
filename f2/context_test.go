package f2

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

// TestNewContextCancelledRecovery asserts that reopening a file whose replay has work to
// do honours a cancelled context: NewContext returns the cancellation error instead of
// replaying the log to completion, and a later open with a live context still recovers.
func TestNewContextCancelledRecovery(t *testing.T) {
	tn := Tunables{
		Shards:                16,
		PageSize:              4096,
		ResidentPagesPerShard: 2,
		Path:                  filepath.Join(t.TempDir(), "f2.db"),
		Durability:            DurabilityNormal,
	}
	s, err := New(tn)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 800; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s2, err := NewContext(ctx, tn)
	if !errors.Is(err, context.Canceled) {
		if s2 != nil {
			s2.Close()
		}
		t.Fatalf("NewContext on a cancelled context returned err=%v, want context.Canceled", err)
	}
	if s2 != nil {
		t.Fatal("NewContext returned a store despite cancellation")
	}

	s3, err := NewContext(context.Background(), tn)
	if err != nil {
		t.Fatalf("NewContext with a live context failed to recover: %v", err)
	}
	defer s3.Close()
	for i := 0; i < 800; i++ {
		got, ok := get(t, s3, tkey(i))
		if !ok || string(got) != string(tval(i)) {
			t.Fatalf("key %d after recovery: ok=%v got=%q", i, ok, got)
		}
	}
}

// TestCheckpointContextCancelled asserts that a cancelled context stops a checkpoint
// before it commits: it returns the cancellation error, no snapshot is written, and a
// later checkpoint with a live context still commits.
func TestCheckpointContextCancelled(t *testing.T) {
	s := mustOpenT(t, durableTunables(t, DurabilityNormal))
	for i := 0; i < 400; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	if s.df.snapRoot >= 0 {
		t.Fatalf("a snapshot committed before the test checkpoint (snapRoot %d)", s.df.snapRoot)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.CheckpointContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("CheckpointContext on a cancelled context returned err=%v, want context.Canceled", err)
	}
	if s.df.snapRoot >= 0 {
		t.Fatalf("a cancelled checkpoint committed a snapshot (snapRoot %d)", s.df.snapRoot)
	}

	if err := s.CheckpointContext(context.Background()); err != nil {
		t.Fatalf("CheckpointContext with a live context failed: %v", err)
	}
	if s.df.snapRoot < 0 {
		t.Fatal("snapRoot still -1 after a committed checkpoint")
	}
}
