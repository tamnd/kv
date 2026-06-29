package f2

import (
	"testing"
	"time"
)

// waitFor polls until cond holds or the deadline passes, the small helper the background
// checkpoint tests use to wait for an asynchronous commit without a fixed sleep.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestCheckpointRunsInBackground proves L7: crossing the checkpoint byte threshold no
// longer runs the checkpoint on the writer. The crossing Set only pokes the background
// loop, which commits the snapshot off the write path. We never call Checkpoint, yet the
// superblock comes to point at a committed snapshot.
func TestCheckpointRunsInBackground(t *testing.T) {
	tn := durableTunables(t, DurabilityNone)
	tn.CheckpointBytes = 64 << 10 // small, so a few thousand sets cross it
	s := mustOpenT(t, tn)

	if s.ckptSig == nil {
		t.Fatal("background checkpoint loop not started for a durable store with a byte threshold")
	}
	const keys = 6000
	for i := 0; i < keys; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "the background checkpoint to commit a snapshot", func() bool {
		s.df.mu.Lock()
		defer s.df.mu.Unlock()
		return s.df.snapRoot >= 0
	})
}

// TestBackgroundCheckpointBoundsRecovery ties L7 to S6: the background checkpoint that the
// write threshold triggers leaves a snapshot on disk, so a crash that loses no flushed
// bytes recovers delta-bound, replaying far fewer records than the full history.
func TestBackgroundCheckpointBoundsRecovery(t *testing.T) {
	tn := durableTunables(t, DurabilityNone)
	tn.CheckpointBytes = 64 << 10
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const keys = 8000
	for i := 0; i < keys; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Overwrite to pile up history and keep crossing the threshold, so the background
	// checkpoint advances its cut well past the first writes.
	for round := 0; round < 4; round++ {
		for i := 0; i < keys; i++ {
			if err := s.Set(tkey(i), tval(i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	waitFor(t, "a background snapshot", func() bool {
		s.df.mu.Lock()
		defer s.df.mu.Unlock()
		return s.df.snapRoot >= 0
	})
	// Quiesce: a tiny delta of fresh writes after the last background checkpoint, then
	// flush and crash so the reopen sees the snapshot plus only that small delta.
	for i := 0; i < 200; i++ {
		if err := s.Set(tkey(i), altVal(i)); err != nil {
			t.Fatal(err)
		}
	}
	flushTails(t, s)
	crash(t, s)

	r := mustOpenT(t, tn)
	total := int64(keys) * 5 // every key written five times before the delta
	st := r.Stats()
	if st.RecoveryRecords >= total {
		t.Fatalf("RecoveryRecords = %d, want well under the %d-record history (delta-bound)", st.RecoveryRecords, total)
	}
	if st.Keys != keys {
		t.Fatalf("Keys = %d after reopen, want %d", st.Keys, keys)
	}
	for i := 0; i < 200; i++ {
		got, ok, err := r.Get(tkey(i))
		if err != nil || !ok || string(got) != string(altVal(i)) {
			t.Fatalf("key %d = (%q, %v, %v), want the delta value", i, got, ok, err)
		}
	}
}

// TestCloseWritesSnapshot checks that a clean close commits an index snapshot, so the
// reopen installs the index and replays nothing rather than the whole generation.
func TestCloseWritesSnapshot(t *testing.T) {
	tn := durableTunables(t, DurabilityNone)
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const keys = 3000
	for i := 0; i < keys; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := mustOpenT(t, tn)
	st := r.Stats()
	if st.RecoveryRecords != 0 {
		t.Fatalf("RecoveryRecords = %d after a clean close, want 0 (snapshot install, no delta)", st.RecoveryRecords)
	}
	if st.Keys != keys {
		t.Fatalf("Keys = %d after reopen, want %d", st.Keys, keys)
	}
	for i := 0; i < keys; i++ {
		got, ok, err := r.Get(tkey(i))
		if err != nil || !ok || string(got) != string(tval(i)) {
			t.Fatalf("key %d = (%q, %v, %v), want %q", i, got, ok, err, tval(i))
		}
	}
}
