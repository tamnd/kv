package hashlog

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// crash stops the background checkpoint loop without the clean-close final checkpoint and
// drops the file descriptor, modelling a process that died after its last background
// checkpoint and before any orderly shutdown. Setting closed before stopping the loop keeps
// an in-flight signal from starting a fresh checkpoint, and bgWG.Wait lets any checkpoint
// already running finish (a valid commit) before the descriptor goes away.
func crash(t *testing.T, s *Store) {
	t.Helper()
	if s.bgStop != nil {
		s.closed.Store(true)
		close(s.bgStop)
		s.bgWG.Wait()
	}
	if err := s.df.f.Close(); err != nil {
		t.Fatalf("crash close: %v", err)
	}
}

// waitForGeneration polls until the committed checkpoint generation reaches want or the
// deadline passes. It reads through CheckpointStats, the race-free atomic generation
// mirror, so it never touches the superblock the background committer reassigns.
func waitForGeneration(t *testing.T, s *Store, want uint64) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if s.CheckpointStats().Generation >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for checkpoint generation %d, got %d", want, s.CheckpointStats().Generation)
}

// bgTunables is a durable config with small pages and a small checkpoint threshold, so a
// few thousand small writes cross the threshold and the background loop checkpoints several
// times. The dial is passed in: Full makes every write durable for the crash tests.
func bgTunables(path string, d Durability) Tunables {
	return Tunables{
		Shards:                4,
		PageSize:              512,
		ExtentSize:            512,
		ResidentPagesPerShard: 2,
		Path:                  path,
		Durability:            d,
		CheckpointBytes:       8 << 10,
	}
}

// TestBackgroundCheckpointRunsOffWritePath proves audit L7: crossing the checkpoint byte
// threshold no longer runs the checkpoint on the writer. The crossing SET only pokes the
// background loop, which commits the snapshot off the write path. We never call Checkpoint,
// yet the committed generation advances on its own.
func TestBackgroundCheckpointRunsOffWritePath(t *testing.T) {
	tn := bgTunables(filepath.Join(t.TempDir(), "bg.hlog"), DurabilityNone)
	s, err := New(tn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.ckptSig == nil {
		t.Fatal("background checkpoint loop not started for a durable store with a byte threshold")
	}
	for i := 0; i < 6000; i++ {
		k := []byte(fmt.Sprintf("k%d", i%1500))
		v := []byte(fmt.Sprintf("v%d", i))
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	// The writes crossed the 8 KiB threshold many times, so a background checkpoint must
	// have committed at least one generation without any explicit Checkpoint call.
	waitForGeneration(t, s, 1)
}

// TestBackgroundCheckpointBoundsRecovery ties L7 to the delta-bound recovery: the
// background checkpoint the write threshold triggers leaves a committed snapshot, so a
// crash that loses no synced bytes replays only the records past the last cut, far fewer
// than the whole history.
func TestBackgroundCheckpointBoundsRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bgrecover.hlog")
	tn := bgTunables(path, DurabilityFull)
	s, err := New(tn)
	if err != nil {
		t.Fatal(err)
	}
	m := newModel()
	const total = 8000
	put := func(i int) {
		k := []byte(fmt.Sprintf("k%d", i%1200))
		v := []byte(fmt.Sprintf("v%d-%d", i, i%1200))
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		m.set(k, v)
	}
	for i := 0; i < total; i++ {
		put(i)
	}
	// Wait for the background loop to commit, then crash. Under Full every write is synced,
	// so the crash keeps the whole acknowledged set; recovery replays only the tail past
	// the last background checkpoint's frontier.
	waitForGeneration(t, s, 1)
	crash(t, s)

	r, err := New(tn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r.Close()
	assertStoreMatches(t, r, m)
	if rs := r.RecoveryStats(); rs.ReplayedRecords >= total {
		t.Fatalf("ReplayedRecords = %d, want well under the %d-record history (delta-bound)", rs.ReplayedRecords, total)
	}
}

// TestCloseWritesCheckpoint checks that a clean close commits a final index snapshot, so
// the reopen installs the index and replays nothing rather than the whole log. This is the
// flip side of the crash tests: an orderly shutdown leaves a delta of zero.
func TestCloseWritesCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bgclose.hlog")
	tn := bgTunables(path, DurabilityNormal)
	s, err := New(tn)
	if err != nil {
		t.Fatal(err)
	}
	m := newModel()
	for i := 0; i < 4000; i++ {
		k := []byte(fmt.Sprintf("k%d", i%1000))
		v := []byte(fmt.Sprintf("v%d", i))
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		m.set(k, v)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := New(tn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r.Close()
	assertStoreMatches(t, r, m)
	if rs := r.RecoveryStats(); rs.ReplayedRecords != 0 {
		t.Fatalf("ReplayedRecords = %d after a clean close, want 0 (final checkpoint, no delta)", rs.ReplayedRecords)
	}
}

// TestCloseIsIdempotent checks the close guard added with the background loop: a second
// Close is a no-op, not a double free of the file or the loop.
func TestCloseIsIdempotent(t *testing.T) {
	tn := bgTunables(filepath.Join(t.TempDir(), "idem.hlog"), DurabilityNone)
	s, err := New(tn)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := s.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close should be a no-op: %v", err)
	}
}
