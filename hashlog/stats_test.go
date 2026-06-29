package hashlog

import (
	"os"
	"path/filepath"
	"testing"
)

// TestStatsMemoryOnly checks the snapshot a memory-only store reports: every key is live
// and resident, nothing spills, no barrier ever fires, and with no compaction there is no
// dead space, so the space amplification reads the no-garbage default of 1.0.
func TestStatsMemoryOnly(t *testing.T) {
	s := mustStore(t, DefaultTunables())
	const keys = 2000
	for i := 0; i < keys; i++ {
		if err := s.Set(key(i), varValue(i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	st := s.Stats()
	if st.LiveKeys != keys {
		t.Fatalf("LiveKeys = %d, want %d", st.LiveKeys, keys)
	}
	if st.ResidentPages == 0 {
		t.Fatal("ResidentPages = 0, want the appended pages resident")
	}
	if st.SpilledPages != 0 {
		t.Fatalf("SpilledPages = %d, want 0 in memory-only mode", st.SpilledPages)
	}
	if st.FsyncCount != 0 || st.FsyncAvgLatency != 0 {
		t.Fatalf("FsyncCount = %d, FsyncAvgLatency = %v, want 0 with no durable file", st.FsyncCount, st.FsyncAvgLatency)
	}
	if st.SpaceAmplification != 1.0 {
		t.Fatalf("SpaceAmplification = %v, want 1.0 with no dead bytes", st.SpaceAmplification)
	}
	if st.MinShardKeys > st.MaxShardKeys {
		t.Fatalf("MinShardKeys %d > MaxShardKeys %d", st.MinShardKeys, st.MaxShardKeys)
	}
	if st.MaxShardKeys > keys || st.MaxShardKeys <= 0 {
		t.Fatalf("MaxShardKeys = %d, want in (0, %d]", st.MaxShardKeys, keys)
	}
	// The per-shard counts must sum back to the total, the consistency the read-lock walk
	// guarantees.
	if st.LiveKeys != s.Len() {
		t.Fatalf("Stats LiveKeys %d disagrees with Len %d", st.LiveKeys, s.Len())
	}
}

// TestStatsDeadBytesAndAmplification drives the durable space accounting: a full-overwrite
// pass with size changes kills every first-version record, so dead bytes climb and the
// space amplification rises above 1.0. A compaction plus a checkpoint then reclaims the
// dead space, and the amplification falls back toward 1.0 with the backlog cleared.
func TestStatsDeadBytesAndAmplification(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stats.hlog")
	s := mustStore(t, compactTunables(path, DurabilityNormal))

	const keys = 400
	for i := 0; i < keys; i++ {
		if err := s.Set(key(i), varValue(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < keys; i++ {
		if err := s.Set(key(i), varValue(i+7)); err != nil { // different size, appends and kills the old record
			t.Fatal(err)
		}
	}

	dirty := s.Stats()
	if dirty.DeadBytes == 0 {
		t.Fatal("DeadBytes = 0 after a full overwrite pass, want the killed first versions counted")
	}
	if dirty.LiveBytes != dirty.LiveExtentBytes-dirty.DeadBytes {
		t.Fatalf("LiveBytes %d != LiveExtentBytes %d - DeadBytes %d", dirty.LiveBytes, dirty.LiveExtentBytes, dirty.DeadBytes)
	}
	if dirty.SpaceAmplification <= 1.0 {
		t.Fatalf("SpaceAmplification = %v, want > 1.0 with dead bytes present", dirty.SpaceAmplification)
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	clean := s.Stats()
	if clean.Compaction.CompactedExtents == 0 {
		t.Fatal("Stats.Compaction.CompactedExtents = 0 after a compaction that retired wholly-dead pages")
	}
	if clean.SpaceAmplification >= dirty.SpaceAmplification {
		t.Fatalf("SpaceAmplification did not fall after compaction: dirty %v, clean %v", dirty.SpaceAmplification, clean.SpaceAmplification)
	}
	if clean.CompactionBacklog != 0 {
		t.Fatalf("CompactionBacklog = %d after the checkpoint freed the holes, want 0", clean.CompactionBacklog)
	}
}

// TestStatsFsyncAccounting checks the durability counters: under the Full dial every Set
// issues a barrier, so the fsync count tracks the writes and the average latency is the
// accumulated wall time over that count. The barrier is stubbed to a no-op so the test
// measures the accounting, not the platform's F_FULLFSYNC latency.
func TestStatsFsyncAccounting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fsync.hlog")
	s := mustStore(t, compactTunables(path, DurabilityFull))
	s.df.syncHook = func(*os.File) error { return nil }

	const writes = 200
	for i := 0; i < writes; i++ {
		if err := s.Set(key(i), varValue(i)); err != nil {
			t.Fatal(err)
		}
	}
	st := s.Stats()
	if st.FsyncCount < writes {
		t.Fatalf("FsyncCount = %d, want at least %d under the Full dial", st.FsyncCount, writes)
	}
	// The reported average must equal the raw accumulated nanos over the raw count, the
	// derivation Stats does.
	wantAvg := s.df.syncNanos.Load() / st.FsyncCount
	if int64(st.FsyncAvgLatency) != wantAvg {
		t.Fatalf("FsyncAvgLatency = %d ns, want %d ns (syncNanos/count)", int64(st.FsyncAvgLatency), wantAvg)
	}
}

// TestStatsRecoveryDuration checks that a reopen times its recovery and surfaces a non-zero
// duration through both Stats and RecoveryStats, while a fresh memory-only store reports
// zero (nothing was replayed).
func TestStatsRecoveryDuration(t *testing.T) {
	if mem := mustStore(t, DefaultTunables()).Stats(); mem.RecoveryDuration != 0 {
		t.Fatalf("memory-only RecoveryDuration = %v, want 0", mem.RecoveryDuration)
	}

	path := filepath.Join(t.TempDir(), "recover.hlog")
	first := mustStore(t, compactTunables(path, DurabilityNormal))
	const keys = 300
	for i := 0; i < keys; i++ {
		if err := first.Set(key(i), varValue(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := mustStore(t, compactTunables(path, DurabilityNormal))
	st := reopened.Stats()
	if st.RecoveryDuration <= 0 {
		t.Fatalf("RecoveryDuration = %v after a reopen that replayed records, want > 0", st.RecoveryDuration)
	}
	if st.RecoveryDuration != st.Recovery.Duration {
		t.Fatalf("Stats.RecoveryDuration %v disagrees with Recovery.Duration %v", st.RecoveryDuration, st.Recovery.Duration)
	}
	if st.LiveKeys != keys {
		t.Fatalf("LiveKeys = %d after reopen, want %d", st.LiveKeys, keys)
	}
}
