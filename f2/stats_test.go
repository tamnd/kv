package f2

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/kv/vfs"
)

// TestStatsAuditFieldsMemoryOnly checks the snapshot a memory-only store reports: every
// key is live, nothing is durable so no barrier ever fires and recovery never runs, and
// with unique keys nothing is stranded, so the space amplification reads the no-garbage
// default of 1.0.
func TestStatsAuditFieldsMemoryOnly(t *testing.T) {
	s := mustOpenT(t, DefaultTunables())
	const keys = 5000
	for i := 0; i < keys; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	st := s.Stats()
	if st.Keys != keys {
		t.Fatalf("Keys = %d, want %d", st.Keys, keys)
	}
	if st.LiveBytes != st.LogBytes-st.DeadBytes {
		t.Fatalf("LiveBytes %d != LogBytes %d - DeadBytes %d", st.LiveBytes, st.LogBytes, st.DeadBytes)
	}
	if st.SpaceAmplification != 1.0 {
		t.Fatalf("SpaceAmplification = %v, want 1.0 with no dead bytes", st.SpaceAmplification)
	}
	if st.FsyncCount != 0 || st.FsyncAvgLatency != 0 {
		t.Fatalf("FsyncCount = %d, FsyncAvgLatency = %v, want 0 with no durable file", st.FsyncCount, st.FsyncAvgLatency)
	}
	if st.RecoveryDuration != 0 || st.RecoveryRecords != 0 {
		t.Fatalf("RecoveryDuration = %v, RecoveryRecords = %d, want 0 in memory-only mode", st.RecoveryDuration, st.RecoveryRecords)
	}
	if st.MinShardKeys > st.MaxShardKeys {
		t.Fatalf("MinShardKeys %d > MaxShardKeys %d", st.MinShardKeys, st.MaxShardKeys)
	}
	if st.MaxShardKeys > keys || st.MaxShardKeys <= 0 {
		t.Fatalf("MaxShardKeys = %d, want in (0, %d]", st.MaxShardKeys, keys)
	}
}

// TestStatsAuditAmplification drives the durable space accounting: f2 appends every write,
// so overwriting the key set strands the old records and the space amplification climbs
// over 1.0. A compaction then rewrites the over-threshold shards and the amplification
// falls back toward 1.0. The None dial keeps the test off the slow F_FULLFSYNC path.
func TestStatsAuditAmplification(t *testing.T) {
	s := mustOpenT(t, durableTunables(t, DurabilityNone))
	const keys = 2000
	for i := 0; i < keys; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	for round := 0; round < 3; round++ { // overwrite every key thrice so the dead fraction clears 0.5
		for i := 0; i < keys; i++ {
			if err := s.Set(tkey(i), tval(i)); err != nil {
				t.Fatal(err)
			}
		}
	}

	dirty := s.Stats()
	if dirty.DeadBytes == 0 {
		t.Fatal("DeadBytes = 0 after overwriting every key, want the stranded versions counted")
	}
	if dirty.SpaceAmplification <= 1.0 {
		t.Fatalf("SpaceAmplification = %v, want > 1.0 with dead bytes present", dirty.SpaceAmplification)
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	clean := s.Stats()
	if clean.SpaceAmplification >= dirty.SpaceAmplification {
		t.Fatalf("SpaceAmplification did not fall after compaction: dirty %v, clean %v", dirty.SpaceAmplification, clean.SpaceAmplification)
	}
	// Every key still reads its last value, so the rewrite preserved the live set.
	if clean.Keys != keys {
		t.Fatalf("Keys = %d after compaction, want %d", clean.Keys, keys)
	}
}

// TestStatsAuditFsync checks the durability counters: under the Full dial every Set issues
// a barrier, so the fsync count tracks the writes and the average latency is the
// accumulated wall time over that count. The barrier is stubbed to a no-op so the test
// measures the accounting, not the platform's F_FULLFSYNC latency.
func TestStatsAuditFsync(t *testing.T) {
	s := mustOpenT(t, durableTunables(t, DurabilityFull))
	s.df.syncHook = func(vfs.File) error { return nil }

	const writes = 300
	for i := 0; i < writes; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	st := s.Stats()
	if st.FsyncCount < writes {
		t.Fatalf("FsyncCount = %d, want at least %d under the Full dial", st.FsyncCount, writes)
	}
	wantAvg := s.df.syncNanos.Load() / st.FsyncCount
	if int64(st.FsyncAvgLatency) != wantAvg {
		t.Fatalf("FsyncAvgLatency = %d ns, want %d ns (syncNanos/count)", int64(st.FsyncAvgLatency), wantAvg)
	}
}

// TestStatsAuditRecovery checks that a reopen times its recovery and counts the records it
// replays, surfacing both through Stats, while a fresh memory-only store reports zero.
func TestStatsAuditRecovery(t *testing.T) {
	if mem := mustOpenT(t, DefaultTunables()).Stats(); mem.RecoveryDuration != 0 || mem.RecoveryRecords != 0 {
		t.Fatalf("memory-only recovery stats = (%v, %d), want (0, 0)", mem.RecoveryDuration, mem.RecoveryRecords)
	}

	tn := Tunables{
		Shards:                8,
		PageSize:              4096,
		ResidentPagesPerShard: 2,
		Path:                  filepath.Join(t.TempDir(), "recover.db"),
		Durability:            DurabilityNormal,
	}
	first, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const keys = 1500
	for i := 0; i < keys; i++ {
		if err := first.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Flush every tail to disk, then crash without a clean close. A clean close would
	// write an index snapshot and the reopen would replay nothing; crashing with no
	// snapshot forces the full replay this test measures.
	flushTails(t, first)
	crash(t, first)

	reopened := mustOpenT(t, tn)
	st := reopened.Stats()
	if st.RecoveryDuration <= 0 {
		t.Fatalf("RecoveryDuration = %v after a reopen that replayed records, want > 0", st.RecoveryDuration)
	}
	if st.RecoveryRecords <= 0 {
		t.Fatalf("RecoveryRecords = %d after a reopen that replayed records, want > 0", st.RecoveryRecords)
	}
	if st.Keys != keys {
		t.Fatalf("Keys = %d after reopen, want %d", st.Keys, keys)
	}
}
