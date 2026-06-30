package f2

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/kv/vfs"
)

// TestSealSyncDecouplesBarriersFromSeals proves the redesign-v2 doc 09 change: under
// the Normal dial a page seal no longer fsyncs the file once per sealed page. With the
// byte cadence set well above the page size, a run that seals many pages issues far
// fewer device barriers than pages, because the barrier follows the bytes-written
// threshold, not the page-roll rate. This is the mechanism that lets a smaller page win
// on space (it seals more often) without the fsync count following it up.
func TestSealSyncDecouplesBarriersFromSeals(t *testing.T) {
	const (
		pageSize  = 4096
		syncEvery = 64 * 1024 // one barrier per 16 sealed pages of cadence
	)
	s := mustOpenT(t, Tunables{
		Shards:                1, // one shard so every seal lands on the same log
		PageSize:              pageSize,
		ResidentPagesPerShard: 4,
		Path:                  filepath.Join(t.TempDir(), "f2.db"),
		Durability:            DurabilityNormal,
		SyncBytes:             syncEvery,
		SyncInterval:          -1,        // no time flusher: the cadence under test is byte-only
		CheckpointBytes:       1 << 40,   // never auto-checkpoint, so seals are the only barriers
	})
	// Count barriers without paying a real device flush, so the test is fast and the
	// count reflects the cadence rather than the host disk.
	s.df.syncHook = func(vfs.File) error { return nil }

	// Write enough distinct keys to seal many pages. Each record is well under a page,
	// so a page holds a few dozen records and the run rolls dozens of pages.
	const writes = 4000
	for i := 0; i < writes; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	sh := s.shards[0]
	sh.mu.RLock()
	sealed := sh.log.npages - sh.log.mutableWindow // pages that have rolled out and sealed
	sh.mu.RUnlock()
	if sealed < 20 {
		t.Fatalf("test too small: only %d pages sealed, want many", sealed)
	}
	barriers := s.df.syncCount.Load()

	// The cadence is one barrier per syncEvery/pageSize sealed pages, so the barrier
	// count must be far below the sealed-page count. Allow generous slack for the
	// initial superblock write and rounding; the point is barriers << seals.
	wantCeil := int64(sealed)*pageSize/syncEvery + 5
	if barriers > wantCeil {
		t.Fatalf("seal-sync did not decouple: %d barriers for %d sealed pages, want <= %d",
			barriers, sealed, wantCeil)
	}
	if barriers >= int64(sealed) {
		t.Fatalf("no decoupling: %d barriers >= %d sealed pages", barriers, sealed)
	}
	t.Logf("%d sealed pages flushed by %d barriers (cadence %d bytes)", sealed, barriers, syncEvery)

	// The deferred barrier must not lose data: every key is still readable, and a clean
	// Close checkpoints so a reopen finds them all.
	for i := 0; i < writes; i++ {
		got, found := get(t, s, tkey(i))
		if !found || string(got) != string(tval(i)) {
			t.Fatalf("key %d: found=%v got=%q", i, found, got)
		}
	}
}

// TestSealSyncCleanCloseRecovers confirms the cadence keeps the clean-close guarantee:
// even when most writes were never barriered by the seal cadence, Close checkpoints and
// a reopen reads back every key. The deferred fsync only changes the crash-loss window,
// never what a clean shutdown preserves.
func TestSealSyncCleanCloseRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f2.db")
	tn := Tunables{
		Shards:       4,
		PageSize:     4096,
		Path:         path,
		Durability:   DurabilityNormal,
		SyncBytes:    1 << 30, // so large the seal cadence never fires on its own
		SyncInterval: -1,      // and no time flusher either
	}
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const writes = 2000
	for i := 0; i < writes; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := s.Close(); err != nil { // checkpoints, so nothing is lost
		t.Fatalf("Close: %v", err)
	}

	s2 := mustOpenT(t, tn)
	for i := 0; i < writes; i++ {
		got, found := get(t, s2, tkey(i))
		if !found || string(got) != string(tval(i)) {
			t.Fatalf("after reopen, key %d: found=%v got=%q", i, found, got)
		}
	}
}
