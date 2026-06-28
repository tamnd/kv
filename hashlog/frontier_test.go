package hashlog

import (
	"os"
	"path/filepath"
	"testing"
)

// dialTunables builds a durable store with a small page and resident budget, at a
// chosen durability dial, so seals and spills both happen during a modest workload.
func dialTunables(path string, d Durability) Tunables {
	return Tunables{
		Shards:                4,
		PageSize:              512,
		ExtentSize:            512,
		ResidentPagesPerShard: 2,
		Path:                  path,
		Durability:            d,
	}
}

// maxFrontier returns the highest durable frontier across the store's shards.
func maxFrontier(s *Store) int64 {
	var m int64
	for _, sh := range s.shards {
		if f := sh.frontier.Load(); f > m {
			m = f
		}
	}
	return m
}

// TestM3NoneNeverSyncs confirms the None dial issues no device barrier and never
// advances any frontier, even though pages spill to extents.
func TestM3NoneNeverSyncs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "none.hlog")
	s, err := New(dialTunables(path, DurabilityNone))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 4000; i++ {
		if err := s.Set(key(i), []byte("a-value-of-some-length")); err != nil {
			t.Fatal(err)
		}
	}
	if s.Spilled() == 0 {
		t.Fatal("None test did not spill; raise the workload")
	}
	if got := s.df.syncCount.Load(); got != 0 {
		t.Fatalf("None issued %d syncs, want 0", got)
	}
	if f := maxFrontier(s); f != 0 {
		t.Fatalf("None advanced a frontier to %d, want 0 (advances only on fsync)", f)
	}
}

// TestM3FullSyncsEverySet confirms the Full dial issues exactly one barrier per SET
// and that after each SET the frontier has reached its record.
func TestM3FullSyncsEverySet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "full.hlog")
	s, err := New(dialTunables(path, DurabilityFull))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const n = 3000
	for i := 0; i < n; i++ {
		if err := s.Set(key(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
		// The store-wide LSN equals the number of sets so far, and the shard that took
		// this set must have a frontier at least that high (its record is synced).
		lsn := s.df.lsn.Load()
		if maxFrontier(s) < int64(lsn) {
			t.Fatalf("after set %d the frontier %d is below the LSN %d", i, maxFrontier(s), lsn)
		}
	}
	if got := s.df.syncCount.Load(); got != n {
		t.Fatalf("Full issued %d syncs for %d sets, want one each", got, n)
	}
}

// TestM3NormalAdvancesOnSealOnly confirms the Normal dial does not sync on a bare
// append, only on a seal: a run of records that fits in the first page advances no
// frontier, and the frontier advances once a roll seals a page.
func TestM3NormalAdvancesOnSealOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "normal.hlog")
	// One shard so every key lands on the same log and the seal count is deterministic.
	tun := Tunables{Shards: 1, PageSize: 512, ExtentSize: 512, ResidentPagesPerShard: 8, Path: path, Durability: DurabilityNormal}
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sh := s.shards[0]

	// Append a handful of small records that all fit in page 0 (no roll yet).
	for i := 0; sh.tailPage == 0; i++ {
		if err := s.Set(key(i), []byte("x")); err != nil {
			t.Fatal(err)
		}
		if sh.tailPage == 0 {
			if s.df.syncCount.Load() != 0 {
				t.Fatalf("Normal synced before any seal (set %d)", i)
			}
			if sh.frontier.Load() != 0 {
				t.Fatalf("Normal advanced the frontier on a bare write (set %d)", i)
			}
		}
	}
	// The set that rolled to page 1 sealed page 0, so exactly one sync has happened and
	// the frontier sits at the highest LSN of the sealed page (below the new tail
	// record, which is not yet sealed).
	if got := s.df.syncCount.Load(); got != 1 {
		t.Fatalf("after the first seal there are %d syncs, want 1", got)
	}
	sealedMax := sh.pageMaxLSN[0]
	if f := sh.frontier.Load(); f != sealedMax {
		t.Fatalf("frontier after first seal is %d, want the sealed page max %d", f, sealedMax)
	}
}

// TestM3FrontierMonotonic drives a Full workload and asserts no shard's frontier ever
// moves backward (I4).
func TestM3FrontierMonotonic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mono.hlog")
	s, err := New(dialTunables(path, DurabilityFull))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	last := make([]int64, len(s.shards))
	for i := 0; i < 6000; i++ {
		if err := s.Set(key(i%1500), value4(i)); err != nil {
			t.Fatal(err)
		}
		for sid, sh := range s.shards {
			f := sh.frontier.Load()
			if f < last[sid] {
				t.Fatalf("shard %d frontier went backward: %d then %d", sid, last[sid], f)
			}
			last[sid] = f
		}
	}
}

func value4(i int) []byte {
	b := make([]byte, 8+i%40)
	for j := range b {
		b[j] = byte('a' + (i+j)%26)
	}
	return b
}

// scanShardFrozenLSNs walks a shard's pages in seal order over a frozen file image and
// returns the set of CRC-valid LSNs found. It uses the live shard's page-to-extent
// mapping to locate the bytes (recovery rebuilds that mapping itself at M5; here it is
// a scaffold). A record that fails to decode ends the scan of that page, which is the
// torn-tail rule.
func scanShardFrozenLSNs(t *testing.T, frozen []byte, sh *shard) map[uint64]bool {
	t.Helper()
	found := map[uint64]bool{}
	for pid := int64(0); pid <= sh.tailPage; pid++ {
		ext := sh.pageExtent[pid]
		if ext < 0 {
			continue // never written to the file
		}
		base := sh.df.logBodyOffset(ext)
		end := base + int64(sh.pageFill[pid])
		if end > int64(len(frozen)) {
			continue // not present in this frozen image
		}
		region := frozen[base:end]
		for pos := 0; pos < len(region); {
			lsn, _, _, _, n, err := decodeDurableRecord(region[pos:])
			if err != nil || n == 0 {
				break
			}
			found[lsn] = true
			pos += n
		}
	}
	return found
}

// TestM3CrashScaffoldFrontierWithinSynced is the crash harness scaffold: it freezes
// the file image at every fsync boundary and confirms the durable frontier never
// names an LSN whose record is not present and CRC-valid in the frozen image. In
// other words, the frontier never runs ahead of what reached the device.
func TestM3CrashScaffoldFrontierWithinSynced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash.hlog")
	// One shard makes the LSNs contiguous (1..N all on this shard), so the check is
	// exact: every LSN at or below the frontier must be in the frozen image.
	tun := Tunables{Shards: 1, PageSize: 256, ExtentSize: 256, ResidentPagesPerShard: 2, Path: path, Durability: DurabilityFull}
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sh := s.shards[0]

	var frozen []byte
	// At each barrier, snapshot the file as it stands (the writes are already in the
	// file; the barrier is what makes them durable), then run the real sync. The
	// frontier is advanced only after this returns, so the snapshot is the synced image
	// the about-to-advance frontier must be covered by.
	s.df.syncHook = func(f *os.File) error {
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		buf := make([]byte, fi.Size())
		if _, err := f.ReadAt(buf, 0); err != nil {
			return err
		}
		frozen = buf
		// Every LSN already claimed by the frontier must be in this image.
		got := scanShardFrozenLSNs(t, buf, sh)
		for lsn := int64(1); lsn <= sh.frontier.Load(); lsn++ {
			if !got[uint64(lsn)] {
				t.Fatalf("frontier %d but LSN %d not in the synced image", sh.frontier.Load(), lsn)
			}
		}
		return platformSyncData(f)
	}

	const n = 2000
	for i := 0; i < n; i++ {
		if err := s.Set(key(i), value4(i)); err != nil {
			t.Fatal(err)
		}
	}

	// Final image: every LSN up to the frontier is present and CRC-valid, and the
	// frontier reached the last write (Full acknowledged all n).
	got := scanShardFrozenLSNs(t, frozen, sh)
	f := sh.frontier.Load()
	if f != int64(n) {
		t.Fatalf("final frontier %d, want %d (Full acknowledged every set)", f, n)
	}
	for lsn := int64(1); lsn <= f; lsn++ {
		if !got[uint64(lsn)] {
			t.Fatalf("LSN %d at or below the frontier is missing from the synced image", lsn)
		}
	}
}
