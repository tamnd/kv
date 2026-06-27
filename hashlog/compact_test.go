package hashlog

import (
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// readWholeFile reads an open durable file's whole image, the frozen-image capture the
// crash-window test recovers from. It reads by offset so it does not disturb the file
// position.
func readWholeFile(t *testing.T, f *os.File) []byte {
	t.Helper()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, fi.Size())
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	return buf
}

// writeWholeFile writes a captured image to a fresh path so it can be reopened as an
// independent durable file and recovered.
func writeWholeFile(t *testing.T, path string, buf []byte) {
	t.Helper()
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

// compact_test.go is the M8 gate (spec 2070 doc 06 section 10): the four tests the
// milestone owes are an oracle (a compacting workload equals the reference model with no
// resurrection), space reclamation (compaction plus a checkpoint actually returns extents
// and bounds the file under churn), crash safety (every window across compaction and the
// checkpoint that frees its extents recovers to the durable prefix), and a concurrent
// race test (compaction against writes and checkpoints under the race detector, including
// the abandoned-copy path). The profile under test is always the durable eviction-possible
// one (inPlaceTunables), the only profile that compacts.

// compactTunables is a durable eviction-possible store sized so a modest churn workload
// spills, seals whole pages below ReadOnlyAddress, and accumulates the dead bytes
// compaction reclaims. A small resident budget keeps it in the larger-than-memory regime
// compaction targets; the 2 KiB extent keeps the index snapshot to a handful of extents,
// so the inline superblock free list (which has no overflow chain until a later milestone,
// doc 03 section 4) comfortably holds the free set across a checkpoint.
func compactTunables(path string, d Durability) Tunables {
	return Tunables{
		Shards:                4,
		PageSize:              2048,
		ExtentSize:            2048,
		ResidentPagesPerShard: 2,
		Path:                  path,
		Durability:            d,
	}
}

// checkConservationM8 is the M8 form of the extent-conservation invariant (doc 03 section
// 9 I7, extended for the snapshot run and the compaction holes M8 adds). Every extent the
// file holds is accounted for exactly once: backing a live page, on the allocator free
// stack, holding the committed index snapshot, or a hole a compaction retired that no
// checkpoint has freed yet (per-shard pendingFree plus the store pendingRetry). It also
// proves no extent backs two pages (aliasing) and none is both in use and free.
func checkConservationM8(t *testing.T, s *Store) {
	t.Helper()
	if s.df == nil {
		return
	}
	inUse := map[int64]bool{}
	for _, sh := range s.shards {
		sh.mu.RLock()
		for _, ext := range sh.pageExtent {
			if ext < 0 {
				continue
			}
			if inUse[ext] {
				sh.mu.RUnlock()
				t.Fatalf("extent %d backs two pages (aliasing)", ext)
			}
			inUse[ext] = true
		}
		sh.mu.RUnlock()
	}
	count, free := s.df.alloc.counts()
	freeSet := map[int64]bool{}
	for _, id := range free {
		if inUse[id] {
			t.Fatalf("extent %d is both in use and free", id)
		}
		if freeSet[id] {
			t.Fatalf("extent %d appears twice on the free stack", id)
		}
		freeSet[id] = true
	}
	holes := 0
	liveCont := int64(0)
	for _, sh := range s.shards {
		sh.mu.RLock()
		holes += len(sh.pendingFree)
		liveCont += sh.liveOversizeExtents
		sh.mu.RUnlock()
	}
	holes += len(s.pendingRetry)
	snap := int64(0)
	if s.df.snapRoot >= 0 {
		snap = s.df.snapCount
	}
	// liveCont are the oversize-cont extents a live spanning value occupies (M9, doc 03
	// section 7). They are allocated but in no page directory, so they would otherwise read
	// as a leak; a superseded chain leaves liveCont and joins the holes via pendingFree, so
	// there is no window where a cont extent is counted twice or not at all.
	accounted := int64(len(inUse)) + int64(len(free)) + snap + int64(holes) + liveCont
	if accounted != count {
		t.Fatalf("conservation: inUse %d + free %d + snapshot %d + holes %d + liveCont %d = %d != count %d",
			len(inUse), len(free), snap, holes, liveCont, accounted, count)
	}
}

// varValue returns a value whose length depends on i, so an overwrite of a key with a
// different i appends a new record (different size, so the in-place same-size path does
// not take it) and credits the old record dead. That is how the test manufactures the
// dead space compaction exists to reclaim (doc 06 section 2.1).
func varValue(i int) []byte {
	n := 40 + (i*37)%200
	b := make([]byte, n)
	for j := range b {
		b[j] = byte('A' + (i+j)%26)
	}
	return b
}

// liveInUse counts the extents currently backing a live page across all shards (an
// extent a compaction retired but a checkpoint has not yet freed is a hole, so it is not
// counted). It is the working-set size the file should track under churn.
func liveInUse(s *Store) int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		for _, ext := range sh.pageExtent {
			if ext >= 0 {
				n++
			}
		}
		sh.mu.RUnlock()
	}
	return n
}

// TestM8CompactReclaimsDeadSpace is the headline space-reclamation gate (doc 06 section
// 10.4): a workload writes a fixed key set, then overwrites every key so the pages that
// held the first versions go wholly dead, and a compaction pass plus a checkpoint returns
// those extents to the allocator. It asserts the dead extents were retired and freed, the
// allocator's in-use count fell, and every live key still reads its last value (no live
// data lost to the copy, no stale data resurrected).
func TestM8CompactReclaimsDeadSpace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reclaim.hlog")
	s := mustStore(t, compactTunables(path, DurabilityNormal))
	m := newModel()

	const keys = 400
	for i := 0; i < keys; i++ {
		v := varValue(i)
		if err := s.Set(key(i), v); err != nil {
			t.Fatal(err)
		}
		m.set(key(i), v)
	}
	// Checkpoint so the live set is captured and the frontier advances; then overwrite every
	// key with a different-size value, which kills every first-version record. The pages that
	// held the first versions are now wholly dead.
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < keys; i++ {
		v := varValue(i + 7) // different size than the first write, so it appends and kills the old record
		if err := s.Set(key(i), v); err != nil {
			t.Fatal(err)
		}
		m.set(key(i), v)
	}
	if s.Spilled() == 0 {
		t.Fatal("workload did not spill; compaction would not exercise the disk path")
	}

	inUseBefore := s.df.alloc.inUse()
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	cs := s.CompactionStats()
	if cs.CompactedExtents == 0 {
		t.Fatal("Compact retired no extents, but a full overwrite pass left wholly-dead pages")
	}
	if cs.RelocatedRecords == 0 {
		t.Fatal("Compact relocated no records, but the live set sits among the dead pages")
	}
	// The retired extents are holes now, freed durably by the checkpoint.
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := s.CompactionStats().FreedExtents; got == 0 {
		t.Fatal("checkpoint freed no compacted extents")
	}
	inUseAfter := s.df.alloc.inUse()
	if inUseAfter >= inUseBefore {
		t.Fatalf("in-use extents did not fall: before %d, after %d", inUseBefore, inUseAfter)
	}
	checkConservationM8(t, s)

	// Every live key still reads its last value: the copy preserved the live records and the
	// retire dropped only dead ones.
	for k, want := range m.live {
		v, ok, err := s.Get([]byte(k))
		if err != nil || !ok {
			t.Fatalf("Get(%q) ok=%v err=%v after compaction", k, ok, err)
		}
		if string(v) != string(want) {
			t.Fatalf("Get(%q) = %q, want last write %q", k, v, want)
		}
	}
	if got := s.Len(); got != len(m.live) {
		t.Fatalf("Len() = %d, want %d after compaction", got, len(m.live))
	}
}

// TestM8FreedExtentsBoundFileUnderChurn proves the freed extents are reused, so the file
// does not grow without bound under sustained overwrite churn (doc 06 section 8, the write
// amplification and total-store-size claim). It runs many rounds of overwrite-everything
// then compact-then-checkpoint and asserts the allocator's in-use count after the last
// round is no larger than after the first: the working set is fixed, so once compaction is
// running the file tracks it instead of growing per overwrite.
func TestM8FreedExtentsBoundFileUnderChurn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "churn.hlog")
	s := mustStore(t, compactTunables(path, DurabilityNormal))
	m := newModel()

	const keys = 300
	for i := 0; i < keys; i++ {
		v := varValue(i)
		if err := s.Set(key(i), v); err != nil {
			t.Fatal(err)
		}
		m.set(key(i), v)
	}

	var firstRoundInUse int64
	const rounds = 8
	for r := 0; r < rounds; r++ {
		for i := 0; i < keys; i++ {
			v := varValue(i + r + 1) // size varies per round so every overwrite appends
			if err := s.Set(key(i), v); err != nil {
				t.Fatal(err)
			}
			m.set(key(i), v)
		}
		if err := s.Compact(); err != nil {
			t.Fatalf("round %d Compact: %v", r, err)
		}
		if err := s.Checkpoint(); err != nil {
			t.Fatalf("round %d Checkpoint: %v", r, err)
		}
		inUse := s.df.alloc.inUse()
		if r == 0 {
			firstRoundInUse = inUse
		} else if inUse > firstRoundInUse+int64(s.t.Shards) {
			// Allow a shard's worth of slack for tail pages mid-fill; the point is it is flat,
			// not climbing by keys*round.
			t.Fatalf("file grew under churn: round 0 in-use %d, round %d in-use %d", firstRoundInUse, r, inUse)
		}
		checkConservationM8(t, s)
	}

	for k, want := range m.live {
		v, ok, _ := s.Get([]byte(k))
		if !ok || string(v) != string(want) {
			t.Fatalf("Get(%q) = %q ok=%v, want %q after churn", k, v, ok, want)
		}
	}
}

// TestM8CompactedEqualsModelOracle is the differential oracle (doc 06 section 10.1): a
// randomized set/delete workload with compaction and checkpoints folded in is checked
// against the reference map at every step, and a final reopen is checked too. Compaction
// must be invisible to the key/value contract: it relocates and reclaims bytes but never
// changes what a key reads, and never resurrects a deleted or overwritten value.
func TestM8CompactedEqualsModelOracle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oracle.hlog")
	tun := compactTunables(path, DurabilityNormal)
	s := mustStore(t, tun)
	m := newModel()
	rng := rand.New(rand.NewSource(99))

	const keys = 500
	const steps = 20000
	for step := 0; step < steps; step++ {
		k := key(rng.Intn(keys))
		switch rng.Intn(10) {
		case 0, 1: // delete
			if err := s.Delete(k); err != nil {
				t.Fatal(err)
			}
			m.del(k)
		default: // set with a size that varies so overwrites append and create dead space
			v := varValue(rng.Intn(300))
			if err := s.Set(k, v); err != nil {
				t.Fatal(err)
			}
			m.set(k, v)
		}
		// Fold compaction and checkpoints in periodically.
		switch {
		case step%2500 == 2499:
			if err := s.Compact(); err != nil {
				t.Fatalf("step %d Compact: %v", step, err)
			}
		case step%2500 == 1249:
			if err := s.Checkpoint(); err != nil {
				t.Fatalf("step %d Checkpoint: %v", step, err)
			}
		}
		// Spot-check the just-touched key against the model every step.
		if want, ok := m.get(k); ok {
			v, found, err := s.Get(k)
			if err != nil || !found || string(v) != string(want) {
				t.Fatalf("step %d Get(%q) = %q found=%v err=%v, want %q", step, k, v, found, err, want)
			}
		} else {
			if _, found, _ := s.Get(k); found {
				t.Fatalf("step %d Get(%q) found a value, model says deleted", step, k)
			}
		}
	}

	if s.CompactionStats().CompactedExtents == 0 {
		t.Fatal("oracle workload never compacted an extent")
	}
	// Full sweep against the model, then a reopen (recovery) and another full sweep, so the
	// compacted on-disk state recovers to exactly the live set.
	for k, want := range m.live {
		v, ok, _ := s.Get([]byte(k))
		if !ok || string(v) != string(want) {
			t.Fatalf("final Get(%q) = %q ok=%v, want %q", k, v, ok, want)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	rs := reopen(t, s, tun)
	defer rs.Close()
	assertStoreMatches(t, rs, m)
	checkConservationM8(t, rs)
}

// TestM8RecoverAfterCompactAndCheckpoint is the clean-window recovery (doc 06 section
// 7.4, the window after the checkpoint that frees the compacted extents commits): once a
// checkpoint has freed the retired extents, a reopen rebuilds the live set from the new
// snapshot and the holes the compaction left are tolerated, not read as corruption.
func TestM8RecoverAfterCompactAndCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recover-clean.hlog")
	tun := compactTunables(path, DurabilityNormal)
	s := mustStore(t, tun)
	m := newModel()

	const keys = 350
	for i := 0; i < keys; i++ {
		v := varValue(i)
		s.Set(key(i), v)
		m.set(key(i), v)
	}
	for i := 0; i < keys; i++ {
		v := varValue(i + 11)
		s.Set(key(i), v)
		m.set(key(i), v)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if s.CompactionStats().FreedExtents == 0 {
		t.Fatal("no extents freed; the hole-tolerant recovery path would not be exercised")
	}

	rs := reopen(t, s, tun)
	defer rs.Close()
	assertStoreMatches(t, rs, m)
	checkConservationM8(t, rs)
	// The recovered store keeps compacting correctly: another full overwrite plus compaction
	// still lands the model, proving recovery rebuilt the dead-byte tally and the frontier.
	for i := 0; i < keys; i++ {
		v := varValue(i + 23)
		rs.Set(key(i), v)
		m.set(key(i), v)
	}
	if err := rs.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := rs.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for k, want := range m.live {
		v, ok, _ := rs.Get([]byte(k))
		if !ok || string(v) != string(want) {
			t.Fatalf("post-recovery Get(%q) = %q ok=%v, want %q", k, v, ok, want)
		}
	}
}

// TestM8CrashBetweenCompactAndCheckpoint is the load-bearing crash window (doc 06 section
// 7.4, the window after a compaction pass but before the checkpoint that frees its
// extents): the source extents are retired in memory but still on disk and still
// referenced by the last committed checkpoint's snapshot, so a crash there recovers to the
// pre-compaction durable prefix with nothing lost and nothing resurrected. The test
// captures the file image right after Compact (before the freeing checkpoint) and recovers
// it.
func TestM8CrashBetweenCompactAndCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crash-window.hlog")
	tun := compactTunables(path, DurabilityFull)
	s := mustStore(t, tun)
	m := newModel()

	const keys = 300
	for i := 0; i < keys; i++ {
		v := varValue(i)
		s.Set(key(i), v)
		m.set(key(i), v)
	}
	// Commit a checkpoint: this is the durable prefix recovery will fall back to. Its
	// snapshot points at these records.
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Overwrite everything (Full syncs each, so all are durable), then compact. The
	// compaction retires the now-dead first-version extents in memory but does NOT free them
	// durably, because no checkpoint follows.
	for i := 0; i < keys; i++ {
		v := varValue(i + 5)
		s.Set(key(i), v)
		m.set(key(i), v)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if s.CompactionStats().CompactedExtents == 0 {
		t.Fatal("nothing compacted; the crash window would be trivial")
	}

	// Freeze the file image as it stands now: post-compaction, pre-freeing-checkpoint. The
	// retired source extents are still in the file, intact, with their headers, and the last
	// committed checkpoint is the one before compaction.
	frozen := readWholeFile(t, s.df.f)
	cp := filepath.Join(dir, "frozen.hlog")
	writeWholeFile(t, cp, frozen)

	rt := tun
	rt.Path = cp
	rs, err := New(rt)
	if err != nil {
		t.Fatalf("recover post-compaction image: %v", err)
	}
	defer rs.Close()
	// Every acknowledged write (Full synced them all) survives: the recovered store is the
	// full model. The relocated copies in the tail carry their original LSNs, so they are
	// last-writer-wins no-ops over the records the snapshot and log already establish, and
	// the still-present source extents reconstruct the live set.
	assertStoreMatches(t, rs, m)
	checkConservationM8(t, rs)
}

// TestM8TombstoneDiscardNoResurrection drives the tombstone-discard rule (doc 06 section
// 3.4): a key is written, deleted, checkpointed (so the deletion is baked into the snapshot
// and the tombstone's LSN falls at or below the committed frontier), and then its page is
// compacted. The tombstone is discardable, so it is dropped rather than copied, and the
// deleted key must stay absent across a reopen, never resurrected by a surviving older data
// record.
func TestM8TombstoneDiscardNoResurrection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tomb.hlog")
	tun := compactTunables(path, DurabilityNormal)
	s := mustStore(t, tun)
	m := newModel()

	const keys = 400
	// Write then immediately delete each key in the first half. Each key's data record and its
	// tombstone land next to each other on the same shard pages, so those pages carry a dead
	// data record (credited dead by the delete) alongside the tombstone. The data bytes push
	// the page over the dead-fraction threshold, and the tombstone rides along on the selected
	// page, which is how a tombstone reaches the discard path (a page of nothing but tombstones
	// has no dead data, so it never gets selected, the known M8 limitation).
	for i := 0; i < keys/2; i++ {
		v := varValue(i)
		s.Set(key(i), v)
		s.Delete(key(i))
		m.set(key(i), v)
		m.del(key(i))
	}
	// Checkpoint so the deletions are baked into the snapshot and the committed frontier
	// passes the tombstone LSNs, making them discardable.
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Write the surviving half. This advances the tail well past the delete pages, sealing them
	// below ReadOnlyAddress so compaction is allowed to select them.
	for i := keys / 2; i < keys; i++ {
		v := varValue(i + 9)
		s.Set(key(i), v)
		m.set(key(i), v)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if s.CompactionStats().DiscardedTombstones == 0 {
		t.Fatal("no tombstones discarded, but half the keys were deleted below the frontier")
	}

	// Deleted keys are absent, survivors read their last value, both live and across a reopen.
	checkAbsentAndLive := func(st *Store, label string) {
		for i := 0; i < keys; i++ {
			v, ok, err := st.Get(key(i))
			if err != nil {
				t.Fatalf("%s Get(key %d): %v", label, i, err)
			}
			if i < keys/2 {
				if ok {
					t.Fatalf("%s deleted key %d resurrected as %q", label, i, v)
				}
			} else {
				want, _ := m.get(key(i))
				if !ok || string(v) != string(want) {
					t.Fatalf("%s survivor key %d = %q ok=%v, want %q", label, i, v, ok, want)
				}
			}
		}
	}
	checkAbsentAndLive(s, "live")
	rs := reopen(t, s, tun)
	defer rs.Close()
	checkAbsentAndLive(rs, "recovered")
}

// TestM8ConcurrentCompactWriteCheckpointRace is the race gate (doc 06 section 10.3): many
// writer goroutines churn disjoint key ranges while a compactor goroutine and a
// checkpointer goroutine run concurrently, all under the race detector. Disjoint ranges
// keep the final state deterministic (each key's last write is known) while the overlap of
// compaction with live overwrites exercises the compare-and-publish abandoned path (a copy
// stranded by a racing overwrite). After the goroutines join, every key reads its known
// final value and the store recovers to it.
func TestM8ConcurrentCompactWriteCheckpointRace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "race.hlog")
	tun := compactTunables(path, DurabilityNormal)
	s := mustStore(t, tun)

	const writers = 4
	const perWriter = 150
	const iters = 4

	var writersWg, bgWg sync.WaitGroup
	stop := make(chan struct{})

	// Writers: each owns key range [w*perWriter, (w+1)*perWriter) and overwrites every key
	// iters times with varying sizes, ending on a known final value (iter iters-1). Disjoint
	// ranges keep the final state deterministic while the overwrites race the compactor.
	for w := 0; w < writers; w++ {
		writersWg.Add(1)
		go func(w int) {
			defer writersWg.Done()
			base := w * perWriter
			for it := 0; it < iters; it++ {
				for i := 0; i < perWriter; i++ {
					if err := s.Set(key(base+i), varValue(base+i+it*7)); err != nil {
						t.Errorf("writer %d set: %v", w, err)
						return
					}
				}
			}
		}(w)
	}

	// Compactor, checkpointer, and a reader loop until the writers finish, contending with
	// the live overwrites so the abandoned-copy path and the locked read path are exercised.
	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := s.Compact(); err != nil {
					t.Errorf("concurrent Compact: %v", err)
					return
				}
			}
		}
	}()
	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := s.Checkpoint(); err != nil {
					t.Errorf("concurrent Checkpoint: %v", err)
					return
				}
			}
		}
	}()
	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		rd := s.NewReader()
		for {
			select {
			case <-stop:
				return
			default:
				rd.Get(key(rand.Intn(writers * perWriter)))
			}
		}
	}()

	writersWg.Wait()
	close(stop)
	bgWg.Wait()

	// Final deterministic check: each key's last write was iter iters-1.
	m := newModel()
	for w := 0; w < writers; w++ {
		base := w * perWriter
		for i := 0; i < perWriter; i++ {
			m.set(key(base+i), varValue(base+i+(iters-1)*7))
		}
	}
	for k, want := range m.live {
		v, ok, err := s.Get([]byte(k))
		if err != nil || !ok || string(v) != string(want) {
			t.Fatalf("post-race Get(%q) = %q ok=%v err=%v, want %q", k, v, ok, err, want)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	rs := reopen(t, s, tun)
	defer rs.Close()
	assertStoreMatches(t, rs, m)
	checkConservationM8(t, rs)
}

// TestM8CompactOffProfileIsNoOp checks the gating (doc 06 section 6, the profile that
// compacts): Compact errors in memory-only mode (no durable file to reclaim) and is a
// silent no-op on the full-resident durable profile, which keeps every page resident and
// never spills, so it has no inPlace shard to compact.
func TestM8CompactOffProfileIsNoOp(t *testing.T) {
	// Memory-only: Compact has no file and returns an error rather than pretending to work.
	mem := mustStore(t, DefaultTunables())
	if err := mem.Compact(); err == nil {
		t.Fatal("Compact on a memory-only store should error")
	}

	// Full-resident durable (no resident cap): every page stays resident, no shard is
	// inPlace, so Compact does nothing and retires nothing.
	path := filepath.Join(t.TempDir(), "fullres.hlog")
	s := mustStore(t, Tunables{Shards: 2, PageSize: 4096, ExtentSize: 4096, ResidentPagesPerShard: 0, Path: path, Durability: DurabilityNormal})
	for i := 0; i < 200; i++ {
		s.Set(key(i), varValue(i))
		s.Set(key(i), varValue(i+1)) // overwrites, but a full-resident store never compacts them
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact on full-resident durable should be a no-op, got %v", err)
	}
	if got := s.CompactionStats().CompactedExtents; got != 0 {
		t.Fatalf("full-resident store compacted %d extents, want 0", got)
	}
}
