package hashlog

import (
	"bytes"
	"path/filepath"
	"testing"
)

// oversize_test.go is the M9 gate (spec 2070 doc 03 section 7, doc 08 M9 row): a value too
// large for one extent is stored as a chain of oversize-cont extents addressed through a
// descriptor in a home log record. The tests own the milestone's promises: a round trip
// across sizes that include a CRC straddling an extent boundary, that an overwrite or delete
// frees the old chain (checkpoint-gated, like a compaction hole), that an acknowledged
// oversize value recovers whole (I6 atomicity) and a half-written chain with no home record
// is reconciled free, that compaction of a home record preserves the descriptor and the
// value still reads, that conservation holds with live cont extents counted, and that the
// profiles without a spanning read path reject an oversize value at SET. The profile under
// test is the durable eviction-possible one (inPlaceTunables), the only one that stores
// oversize values.

// bigVal returns a deterministic value of n bytes keyed by seed, so an overwrite with a
// different seed is detectable and a recovered value can be checked byte for byte.
func bigVal(seed, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((seed*131 + i*7) % 251)
	}
	return b
}

// sumLiveCont totals the live oversize-cont extents across all shards, the per-shard
// accounting an oversize SET raises and a supersede or delete lowers.
func sumLiveCont(s *Store) int64 {
	var n int64
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += sh.liveOversizeExtents
		sh.mu.RUnlock()
	}
	return n
}

// sumPendingFree totals the per-shard pending-free lists, the holes (including cont extents a
// supersede retired) waiting for the next checkpoint to return them to the allocator.
func sumPendingFree(s *Store) int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += len(sh.pendingFree)
		sh.mu.RUnlock()
	}
	return n
}

// TestM9OversizeRoundTrip stores values across the size classes that matter (just over a
// page, several extents, and a length that puts the trailing CRC across an extent boundary)
// and reads each back byte for byte. It confirms the oversize path was taken (the counter
// climbed and live cont extents were charged) and that conservation holds with those cont
// extents counted.
func TestM9OversizeRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "roundtrip.hlog")
	s := mustStore(t, inPlaceTunables(path, DurabilityNormal))
	defer s.Close()

	// inPlaceTunables uses a 512-byte extent, so each of these overflows one extent and spans
	// a different number of cont extents. 510 puts the value end two bytes shy of the first
	// extent boundary, so its 4-byte trailing CRC straddles into the second cont extent, the
	// case the back-to-back payload layout exists to handle.
	sizes := []int{510, 1000, 1500, 4096, 9000}
	want := map[string][]byte{}
	for i, n := range sizes {
		k := key(i)
		v := bigVal(i+1, n)
		if err := s.Set(k, v); err != nil {
			t.Fatalf("Set oversize %d bytes: %v", n, err)
		}
		want[string(k)] = v
	}

	if got := s.OversizeValues(); got != int64(len(sizes)) {
		t.Fatalf("OversizeValues = %d, want %d", got, len(sizes))
	}
	if sumLiveCont(s) == 0 {
		t.Fatal("no live cont extents charged for the oversize values")
	}
	for ks, v := range want {
		got, ok, err := s.Get([]byte(ks))
		if err != nil || !ok {
			t.Fatalf("Get(%q) ok=%v err=%v", ks, ok, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("Get(%q) = %d bytes, want %d, content mismatch", ks, len(got), len(v))
		}
	}
	checkConservationM8(t, s)
}

// TestM9OversizeOverwriteAndDeleteFreeCont drives the chain lifecycle. An oversize value is
// stored, then overwritten (first by another oversize value, then by a small inline value),
// then a second oversize key is deleted. Each supersede must charge the old chain to the
// pending-free list and drop the live-cont count, and the next checkpoint must drain the
// pending list back to the allocator. Conservation holds at every step and the live values
// read correct.
func TestM9OversizeOverwriteAndDeleteFreeCont(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lifecycle.hlog")
	s := mustStore(t, inPlaceTunables(path, DurabilityNormal))
	defer s.Close()

	ka, kb := key(1), key(2)
	if err := s.Set(ka, bigVal(1, 1500)); err != nil { // 3 cont extents
		t.Fatal(err)
	}
	if err := s.Set(kb, bigVal(2, 2200)); err != nil {
		t.Fatal(err)
	}
	liveAfterTwo := sumLiveCont(s)
	if liveAfterTwo == 0 {
		t.Fatal("no cont extents charged")
	}
	checkConservationM8(t, s)

	// Overwrite ka with another oversize value: its old chain is retired to pending-free, a new
	// chain is charged. The net live-cont count changes only by the size difference, never below
	// the new chain's length.
	if err := s.Set(ka, bigVal(9, 3000)); err != nil {
		t.Fatal(err)
	}
	if sumPendingFree(s) == 0 {
		t.Fatal("overwriting an oversize value retired no cont extents to pending-free")
	}
	checkConservationM8(t, s)

	// Overwrite ka again with a small inline value: the chain is retired and no new chain is
	// charged, so ka stops contributing to the live-cont count.
	if err := s.Set(ka, []byte("small")); err != nil {
		t.Fatal(err)
	}
	// Delete kb: its chain is retired too. Now no live oversize value remains.
	if err := s.Delete(kb); err != nil {
		t.Fatal(err)
	}
	if got := sumLiveCont(s); got != 0 {
		t.Fatalf("live cont extents = %d after retiring every oversize value, want 0", got)
	}
	checkConservationM8(t, s)

	// The retired chains are still holes (pending-free), not yet returned. A checkpoint drains
	// them to the allocator. Conservation holds across the transition.
	if sumPendingFree(s) == 0 {
		t.Fatal("expected retired cont extents pending before the checkpoint")
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := sumPendingFree(s); got != 0 {
		t.Fatalf("pending-free = %d after checkpoint, want 0 (cont extents returned)", got)
	}
	checkConservationM8(t, s)

	// The live values are exactly what the last writes left: ka small inline, kb absent.
	v, ok, err := s.Get(ka)
	if err != nil || !ok || string(v) != "small" {
		t.Fatalf("Get(ka) = %q ok=%v err=%v, want small", v, ok, err)
	}
	if _, ok, _ := s.Get(kb); ok {
		t.Fatal("Get(kb) present after delete")
	}
}

// TestM9OversizeRecovery stores oversize values, reopens, and checks every value survives
// whole. The reopen must fold each live chain's cont extents into the allocator's in-use set
// (collectLiveOversize), so a later write cannot hand a live value's cont extent to another
// value. The test proves that by writing fresh keys after recovery and rechecking every
// original value, then asserting conservation.
func TestM9OversizeRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "recover.hlog")
	tun := inPlaceTunables(path, DurabilityFull)
	s := mustStore(t, tun)

	want := map[string][]byte{}
	for i := 0; i < 12; i++ {
		k := key(i)
		v := bigVal(i+1, 800+i*321) // every value spans more than one extent
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		want[string(k)] = v
	}
	// A checkpoint so recovery exercises the snapshot-seed path for the oversize marker, not
	// only the log replay path.
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// A few post-checkpoint oversize writes so recovery replays oversize records from the log
	// delta as well.
	for i := 12; i < 16; i++ {
		k := key(i)
		v := bigVal(i+1, 1234+i*77)
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		want[string(k)] = v
	}

	rs := reopen(t, s, tun)
	defer rs.Close()

	for ks, v := range want {
		got, ok, err := rs.Get([]byte(ks))
		if err != nil || !ok {
			t.Fatalf("after reopen Get(%q) ok=%v err=%v", ks, ok, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("after reopen Get(%q) content mismatch (%d vs %d bytes)", ks, len(got), len(v))
		}
	}
	checkConservationM8(t, rs)

	// Write new keys (inline and oversize) after recovery. If a live chain's cont extent had
	// been mistaken for free, the allocator would hand it to one of these and corrupt the
	// original value. Recheck every original afterward.
	for i := 100; i < 140; i++ {
		if err := rs.Set(key(i), bigVal(i, 1100)); err != nil {
			t.Fatal(err)
		}
	}
	for ks, v := range want {
		got, ok, err := rs.Get([]byte(ks))
		if err != nil || !ok || !bytes.Equal(got, v) {
			t.Fatalf("original Get(%q) corrupted by post-recovery allocation (ok=%v err=%v)", ks, ok, err)
		}
	}
	checkConservationM8(t, rs)
}

// TestM9OversizeAtomicityAcknowledged is the I6 all-or-nothing gate for the acknowledged
// side (doc 03 section 7). Under Full a SET does not return until its home record is in a
// synced extent, and the whole-file barrier that synced it has already flushed the earlier
// cont writes. So the image captured the instant the SET returns must recover the value
// whole: every cont extent present, the trailing CRC valid, the value byte-for-byte intact.
func TestM9OversizeAtomicityAcknowledged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ack.hlog")
	tun := inPlaceTunables(path, DurabilityFull)
	s := mustStore(t, tun)

	k := key(1)
	prev := []byte("prior-small-value")
	if err := s.Set(k, prev); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Acknowledged oversize write. Full syncs the home record, which flushes the cont writes.
	big := bigVal(7, 5000)
	if err := s.Set(k, big); err != nil {
		t.Fatal(err)
	}

	// Freeze the file the instant the SET returned and recover the frozen image independently.
	frozen := readWholeFile(t, s.df.f)
	cp := filepath.Join(dir, "ack-frozen.hlog")
	writeWholeFile(t, cp, frozen)
	s.Close()

	rt := tun
	rt.Path = cp
	rs, err := New(rt)
	if err != nil {
		t.Fatalf("recover acknowledged image: %v", err)
	}
	defer rs.Close()
	got, ok, err := rs.Get(k)
	if err != nil || !ok {
		t.Fatalf("acknowledged oversize value missing after recovery: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, big) {
		t.Fatalf("acknowledged oversize value not recovered whole (%d vs %d bytes)", len(got), len(big))
	}
	checkConservationM8(t, rs)
}

// TestM9OversizeOrphanChainReconciled is the I6 un-acknowledged side. A cont chain is written
// to the file with no home record ever appended, exactly the residue a crash between the cont
// writes and the home record sync leaves. Recovery finds no live oversize entry pointing at
// those extents, so it must reconcile them back to the allocator's free set rather than leak
// them or treat them as live. The prior value of the key stays intact and conservation holds.
func TestM9OversizeOrphanChainReconciled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "orphan.hlog")
	tun := inPlaceTunables(path, DurabilityFull)
	s := mustStore(t, tun)

	k := key(1)
	prev := []byte("survivor")
	if err := s.Set(k, prev); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Write a cont chain with no home record: this is the half-written state a crash before the
	// home record sync leaves behind. It allocates and physically extends the file, so the
	// frozen image carries the orphan extents.
	sh := s.shards[0]
	sh.mu.Lock()
	head, cnt, err := sh.writeOversizeChain(bigVal(3, 4000))
	sh.mu.Unlock()
	if err != nil {
		t.Fatalf("write orphan chain: %v", err)
	}
	if cnt < 2 {
		t.Fatalf("orphan chain too short to be meaningful: %d", cnt)
	}

	frozen := readWholeFile(t, s.df.f)
	cp := filepath.Join(dir, "orphan-frozen.hlog")
	writeWholeFile(t, cp, frozen)
	s.Close()

	rt := tun
	rt.Path = cp
	rs, err := New(rt)
	if err != nil {
		t.Fatalf("recover orphan image: %v", err)
	}
	defer rs.Close()

	// The prior value survives and no oversize value exists. The orphan extents are not charged
	// as live cont extents.
	v, ok, err := rs.Get(k)
	if err != nil || !ok || string(v) != "survivor" {
		t.Fatalf("Get(k) = %q ok=%v err=%v, want survivor", v, ok, err)
	}
	if got := sumLiveCont(rs); got != 0 {
		t.Fatalf("orphan chain charged as %d live cont extents, want 0", got)
	}
	// The orphan extents are free now: recovery's allocator reconciliation put every physical
	// extent not in use onto the free stack. Conservation proves the count balances.
	checkConservationM8(t, rs)
	_, free := rs.df.alloc.counts()
	freeSet := map[int64]bool{}
	for _, id := range free {
		freeSet[id] = true
	}
	for i := int64(0); i < cnt; i++ {
		if !freeSet[head+i] {
			t.Fatalf("orphan cont extent %d not reconciled free", head+i)
		}
	}
}

// TestM9OversizeCompaction forces compaction of pages that hold oversize home records and
// checks the home record is copied forward with its descriptor preserved, so the value still
// reads after the relocation. The chain itself stays in place (the divergence noted in the
// implementation doc), so the value bytes are never rewritten. It overwrites each oversize
// key several times to pile dead home records onto sealed pages, compacts, checkpoints, and
// verifies live values both before and after a reopen.
func TestM9OversizeCompaction(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "compact.hlog")
	tun := inPlaceTunables(path, DurabilityNormal)
	s := mustStore(t, tun)
	m := newModel()

	const keys = 60
	// Seed every key with an oversize value.
	for i := 0; i < keys; i++ {
		v := bigVal(i+1, 900+(i*53)%1500)
		if err := s.Set(key(i), v); err != nil {
			t.Fatal(err)
		}
		m.set(key(i), v)
	}
	// Overwrite each key several times with a fresh oversize value. Each overwrite appends a new
	// home record and kills the old one, so the early pages fill with dead home records and seal
	// below the read-only address, which is what compaction selects.
	for round := 1; round <= 4; round++ {
		for i := 0; i < keys; i++ {
			v := bigVal(i+round*1000, 700+((i+round)*91)%1700)
			if err := s.Set(key(i), v); err != nil {
				t.Fatal(err)
			}
			m.set(key(i), v)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if s.CompactionStats().CompactedExtents == 0 {
		t.Fatal("nothing compacted; the oversize relocation path was not exercised")
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Every live value reads its last write through the relocated home records.
	assertOversizeMatches(t, s, m)
	checkConservationM8(t, s)

	// And survives a reopen: the relocated home records carry the oversize marker and descriptor
	// into the recovered index.
	rs := reopen(t, s, tun)
	defer rs.Close()
	assertOversizeMatches(t, rs, m)
	checkConservationM8(t, rs)
}

// assertOversizeMatches checks every live model key reads back its exact value, the
// byte-for-byte form assertStoreMatches checks but inlined here to compare large values
// without the formatting an equality failure would otherwise dump.
func assertOversizeMatches(t *testing.T, s *Store, m *model) {
	t.Helper()
	if got := s.Len(); got != len(m.live) {
		t.Fatalf("Len() = %d, want %d", got, len(m.live))
	}
	for k, want := range m.live {
		v, ok, err := s.Get([]byte(k))
		if err != nil || !ok {
			t.Fatalf("Get(%q) ok=%v err=%v", k, ok, err)
		}
		if !bytes.Equal(v, want) {
			t.Fatalf("Get(%q) content mismatch (%d vs %d bytes)", k, len(v), len(want))
		}
	}
}

// TestM9OversizeRejectedOffInPlace confirms the gate: only the durable eviction-possible
// profile stores oversize values (doc 03 section 7). The full-resident lock-free profile
// aliases the page on read and cannot return a spanning value zero-copy, and the memory-only
// profile has no file to span into, so both reject an over-page value at SET rather than
// carry an oversize branch on their read paths.
func TestM9OversizeRejectedOffInPlace(t *testing.T) {
	big := bigVal(1, 8192)
	cases := []struct {
		name string
		tun  Tunables
	}{
		// A memory-only store with a page smaller than the value: it has no file to span into,
		// so an over-page value is rejected as it always has been. DefaultTunables uses a large
		// page (the benchmarked ceiling), so a small explicit page is needed to drive the path.
		{"memory-only", Tunables{Shards: 2, PageSize: 1 << 12}},
		{"durable-full-resident", Tunables{Shards: 2, PageSize: 1 << 12, ResidentPagesPerShard: 0, Path: "PATH"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tun := tc.tun
			if tun.Path == "PATH" {
				tun.Path = filepath.Join(t.TempDir(), "x.hlog")
			}
			s := mustStore(t, tun)
			defer s.Close()
			err := s.Set(key(1), big)
			if err == nil {
				t.Fatal("Set of an over-page value succeeded on a profile that should reject it")
			}
		})
	}
	// And the value is accepted on the in-place profile, the positive control for the gate.
	path := filepath.Join(t.TempDir(), "accept.hlog")
	s := mustStore(t, inPlaceTunables(path, DurabilityNormal))
	defer s.Close()
	if err := s.Set(key(1), big); err != nil {
		t.Fatalf("in-place profile rejected an oversize value: %v", err)
	}
	got, ok, err := s.Get(key(1))
	if err != nil || !ok || !bytes.Equal(got, big) {
		t.Fatalf("Get after accepted oversize Set ok=%v err=%v equal=%v", ok, err, bytes.Equal(got, big))
	}
}

// TestM9OversizeChurnConservation runs a mixed inline-and-oversize churn with periodic
// compaction and checkpoints, asserting the extent-conservation invariant holds throughout
// and the model matches at the end and across a reopen. It is the integration sweep that ties
// the oversize accounting (live cont extents, pending-free holes) into the M8 conservation
// ledger under sustained pressure.
func TestM9OversizeChurnConservation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "churn.hlog")
	tun := inPlaceTunables(path, DurabilityNormal)
	s := mustStore(t, tun)
	m := newModel()

	const keys = 80
	for step := 0; step < 6; step++ {
		for i := 0; i < keys; i++ {
			k := key(i)
			// Alternate sizes so a key flips between inline and oversize across steps, exercising
			// every supersede transition: inline->oversize, oversize->oversize, oversize->inline.
			var v []byte
			switch (i + step) % 3 {
			case 0:
				v = bigVal(i+step*7, 1400+(i*31)%2000) // oversize
			case 1:
				v = bigVal(i+step*7, 64) // inline
			default:
				v = bigVal(i+step*7, 2600+(i*17)%1200) // larger oversize
			}
			if err := s.Set(k, v); err != nil {
				t.Fatalf("step %d set %d: %v", step, i, err)
			}
			m.set(k, v)
		}
		// Delete a slice each step, then compact and checkpoint.
		for i := step; i < keys; i += 11 {
			if err := s.Delete(key(i)); err != nil {
				t.Fatal(err)
			}
			m.del(key(i))
		}
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		checkConservationM8(t, s)
	}

	assertOversizeMatches(t, s, m)
	// The oversize counter is runtime observability, not recovered state, so check it on the
	// live store before the reopen resets it.
	if s.OversizeValues() == 0 {
		t.Fatal("churn stored no oversize values; the sweep did not exercise the path")
	}
	rs := reopen(t, s, tun)
	defer rs.Close()
	assertOversizeMatches(t, rs, m)
	checkConservationM8(t, rs)
}
