package f2

import (
	"fmt"
	"sort"
	"testing"
)

func altVal(i int) []byte { return []byte(fmt.Sprintf("alt-%08d-payload", i)) }

// flushTails pushes every shard's tail page to the file without writing a snapshot or
// touching the superblock, so a following crash leaves the post-checkpoint records on
// disk under the still-committed earlier snapshot. It models the steady state recovery
// must handle: a snapshot plus a delta of records appended after it.
func flushTails(t *testing.T, s *Store) {
	t.Helper()
	for _, sh := range s.shards {
		sh.mu.Lock()
		err := sh.log.flushTail()
		sh.mu.Unlock()
		if err != nil {
			t.Fatalf("flushTail: %v", err)
		}
	}
}

// liveWords returns a shard's live slot words, the multiset the snapshot must store, read
// the same way captureSnap reads them.
func liveWords(sh *shard) []uint64 {
	idx := sh.index.Load()
	var ws []uint64
	for i := range idx.slots {
		w := idx.slots[i].Load()
		if w == 0 || w&slotTombstone != 0 {
			continue
		}
		ws = append(ws, w)
	}
	return ws
}

func sortedU64(in []uint64) []uint64 {
	out := append([]uint64(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestSnapStreamCodec round-trips the per-shard sections through the stream encoder and
// rejects a truncated stream, the on-disk format checked without touching a file.
func TestSnapStreamCodec(t *testing.T) {
	in := []shardSnap{
		{gen: 0, frontier: 4096, logBytes: 1000, deadBytes: 0, slots: []uint64{makeSlot(1, 0), makeSlot(2, 7)}},
		{gen: 3, frontier: 9000, logBytes: 5000, deadBytes: 200, slots: nil},
		{gen: 1, frontier: 20, logBytes: 20, deadBytes: 0, slots: []uint64{makeSlot(5, 9)}},
	}
	buf := encodeSnapStream(in)
	out, err := decodeSnapStream(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("shard count = %d, want %d", len(out), len(in))
	}
	for i := range in {
		if out[i].gen != in[i].gen || out[i].frontier != in[i].frontier ||
			out[i].logBytes != in[i].logBytes || out[i].deadBytes != in[i].deadBytes {
			t.Fatalf("shard %d header mismatch: %+v vs %+v", i, out[i], in[i])
		}
		if !equalU64(out[i].slots, in[i].slots) {
			t.Fatalf("shard %d slots = %v, want %v", i, out[i].slots, in[i].slots)
		}
	}

	if _, err := decodeSnapStream(buf[:len(buf)-1]); err != errSnapTorn {
		t.Fatalf("truncated stream error = %v, want errSnapTorn", err)
	}
	if _, err := decodeSnapStream(buf[:4]); err != errSnapTorn {
		t.Fatalf("header-only stream error = %v, want errSnapTorn", err)
	}
}

// TestSnapCheckpointRoundTrip writes a checkpoint, reads the committed snapshot back off
// disk through the block chain, and checks every shard's stored section against the live
// index. The None dial keeps the test off F_FULLFSYNC; the snapshot still writes and the
// superblock still commits, only the barriers are skipped.
func TestSnapCheckpointRoundTrip(t *testing.T) {
	s := mustOpenT(t, durableTunables(t, DurabilityNone))
	const keys = 3000
	for i := 0; i < keys; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	df := s.df
	if df.snapRoot < 0 {
		t.Fatal("snapRoot still -1 after a checkpoint")
	}
	nb, err := df.fileBlocks()
	if err != nil {
		t.Fatal(err)
	}
	stream, chain, err := df.readSnapshot(df.snapRoot, df.snapSeq, nb)
	if err != nil {
		t.Fatalf("readSnapshot: %v", err)
	}
	if !equalU64(int64sToU64(chain), int64sToU64(df.snapBlocks)) {
		t.Fatalf("read-back chain %v != committed chain %v", chain, df.snapBlocks)
	}
	snaps, err := decodeSnapStream(stream)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(snaps) != len(s.shards) {
		t.Fatalf("snapshot covers %d shards, want %d", len(snaps), len(s.shards))
	}

	total := 0
	for i, sh := range s.shards {
		sh.mu.RLock()
		wantWords := sortedU64(liveWords(sh))
		wantGen, wantFront := sh.log.gen, sh.log.tail
		wantLog, wantDead := sh.logBytes, sh.deadBytes
		sh.mu.RUnlock()

		sec := snaps[i]
		if sec.gen != wantGen || sec.frontier != wantFront || sec.logBytes != wantLog || sec.deadBytes != wantDead {
			t.Fatalf("shard %d section = (gen %d, front %d, log %d, dead %d), want (%d, %d, %d, %d)",
				i, sec.gen, sec.frontier, sec.logBytes, sec.deadBytes, wantGen, wantFront, wantLog, wantDead)
		}
		if !equalU64(sortedU64(sec.slots), wantWords) {
			t.Fatalf("shard %d stored %d live slots, index has %d", i, len(sec.slots), len(wantWords))
		}
		total += len(sec.slots)
	}
	if total != keys {
		t.Fatalf("snapshot holds %d live slots across shards, want %d unique keys", total, keys)
	}
}

// TestSnapBlockConservation checks that after a checkpoint every block the file spans is
// accounted exactly once: claimed by a live data page, sitting on the free list, or part
// of the committed snapshot chain, and that the three sets are disjoint. The snapshot
// chain is neither live data nor free, the S7 invariant.
func TestSnapBlockConservation(t *testing.T) {
	s := mustOpenT(t, durableTunables(t, DurabilityNone))
	const keys = 4000
	for i := 0; i < keys; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	for round := 0; round < 2; round++ { // strand some versions so compaction has something to free
		for i := 0; i < keys; i++ {
			if err := s.Set(tkey(i), tval(i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	assertConserved(t, s)

	// A second checkpoint must free the prior chain and conserve again.
	priorChain := append([]int64(nil), s.df.snapBlocks...)
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint 2: %v", err)
	}
	if equalU64(int64sToU64(s.df.snapBlocks), int64sToU64(priorChain)) {
		t.Fatal("second checkpoint reused the same chain blocks, want a fresh chain")
	}
	assertConserved(t, s)
}

// assertConserved verifies the live-page, free-list, and snapshot-chain block sets
// partition [0, allocHigh).
func assertConserved(t *testing.T, s *Store) {
	t.Helper()
	live := map[int64]string{}
	for _, sh := range s.shards {
		sh.mu.RLock()
		for pi := 0; pi < sh.log.npages; pi++ {
			b := sh.log.pageBlock[pi]
			if other, dup := live[b]; dup {
				sh.mu.RUnlock()
				t.Fatalf("block %d claimed by two live pages (%s)", b, other)
			}
			live[b] = "page"
		}
		sh.mu.RUnlock()
	}

	df := s.df
	df.mu.Lock()
	high := df.allocHigh
	free := append([]int64(nil), df.free...)
	chain := append([]int64(nil), df.snapBlocks...)
	df.mu.Unlock()

	freeSet := map[int64]bool{}
	for _, b := range free {
		if freeSet[b] {
			t.Fatalf("block %d on the free list twice", b)
		}
		freeSet[b] = true
		if _, isLive := live[b]; isLive {
			t.Fatalf("block %d both live and free", b)
		}
	}
	chainSet := map[int64]bool{}
	for _, b := range chain {
		chainSet[b] = true
		if _, isLive := live[b]; isLive {
			t.Fatalf("block %d both live and in the snapshot chain", b)
		}
		if freeSet[b] {
			t.Fatalf("block %d both free and in the snapshot chain", b)
		}
	}
	for b := int64(0); b < high; b++ {
		_, isLive := live[b]
		if !isLive && !freeSet[b] && !chainSet[b] {
			t.Fatalf("block %d accounted by none of live/free/chain", b)
		}
	}
}

// TestSnapDeltaRecovery checks the S6-2 core: a reopen installs the committed snapshot and
// replays only the records appended after the frontier, reconstructing the same logical
// state a full replay would, while touching far fewer records. After a checkpoint over set
// A, the delta adds new keys, overwrites part of A, and deletes part of A; a crash (no
// clean-close snapshot) then reopens against the checkpoint's snapshot plus that delta.
func TestSnapDeltaRecovery(t *testing.T) {
	tn := durableTunables(t, DurabilityNone)
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const a = 4000
	for i := 0; i < a; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Delta after the cut: 1000 new keys, 500 overwrites, 300 deletes.
	for i := a; i < a+1000; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 500; i++ {
		if err := s.Set(tkey(i), altVal(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 500; i < 800; i++ {
		if err := s.Delete(tkey(i)); err != nil {
			t.Fatal(err)
		}
	}
	flushTails(t, s)
	crash(t, s)

	r := mustOpenT(t, tn)
	st := r.Stats()
	// Delta-bound, not history-bound: only the post-checkpoint records are replayed,
	// the snapshot supplies the rest as slot words, never decoded.
	if st.RecoveryRecords >= a {
		t.Fatalf("RecoveryRecords = %d, want < %d (delta-bound replay)", st.RecoveryRecords, a)
	}
	if st.RecoveryRecords == 0 {
		t.Fatal("RecoveryRecords = 0, want the delta replayed")
	}

	for i := 0; i < 500; i++ { // overwritten
		got, ok, err := r.Get(tkey(i))
		if err != nil || !ok || string(got) != string(altVal(i)) {
			t.Fatalf("key %d = (%q, %v, %v), want overwritten value", i, got, ok, err)
		}
	}
	for i := 500; i < 800; i++ { // deleted
		if _, ok, _ := r.Get(tkey(i)); ok {
			t.Fatalf("key %d present, want deleted", i)
		}
	}
	for i := 800; i < a+1000; i++ { // untouched A tail and the new keys
		got, ok, err := r.Get(tkey(i))
		if err != nil || !ok || string(got) != string(tval(i)) {
			t.Fatalf("key %d = (%q, %v, %v), want %q", i, got, ok, err, tval(i))
		}
	}
}

// TestSnapGenerationMismatchFallback checks that a compaction after a checkpoint, which
// bumps the generation and strands the snapshot's generation-relative addresses, makes
// recovery ignore the snapshot and full-replay the compacted generation, still correct.
func TestSnapGenerationMismatchFallback(t *testing.T) {
	tn := durableTunables(t, DurabilityNone)
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const keys = 3000
	for round := 0; round < 4; round++ { // overwrite so every shard clears the compaction threshold
		for i := 0; i < keys; i++ {
			if err := s.Set(tkey(i), tval(i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.Checkpoint(); err != nil { // snapshot at the pre-compaction generation
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := s.Compact(); err != nil { // bumps the generation, stranding the snapshot's addresses
		t.Fatalf("Compact: %v", err)
	}
	flushTails(t, s)
	crash(t, s)

	r := mustOpenT(t, tn)
	for i := 0; i < keys; i++ {
		got, ok, err := r.Get(tkey(i))
		if err != nil || !ok || string(got) != string(tval(i)) {
			t.Fatalf("key %d = (%q, %v, %v) after gen-mismatch recovery, want %q", i, got, ok, err, tval(i))
		}
	}
}

// TestSnapTornSnapshotFallback corrupts the committed snapshot chain on disk and checks
// that recovery rejects it whole and full-replays, losing no data. The chain's first
// block is overwritten with garbage so its CRC fails.
func TestSnapTornSnapshotFallback(t *testing.T) {
	tn := durableTunables(t, DurabilityNone)
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const keys = 2000
	for i := 0; i < keys; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	root := s.df.snapRoot
	off := s.df.blockOffset(root)
	flushTails(t, s)
	// Scribble over the snapshot root block so its CRC no longer checks.
	garbage := make([]byte, 64)
	for i := range garbage {
		garbage[i] = 0xAB
	}
	if _, err := s.df.f.WriteAt(garbage, off); err != nil {
		t.Fatalf("corrupt snapshot: %v", err)
	}
	crash(t, s)

	r := mustOpenT(t, tn)
	for i := 0; i < keys; i++ {
		got, ok, err := r.Get(tkey(i))
		if err != nil || !ok || string(got) != string(tval(i)) {
			t.Fatalf("key %d = (%q, %v, %v) after torn-snapshot recovery, want %q", i, got, ok, err, tval(i))
		}
	}
}

func int64sToU64(in []int64) []uint64 {
	out := make([]uint64, len(in))
	for i, v := range in {
		out[i] = uint64(v)
	}
	return sortedU64(out)
}
