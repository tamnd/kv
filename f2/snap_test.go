package f2

import (
	"sort"
	"testing"
)

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

func int64sToU64(in []int64) []uint64 {
	out := make([]uint64, len(in))
	for i, v := range in {
		out[i] = uint64(v)
	}
	return sortedU64(out)
}
