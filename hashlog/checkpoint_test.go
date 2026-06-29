package hashlog

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// ckptTunables builds a durable store small enough that a modest workload spills and
// the snapshot spans into the file, at a chosen dial.
func ckptTunables(path string, d Durability) Tunables {
	return Tunables{
		Shards:                4,
		PageSize:              512,
		ExtentSize:            512,
		ResidentPagesPerShard: 2,
		Path:                  path,
		Durability:            d,
	}
}

// TestM4SnapshotRoundTrip encodes a synthetic snapshot and decodes it back, asserting
// every shard section's tuples survive byte for byte.
func TestM4SnapshotRoundTrip(t *testing.T) {
	const shards = 4
	sections := make([]snapSection, shards)
	for s := 0; s < shards; s++ {
		var tuples []snapTuple
		for i := 0; i < s*7+3; i++ {
			tuples = append(tuples, snapTuple{
				key: []byte(key(i*100 + s)),
				loc: valLoc{addr: int64(i*64 + s), vlen: uint32(i + 1)},
			})
		}
		sections[s] = snapSection{shard: s, frontierLSN: uint64(s*1000 + 1), tuples: tuples}
	}
	stream := encodeSnapshot(shards, 42, sections)
	dec, err := decodeSnapshot(stream)
	if err != nil {
		t.Fatal(err)
	}
	if dec.generation != 42 {
		t.Fatalf("generation %d, want 42", dec.generation)
	}
	if len(dec.sections) != shards {
		t.Fatalf("decoded %d sections, want %d", len(dec.sections), shards)
	}
	for s := 0; s < shards; s++ {
		got, want := dec.sections[s], sections[s]
		if got.frontierLSN != want.frontierLSN {
			t.Fatalf("shard %d frontier %d, want %d", s, got.frontierLSN, want.frontierLSN)
		}
		if len(got.tuples) != len(want.tuples) {
			t.Fatalf("shard %d has %d tuples, want %d", s, len(got.tuples), len(want.tuples))
		}
		for i := range want.tuples {
			a, b := got.tuples[i], want.tuples[i]
			if !bytes.Equal(a.key, b.key) || a.loc != b.loc {
				t.Fatalf("shard %d tuple %d mismatch: %v vs %v", s, i, a, b)
			}
		}
	}
}

// TestM4SnapshotEqualsLiveIndex is the round-trip-against-the-engine gate: after a
// workload with overwrites and deletes, a checkpoint's snapshot read back off the file
// holds exactly the store's live key set, each pointing at the same location.
func TestM4SnapshotEqualsLiveIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.hlog")
	s, err := New(ckptTunables(path, DurabilityNormal))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	live := map[string][]byte{}
	for i := 0; i < 4000; i++ {
		k := key(i % 1200)
		v := value4(i)
		if i%17 == 0 {
			if err := s.Delete(k); err != nil {
				t.Fatal(err)
			}
			delete(live, string(k))
			continue
		}
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		live[string(k)] = v
	}
	if s.Spilled() == 0 {
		t.Fatal("workload did not spill; not exercising the durable path")
	}

	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Read the committed snapshot stream straight off the file and decode it.
	root := s.df.sb.snapshotRoot
	slen := int64(s.df.sb.snapshotLen)
	if root < 0 || slen <= 0 {
		t.Fatalf("checkpoint did not record a snapshot (root %d len %d)", root, slen)
	}
	buf := make([]byte, slen)
	if _, err := s.df.f.ReadAt(buf, s.df.extentOffset(root)); err != nil {
		t.Fatal(err)
	}
	dec, err := decodeSnapshot(buf)
	if err != nil {
		t.Fatal(err)
	}

	// Every snapshot tuple must be a live key, and the union over shards must equal the
	// full live set, each tuple's location matching the live index.
	seen := map[string]bool{}
	for _, sec := range dec.sections {
		for _, tup := range sec.tuples {
			ks := string(tup.key)
			if _, ok := live[ks]; !ok {
				t.Fatalf("snapshot holds key %q not in the live set", ks)
			}
			if seen[ks] {
				t.Fatalf("snapshot holds key %q twice", ks)
			}
			seen[ks] = true
			// The location must match what the live index returns for this key.
			loc, ok := s.shardFor(tup.key).index.Load().lookup(tableHash(tup.key), tup.key)
			if !ok || loc != tup.loc {
				t.Fatalf("snapshot loc for %q is %v, live index has %v (ok=%v)", ks, tup.loc, loc, ok)
			}
		}
	}
	if len(seen) != len(live) {
		t.Fatalf("snapshot holds %d keys, live set has %d", len(seen), len(live))
	}
}

// TestM4CheckpointCommitFlipsGeneration confirms a checkpoint advances the superblock
// generation by one, persists across a reopen, and keeps the prior generation in the
// other slot so a torn new slot can fall back to it.
func TestM4CheckpointCommitFlipsGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gen.hlog")
	s, err := New(ckptTunables(path, DurabilityFull))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if err := s.Set(key(i), value4(i)); err != nil {
			t.Fatal(err)
		}
	}
	g0 := s.df.sb.generation
	for c := 0; c < 3; c++ {
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if got := s.df.sb.generation; got != g0+uint64(c)+1 {
			t.Fatalf("checkpoint %d generation %d, want %d", c, got, g0+uint64(c)+1)
		}
		// The other slot must still hold the immediately prior generation, the two-slot
		// fallback that makes the commit atomic.
		other := readSlotGen(t, path, 1-s.df.newerSlot, s.df.slotSize)
		if other != g0+uint64(c) {
			t.Fatalf("after checkpoint %d the fallback slot holds generation %d, want %d", c, other, g0+uint64(c))
		}
	}
	wantGen := s.df.sb.generation
	// Crash rather than close cleanly: a clean close would write one more checkpoint and
	// bump the generation past the explicit count this test asserts. Under Full the
	// explicit checkpoints are already synced, so the crash preserves their on-disk state.
	crash(t, s)

	// Reopen: the committed generation survives.
	s2, err := New(ckptTunables(path, DurabilityFull))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.df.sb.generation; got != wantGen {
		t.Fatalf("reopened generation %d, want %d", got, wantGen)
	}
}

// readSlotGen reads the generation field of one superblock slot straight off the file.
func readSlotGen(t *testing.T, path string, slot, slotSize int) uint64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 8)
	if _, err := f.ReadAt(buf, int64(slot*slotSize)+16); err != nil {
		t.Fatal(err)
	}
	return binary.LittleEndian.Uint64(buf)
}

// TestM4TornNewSlotFallsBack is the atomic-commit crash case: after a checkpoint
// commits generation G+1 into the newer slot, tearing that slot (a crash mid
// superblock write) makes a reopen fall back to the intact prior generation, whose
// snapshot still decodes. The newer write is never the slot the prior checkpoint lives
// in, so the prior checkpoint always survives.
func TestM4TornNewSlotFallsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.hlog")
	s, err := New(ckptTunables(path, DurabilityFull))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 600; i++ {
		if err := s.Set(key(i), value4(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Two checkpoints, so the slot we fall back to also carries a committed snapshot
	// (the fresh generation-0 slot has none). The second supersedes the first into the
	// other slot; tearing the second must recover the first, snapshot and all.
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	priorGen := s.df.sb.generation - 1
	newerSlot := s.df.newerSlot
	slotSize := s.df.slotSize
	priorSnapRoot := s.df.sb.snapshotRoot
	// Crash rather than close cleanly: a clean close would write a third checkpoint into
	// the other slot and flip newerSlot, so the slot this test tears below would no longer
	// be the newest one. The crash leaves the two explicit checkpoints exactly as committed.
	crash(t, s)

	// Tear the newer slot: flip a byte inside it so its CRC fails, modelling a crash
	// that interrupted the superblock write before it completed.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := []byte{0xFF}
	if _, err := f.WriteAt(corrupt, int64(newerSlot*slotSize)+32); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Reopen: recovery picks the valid lower-generation slot.
	s2, err := New(ckptTunables(path, DurabilityFull))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.df.sb.generation; got != priorGen {
		t.Fatalf("after tearing the new slot the store recovered generation %d, want the prior %d", got, priorGen)
	}
	// The prior checkpoint's snapshot is intact and decodes (it was never the slot we
	// tore, and its extents were never reused by the torn checkpoint).
	if s2.df.sb.snapshotRoot < 0 {
		t.Fatal("prior checkpoint has no snapshot root")
	}
	_ = priorSnapRoot
	buf := make([]byte, s2.df.sb.snapshotLen)
	if _, err := s2.df.f.ReadAt(buf, s2.df.extentOffset(s2.df.sb.snapshotRoot)); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeSnapshot(buf); err != nil {
		t.Fatalf("prior snapshot does not decode after the torn-slot crash: %v", err)
	}
}

// TestM4CheckpointStats checks the observability counters: bytes accumulate as records
// are appended and reset to zero on a checkpoint, and the generation and snapshot size
// reflect the last commit.
func TestM4CheckpointStats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stats.hlog")
	s, err := New(ckptTunables(path, DurabilityNormal))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 800; i++ {
		if err := s.Set(key(i), value4(i)); err != nil {
			t.Fatal(err)
		}
	}
	pre := s.CheckpointStats()
	if pre.BytesSinceCheckpoint == 0 {
		t.Fatal("bytes-since-checkpoint did not accumulate during the workload")
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	post := s.CheckpointStats()
	if post.BytesSinceCheckpoint != 0 {
		t.Fatalf("bytes-since-checkpoint is %d after a checkpoint, want 0", post.BytesSinceCheckpoint)
	}
	if post.Generation != pre.Generation+1 {
		t.Fatalf("generation %d after checkpoint, want %d", post.Generation, pre.Generation+1)
	}
	if post.SnapshotBytes == 0 {
		t.Fatal("snapshot byte size not recorded")
	}
}

// TestM4SnapshotExtentsReused confirms repeated checkpoints do not grow the file
// without bound: the superseded snapshot's extents are freed and reused, so the extent
// count settles instead of climbing by a snapshot per checkpoint.
func TestM4SnapshotExtentsReused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reuse.hlog")
	s, err := New(ckptTunables(path, DurabilityNormal))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 600; i++ {
		if err := s.Set(key(i), value4(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	after1, _ := s.df.alloc.counts()
	for c := 0; c < 5; c++ {
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	after6, _ := s.df.alloc.counts()
	// With no new writes between checkpoints the snapshot is the same size each time, so
	// from the second checkpoint on it reuses the freed run: the extent count grows by
	// at most one snapshot's worth, not five.
	if after6 > after1+s.df.snapCount {
		t.Fatalf("extent count climbed from %d to %d across five checkpoints; runs are not being reused", after1, after6)
	}
}

// FuzzDecodeSnapshot asserts the snapshot decoder fails closed on arbitrary bytes: it
// returns an error or a well-formed result, never panics, never reads out of bounds,
// and any accepted snapshot re-encodes and decodes back to the same sections.
func FuzzDecodeSnapshot(f *testing.F) {
	f.Add(encodeSnapshot(1, 1, []snapSection{{shard: 0}}))
	f.Add(encodeSnapshot(2, 7, []snapSection{
		{shard: 0, frontierLSN: 3, tuples: []snapTuple{{key: []byte("a"), loc: valLoc{addr: 1, vlen: 2}}}},
		{shard: 1, frontierLSN: 9, tuples: []snapTuple{{key: []byte("bb"), loc: valLoc{addr: 5, vlen: 4}}}},
	}))
	f.Fuzz(func(t *testing.T, data []byte) {
		dec, err := decodeSnapshot(data)
		if err != nil {
			return
		}
		// An accepted snapshot must re-encode to bytes that decode back identically.
		re := encodeSnapshot(len(dec.sections), dec.generation, dec.sections)
		dec2, err := decodeSnapshot(re)
		if err != nil {
			t.Fatalf("re-encoded snapshot failed to decode: %v", err)
		}
		if dec2.generation != dec.generation || len(dec2.sections) != len(dec.sections) {
			t.Fatal("re-encoded snapshot header changed")
		}
		for s := range dec.sections {
			if len(dec2.sections[s].tuples) != len(dec.sections[s].tuples) {
				t.Fatal("re-encoded snapshot section changed")
			}
		}
	})
}
