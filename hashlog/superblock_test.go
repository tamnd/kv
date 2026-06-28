package hashlog

import (
	"reflect"
	"testing"
)

// sampleSuperblock builds a populated superblock for round-trip tests.
func sampleSuperblock(shardCount int, gen uint64, free []int64) *superblock {
	sb := newSuperblock(shardCount, 1<<20)
	sb.generation = gen
	sb.extentCount = 42
	sb.lsnHighWater = 1000 + gen
	sb.snapshotRoot = 7
	sb.snapshotLen = 4096
	sb.free = free
	for i := range sb.frontiers {
		sb.frontiers[i] = shardFrontier{
			frontierLSN: uint64(100 + i),
			tailExtent:  int64(i),
			tailOff:     uint64(200 + i),
		}
	}
	return sb
}

func TestSuperblockRoundTrip(t *testing.T) {
	cases := []struct {
		shards int
		free   []int64
	}{
		{shards: 1, free: []int64{}},
		{shards: 4, free: []int64{0}},
		{shards: 16, free: []int64{9, 8, 7, 6, 5}},
		{shards: 64, free: []int64{}},
		{shards: 256, free: []int64{1, 2, 3}},
	}
	for _, c := range cases {
		sb := sampleSuperblock(c.shards, 5, c.free)
		for slotID := 0; slotID < 2; slotID++ {
			buf, err := sb.encode(slotID)
			if err != nil {
				t.Fatalf("shards=%d encode: %v", c.shards, err)
			}
			if len(buf) != superblockSlotSize(c.shards) {
				t.Fatalf("shards=%d slot size %d, want %d", c.shards, len(buf), superblockSlotSize(c.shards))
			}
			got, err := decodeSuperblock(buf)
			if err != nil {
				t.Fatalf("shards=%d decode: %v", c.shards, err)
			}
			if !reflect.DeepEqual(got, sb) {
				t.Fatalf("shards=%d round-trip mismatch:\n got %+v\nwant %+v", c.shards, got, sb)
			}
		}
	}
}

func TestSuperblockSlotSizeFits256Shards(t *testing.T) {
	// The default 256 shards do not fit a 4 KiB slot; the slot rounds up to 8 KiB.
	if got := superblockSlotSize(256); got != 8192 {
		t.Fatalf("256-shard slot size %d, want 8192", got)
	}
	// A small shard count keeps the 4 KiB slot the doc names.
	if got := superblockSlotSize(4); got != 4096 {
		t.Fatalf("4-shard slot size %d, want 4096", got)
	}
}

func TestSuperblockCRCCatchesBitFlip(t *testing.T) {
	sb := sampleSuperblock(16, 3, []int64{1, 2})
	buf, err := sb.encode(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, off := range []int{0, 8, 16, sbSlotHeaderSize, sbSlotHeaderSize + 8, len(buf) - 8} {
		bad := append([]byte(nil), buf...)
		bad[off] ^= 0x01
		if _, err := decodeSuperblock(bad); err == nil {
			t.Fatalf("flip at offset %d was not rejected", off)
		}
	}
}

func TestSuperblockFreeOverflowRejected(t *testing.T) {
	shards := 256
	sb := newSuperblock(shards, 1<<20)
	// One past the inline capacity must fail to encode (overflow chain is M4).
	cap := inlineFreeCapacity(shards)
	sb.free = make([]int64, cap+1)
	if _, err := sb.encode(0); err == nil {
		t.Fatalf("free list of %d (cap %d) should not encode inline", cap+1, cap)
	}
	// Exactly at capacity must encode and round-trip.
	sb.free = make([]int64, cap)
	for i := range sb.free {
		sb.free[i] = int64(i)
	}
	buf, err := sb.encode(0)
	if err != nil {
		t.Fatalf("free list at capacity should encode: %v", err)
	}
	got, err := decodeSuperblock(buf)
	if err != nil {
		t.Fatalf("decode at capacity: %v", err)
	}
	if len(got.free) != cap {
		t.Fatalf("decoded free len %d, want %d", len(got.free), cap)
	}
}

func TestPickNewer(t *testing.T) {
	a := sampleSuperblock(8, 5, nil)
	b := sampleSuperblock(8, 6, nil)
	if pickNewer(a, b) != b {
		t.Fatal("higher generation should win")
	}
	if pickNewer(b, a) != b {
		t.Fatal("higher generation should win regardless of order")
	}
	if pickNewer(nil, a) != a {
		t.Fatal("a valid slot should win over a torn one")
	}
	if pickNewer(a, nil) != a {
		t.Fatal("a valid slot should win over a torn one")
	}
	if pickNewer(nil, nil) != nil {
		t.Fatal("two torn slots yield nil")
	}
}

func FuzzDecodeSuperblock(f *testing.F) {
	// Seed with a valid slot, a bit-flipped slot, and some short buffers.
	valid, _ := sampleSuperblock(16, 1, []int64{1, 2, 3}).encode(0)
	f.Add(valid)
	flipped := append([]byte(nil), valid...)
	flipped[100] ^= 0xff
	f.Add(flipped)
	f.Add([]byte{})
	f.Add(make([]byte, 100))
	f.Add([]byte(sbMagic))

	f.Fuzz(func(t *testing.T, data []byte) {
		// The contract is fail-closed: never panic, never read out of bounds, never
		// allocate unboundedly. A returned superblock must be self-consistent.
		sb, err := decodeSuperblock(data)
		if err != nil {
			if sb != nil {
				t.Fatal("error returned with a non-nil superblock")
			}
			return
		}
		if int(sb.shardCount) <= 0 || int(sb.shardCount) > maxShardCount {
			t.Fatalf("decoded absurd shardCount %d", sb.shardCount)
		}
		if len(sb.frontiers) != int(sb.shardCount) {
			t.Fatalf("frontiers %d != shardCount %d", len(sb.frontiers), sb.shardCount)
		}
		// A successful decode must re-encode to the same bytes (the slot is canonical).
		reb, err := sb.encode(0)
		if err != nil {
			t.Fatalf("re-encode of a decoded slot failed: %v", err)
		}
		if got, err := decodeSuperblock(reb); err != nil || !reflect.DeepEqual(got, sb) {
			t.Fatalf("decode is not idempotent")
		}
	})
}
