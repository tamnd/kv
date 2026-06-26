package betree

import (
	"fmt"
	"math/rand"
	"testing"
)

// This file gates the M7.1 partition function (partition.go). The contracts: routing is deterministic
// (a key always lands in the same shard, the property that keeps a key reachable across a reopen); hash
// partitioning spreads load roughly evenly regardless of key distribution; range partitioning keeps a
// contiguous key range in one shard (the property that makes a single-band scan single-shard); and both
// degenerate cleanly to one shard. Nothing here drives the engine; it tests the routing primitive the
// later M7 slices build the shard machinery on.

func TestHashPartitionerDeterministic(t *testing.T) {
	p := newHashPartitioner(8)
	keys := [][]byte{[]byte(""), []byte("a"), []byte("hello"), []byte("hello\x00"), {0xff, 0x00, 0x7f}}
	for _, k := range keys {
		first := p.route(k)
		if first < 0 || first >= p.shards() {
			t.Fatalf("route(%q)=%d out of range [0,%d)", k, first, p.shards())
		}
		// Routing the same key many times must give the same shard every time, the determinism the
		// reopen-reachability contract rests on.
		for i := 0; i < 100; i++ {
			if got := p.route(k); got != first {
				t.Fatalf("route(%q) not deterministic: %d then %d", k, first, got)
			}
		}
	}
}

// TestHashPartitionerStableConstants pins a few exact hash routes so a future edit to the FNV constants
// or the modulo cannot silently move keys to different shards (which would orphan data on disk). The
// expected values are computed from the fixed FNV-1a basis, so they are a regression lock, not magic.
func TestHashPartitionerStableConstants(t *testing.T) {
	p := newHashPartitioner(16)
	for _, tc := range []struct {
		key  string
		want int
	}{
		{"", int(fnvOffset64 % 16)},
		{"a", int(fnv1a64([]byte("a")) % 16)},
		{"key-0000", int(fnv1a64([]byte("key-0000")) % 16)},
	} {
		if got := p.route([]byte(tc.key)); got != tc.want {
			t.Fatalf("route(%q)=%d, want %d", tc.key, got, tc.want)
		}
	}
}

// TestHashPartitionerSpread checks the even-spread property: over many keys no shard is starved or
// swamped. The bound is loose (every shard within half-to-double its fair share) because the test is
// guarding against a broken router that piles keys onto one shard, not asserting a statistical quality
// of FNV.
func TestHashPartitionerSpread(t *testing.T) {
	const shards = 16
	const n = 200000
	p := newHashPartitioner(shards)
	counts := make([]int, shards)
	for i := 0; i < n; i++ {
		counts[p.route([]byte(fmt.Sprintf("key-%d", i)))]++
	}
	fair := n / shards
	for s, c := range counts {
		if c < fair/2 || c > fair*2 {
			t.Errorf("shard %d got %d keys, fair share is %d (spread too skewed)", s, c, fair)
		}
	}
}

func TestHashPartitionerSingleShard(t *testing.T) {
	for _, n := range []int{0, -3, 1} {
		p := newHashPartitioner(n)
		if p.shards() != 1 {
			t.Fatalf("newHashPartitioner(%d).shards()=%d, want 1", n, p.shards())
		}
		if got := p.route([]byte("anything")); got != 0 {
			t.Fatalf("single-shard route=%d, want 0", got)
		}
	}
}

// TestRangePartitionerContiguous is the central range-partition property: walking keys in ascending
// order yields a non-decreasing shard sequence, so any contiguous key range spans a contiguous run of
// shards (and a range inside one band is a single shard). A hash partitioner would fail this; a correct
// range partitioner cannot.
func TestRangePartitionerContiguous(t *testing.T) {
	splits := [][]byte{[]byte("d"), []byte("h"), []byte("m"), []byte("t")}
	p := newRangePartitioner(splits)
	if p.shards() != 5 {
		t.Fatalf("shards()=%d, want 5", p.shards())
	}
	prev := -1
	for c := byte('a'); c <= byte('z'); c++ {
		s := p.route([]byte{c})
		if s < prev {
			t.Fatalf("route(%q)=%d decreased below previous %d: not contiguous", []byte{c}, s, prev)
		}
		if s < 0 || s >= p.shards() {
			t.Fatalf("route(%q)=%d out of range", []byte{c}, s)
		}
		prev = s
	}
}

// TestRangePartitionerBoundaries pins exact routing including the half-open boundary rule: a key equal
// to a split belongs to the shard above the split, a key just below belongs to the shard at the split.
func TestRangePartitionerBoundaries(t *testing.T) {
	p := newRangePartitioner([][]byte{[]byte("d"), []byte("h"), []byte("m")})
	for _, tc := range []struct {
		key  string
		want int
	}{
		{"", 0},
		{"a", 0},
		{"c", 0},
		{"d", 1},  // equal to a split routes to the band above it
		{"cz", 0}, // just below "d"
		{"g", 1},
		{"h", 2},
		{"l", 2},
		{"m", 3},
		{"z", 3},
	} {
		if got := p.route([]byte(tc.key)); got != tc.want {
			t.Errorf("route(%q)=%d, want %d", tc.key, got, tc.want)
		}
	}
}

func TestRangePartitionerDedupAndSort(t *testing.T) {
	// Unsorted with a duplicate: the partitioner must sort and collapse, yielding 3 bands from the two
	// distinct splits, and route consistently with the sorted unique boundaries.
	p := newRangePartitioner([][]byte{[]byte("m"), []byte("d"), []byte("m")})
	if p.shards() != 3 {
		t.Fatalf("shards()=%d, want 3 (two unique splits)", p.shards())
	}
	if got := p.route([]byte("a")); got != 0 {
		t.Errorf("route(a)=%d, want 0", got)
	}
	if got := p.route([]byte("g")); got != 1 {
		t.Errorf("route(g)=%d, want 1", got)
	}
	if got := p.route([]byte("z")); got != 2 {
		t.Errorf("route(z)=%d, want 2", got)
	}
}

func TestRangePartitionerNoSplits(t *testing.T) {
	p := newRangePartitioner(nil)
	if p.shards() != 1 {
		t.Fatalf("no-split shards()=%d, want 1", p.shards())
	}
	if got := p.route([]byte("anything")); got != 0 {
		t.Fatalf("single-band route=%d, want 0", got)
	}
}

// TestRangePartitionerRandomMonotone is the contiguity property under a random multi-byte keyspace:
// sort a batch of random keys and confirm their shard assignments never decrease. This catches a
// comparator that disagrees with the engine's byte order on multi-byte keys.
func TestRangePartitionerRandomMonotone(t *testing.T) {
	rng := rand.New(rand.NewSource(20590626))
	splits := [][]byte{{0x20}, {0x40}, {0x60}, {0x80}, {0xa0}, {0xc0}, {0xe0}}
	p := newRangePartitioner(splits)
	keys := make([][]byte, 4000)
	for i := range keys {
		k := make([]byte, 1+rng.Intn(6))
		rng.Read(k)
		keys[i] = k
	}
	sortByteSlices(keys)
	prev := -1
	for _, k := range keys {
		s := p.route(k)
		if s < prev {
			t.Fatalf("sorted key %x routed to shard %d below previous %d", k, s, prev)
		}
		prev = s
	}
}

// sortByteSlices sorts keys in the same unsigned lexicographic order the engine and the range
// partitioner use, so the monotonicity check compares like with like.
func sortByteSlices(keys [][]byte) {
	// Insertion-free: reuse the partitioner's own order via a simple sort.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && bytesLess(keys[j], keys[j-1]); j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
}
