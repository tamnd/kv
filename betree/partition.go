package betree

// This file is the first slice of milestone M7, the logical shard-per-goroutine mode (doc 05 section
// 6, decision D9). M7 is optional and gated: it partitions the keyspace into N independent shards,
// each with its own WAL buffer, hot tail, and lock domain, so two writers committing to different
// shards share no mutex and no log buffer and commit fully in parallel. That is the whole payoff, the
// last cross-core latch removed from the commit path, the reachable piece of thread-per-core in pure
// Go (true core pinning is rejected in D9 because the Go scheduler migrates goroutines and the
// runtime's own goroutines float across cores regardless). The mode is off by default and opt-in,
// because a workload concentrated on one hot key funnels every write to one shard and defeats the
// parallelism, so the trade is workload-dependent and the operator chooses it.
//
// The foundation the rest of M7 routes through is the partition function: the rule that sends a key to
// exactly one shard. Everything else in the milestone, the per-shard tail and WAL buffer, the
// cross-shard scan merge, the cross-shard commit, is built on top of a key landing in a definite
// shard, so this slice lands that rule first, by itself, fully tested, before any shard machinery
// hangs off it. Nothing here is on a live path yet; it is the routing primitive the sharded core wires
// onto in the later slices, alongside the engine and off the default path exactly like the M6
// substrate.
//
// Two disciplines the partition function must hold, both load-bearing.
//
// Determinism across reopens. A key must route to the same shard every time the database is opened, or
// its data becomes unreachable after a restart (it was written into shard 3's tree and a later open
// looks for it in shard 5). So the partition function cannot use anything that varies per process: not
// a randomized seed, not maphash (which reseeds every process by design), not the address of the key.
// The hash partitioner uses FNV-1a with the standard fixed offset basis, a pure function of the key
// bytes, so the same key hashes the same forever.
//
// The two partitioners answer the two halves of the trade doc 05 names. Hash partitioning spreads the
// load evenly across shards regardless of key distribution (the hash decorrelates key order), which is
// what a write-heavy workload wants, at the cost that adjacent keys land in different shards so a range
// scan has to merge across all of them. Range partitioning keeps a contiguous key range in one shard,
// so a scan confined to that range is single-shard and full-bandwidth, at the cost that a skewed load
// hot-spots one shard. Neither is universally right, which is why both are offered and why the mode is
// gated rather than a default.

import "sort"

// partitioner maps a key to one of a fixed number of shards. It is a pure, deterministic function of
// the key bytes: the same key always routes to the same shard, this process and every future one, so a
// key written into a shard is found in that shard after a reopen. shards reports the partition width;
// route returns a shard index in [0, shards).
type partitioner interface {
	// shards is the number of partitions. Always at least one.
	shards() int
	// route returns the shard index in [0, shards()) that owns key. It must be deterministic and
	// total: every key, including the empty key, routes to exactly one shard.
	route(key []byte) int
}

// fnvOffset64 and fnvPrime64 are the standard 64-bit FNV-1a constants. They are fixed by the algorithm,
// so a key hashes identically across processes and builds, which is what makes hash routing stable
// across reopens. Spelled out here rather than imported so the partitioner carries no dependency and
// the determinism is visible at the call site.
const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

// fnv1a64 is FNV-1a over b: start at the offset basis, and for each byte xor it in then multiply by the
// prime. It is a pure function of the bytes, the property the routing stability depends on.
func fnv1a64(b []byte) uint64 {
	h := fnvOffset64
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

// hashPartitioner routes by the FNV-1a hash of the key modulo the shard count, so the load spreads
// evenly across shards no matter how the keys are distributed (the hash decorrelates any order or
// clustering in the keys themselves). This is the default partitioner, the one a write-heavy workload
// wants, and the cost it pays is that adjacent keys land in different shards so an ordered scan must
// merge across shards (the merge is the next slice).
type hashPartitioner struct {
	n int
}

// newHashPartitioner builds a hash partitioner over n shards. A non-positive n is clamped to one shard,
// which routes every key to shard zero and makes the sharded core behave like the single-shard one, the
// safe degenerate case.
func newHashPartitioner(n int) hashPartitioner {
	if n < 1 {
		n = 1
	}
	return hashPartitioner{n: n}
}

func (h hashPartitioner) shards() int { return h.n }

func (h hashPartitioner) route(key []byte) int {
	if h.n == 1 {
		return 0
	}
	return int(fnv1a64(key) % uint64(h.n))
}

// rangePartitioner routes by contiguous key range: it holds N-1 sorted split keys and a key routes to
// the shard whose half-open range contains it. Keys below the first split go to shard 0, keys in
// [split[i-1], split[i]) go to shard i, and keys at or above the last split go to the final shard. The
// splits partition the keyspace into N contiguous bands, so a scan confined to one band is single-shard
// and needs no merge, which is the property range partitioning buys back at the cost of hot-spotting a
// skewed load onto one band.
type rangePartitioner struct {
	// splits are the N-1 boundary keys, strictly ascending. An empty splits slice is the single-shard
	// degenerate case (the whole keyspace is one band). Each split is the inclusive lower bound of the
	// shard above it, so the bands are half-open [split[i-1], split[i]).
	splits [][]byte
}

// newRangePartitioner builds a range partitioner from the given boundary keys. The caller passes the
// N-1 splits that divide the keyspace into N bands; they are copied and sorted so the partitioner owns
// stable, ascending boundaries regardless of how they arrive. Duplicate splits are collapsed, since two
// equal boundaries would name an empty band no key can route to; the resulting shard count is
// len(unique splits)+1. Passing no splits yields a single-shard partitioner.
func newRangePartitioner(splits [][]byte) rangePartitioner {
	cp := make([][]byte, 0, len(splits))
	for _, s := range splits {
		cp = append(cp, append([]byte(nil), s...))
	}
	sort.Slice(cp, func(i, j int) bool { return bytesLess(cp[i], cp[j]) })
	// Collapse adjacent duplicates so every retained split opens a distinct, non-empty band.
	uniq := cp[:0]
	for i, s := range cp {
		if i == 0 || !bytesEqual(s, cp[i-1]) {
			uniq = append(uniq, s)
		}
	}
	return rangePartitioner{splits: uniq}
}

func (r rangePartitioner) shards() int { return len(r.splits) + 1 }

func (r rangePartitioner) route(key []byte) int {
	// The owning shard is the number of splits at or below key: a key below every split routes to shard
	// 0, a key at or above the last split routes to the final shard, and a key in band i routes to i.
	// sort.Search returns the index of the first split strictly greater than key, which is exactly that
	// count.
	return sort.Search(len(r.splits), func(i int) bool {
		return bytesLess(key, r.splits[i])
	})
}

// bytesLess and bytesEqual are the unsigned byte-order comparisons the range partitioner sorts and
// routes by, the same lexicographic order the engine keys in. They are spelled out rather than reaching
// for bytes.Compare so this file stands alone as the routing primitive.
func bytesLess(a, b []byte) bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

func bytesEqual(a, b []byte) bool {
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
