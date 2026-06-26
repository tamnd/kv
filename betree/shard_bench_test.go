package betree

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
)

// This file is the fourth and final substance slice of milestone M7, the honesty piece doc 08 names for
// the milestone: the hot-monotonic-key funnel benchmark that documents the funnel regression so the
// opt-in stays honest about where sharding buys nothing. The first three slices built the machinery that
// makes sharding fast when the load spreads (the partition function, the cross-shard merge, the
// cross-shard commit coordinator). This slice measures and pins the case where it does not.
//
// The funnel. Logical sharding parallelizes the commit path by giving each shard its own lock domain, so
// two committers on different shards share no mutex. That win is entirely conditional on the writes
// actually landing in different shards. A workload whose writes concentrate on one key, or on a
// monotonically advancing key under range partitioning, routes every write to a single shard: the other
// shards sit idle, every committer serializes on the one shard's lock, and the sharded coordinator
// performs like a single-shard one (a little worse, in fact, for the routing and frontier overhead it
// still pays). This is not a bug to fix; it is the boundary of what logical sharding can do, and the
// reason the mode is opt-in rather than a default. The benchmark exists so the boundary is measured and
// documented rather than discovered in production.
//
// What is measured here, and what is asserted. The benchmark BenchmarkShardCommit reports commit
// throughput under concurrent committers for two workloads at several shard counts: a spread workload
// (every write a distinct key, hash-routed, so the writes fan across shards) and a hot-key workload
// (every write to one key, so they funnel to one shard). The hot-key numbers stay flat near the
// single-shard figure at every shard count, which is the funnel made visible: more shards buy a hot key
// nothing, because every committer serializes on the one shard it routes to.
//
// An honest finding the spread numbers carry. On this in-memory proof substrate the spread workload does
// not pull ahead of the hot-key one as shards rise either, and the reason is worth recording rather than
// hiding: the per-shard work a commit does here is tiny (encode an internal key, append it to a slice),
// so the per-commit cost is dominated by the coordination that is global by design, the one monotonic
// version counter and the short frontier critical section, plus the few allocations each commit makes.
// With the per-shard lock barely contended even at one shard, adding shards cannot move a number the
// global section already floors. That floor is the residual serialization doc 05 names as the one piece
// of global coordination left, and the spread parallelism the design predicts only becomes visible once
// the per-shard work is the dominant cost, which is the pager-backed per-shard commit (tree descent, WAL
// append, hot-tail insert) the M8 integration adds under the shard lock. The benchmark records the
// coordination floor as it stands; it does not pretend a scaling the substrate has not earned yet.
//
// Because the timing is both noisy and substrate-limited, the funnel is pinned for CI by the
// deterministic tests below, which assert the structural cause of the regression directly: the per-shard
// load distribution. A hot key puts all its load on one shard, a monotonic tail under range partitioning
// puts all its load on the top shard, and only a spread workload distributes, which is exactly the
// precondition the parallelism needs. Those assertions hold the funnel story stable without depending on
// wall-clock timing.

// oneByte is the tiny value every benchmark write carries, so the benchmark measures commit coordination
// rather than value copying.
var oneByte = []byte{0x01}

// hotKey is the single key the hot-key workload hammers; every write to it routes to one shard under
// every partitioner, which is the funnel the benchmark documents.
var hotKey = []byte("hot-counter")

// shardLoadHistogram routes each key through the partitioner and returns the per-shard count, the write
// distribution that decides whether a workload can parallelize across shards. An even histogram is the
// precondition for the sharding win; a histogram with one tall bar is the funnel.
func shardLoadHistogram(p partitioner, keys [][]byte) []int {
	load := make([]int, p.shards())
	for _, k := range keys {
		load[p.route(k)]++
	}
	return load
}

// maxLoadFraction returns the share of writes that landed in the single busiest shard. It is 1.0 when one
// shard takes everything (the funnel) and near 1/shards when the load spreads evenly (the win).
func maxLoadFraction(load []int) float64 {
	total, max := 0, 0
	for _, n := range load {
		total += n
		if n > max {
			max = n
		}
	}
	if total == 0 {
		return 0
	}
	return float64(max) / float64(total)
}

// spreadKey is the distinct-key generator both the spread benchmark and the range-split helper key on, a
// fixed-width decimal so lexicographic order matches numeric order and even range splits divide it
// cleanly.
func spreadKey(i int) []byte { return []byte(fmt.Sprintf("key-%08d", i)) }

// evenRangeSplits builds shards-1 boundary keys evenly spaced over the [0, span) spreadKey domain, so a
// range partitioner over them carves that domain into equal contiguous bands. It lets the range tests
// reason about which band a key falls in.
func evenRangeSplits(shards, span int) [][]byte {
	splits := make([][]byte, 0, shards-1)
	for i := 1; i < shards; i++ {
		splits = append(splits, spreadKey(i*span/shards))
	}
	return splits
}

// TestHotKeyFunnelsToOneShard pins the core of the funnel: a single key, written any number of times,
// routes every write to exactly one shard under both partitioners. The max load fraction is 1.0 and only
// one shard is non-empty, which is why a hot-key workload cannot use more than one shard's worth of
// commit parallelism no matter how many shards are configured.
func TestHotKeyFunnelsToOneShard(t *testing.T) {
	const shards = 16
	const writes = 100000

	keys := make([][]byte, writes)
	for i := range keys {
		keys[i] = hotKey
	}

	parts := map[string]partitioner{
		"hash":  newHashPartitioner(shards),
		"range": newRangePartitioner(evenRangeSplits(shards, 1_000_000)),
	}
	for name, p := range parts {
		load := shardLoadHistogram(p, keys)
		if f := maxLoadFraction(load); f != 1.0 {
			t.Errorf("%s: hot key spread to busiest-shard fraction %.3f, want 1.0 (one shard takes all)", name, f)
		}
		nonempty := 0
		for _, n := range load {
			if n > 0 {
				nonempty++
			}
		}
		if nonempty != 1 {
			t.Errorf("%s: hot key touched %d shards, want exactly 1", name, nonempty)
		}
	}
}

// TestMonotonicKeyFunnelsUnderRange pins the monotonic half of the funnel: an append-only writer whose
// keys keep advancing always writes into the top band of a range partitioner, so every write funnels to
// the last shard even though the keys are all distinct. The same monotonic tail under hash partitioning
// scatters across shards, which is the contrast that explains why hash is the default for write-heavy
// load and range is the opt-in for scan locality.
func TestMonotonicKeyFunnelsUnderRange(t *testing.T) {
	const shards = 16
	const span = 1_000_000
	const tail = 5000

	// The newest tail of an append-only key space, every key above the last split.
	keys := make([][]byte, 0, tail)
	for i := span - tail; i < span; i++ {
		keys = append(keys, spreadKey(i))
	}

	rangeLoad := shardLoadHistogram(newRangePartitioner(evenRangeSplits(shards, span)), keys)
	if rangeLoad[shards-1] != len(keys) {
		t.Errorf("range: monotonic tail put %d/%d writes in the top shard, want all of them", rangeLoad[shards-1], len(keys))
	}

	hashFraction := maxLoadFraction(shardLoadHistogram(newHashPartitioner(shards), keys))
	if hashFraction > 0.25 {
		t.Errorf("hash: monotonic tail concentrated at busiest-shard fraction %.3f, want it spread well below 0.25", hashFraction)
	}
}

// TestSpreadWorkloadDistributes pins the other side of the trade: a distinct-key workload under hash
// partitioning spreads close to evenly, the precondition the commit parallelism needs. The busiest shard
// holds within half-again of its fair share, so no single lock domain becomes the bottleneck the funnel
// makes it.
func TestSpreadWorkloadDistributes(t *testing.T) {
	const shards = 16
	const writes = 160000

	keys := make([][]byte, writes)
	for i := range keys {
		keys[i] = spreadKey(i)
	}
	load := shardLoadHistogram(newHashPartitioner(shards), keys)
	fair := 1.0 / float64(shards)
	if f := maxLoadFraction(load); f > 1.5*fair {
		t.Errorf("hash: busiest-shard fraction %.3f over %d shards, want near the fair share %.3f", f, shards, fair)
	}
}

// BenchmarkShardCommit reports commit throughput under concurrent committers for the spread workload
// (distinct keys fanning across shards) and the hot-key workload (one key funneling to one shard), at a
// range of shard counts. The funnel documentation is in reading the numbers across shard counts: hotkey
// per-op stays flat near the shards=1 figure because every committer serializes on the one shard the hot
// key routes to. On this in-memory substrate spread stays flat too, because the per-commit cost is
// dominated by the global version counter, the frontier critical section, and the per-commit allocations
// rather than by the per-shard lock the spread workload would parallelize (see the file header). Run it
// with go test -run '^$' -bench BenchmarkShardCommit -benchmem in the betree package.
func BenchmarkShardCommit(b *testing.B) {
	for _, shards := range []int{1, 4, 16} {
		shards := shards

		b.Run(fmt.Sprintf("spread/shards=%d", shards), func(b *testing.B) {
			c := newShardCoord(newHashPartitioner(shards))
			var ctr atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := ctr.Add(1)
					key := []byte("key-" + strconv.FormatUint(id, 10))
					c.Commit([]shardWrite{{key: key, val: oneByte}})
				}
			})
		})

		b.Run(fmt.Sprintf("hotkey/shards=%d", shards), func(b *testing.B) {
			c := newShardCoord(newHashPartitioner(shards))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c.Commit([]shardWrite{{key: hotKey, val: oneByte}})
				}
			})
		})
	}
}
