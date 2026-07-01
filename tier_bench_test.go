package kv

import (
	"path/filepath"
	"testing"
)

// This file measures the tiered store end to end, so the note 179 thesis is checked against a
// real Set and Get through migration, not an isolated index Put. The workload is skewed,
// because that is what the hot/cold split is for: a small hot set takes most of the traffic,
// which is the common shape for a key-value store and the one F2 targets. Under uniform random
// access there is no hot set and the cold tier carries everything, so a skewed access pattern
// is the honest test of the architecture, and the skew here is deterministic, no rand, which
// the harness forbids.

const tierBenchKeys = 1 << 20 // ~1M distinct keys, far past one hot segment
const tierHotKeys = 1 << 12   // 4K hot keys take most of the traffic
const tierBenchVal = "a-record-value-around-a-hundred-bytes-so-the-write-path-moves-realistic-payload-sizes-xx"

// skewIndex maps a loop counter to a key id with a fixed skew: most iterations hit the hot
// set, a minority spread across the cold keyspace. The split is by a cheap bit test so the
// pattern is deterministic and allocation-free.
func skewIndex(i uint64) uint64 {
	if i&7 == 0 { // one in eight goes cold, spread across the whole space
		return (i * 2654435761) & (tierBenchKeys - 1)
	}
	return (i * 40503) & (tierHotKeys - 1) // the rest stay in the hot set
}

func openTierBench(b *testing.B) *TieredDB {
	path := filepath.Join(b.TempDir(), "tier.log")
	// One hot segment holds the hot set comfortably; the cold tier and its index are sized to
	// the whole keyspace, but they sit off the write hot path, which is the point.
	d, err := OpenTiered(path, 1<<20, tierHotKeys*4, 1<<22, tierBenchKeys, 1<<16)
	if err != nil {
		b.Fatal(err)
	}
	return d
}

// BenchmarkTieredSetSkewed measures write throughput under skew. Most writes update a hot key,
// which stays in the small active segment and its bounded index, so the per-write index cost
// is the cache-resident one note 179 measured, not the keyspace scatter, even though the store
// holds a million keys.
func BenchmarkTieredSetSkewed(b *testing.B) {
	d := openTierBench(b)
	defer d.Close()
	key := make([]byte, 8)
	val := []byte(tierBenchVal)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range uint64(b.N) {
		fixedKey(key, skewIndex(i))
		d.Set(key, val)
	}
}

// BenchmarkTieredGetSkewed measures read throughput under skew after a fill. A hot key is
// served from the active or sealed segment, a recently missed cold key from the read cache, so
// most reads never touch the file. This is the number the read-cache and hot tier are built to
// move.
func BenchmarkTieredGetSkewed(b *testing.B) {
	d := openTierBench(b)
	defer d.Close()
	key := make([]byte, 8)
	val := []byte(tierBenchVal)
	for i := range uint64(tierBenchKeys) {
		fixedKey(key, i)
		d.Set(key, val)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		k := make([]byte, 8)
		scratch := make([]byte, 0, 256)
		var i uint64
		for pb.Next() {
			fixedKey(k, skewIndex(i))
			d.Get(k, scratch)
			i++
		}
	})
}
