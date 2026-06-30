package hash

import (
	"hash/fnv"
	"hash/maphash"
	"math"
	"testing"
)

func benchHash(b *testing.B, h func(key []byte) uint64) {
	keys := HashKeys(4096, BenchKeyLen)
	b.ResetTimer()
	var sink uint64
	for i := 0; i < b.N; i++ {
		sink ^= h(keys[i&4095])
	}
	runtimeSink = sink
}

var runtimeSink uint64

func BenchmarkHashMaphashBytes(b *testing.B) {
	benchHash(b, func(key []byte) uint64 { return maphash.Bytes(HashSeed, key) })
}

func BenchmarkHashMaphashStream(b *testing.B) {
	benchHash(b, func(key []byte) uint64 {
		var h maphash.Hash
		h.SetSeed(HashSeed)
		h.Write(key)
		return h.Sum64()
	})
}

func BenchmarkHashFNVInterface(b *testing.B) {
	benchHash(b, func(key []byte) uint64 {
		h := fnv.New64a()
		h.Write(key)
		return h.Sum64()
	})
}

func BenchmarkHashFNVInline(b *testing.B) {
	benchHash(b, InlineFNV1a)
}

// TestHashSpread is the quality gate behind the speed board: a fast hash is only the right
// choice if it also spreads keys evenly, or the index degrades to long probe chains. It hashes
// many keys into buckets and checks the distribution is near uniform with a chi-square
// statistic, so the speed winner is not a hash that clusters.
func TestHashSpread(t *testing.T) {
	const n = 1 << 16
	const buckets = 1 << 10
	keys := HashKeys(n, BenchKeyLen)
	seed := maphash.MakeSeed()

	counts := make([]int, buckets)
	for _, k := range keys {
		counts[maphash.Bytes(seed, k)&(buckets-1)]++
	}

	expected := float64(n) / float64(buckets)
	var chi2 float64
	for _, c := range counts {
		d := float64(c) - expected
		chi2 += d * d / expected
	}
	mean := float64(buckets - 1)
	sigma := math.Sqrt(2 * mean)
	if chi2 > mean+8*sigma {
		t.Fatalf("hash spread too clustered: chi2=%.1f, expected near %.0f (sigma %.1f)", chi2, mean, sigma)
	}
}
