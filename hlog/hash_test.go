package hlog

import (
	"hash/maphash"
	"math"
	"testing"
)

// TestHashSpread is the quality gate behind the speed benchmark: a fast hash is only the
// right choice if it also spreads keys evenly, or the index degrades to long probe
// chains. It hashes many keys into buckets and checks the bucket-count distribution is
// close to uniform with a chi-square statistic, so the speed winner is not a hash that
// clusters. This guards the chosen index hash, maphash.Bytes, against a regression to a
// poorly distributing function.
func TestHashSpread(t *testing.T) {
	const n = 1 << 16
	const buckets = 1 << 10
	keys := hashKeys(n, benchKeyLen)
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
	// For buckets-1 degrees of freedom the chi-square mean is buckets-1 and the standard
	// deviation is sqrt(2*(buckets-1)). A healthy hash lands within a few sigma. A wide
	// bound here keeps the test about catching a clustering hash, not about policing
	// normal statistical noise.
	mean := float64(buckets - 1)
	sigma := math.Sqrt(2 * mean)
	if chi2 > mean+8*sigma {
		t.Fatalf("hash spread too clustered: chi2=%.1f, expected near %.0f (sigma %.1f)", chi2, mean, sigma)
	}
}
