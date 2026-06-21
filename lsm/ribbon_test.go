package lsm

import (
	"fmt"
	"testing"
)

// ribbonTestKeys builds n distinct ascending user keys, the membership set a filter is
// built over.
func ribbonTestKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%08d", i))
	}
	return keys
}

// TestRibbonNoFalseNegatives is the safety property the read path leans on: every key
// the filter was built over must test positive. A false negative would make a point read
// skip a segment that actually holds the key, which is a correctness bug, not a tuning
// one. The filter solves each key's equation by construction, so this must hold for every
// key at every size.
func TestRibbonNoFalseNegatives(t *testing.T) {
	for _, n := range []int{1, 2, 5, 33, 100, 1000, 5000} {
		keys := ribbonTestKeys(n)
		f := buildRibbon(keys, bloomBitsPerKey)
		if f == nil {
			t.Fatalf("n=%d: ribbon construction failed at every seed", n)
		}
		for i, k := range keys {
			if !f.mayContain(k) {
				t.Fatalf("n=%d: false negative on inserted key %d (%q)", n, i, k)
			}
		}
	}
}

// TestRibbonFalsePositiveRate checks the filter actually rejects most absent keys, and at
// a rate in the neighborhood the fingerprint width predicts. A filter that passed
// everything would be safe but useless; this pins that the Ribbon is a real filter. The
// bound is loose because the rate is statistical and the sample finite, but a broken
// build (say, an all-ones Z) would blow past it.
func TestRibbonFalsePositiveRate(t *testing.T) {
	const n = 4000
	keys := ribbonTestKeys(n)
	f := buildRibbon(keys, bloomBitsPerKey)
	if f == nil {
		t.Fatal("ribbon construction failed at every seed")
	}
	// Probe keys disjoint from the membership set: a different prefix the build never saw.
	const probes = 20000
	fp := 0
	for i := 0; i < probes; i++ {
		k := []byte(fmt.Sprintf("absent%08d", i))
		if f.mayContain(k) {
			fp++
		}
	}
	rate := float64(fp) / float64(probes)
	// bloomBitsPerKey is 10, so the fingerprint width is round(0.6931*10)=7 bits and the
	// expected rate is 2^-7 ~ 0.0078. Allow generous slack for sampling noise; the point
	// is that the filter rejects the overwhelming majority of absent keys.
	if rate > 0.05 {
		t.Fatalf("false-positive rate %.4f over %d probes is far above the ~0.008 the fingerprint width predicts", rate, probes)
	}
}

// TestRibbonRoundTripThroughEncode checks the filter survives the serialization a
// segment performs: encode to a blob, decode back, and every membership answer must be
// identical, since the decoded filter is what a reopened database actually probes.
func TestRibbonRoundTripThroughEncode(t *testing.T) {
	const n = 2000
	keys := ribbonTestKeys(n)
	f := buildRibbon(keys, bloomBitsPerKey)
	if f == nil {
		t.Fatal("ribbon construction failed at every seed")
	}
	got := decodeRibbon(f.encode())
	if got == nil {
		t.Fatal("decodeRibbon returned nil for a valid blob")
	}
	if got.m != f.m || got.r != f.r || got.seed != f.seed {
		t.Fatalf("decoded params differ: got {m:%d r:%d seed:%d}, want {m:%d r:%d seed:%d}", got.m, got.r, got.seed, f.m, f.r, f.seed)
	}
	for i, k := range keys {
		if f.mayContain(k) != got.mayContain(k) {
			t.Fatalf("membership disagrees after round-trip on key %d (%q)", i, k)
		}
	}
	// And absent keys must agree too, so the decode reproduces the exact bit pattern, not
	// just the positives.
	for i := 0; i < 5000; i++ {
		k := []byte(fmt.Sprintf("absent%08d", i))
		if f.mayContain(k) != got.mayContain(k) {
			t.Fatalf("membership disagrees after round-trip on absent key %q", k)
		}
	}
}

// TestRibbonSmallerThanBloomAtMatchedRate is the space claim the option exists to make:
// at a budget chosen so the two filters land at a comparable false-positive rate, the
// Ribbon's stored bytes are no larger than the Bloom's, and in practice smaller. It
// compares the serialized blob sizes directly.
func TestRibbonSmallerThanBloomAtMatchedRate(t *testing.T) {
	const (
		n          = 10000
		bitsPerKey = 10 // Bloom ~1% rate; Ribbon fingerprint round(0.6931*10)=7 bits, ~0.8%
	)
	keys := ribbonTestKeys(n)

	bloom := newBloom(n, bitsPerKey)
	for _, k := range keys {
		bloom.add(k)
	}
	ribbon := buildRibbon(keys, bitsPerKey)
	if ribbon == nil {
		t.Fatal("ribbon construction failed at every seed")
	}

	bloomBytes := len(bloom.encode())
	ribbonBytes := len(ribbon.encode())
	if ribbonBytes >= bloomBytes {
		t.Fatalf("ribbon blob %d bytes is not smaller than bloom blob %d bytes at matched rate", ribbonBytes, bloomBytes)
	}
	t.Logf("at n=%d, %d bits/key: bloom %d bytes, ribbon %d bytes (%.1f%% of bloom)",
		n, bitsPerKey, bloomBytes, ribbonBytes, 100*float64(ribbonBytes)/float64(bloomBytes))
}

// TestRibbonThroughSegment carries the filter through the real segment path: a segment
// written with kind filterRibbon, reopened cold from its footer page, must reconstruct a
// Ribbon filter that still rejects absent keys and never rejects a present one. This is
// what proves the footer discriminator and the blob encode/decode wire the filter
// correctly end to end, the same path a reopened database takes.
func TestRibbonThroughSegment(t *testing.T) {
	pgr := newSegPager(t)
	const n = 1500
	cells := make([]cell, n)
	for i := 0; i < n; i++ {
		cells[i] = cell{ik(fmt.Sprintf("key%08d", i), 1), []byte(fmt.Sprintf("v%d", i))}
	}
	seg, err := writeSegment(pgr, bloomBitsPerKey, filterRibbon, sourceOf(cells))
	if err != nil {
		t.Fatalf("writeSegment: %v", err)
	}
	if _, ok := seg.filter.(*ribbonFilter); !ok {
		t.Fatalf("segment filter is %T, want *ribbonFilter", seg.filter)
	}

	reopened, err := openSegment(pgr, seg.footer)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	rf, ok := reopened.filter.(*ribbonFilter)
	if !ok {
		t.Fatalf("reopened filter is %T, want *ribbonFilter", reopened.filter)
	}
	// Every present key must survive the round trip with a positive answer.
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%08d", i))
		if !rf.mayContain(k) {
			t.Fatalf("reopened ribbon false-negatives present key %q", k)
		}
	}
	// Absent keys are rejected at the fingerprint rate, so most of a disjoint sample
	// misses; a filter that lost its bits on reopen would pass them all.
	passed := 0
	const probes = 5000
	for i := 0; i < probes; i++ {
		k := []byte(fmt.Sprintf("absent%08d", i))
		if rf.mayContain(k) {
			passed++
		}
	}
	if float64(passed)/float64(probes) > 0.05 {
		t.Fatalf("reopened ribbon passed %d/%d absent keys, far above the fingerprint rate", passed, probes)
	}
}
