package lsm

import "math"

// bloomFilter is a per-segment Bloom filter over the segment's distinct user keys
// (spec 06 §5). A point read consults it before touching a segment's block index or
// data pages: a negative answer is definitive, so the segment is skipped; a positive
// answer may be a false positive, so the read proceeds and the block index confirms.
// For a workload whose keys are spread across many segments this turns most point
// misses from one index seek per segment into one cheap in-memory probe per segment.
//
// The construction is the classic double-hashing Bloom filter (Kirsch-Mitzenmacher):
// one base hash of the key yields two 32-bit values, and the k probe positions are
// h1 + i*h2 for i in [0, k). The bit budget is a per-key rate, and that rate now varies
// by level: a segment carries its own probe count k in its footer and its bit-array
// length in its filter pages, so two segments can hold filters of different sizes with
// no format change. bloomBitsForLevel is the Monkey allocation that picks the rate from
// the level a segment is written at.

const (
	// bloomBitsTop is the bit budget per key at L0 and L1, the smallest and most
	// frequently probed levels. Twelve bits gives roughly a quarter-percent
	// false-positive rate at the optimal probe count, cheap because these levels hold
	// few keys.
	bloomBitsTop = 12
	// bloomBitsFloor is the budget the deepest levels fall to. They hold most of the
	// keys, so a bit spent there buys the least reduction in total false positives, and
	// Monkey starves them first. Four bits is roughly a fifteen-percent rate, which still
	// skips most segments a key was never in.
	bloomBitsFloor = 4
	// bloomBitsPerKey is the flat default a filter built without level context uses: a
	// direct construction in a test, or any caller that does not pin a level. Ten bits is
	// the usual one-percent-rate default.
	bloomBitsPerKey = 10
)

// monkeyStep is the bits-per-key Monkey removes for each level deeper, ln(T)/ln(2)^2,
// the additive step that makes each level's false-positive rate a factor of T larger
// than the level above it (Dayan, Athanassoulis, Idreos; Monkey, SIGMOD 2017). Under a
// fixed total bit budget that allocation minimizes the sum of false positives across the
// tree, because the levels with the most keys are the least cost-effective to filter. It
// is clamped to at least one so the budget always decreases with depth.
func monkeyStep(levelRatio int) int {
	if levelRatio < 2 {
		return 1
	}
	step := int(math.Round(math.Log(float64(levelRatio)) / (math.Ln2 * math.Ln2)))
	if step < 1 {
		step = 1
	}
	return step
}

// bloomBitsForLevel is the Monkey bit budget for a segment written at the given level:
// the top budget for L0 and L1, dropping by monkeyStep for each level below L1, floored.
// A shallower (smaller) level gets more bits because its filter is cheap and a read
// probes it as often as any other; the deepest (largest) levels fall to the floor
// because the same bits buy far fewer avoided false positives there.
func bloomBitsForLevel(level, levelRatio int) int {
	if level <= 1 {
		return bloomBitsTop
	}
	bits := bloomBitsTop - (level-1)*monkeyStep(levelRatio)
	if bits < bloomBitsFloor {
		bits = bloomBitsFloor
	}
	return bits
}

// bloomFilter holds the bit array and the probe count. A nil filter, or one with no
// bits, means "no filter", and mayContain conservatively returns true so the read
// always proceeds.
type bloomFilter struct {
	bits []byte // the bit array; bit i lives in bits[i/8] at position i%8
	k    uint32 // number of probes per key
}

// bloomK returns the probe count that minimizes the false-positive rate for a given
// bits-per-key, k = ln2 * bitsPerKey, clamped to a sane range.
func bloomK(bitsPerKey int) uint32 {
	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}

// newBloom returns an empty filter sized for numKeys distinct keys at bitsPerKey
// bits each, with a small floor so even a tiny segment carries a usable filter.
func newBloom(numKeys, bitsPerKey int) *bloomFilter {
	bits := numKeys * bitsPerKey
	if bits < 64 {
		bits = 64
	}
	nbytes := (bits + 7) / 8
	return &bloomFilter{bits: make([]byte, nbytes), k: bloomK(bitsPerKey)}
}

// nbits reports the bit-array length, the modulus every probe reduces against.
func (f *bloomFilter) nbits() uint32 { return uint32(len(f.bits)) * 8 }

// add records key in the filter.
func (f *bloomFilter) add(key []byte) {
	h := bloomHash(key)
	delta := (h >> 17) | (h << 15) // a second independent hash by rotation
	n := f.nbits()
	for i := uint32(0); i < f.k; i++ {
		bit := h % n
		f.bits[bit/8] |= 1 << (bit % 8)
		h += delta
	}
}

// mayContain reports whether key might be in the segment. False is definitive: the
// key was never added. True may be a false positive. A nil or empty filter returns
// true, so a segment without a filter is always read.
func (f *bloomFilter) mayContain(key []byte) bool {
	if f == nil || len(f.bits) == 0 {
		return true
	}
	h := bloomHash(key)
	delta := (h >> 17) | (h << 15)
	n := f.nbits()
	for i := uint32(0); i < f.k; i++ {
		bit := h % n
		if f.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// bloomHash is a 32-bit FNV-1a hash, a self-contained dependency-free hash adequate
// for a Bloom filter's probe derivation. The constant must stay frozen so a filter
// written by one build is read identically by the next.
func bloomHash(b []byte) uint32 {
	const (
		offset = 2166136261
		prime  = 16777619
	)
	h := uint32(offset)
	for _, c := range b {
		h ^= uint32(c)
		h *= prime
	}
	return h
}
