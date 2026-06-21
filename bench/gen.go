package bench

import (
	"encoding/binary"
	"math/rand"
)

// Distribution is how a workload picks which key to touch next. The choice is the whole
// story of a benchmark: a uniform distribution defeats the cache and the Bloom filters,
// a Zipfian one rewards them, and a sequential one walks the keyspace in order the way a
// bulk load or a scan does. Reporting a throughput number without naming the distribution
// it ran under is meaningless, so every workload carries one (spec 21 §2).
type Distribution uint8

const (
	// Sequential walks keys 0,1,2,... in order, the bulk-load and scan access pattern.
	Sequential Distribution = iota
	// Uniform draws every key with equal probability, the cache-hostile pattern that
	// measures the cold read path and the filters' true negative cost.
	Uniform
	// Zipfian draws a few hot keys far more often than the cold tail, the skewed pattern
	// real workloads show and the one the cache and the recent-write paths are tuned for
	// (spec 21 §2.2, point-read under skew).
	Zipfian
)

// Generator turns a deterministic stream of draws into concrete keys and values. Every
// number it produces comes from a seeded PRNG, never from the wall clock, so two runs with
// the same seed touch the same keys in the same order and a result is reproducible across
// machines and across time (spec 21 §5). The generator is not safe for concurrent use; a
// concurrent workload gives each worker its own generator with its own seed offset.
type Generator struct {
	keyCount   int
	keyLen     int
	valLen     int
	dist       Distribution
	rng        *rand.Rand
	zipf       *rand.Zipf
	seqCursor  uint64
	valScratch []byte
}

// GenConfig parameterizes a Generator. KeyCount is the size of the keyspace the draws
// range over, KeyLen and ValLen the fixed encoded widths in bytes, Dist the access
// distribution, and Seed the PRNG seed that makes the run deterministic.
type GenConfig struct {
	KeyCount int
	KeyLen   int
	ValLen   int
	Dist     Distribution
	Seed     int64
	// ZipfS is the Zipfian skew exponent (s > 1); larger is more skewed. Ignored unless
	// Dist is Zipfian. Zero defaults to a moderate 1.1, a realistic web-traffic skew.
	ZipfS float64
}

// minimum widths keep a key big enough to hold its encoded index and a value non-empty.
const (
	minKeyLen = 16
	minValLen = 8
)

// NewGenerator builds a Generator from cfg, clamping the widths to sane minimums and
// seeding the PRNG so the draw sequence is fixed by cfg.Seed alone.
func NewGenerator(cfg GenConfig) *Generator {
	if cfg.KeyLen < minKeyLen {
		cfg.KeyLen = minKeyLen
	}
	if cfg.ValLen < minValLen {
		cfg.ValLen = minValLen
	}
	if cfg.KeyCount < 1 {
		cfg.KeyCount = 1
	}
	rng := rand.New(rand.NewSource(cfg.Seed))
	g := &Generator{
		keyCount:   cfg.KeyCount,
		keyLen:     cfg.KeyLen,
		valLen:     cfg.ValLen,
		dist:       cfg.Dist,
		rng:        rng,
		valScratch: make([]byte, cfg.ValLen),
	}
	if cfg.Dist == Zipfian {
		s := cfg.ZipfS
		if s <= 1 {
			s = 1.1
		}
		// rand.NewZipf draws from [0, imax]; imax is the largest key index.
		g.zipf = rand.NewZipf(rng, s, 1, uint64(cfg.KeyCount-1))
	}
	return g
}

// nextIndex returns the next key index according to the distribution. Sequential advances
// a cursor and wraps at the keyspace end; uniform and Zipfian draw from the PRNG.
func (g *Generator) nextIndex() uint64 {
	switch g.dist {
	case Sequential:
		i := g.seqCursor % uint64(g.keyCount)
		g.seqCursor++
		return i
	case Zipfian:
		return g.zipf.Uint64()
	default: // Uniform
		return uint64(g.rng.Intn(g.keyCount))
	}
}

// Key returns the key for index i, written into dst (which must be at least KeyLen) and
// re-sliced to KeyLen. The encoding is a fixed-width big-endian index followed by a stable
// ASCII filler, so keys sort in index order and every key is exactly KeyLen bytes. dst is
// reused across calls to keep the hot path allocation-free; copy the bytes if you retain
// them past the next call.
func (g *Generator) Key(dst []byte, i uint64) []byte {
	if cap(dst) < g.keyLen {
		dst = make([]byte, g.keyLen)
	}
	dst = dst[:g.keyLen]
	// A fixed "key:" tag then the 8-byte big-endian index, so the lexical order matches the
	// numeric order and a sequential scan visits indices in order.
	n := copy(dst, "key:")
	binary.BigEndian.PutUint64(dst[n:], i)
	// Pad the remainder with a deterministic, mildly compressible filler.
	for j := n + 8; j < g.keyLen; j++ {
		dst[j] = byte('a' + (j % 26))
	}
	return dst
}

// NextKey draws the next key index from the distribution and returns its key in dst.
func (g *Generator) NextKey(dst []byte) []byte {
	return g.Key(dst, g.nextIndex())
}

// Value returns a deterministic value for index i. The value carries a long shared prefix
// so a page of values compresses (it exercises the block-compression path, spec 13) and a
// per-index tail so distinct keys carry distinct values and a read can be checked against
// the key it came from. The returned slice is reused across calls.
func (g *Generator) Value(i uint64) []byte {
	v := g.valScratch
	// A shared, compressible prefix fills most of the value.
	const prefix = "kv-benchmark-value-shared-prefix-"
	n := copy(v, prefix)
	// The index, big-endian, distinguishes values; the rest is stable filler.
	if n+8 <= len(v) {
		binary.BigEndian.PutUint64(v[n:], i)
		n += 8
	}
	for j := n; j < len(v); j++ {
		v[j] = byte('0' + (j % 10))
	}
	return v
}

// KeyCount is the size of the keyspace the generator draws over.
func (g *Generator) KeyCount() int { return g.keyCount }

// BytesPerOp is the logical bytes one key+value pair represents, the denominator of the
// space and write amplification ratios (spec 21 §1).
func (g *Generator) BytesPerOp() int { return g.keyLen + g.valLen }
