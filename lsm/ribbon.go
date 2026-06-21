package lsm

import (
	"math/bits"

	"github.com/tamnd/kv/format"
)

// ribbonFilter is the opt-in alternative to the per-segment Bloom filter (spec 06 §5).
// It answers the same question, "might this segment hold the key", with the same
// one-sided guarantee a Bloom filter gives: a negative is definitive, so the segment
// is skipped, and a positive may be a false positive, so the read proceeds and the
// block index confirms. What it changes is the space it spends to hit a given
// false-positive rate. A Bloom filter at b bits per key reaches a rate near 0.6185^b;
// a Ribbon filter storing r fingerprint bits per slot reaches 2^-r with only a small
// constant of slot overhead, so the same rate costs meaningfully fewer bits, which is
// exactly the win on the deep, cold levels where filters dominate the resident set
// (Dillinger and Walzer, "Ribbon filter: practically smaller than Bloom and Xor",
// 2021). Bloom stays the default for its simpler, faster probes on the hot levels.
//
// The construction is Standard Ribbon: each key contributes one equation over GF(2)
// in a banded coefficient matrix, the band a width-w window of consecutive solution
// slots starting at a hashed position, and the right-hand side the key's r-bit
// fingerprint. Inserting a key is on-the-fly Gaussian elimination against the rows
// already pivoted, which keeps the matrix in echelon form as it is built. Once every
// key is inserted, back-substitution solves for the solution vector Z, r bits per
// slot, and that vector is all the filter stores. A query recomputes the key's band,
// coefficients, and fingerprint, XORs the Z slots its coefficients select, and reports
// a hit when that XOR equals the fingerprint. An inserted key always hits, because its
// equation is satisfied by construction, so the filter never reports a false negative,
// the property the read path depends on. A non-key hits only when its fingerprint
// collides, with probability 2^-r.
//
// Construction can fail: if a key's equation reduces to zero coefficients with a
// nonzero fingerprint the banded system is inconsistent at that seed. A little slot
// overcapacity makes that rare, and a failure simply reseeds and rebuilds. If every
// seed in a bounded budget fails, the caller falls back to a Bloom filter, so the
// writer is infallible and a segment always carries a working filter.

// ribbonWidth is the band width w: each key's equation touches w consecutive solution
// slots. It is fixed at 32 so a coefficient row and its shifted eliminations both fit
// in a uint64 (a stored row's leading bit sits at relative position 0 and spans at most
// w-1 more bits, and an incoming row is reduced by shifting a stored row left by up to
// w-1, so the widest live value occupies under 2w-1 < 64 bits).
const ribbonWidth = 32

// ribbon construction tuning. ribbonMaxSeeds bounds the reseed budget before the
// caller falls back to Bloom; ribbon construction succeeds on the first seed for the
// overwhelming majority of inputs at this overcapacity, so the budget is a safety net,
// not a hot path.
const ribbonMaxSeeds = 16

// ribbonFingerprintFactor maps a Bloom-style bits-per-key budget to a Ribbon
// fingerprint width. A Bloom filter at b bits per key reaches a false-positive rate of
// roughly 0.6185^b = 2^(-0.6931*b), so r = round(0.6931*b) fingerprint bits give a
// Ribbon filter the same rate. At its ~12% slot overcapacity that lands the Ribbon's
// total bits below the Bloom's at the same rate, the lower-space trade the option
// exists to offer.
const ribbonFingerprintFactor = 0.6931

// ribbonFilter holds the solved Z vector bit-packed at r bits per slot, plus the
// parameters a query needs to rederive a key's band, coefficients, and fingerprint:
// the slot count m, the fingerprint width r, and the seed the successful build used.
type ribbonFilter struct {
	z    []byte // solution vector, r bits per slot, slot i at bit offset i*r
	m    int    // number of solution slots
	r    int    // fingerprint bits per slot
	seed uint64 // hash seed the successful construction settled on
}

// ribbonRow is one pivoted equation held during construction: its coefficient mask
// normalized so the pivot slot is relative bit 0, and its right-hand-side fingerprint.
type ribbonRow struct {
	coeff uint64
	rhs   uint64
	used  bool
}

// ribbonFingerprintBits turns a bits-per-key budget into a fingerprint width, clamped
// to [1, ribbonWidth]; the upper bound is the entropy a single hash word affords a
// fingerprint here.
func ribbonFingerprintBits(bitsPerKey int) int {
	r := int(float64(bitsPerKey)*ribbonFingerprintFactor + 0.5)
	if r < 1 {
		r = 1
	}
	if r > ribbonWidth {
		r = ribbonWidth
	}
	return r
}

// ribbonSlots sizes the solution vector for n keys: n plus about an eighth for
// overcapacity, plus a width's margin so the high band positions are not starved and
// the system stays solvable at the first seed for nearly all inputs.
func ribbonSlots(n int) int {
	m := n + n/8 + ribbonWidth
	if m < ribbonWidth {
		m = ribbonWidth
	}
	return m
}

// buildSegFilter builds the segment filter of the requested kind over keys at the given
// bits-per-key budget. A Ribbon build that cannot be seeded falls back to a Bloom
// filter, so the writer always returns a working filter and the caller never has to
// handle a build failure.
func buildSegFilter(kind filterKind, keys [][]byte, bitsPerKey int) segFilter {
	if kind == filterRibbon {
		if rf := buildRibbon(keys, bitsPerKey); rf != nil {
			return rf
		}
	}
	bf := newBloom(len(keys), bitsPerKey)
	for _, k := range keys {
		bf.add(k)
	}
	return bf
}

// buildRibbon constructs a Ribbon filter over keys at the given bits-per-key budget,
// reseeding on a construction failure up to the seed budget. It returns nil when every
// seed fails, the signal for the caller to fall back to a Bloom filter. keys must hold
// the segment's distinct user keys; duplicates would add redundant equations but not
// break correctness.
func buildRibbon(keys [][]byte, bitsPerKey int) *ribbonFilter {
	r := ribbonFingerprintBits(bitsPerKey)
	m := ribbonSlots(len(keys))
	for attempt := 0; attempt < ribbonMaxSeeds; attempt++ {
		// Vary the seed by attempt; seed 0 is excluded so the homogeneous hash mixing
		// always has a nonzero base to fold in.
		seed := uint64(attempt)*0x9e3779b97f4a7c15 + 0x1
		f := &ribbonFilter{m: m, r: r, seed: seed}
		if f.construct(keys) {
			return f
		}
	}
	return nil
}

// construct inserts every key's equation by on-the-fly Gaussian elimination, then
// back-substitutes for Z. It returns false when the banded system is inconsistent at
// this seed, leaving f unusable so the caller reseeds.
func (f *ribbonFilter) construct(keys [][]byte) bool {
	rows := make([]ribbonRow, f.m)
	for _, key := range keys {
		s, c, b := f.derive(key)
		if !insertRibbonRow(rows, s, c, b) {
			return false
		}
	}
	f.backSubstitute(rows)
	return true
}

// insertRibbonRow folds one equation into the echelon rows by on-the-fly Gaussian
// elimination. The equation's band starts at slot s with coefficient mask c (low bit
// forced set, relative to s) and fingerprint b. The mask is carried relative to its
// current leading column, shifted right as the leading column advances, so it stays
// within the band width: each stored pivot row has its leading bit at relative 0 and
// spans w bits, and the incoming row is aligned to the same column before the XOR, so
// the result never exceeds w bits and the band never grows past the matrix. A row whose
// coefficients vanish with a nonzero fingerprint is inconsistent and fails the build.
func insertRibbonRow(rows []ribbonRow, s int, c, b uint64) bool {
	col := s
	for {
		if c == 0 {
			// Consistent only if the fingerprint also vanished; otherwise the banded
			// system has no solution at this seed.
			return b == 0
		}
		// Advance to the equation's current leading column, keeping c relative to it.
		tz := bits.TrailingZeros64(c)
		c >>= uint(tz)
		col += tz
		if !rows[col].used {
			// The pivot column is free: this row becomes its pivot, leading bit at
			// relative 0.
			rows[col] = ribbonRow{coeff: c, rhs: b, used: true}
			return true
		}
		// Eliminate against the stored pivot row, which shares this leading column and is
		// already relative to it, so the XOR stays within the band. The leading bit
		// cancels (both are 1), and the loop advances to the next leading column.
		c ^= rows[col].coeff
		b ^= rows[col].rhs
	}
}

// backSubstitute solves the echelon rows for Z, processing slots high to low so each
// pivot row's off-diagonal coefficients reference slots already solved. A free slot
// (no pivot row) takes Z = 0.
func (f *ribbonFilter) backSubstitute(rows []ribbonRow) {
	f.z = make([]byte, (f.m*f.r+7)/8)
	mask := uint64(1)<<uint(f.r) - 1
	for p := f.m - 1; p >= 0; p-- {
		if !rows[p].used {
			continue // free variable, Z[p] stays zero
		}
		v := rows[p].rhs
		coeff := rows[p].coeff
		for j := 1; j < ribbonWidth; j++ {
			if coeff&(1<<uint(j)) != 0 {
				v ^= f.laneGet(p + j)
			}
		}
		f.laneSet(p, v&mask)
	}
}

// derive maps a key to its equation: the band start s in [0, m-w], the coefficient mask
// c (w bits with the low bit forced set so the band always has a pivot), and the r-bit
// fingerprint b. Two independent hash words give s its own entropy and split the other
// word's halves between the coefficients and the fingerprint.
func (f *ribbonFilter) derive(key []byte) (s int, c, b uint64) {
	h1 := ribbonHash(key, f.seed)
	h2 := ribbonHash(key, f.seed*0x9e3779b97f4a7c15+1)
	span := f.m - ribbonWidth + 1
	s = int(h1 % uint64(span))
	c = uint64(uint32(h2)) | 1
	b = (h2 >> 32) & (uint64(1)<<uint(f.r) - 1)
	return s, c, b
}

// mayContain reports whether key might be in the segment. It rederives the key's band,
// XORs the Z slots its coefficients select, and reports a hit when that XOR equals the
// key's fingerprint. An inserted key always hits; a non-key hits with probability 2^-r.
func (f *ribbonFilter) mayContain(key []byte) bool {
	if f == nil || len(f.z) == 0 {
		return true
	}
	s, c, b := f.derive(key)
	var acc uint64
	for j := 0; j < ribbonWidth; j++ {
		if c&(1<<uint(j)) != 0 {
			acc ^= f.laneGet(s + j)
		}
	}
	return acc == b
}

// laneGet reads slot i's r-bit value out of the packed Z vector.
func (f *ribbonFilter) laneGet(i int) uint64 {
	base := i * f.r
	var v uint64
	for bit := 0; bit < f.r; bit++ {
		p := base + bit
		if f.z[p>>3]&(1<<uint(p&7)) != 0 {
			v |= 1 << uint(bit)
		}
	}
	return v
}

// laneSet writes slot i's r-bit value into the packed Z vector. The value is assumed
// already masked to r bits.
func (f *ribbonFilter) laneSet(i int, v uint64) {
	base := i * f.r
	for bit := 0; bit < f.r; bit++ {
		if v&(1<<uint(bit)) != 0 {
			p := base + bit
			f.z[p>>3] |= 1 << uint(p&7)
		}
	}
}

// encode serializes the filter into the blob its segment filter pages hold: a small
// self-describing header (fingerprint width, slot count, seed) followed by the packed Z
// vector. The segment footer's filter-kind discriminator tells the reader to decode a
// blob this way rather than as a Bloom bit array.
func (f *ribbonFilter) encode() []byte {
	out := make([]byte, 0, 16+len(f.z))
	out = format.AppendUvarint(out, uint64(f.r))
	out = format.AppendUvarint(out, uint64(f.m))
	out = format.AppendUvarint(out, f.seed)
	out = append(out, f.z...)
	return out
}

// decodeRibbon reconstructs a Ribbon filter from the blob encode wrote. A blob too
// short to hold the header yields nil, which leaves the segment filterless and so
// always read, the same conservative fallback a missing filter takes.
func decodeRibbon(blob []byte) *ribbonFilter {
	r, n := format.Uvarint(blob)
	if n <= 0 {
		return nil
	}
	off := n
	m, n := format.Uvarint(blob[off:])
	if n <= 0 {
		return nil
	}
	off += n
	seed, n := format.Uvarint(blob[off:])
	if n <= 0 {
		return nil
	}
	off += n
	return &ribbonFilter{
		z:    append([]byte(nil), blob[off:]...),
		m:    int(m),
		r:    int(r),
		seed: seed,
	}
}

// ribbonHash is a seeded 64-bit hash: FNV-1a over the key folded into the seed, then a
// SplitMix64 finalizer to scatter the avalanche the band start, coefficients, and
// fingerprint each draw from. The constants must stay frozen so a filter written by one
// build reads identically in the next.
func ribbonHash(key []byte, seed uint64) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := seed ^ offset
	for _, c := range key {
		h ^= uint64(c)
		h *= prime
	}
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}
