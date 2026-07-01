package kv

import (
	"math"
	"sync/atomic"
	"testing"
)

// This file mirrors the kvbench ycsb-b cell: a zipfian read-mostly mix, 95 percent Get and 5
// percent Set over a hot-skewed keyspace, 1 KiB value. The published board shows ycsb-b is the
// tightest cell on slow amd64 cores, squeezed from both sides: the 5 percent updates land on the
// same hot keys and churn the hot tier, while the zipfian skew lets a B-tree or LSM competitor
// serve those hot keys from cache faster than uniform readrandom. Profiling this shape is how we
// see where the write-mixed read path spends its time, so the lever that lifts it is measured and
// not guessed. The distribution matches the harness: theta 0.99, scrambled so the hot set is not
// clustered at the low end.

const mixKeys = 100000
const mixValBytes = 1024

// zipfParams holds the immutable zipfian constants. They are computed once (the zeta sum is the
// expensive part) and shared read-only across the parallel goroutines, each of which carries its
// own cheap rng so there is no shared mutable generator state on the hot path.
type zipfParams struct {
	n            uint64
	theta, alpha float64
	zetan, eta   float64
}

func newZipfParams(n uint64, theta float64) zipfParams {
	zeta2 := zeta(2, theta)
	zetan := zeta(n, theta)
	return zipfParams{
		n:     n,
		theta: theta,
		alpha: 1.0 / (1.0 - theta),
		zetan: zetan,
		eta:   (1 - math.Pow(2.0/float64(n), 1-theta)) / (1 - zeta2/zetan),
	}
}

func zeta(n uint64, theta float64) float64 {
	sum := 0.0
	for i := uint64(1); i <= n; i++ {
		sum += 1.0 / math.Pow(float64(i), theta)
	}
	return sum
}

// next draws a zipfian key id given a uniform sample u in [0,1), matching the harness generator
// including the multiplicative scramble that scatters the hot keys across the keyspace.
func (z zipfParams) next(u float64) uint64 {
	uz := u * z.zetan
	if uz < 1.0 {
		return 0
	}
	if uz < 1.0+math.Pow(0.5, z.theta) {
		return 1
	}
	idx := uint64(float64(z.n) * math.Pow(z.eta*u-z.eta+1.0, z.alpha))
	if idx >= z.n {
		idx = z.n - 1
	}
	return (idx*2654435761 + 0x5bd1e995) % z.n
}

// splitmix is the same deterministic PRNG the harness uses, per-goroutine so the parallel read
// path has no shared rng contention that would mask the engine's own contention in a profile.
type splitmix struct{ s uint64 }

func (r *splitmix) next() uint64 {
	r.s += 0x9E3779B97F4A7C15
	z := r.s
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *splitmix) float64() float64 {
	return float64(r.next()>>11) / float64(uint64(1)<<53)
}

// BenchmarkMixedYCSBB is the ycsb-b shape: fill the keyspace, then run a parallel 95/5 read/write
// mix drawn zipfian. Set takes a fresh value so the write path moves real payload. This is the
// cell the board flags, so its profile is the one that points at the lever.
func BenchmarkMixedYCSBB(b *testing.B) {
	d := openUniform(b)
	defer d.Close()
	zp := newZipfParams(mixKeys, 0.99)
	key := make([]byte, 16)
	val := uniValue()
	for i := range uint64(mixKeys) {
		fixedKey(key, i)
		d.Set(key, val)
	}
	if err := d.Sync(); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(mixValBytes)
	b.ReportAllocs()
	b.ResetTimer()
	var seed atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		rng := splitmix{s: seed.Add(0x1234567)}
		k := make([]byte, 16)
		wval := uniValue()
		scratch := make([]byte, 0, mixValBytes)
		for pb.Next() {
			id := zp.next(rng.float64())
			fixedKey(k, id)
			if rng.next()%100 < 5 { // 5 percent updates, the ycsb-b write fraction
				d.Set(k, wval)
			} else {
				d.Get(k, scratch)
			}
		}
	})
}
