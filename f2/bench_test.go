package f2

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/hashlog"
)

// fillF2 returns a store preloaded with n keys, for read benchmarks.
func fillF2(b *testing.B, n int) *Store {
	b.Helper()
	s, err := New(DefaultTunables())
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
	return s
}

func fillHashlog(b *testing.B, n int) *hashlog.Store {
	b.Helper()
	s, err := hashlog.New(hashlog.Tunables{Shards: 256, PageSize: 1 << 20})
	if err != nil {
		b.Fatalf("hashlog.New: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			b.Fatalf("hashlog Set: %v", err)
		}
	}
	return s
}

const benchKeys = 1 << 20

func BenchmarkF2Set(b *testing.B) {
	s, _ := New(DefaultTunables())
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Set(tkey(i%benchKeys), tval(i))
	}
}

func BenchmarkHashlogSet(b *testing.B) {
	s, _ := hashlog.New(hashlog.Tunables{Shards: 256, PageSize: 1 << 20})
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Set(tkey(i%benchKeys), tval(i))
	}
}

func BenchmarkF2Get(b *testing.B) {
	s := fillF2(b, benchKeys)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = s.Get(tkey(i % benchKeys))
	}
}

func BenchmarkHashlogGet(b *testing.B) {
	s := fillHashlog(b, benchKeys)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = s.Get(tkey(i % benchKeys))
	}
}

// BenchmarkF2GetParallel exercises the lock-free read path under contention, the
// regime f2 is built for: every goroutine probes with atomic loads only, so the
// aggregate should scale with cores rather than collapse on a shared lock.
func BenchmarkF2GetParallel(b *testing.B) {
	s := fillF2(b, benchKeys)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _, _ = s.Get(tkey(i % benchKeys))
			i++
		}
	})
}

func BenchmarkHashlogGetParallel(b *testing.B) {
	s := fillHashlog(b, benchKeys)
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _, _ = s.Get(tkey(i % benchKeys))
			i++
		}
	})
}

// BenchmarkScaleExtrapolate is not a timing benchmark; it is the billions-of-keys
// memory proof printed as a table. It measures the real resident index cost per
// key at a few million keys, where the per-key cost has already converged to its
// asymptote (the table has grown many times), then multiplies that constant out
// to 1B and 10B keys for both f2 and an estimate of hashlog's full-key index.
// Building a literal billion-key store would need terabytes; the point of a flat
// per-key cost is that the small measurement extrapolates exactly.
func BenchmarkScaleExtrapolate(b *testing.B) {
	const measure = 4_000_000
	s := fillF2(b, measure)
	defer s.Close()
	st := s.Stats()
	f2PerKey := st.BytesPerKey()

	// hashlog's resident index holds, per live entry, a 64-bit hash, a value
	// location (addr int64 + vlen uint32), the key bytes, the slice header for the
	// key, and an atomic.Pointer slot, plus its own table load factor. For a
	// 16-byte key that lands near 74 bytes per key; we state the model rather than
	// run hashlog at this size so the table is reproducible.
	const hashlogPerKey = 74.0

	b.ReportMetric(f2PerKey, "f2-bytes/key")
	b.Logf("measured f2 index cost: %.2f bytes/key (key len %d) over %d keys",
		f2PerKey, len(tkey(0)), measure)
	b.Logf("%-12s %14s %14s %10s", "keys", "f2 index", "hashlog index", "ratio")
	for _, n := range []int64{1_000_000, 100_000_000, 1_000_000_000, 10_000_000_000} {
		f2GiB := float64(n) * f2PerKey / (1 << 30)
		hlGiB := float64(n) * hashlogPerKey / (1 << 30)
		b.Logf("%-12s %11.1f GiB %11.1f GiB %9.1fx",
			humanCount(n), f2GiB, hlGiB, hlGiB/f2GiB)
	}
}

func humanCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%dB", n/1_000_000_000)
	case n >= 1_000_000:
		return fmt.Sprintf("%dM", n/1_000_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}
