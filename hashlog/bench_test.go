package hashlog

import (
	"path/filepath"
	"strconv"
	"testing"
)

// These benchmarks travel with the durable code so a regression in the spill path
// shows up locally during the build. Their numbers are not published: per spec 2070
// doc 08 the only durable performance number that ships is the M10 real-hardware
// gate. The resident benchmark here is a guard that the memory-only ceiling did not
// move, not a headline figure.

func benchKey(i int) []byte {
	return []byte("k:" + strconv.Itoa(i))
}

func benchValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

// BenchmarkDurableSpillGet reads from a store whose resident budget is a fraction of
// its working set, so most reads fault a page in from the one file. This is the
// larger-than-memory read path M1 introduces.
func BenchmarkDurableSpillGet(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.hlog")
	t := Tunables{
		Shards:                8,
		PageSize:              4096,
		ExtentSize:            4096,
		ResidentPagesPerShard: 4,
		Path:                  path,
	}
	s, err := New(t)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	const keys = 50000
	v := benchValue(64)
	for i := 0; i < keys; i++ {
		if err := s.Set(benchKey(i), v); err != nil {
			b.Fatal(err)
		}
	}
	if s.Spilled() == 0 {
		b.Fatal("benchmark did not spill; not exercising the durable read path")
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := benchKey(i % keys)
			if _, _, err := s.Get(k); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkDurableDialSet measures the SET cost at each durability dial so the price
// of the barrier is visible: None pays nothing, Normal pays a barrier per seal, Full
// pays a true device flush before every SET returns. The gap between them is the dial
// doing its job, not a regression. Numbers stay local until the M10 hardware gate.
func BenchmarkDurableDialSet(b *testing.B) {
	dials := []struct {
		name string
		d    Durability
	}{
		{"None", DurabilityNone},
		{"Normal", DurabilityNormal},
		{"Full", DurabilityFull},
	}
	v := benchValue(64)
	for _, dl := range dials {
		b.Run(dl.name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "dial.hlog")
			t := Tunables{
				Shards:                8,
				PageSize:              4096,
				ExtentSize:            4096,
				ResidentPagesPerShard: 8,
				Path:                  path,
				Durability:            dl.d,
			}
			s, err := New(t)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Set(benchKey(i), v); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCheckpoint measures one checkpoint over a populated store: capture every
// shard's cut, encode the snapshot, write and barrier its extents, and flip the
// superblock. It is the cost of the periodic durability artifact, amortized over the
// CheckpointBytes of appends between checkpoints, so it is paid rarely. Reported per op
// where one op is one full checkpoint. Numbers stay local until the M10 hardware gate.
func BenchmarkCheckpoint(b *testing.B) {
	path := filepath.Join(b.TempDir(), "ckpt.hlog")
	t := Tunables{
		Shards:                8,
		PageSize:              4096,
		ExtentSize:            4096,
		ResidentPagesPerShard: 8,
		Path:                  path,
		Durability:            DurabilityNormal,
	}
	s, err := New(t)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	const keys = 50000
	v := benchValue(64)
	for i := 0; i < keys; i++ {
		if err := s.Set(benchKey(i), v); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Checkpoint(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkResidentCeilingGet is the memory-only path the durable work must never
// slow. It mirrors the resident config the head-to-head bench uses, so a drift here
// flags that the durable branch leaked cost into the full-resident GET.
func BenchmarkResidentCeilingGet(b *testing.B) {
	t := Tunables{Shards: 8, PageSize: 1 << 20, ResidentPagesPerShard: 0}
	s, err := New(t)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	const keys = 50000
	v := benchValue(64)
	for i := 0; i < keys; i++ {
		if err := s.Set(benchKey(i), v); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, _, err := s.Get(benchKey(i % keys)); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
