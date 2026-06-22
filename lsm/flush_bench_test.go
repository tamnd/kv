package lsm

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// BenchmarkSustainedWriteFlush drives a steady stream of writes through a memtable cap set
// low enough that the stream crosses a flush boundary many times, the regime W3 targets. It
// reports the average write cost as ns/op and the per-write latency distribution as p50, p99,
// and p999 in microseconds. The synchronous flush this slice replaced put a whole segment
// build on the one write that crossed the cap, so every few hundred writes spiked while the
// rest were cheap, the sawtooth the exit criterion names. With the background flusher the
// crossing write only swaps in a fresh memtable and returns, so the tail tracks the body
// until the flusher genuinely cannot keep up, at which point the bounded queue turns the
// overrun into backpressure shared across a memtable's worth of writes rather than a spike on
// one. The gap between p50 and p999 is the residual sawtooth, the number to watch.
func BenchmarkSustainedWriteFlush(b *testing.B) {
	l := newLSMBench(b)
	// A small cap so a few hundred writes seal a memtable; the run then crosses the flush
	// boundary repeatedly and the background flusher must keep up under steady backpressure.
	l.memtableCap = 256 * 1024
	val := make([]byte, 512)
	for i := range val {
		val[i] = byte(i)
	}

	samples := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := engine.NewWriteBatch(uint64(i + 1))
		batch.Set([]byte(fmt.Sprintf("key%012d", i)), val)
		start := time.Now()
		if err := l.Apply(batch, uint64(i+1)); err != nil {
			b.Fatalf("apply: %v", err)
		}
		samples = append(samples, time.Since(start))
	}
	b.StopTimer()

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	pct := func(p float64) float64 {
		if len(samples) == 0 {
			return 0
		}
		idx := int(p * float64(len(samples)-1))
		return float64(samples[idx].Nanoseconds()) / 1000
	}
	b.ReportMetric(pct(0.50), "p50-us/write")
	b.ReportMetric(pct(0.99), "p99-us/write")
	b.ReportMetric(pct(0.999), "p999-us/write")
}

// newLSMBench is newLSM for a benchmark: a benchmark gets *testing.B, not *testing.T, so it
// needs its own helper, but the body is the same as newLSM down to the Close cleanup.
func newLSMBench(b *testing.B) *LSM {
	b.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "bench.kv", pager.Options{
		PageSize:    4096,
		CacheFrames: 256,
		Engine:      format.EngineLSM,
	})
	if err != nil {
		b.Fatalf("create pager: %v", err)
	}
	l := New(p)
	if err := l.Open(&engine.Env{}); err != nil {
		b.Fatalf("open lsm: %v", err)
	}
	b.Cleanup(func() { l.Close() })
	return l
}
