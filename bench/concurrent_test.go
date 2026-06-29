package bench

import (
	"testing"
	"time"

	"github.com/tamnd/kv"
)

// TestConcurrentRunAccounting runs the parallel run phase on both engines and a mix of
// workloads and checks the books balance: every operation a worker started either committed
// (Ops) or lost its retry race and was dropped (Dropped), and the two sum to the configured
// op count exactly. It also confirms the disclosed concurrency is recorded and the latency
// percentiles, merged across workers, stay ordered.
func TestConcurrentRunAccounting(t *testing.T) {
	const workers = 4
	workloads := []Workload{
		{Name: "ycsb-a", Dist: Uniform, ReadFraction: 0.5},
		{Name: "ycsb-c", Dist: Zipfian, ReadFraction: 1},
		{Name: "write-saturated", Dist: Uniform, ReadFraction: 0},
	}
	for _, engine := range []kv.EngineKind{kv.BTree, kv.LSM} {
		for _, w := range workloads {
			t.Run(engineName(engine)+"/"+w.Name, func(t *testing.T) {
				cfg := smokeConfig(engine, t.TempDir())
				cfg.KeyCount = 2000
				cfg.Ops = 2000
				cfg.Concurrency = workers

				res, err := Run(cfg, w)
				if err != nil {
					t.Fatalf("run: %v", err)
				}
				if res.Setup.Concurrency != workers {
					t.Fatalf("disclosed concurrency = %d, want %d", res.Setup.Concurrency, workers)
				}
				if res.Ops+res.Dropped != int64(cfg.Ops) {
					t.Fatalf("ops %d + dropped %d != configured %d", res.Ops, res.Dropped, cfg.Ops)
				}
				if res.Ops <= 0 {
					t.Fatalf("no ops completed")
				}
				// A read-only workload can never conflict, so it must drop nothing.
				if w.ReadFraction >= 1 && !w.RMW && res.Dropped != 0 {
					t.Fatalf("read-only workload dropped %d ops", res.Dropped)
				}
				checkLatency(t, "reads", res.Reads)
				checkLatency(t, "writes", res.Writes)
			})
		}
	}
}

// TestSplitOpsSumsAndBalances checks the op splitter hands out every op and spreads the
// remainder so no worker is more than one op heavier than another.
func TestSplitOpsSumsAndBalances(t *testing.T) {
	cases := []struct {
		total, workers int
	}{
		{0, 1}, {1, 1}, {10, 1}, {10, 3}, {100, 7}, {2000, 4}, {5, 8},
	}
	for _, c := range cases {
		parts := splitOps(c.total, c.workers)
		if len(parts) != c.workers {
			t.Fatalf("splitOps(%d,%d) returned %d parts", c.total, c.workers, len(parts))
		}
		sum, min, max := 0, 1<<62, 0
		for _, p := range parts {
			sum += p
			if p < min {
				min = p
			}
			if p > max {
				max = p
			}
		}
		if sum != c.total {
			t.Fatalf("splitOps(%d,%d) sums to %d", c.total, c.workers, sum)
		}
		if max-min > 1 {
			t.Fatalf("splitOps(%d,%d) unbalanced: min %d max %d", c.total, c.workers, min, max)
		}
	}
}

// TestHistogramMerge checks merging two histograms yields the percentiles of the combined
// sample set, not an average of the two summaries.
func TestHistogramMerge(t *testing.T) {
	a := NewHistogram(0)
	b := NewHistogram(0)
	for i := 1; i <= 50; i++ {
		a.Record(time.Duration(i))
	}
	for i := 51; i <= 100; i++ {
		b.Record(time.Duration(i))
	}
	a.Merge(b)
	if a.Count() != 100 {
		t.Fatalf("merged count = %d, want 100", a.Count())
	}
	s := a.Summary()
	if s.Min != 1 || s.Max != 100 {
		t.Fatalf("merged min/max = %d/%d, want 1/100", s.Min, s.Max)
	}
	if s.P50 != 50 || s.P99 != 99 {
		t.Fatalf("merged p50/p99 = %d/%d, want 50/99", s.P50, s.P99)
	}
}
