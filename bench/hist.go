package bench

import (
	"math"
	"sort"
	"time"
)

// Histogram records operation latencies and reports their distribution. The tail is the
// point: a benchmark that reports only the mean hides the compaction-induced p999 stall
// that is exactly what a user feels (spec 21 §1, §3). So this keeps a per-sample record and
// reports p50/p90/p99/p999/max, not just an average.
//
// The implementation keeps every sample. A microbenchmark run is bounded (millions of ops
// at most), and exact percentiles from the raw samples are simpler to trust than a bucketed
// approximation. A future slice can swap in a log-bucketed histogram if a run ever needs to
// record more samples than fit in memory; the reported percentiles are the contract, the
// storage behind them is not.
type Histogram struct {
	samples []time.Duration
	sum     time.Duration
	min     time.Duration
	max     time.Duration
}

// NewHistogram returns an empty histogram sized to hint expected samples to avoid growth
// churn on the hot path. hint may be zero.
func NewHistogram(hint int) *Histogram {
	if hint < 0 {
		hint = 0
	}
	return &Histogram{samples: make([]time.Duration, 0, hint), min: math.MaxInt64}
}

// Record adds one latency sample. It is O(1) amortized and allocation-free once the backing
// slice has grown, so it does not perturb the very latency it measures.
func (h *Histogram) Record(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.samples = append(h.samples, d)
	h.sum += d
	if d < h.min {
		h.min = d
	}
	if d > h.max {
		h.max = d
	}
}

// Count is how many samples were recorded.
func (h *Histogram) Count() int { return len(h.samples) }

// Merge folds another histogram's samples into this one, so a concurrent run's per-worker
// histograms combine into one distribution before it is summarized. The percentiles of the
// merged set are the true percentiles across all workers, not an average of per-worker
// percentiles, which would understate the global tail.
func (h *Histogram) Merge(other *Histogram) {
	if other == nil || len(other.samples) == 0 {
		return
	}
	h.samples = append(h.samples, other.samples...)
	h.sum += other.sum
	if other.min < h.min {
		h.min = other.min
	}
	if other.max > h.max {
		h.max = other.max
	}
}

// Latency summarizes a histogram. All durations are nanoseconds when marshaled, named in
// the field tags, so the JSON is unambiguous about units (spec 21 §5).
type Latency struct {
	Count int           `json:"count"`
	Mean  time.Duration `json:"mean_ns"`
	P50   time.Duration `json:"p50_ns"`
	P90   time.Duration `json:"p90_ns"`
	P99   time.Duration `json:"p99_ns"`
	P999  time.Duration `json:"p999_ns"`
	Min   time.Duration `json:"min_ns"`
	Max   time.Duration `json:"max_ns"`
}

// Summary computes the latency distribution. It sorts a copy of the samples so the caller's
// histogram stays usable, and returns a zero Latency for an empty histogram.
func (h *Histogram) Summary() Latency {
	n := len(h.samples)
	if n == 0 {
		return Latency{}
	}
	sorted := make([]time.Duration, n)
	copy(sorted, h.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return Latency{
		Count: n,
		Mean:  h.sum / time.Duration(n),
		P50:   percentile(sorted, 0.50),
		P90:   percentile(sorted, 0.90),
		P99:   percentile(sorted, 0.99),
		P999:  percentile(sorted, 0.999),
		Min:   sorted[0],
		Max:   sorted[n-1],
	}
}

// percentile returns the p-quantile of an ascending slice using the nearest-rank method,
// which is exact for the samples held and never interpolates a value that was not observed.
func percentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	// Nearest-rank: rank = ceil(p*n), clamped to [1, n], then index rank-1.
	rank := int(math.Ceil(p * float64(n)))
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}
