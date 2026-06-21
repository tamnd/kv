package bench

import (
	"fmt"
	"time"
)

// Threshold is how much a metric may move before it counts as a regression. The fractions
// are the slack a run is allowed against its baseline: throughput may fall by at most
// MaxThroughputDrop, and a p99 latency may rise by at most MaxLatencyIncrease. Spec 21 §5
// gates a release on exactly this kind of bound, so a throughput drop or a p999 spike fails
// the gate rather than sliding by unnoticed.
type Threshold struct {
	// MaxThroughputDrop is the largest fractional fall in ops/sec tolerated (0.10 = 10%).
	MaxThroughputDrop float64
	// MaxLatencyIncrease is the largest fractional rise in p99 tolerated (0.20 = 20%).
	MaxLatencyIncrease float64
}

// DefaultThreshold is a starting gate: a 10% throughput drop or a 20% p99 rise is a
// regression. The absolute numbers are noisier on shared CI hardware than on the fixed
// reference machine, so kvbench tightens these; this is the in-repo default.
func DefaultThreshold() Threshold {
	return Threshold{MaxThroughputDrop: 0.10, MaxLatencyIncrease: 0.20}
}

// Regression is one metric that moved past its threshold between a baseline and a current
// report. It names the run, the metric, the two values, and the fractional change, so a
// gate failure message is specific rather than "something got slower".
type Regression struct {
	Engine   string
	Workload string
	Metric   string
	Baseline float64
	Current  float64
	// Change is the fractional move: negative for a throughput fall, positive for a latency
	// rise, in both cases the amount that broke the threshold.
	Change float64
}

// String renders a regression as a one-line gate message.
func (r Regression) String() string {
	return fmt.Sprintf("%s/%s %s regressed %.1f%%: baseline %.4g, current %.4g",
		r.Engine, r.Workload, r.Metric, r.Change*100, r.Baseline, r.Current)
}

// Compare checks a current report against a baseline and returns every regression past the
// threshold. It matches results by engine and workload; a baseline result missing from the
// current report is itself reported as a coverage regression, because silently dropping a
// workload from the gate is the kind of hidden gap spec 21 §3 warns against. Results new in
// the current report (no baseline) are skipped, since there is nothing to regress against.
func Compare(baseline, current Report, th Threshold) []Regression {
	cur := make(map[string]Result, len(current.Results))
	for _, r := range current.Results {
		cur[r.Engine+"/"+r.Workload] = r
	}

	var regs []Regression
	for _, b := range baseline.Results {
		key := b.Engine + "/" + b.Workload
		c, ok := cur[key]
		if !ok {
			regs = append(regs, Regression{
				Engine: b.Engine, Workload: b.Workload, Metric: "coverage",
				Baseline: 1, Current: 0, Change: -1,
			})
			continue
		}

		// Throughput regression: a fall past the tolerated drop.
		if b.Throughput > 0 {
			drop := (b.Throughput - c.Throughput) / b.Throughput
			if drop > th.MaxThroughputDrop {
				regs = append(regs, Regression{
					Engine: b.Engine, Workload: b.Workload, Metric: "throughput",
					Baseline: b.Throughput, Current: c.Throughput, Change: -drop,
				})
			}
		}

		// Latency regressions: a p99 rise past the tolerated increase, read and write side
		// each checked where the baseline recorded that op kind.
		regs = appendLatencyRegression(regs, b, c, th, "read-p99", b.Reads.P99, c.Reads.P99, b.Reads.Count)
		regs = appendLatencyRegression(regs, b, c, th, "write-p99", b.Writes.P99, c.Writes.P99, b.Writes.Count)
	}
	return regs
}

// appendLatencyRegression adds a latency regression when current p99 rises past the
// threshold over a baseline that actually measured that op kind.
func appendLatencyRegression(regs []Regression, b, c Result, th Threshold, metric string, base, curr time.Duration, baseCount int) []Regression {
	if baseCount == 0 || base <= 0 {
		return regs
	}
	rise := float64(curr-base) / float64(base)
	if rise > th.MaxLatencyIncrease {
		regs = append(regs, Regression{
			Engine: b.Engine, Workload: b.Workload, Metric: metric,
			Baseline: float64(base), Current: float64(curr), Change: rise,
		})
	}
	return regs
}
