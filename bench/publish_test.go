package bench

import (
	"strings"
	"testing"
	"time"
)

// synthReport builds a small report with hand-chosen numbers so the renderer and the tradeoff
// evaluator can be tested without running the heavy suite. The numbers are arranged so every
// fact the evaluator checks points the way the architecture predicts: the B-tree serves
// cache-resident reads in the microsecond class with no extra I/O, the LSM writes less per op
// and ingests at least as fast once un-fsync-pinned, and nothing is dropped.
func synthReport() Report {
	setup := Setup{GOOS: "darwin", GOARCH: "arm64", NumCPU: 8, GoVersion: "go1.26.4",
		KeyCount: 1000, KeyLen: 24, ValLen: 64, Distribution: "zipfian", Seed: 1,
		Synchronous: "full", BatchSize: 100, Concurrency: 1}
	mk := func(engine, workload string, tput float64, readP99, writeP99, gcPause time.Duration, dropped int64, sp, wf, ra float64) Result {
		return Result{
			Workload: workload, Engine: engine, Setup: setup,
			Ops: 1000, Duration: time.Second, Dropped: dropped, Throughput: tput,
			Reads:         Latency{Count: 1000, P99: readP99},
			Writes:        Latency{Count: 1000, P99: writeP99},
			Amplification: Amplification{Space: sp, Write: wf, Read: ra},
			GC:            GCStats{MaxPause: gcPause},
		}
	}
	return Report{
		Label: "synthetic",
		Results: []Result{
			// btree serves cache-resident ycsb-c reads in microseconds with zero read-amp.
			mk("btree", "ycsb-c", 250000, 3*time.Microsecond, 0, 200*time.Microsecond, 0, 1.0, 3.4, 0.0),
			mk("lsm", "ycsb-c", 250000, 2*time.Microsecond, 0, 300*time.Microsecond, 0, 1.0, 1.1, 0.0),
			// On write-saturated the LSM writes less per op (lower write-factor) than the B-tree.
			mk("btree", "write-saturated", 285, 0, 6*time.Millisecond, 200*time.Microsecond, 0, 1.0, 4.5, readNotMeasured),
			mk("lsm", "write-saturated", 296, 0, 6*time.Millisecond, 300*time.Microsecond, 0, 1.0, 3.0, readNotMeasured),
			// On bulk-load, un-fsync-pinned, the LSM ingests at least as fast as the B-tree.
			mk("btree", "bulk-load", 18000, 0, 75*time.Microsecond, 200*time.Microsecond, 0, 1.0, 3.4, readNotMeasured),
			mk("lsm", "bulk-load", 25000, 0, 50*time.Microsecond, 300*time.Microsecond, 0, 1.0, 1.1, readNotMeasured),
		},
	}
}

// TestTradeoffReadsTheNumbers checks the evaluator turns the measured cells into the right
// verdicts: the B-tree read tail is microsecond-class with no extra I/O, the LSM write-factor
// sits below the B-tree's, the LSM bulk ingest is at or above the B-tree's, and nothing dropped.
func TestTradeoffReadsTheNumbers(t *testing.T) {
	fs := Tradeoff(synthReport())
	by := map[string]Finding{}
	for _, f := range fs {
		by[f.Target] = f
	}
	want := []string{
		"Read latency reference (B-tree, YCSB-C cache-resident)",
		"Write amplification (LSM below B-tree, write-saturated)",
		"Bulk ingest (LSM at or above B-tree, un-fsync-pinned)",
		"No silent drops (spec 21 §3)",
	}
	for _, w := range want {
		f, ok := by[w]
		if !ok {
			t.Fatalf("missing finding %q", w)
		}
		if !f.Holds {
			t.Errorf("finding %q should hold on the synthetic report, observed %q", w, f.Observed)
		}
	}
}

// TestTradeoffFlagsInversion pushes the B-tree read tail into the millisecond range and inverts
// the write-factor so the LSM writes more per op, and confirms the evaluator reports the claims
// as not holding rather than papering over a bad result.
func TestTradeoffFlagsInversion(t *testing.T) {
	rep := synthReport()
	// A millisecond read tail is no longer microsecond-class; an LSM write-factor above the
	// B-tree's inverts the write-amplification corner.
	rep.find("btree", "ycsb-c").Reads.P99 = 5 * time.Millisecond
	rep.find("lsm", "write-saturated").Amplification.Write = 9.0

	by := map[string]Finding{}
	for _, f := range Tradeoff(rep) {
		by[f.Target] = f
	}
	if by["Read latency reference (B-tree, YCSB-C cache-resident)"].Holds {
		t.Error("read-latency claim should not hold once the B-tree tail is in the millisecond range")
	}
	if by["Write amplification (LSM below B-tree, write-saturated)"].Holds {
		t.Error("write-amplification claim should not hold once the LSM writes more per op than the B-tree")
	}
}

// TestTradeoffFlagsCacheMiss confirms a read phase that streams page I/O is not credited as the
// cache-resident reference even if its tail is microsecond-class: the reference claim is about a
// warm read, and a non-zero read-amp means the buffer pool was not holding the working set.
func TestTradeoffFlagsCacheMiss(t *testing.T) {
	rep := synthReport()
	rep.find("btree", "ycsb-c").Amplification.Read = 1.5

	for _, f := range Tradeoff(rep) {
		if f.Target == "Read latency reference (B-tree, YCSB-C cache-resident)" && f.Holds {
			t.Fatalf("read-reference claim should not hold at 1.5 read-ios/op, observed %q", f.Observed)
		}
	}
}

// TestTradeoffFlagsDrops confirms a dropped op anywhere in the suite fails the honesty target.
func TestTradeoffFlagsDrops(t *testing.T) {
	rep := synthReport()
	rep.find("lsm", "write-saturated").Dropped = 7
	for _, f := range Tradeoff(rep) {
		if f.Target == "No silent drops (spec 21 §3)" && f.Holds {
			t.Fatalf("honesty target should fail with 7 dropped ops, observed %q", f.Observed)
		}
	}
}

// TestRenderReportShape checks the Markdown carries the setup, a row for every cell, and the
// tradeoff section, so the published document is self-describing.
func TestRenderReportShape(t *testing.T) {
	md := RenderReport(synthReport())
	for _, want := range []string{
		"# kv benchmark results",
		"## Setup",
		"darwin/arm64",
		"## Per-workload numbers",
		"| ycsb-c | btree |",
		"| write-saturated | lsm |",
		"| bulk-load | lsm |",
		"## Tradeoff and targets",
		"Read latency reference (B-tree, YCSB-C cache-resident)",
		"## Garbage collection (process-global context)",
		"go run ./cmd/bench",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("rendered report missing %q", want)
		}
	}
	// A write-only cell must print the read-amp sentinel as a dash, not -1.00.
	if strings.Contains(md, "-1.00") {
		t.Error("report printed the not-measured sentinel as -1.00 instead of a dash")
	}
}
