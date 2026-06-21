package bench

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/tamnd/kv"
)

// TestRunSuiteCoversMatrix runs a small suite over both engines and a couple of workloads
// and checks the report covers the full matrix, is deterministically ordered, and survives
// a write/read round trip through the JSON the regression gate reads.
func TestRunSuiteCoversMatrix(t *testing.T) {
	tmpl := smokeConfig(kv.BTree, t.TempDir())
	tmpl.KeyCount = 500
	tmpl.Ops = 500
	engines := []kv.EngineKind{kv.BTree, kv.LSM}
	workloads := []Workload{
		{Name: "ycsb-c", Dist: Zipfian, ReadFraction: 1},
		{Name: "write-saturated", Dist: Uniform, ReadFraction: 0},
	}

	rep, err := RunSuite(tmpl, engines, workloads)
	if err != nil {
		t.Fatalf("run suite: %v", err)
	}
	if len(rep.Results) != len(engines)*len(workloads) {
		t.Fatalf("report has %d results, want %d", len(rep.Results), len(engines)*len(workloads))
	}

	// Every (engine, workload) pair appears exactly once.
	seen := map[string]bool{}
	for _, r := range rep.Results {
		seen[r.Engine+"/"+r.Workload] = true
		if r.Ops <= 0 {
			t.Fatalf("%s/%s measured no ops", r.Engine, r.Workload)
		}
	}
	for _, e := range engines {
		for _, w := range workloads {
			if !seen[engineName(e)+"/"+w.Name] {
				t.Fatalf("missing result for %s/%s", engineName(e), w.Name)
			}
		}
	}

	// Deterministic order: engine then workload, non-decreasing.
	for i := 1; i < len(rep.Results); i++ {
		prev, cur := rep.Results[i-1], rep.Results[i]
		if prev.Engine > cur.Engine || (prev.Engine == cur.Engine && prev.Workload > cur.Workload) {
			t.Fatalf("results not ordered at %d: %s/%s before %s/%s", i, prev.Engine, prev.Workload, cur.Engine, cur.Workload)
		}
	}

	// Write to disk and read back; the loaded report must equal the original.
	path := filepath.Join(t.TempDir(), "report.json")
	if err := rep.WriteJSON(path); err != nil {
		t.Fatalf("write report: %v", err)
	}
	back, err := ReadReport(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	a, _ := json.Marshal(rep)
	b, _ := json.Marshal(back)
	if string(a) != string(b) {
		t.Fatalf("report changed across write/read round trip")
	}
}

// result builds a synthetic result with a throughput and a read/write p99, enough to drive
// the comparison logic without running a database.
func result(engine, workload string, throughput float64, readP99, writeP99 int64) Result {
	r := Result{Engine: engine, Workload: workload, Throughput: throughput}
	if readP99 > 0 {
		r.Reads = Latency{Count: 1, P99: time.Duration(readP99)}
	}
	if writeP99 > 0 {
		r.Writes = Latency{Count: 1, P99: time.Duration(writeP99)}
	}
	return r
}

// TestCompareFlagsRegressions drives the regression gate with synthetic reports: a healthy
// current report trips nothing, a slower one trips throughput and latency, and a missing
// workload trips a coverage regression.
func TestCompareFlagsRegressions(t *testing.T) {
	th := DefaultThreshold() // 10% throughput drop, 20% p99 rise

	baseline := Report{Results: []Result{
		result("btree", "ycsb-c", 100000, 1000, 0),
		result("lsm", "write-saturated", 50000, 0, 2000),
	}}

	// Identical report: no regressions.
	if regs := Compare(baseline, baseline, th); len(regs) != 0 {
		t.Fatalf("identical report flagged %d regressions: %v", len(regs), regs)
	}

	// Within tolerance: 5% throughput drop and 10% p99 rise are under the gate.
	ok := Report{Results: []Result{
		result("btree", "ycsb-c", 95000, 1100, 0),
		result("lsm", "write-saturated", 50000, 0, 2000),
	}}
	if regs := Compare(baseline, ok, th); len(regs) != 0 {
		t.Fatalf("in-tolerance report flagged regressions: %v", regs)
	}

	// Past tolerance: a 30% throughput drop and a 50% read-p99 rise on btree/ycsb-c, and
	// lsm/write-saturated dropped from the report entirely.
	bad := Report{Results: []Result{
		result("btree", "ycsb-c", 70000, 1500, 0),
	}}
	regs := Compare(baseline, bad, th)
	got := map[string]Regression{}
	for _, r := range regs {
		got[r.Engine+"/"+r.Workload+"/"+r.Metric] = r
	}
	if _, ok := got["btree/ycsb-c/throughput"]; !ok {
		t.Fatalf("missing throughput regression, got %v", regs)
	}
	if _, ok := got["btree/ycsb-c/read-p99"]; !ok {
		t.Fatalf("missing read-p99 regression, got %v", regs)
	}
	if _, ok := got["lsm/write-saturated/coverage"]; !ok {
		t.Fatalf("missing coverage regression for dropped workload, got %v", regs)
	}
	// The throughput regression's reported change is the actual fractional drop.
	if tr := got["btree/ycsb-c/throughput"]; tr.Change > -0.25 {
		t.Fatalf("throughput change %.3f should be a fall past 25%%", tr.Change)
	}
}
