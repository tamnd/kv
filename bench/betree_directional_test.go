package bench

import (
	"os"
	"sort"
	"testing"

	"github.com/tamnd/kv"
)

// TestBetreeDirectional is an in-env directional read of the Bε-tree core (kv.Beta)
// against the two shipped cores (btree, lsm) it is being built to replace. It is NOT the
// doc-09 published measurement and it is NOT a comparison against external competitors
// (bbolt, pebble, badger, pogreb): those live in the out-of-env kvbench sweep on the
// reference NVMe machine and this repo carries no external dependency to run them. This
// test answers only the pre-flip question "is the new core on track to beat what it
// replaces," on the developer machine, on a partially-integrated core (no arena, no
// zero-copy frame hand-back yet), so the figure to read is the shape and the direction,
// not an absolute.
//
// It runs off by default so the normal suite stays untouched; set BENCH_BETREE=1 to run it.
func TestBetreeDirectional(t *testing.T) {
	if os.Getenv("BENCH_BETREE") == "" {
		t.Skip("directional bench is opt-in: set BENCH_BETREE=1 to run (it runs the full suite on three engines and takes minutes)")
	}

	tmpl := DefaultConfig(kv.BTree, t.TempDir())
	tmpl.KeyCount = 20000
	tmpl.Ops = 20000
	tmpl.Concurrency = 1
	tmpl.Seed = 1

	rep, err := RunSuite(tmpl, []kv.EngineKind{kv.BTree, kv.LSM, kv.Beta}, Standard())
	if err != nil {
		t.Fatalf("run suite: %v", err)
	}

	// Index throughput by engine and workload, and collect the workload order.
	type cell struct{ tput float64 }
	byEng := map[string]map[string]cell{}
	workloadSet := map[string]bool{}
	var dropped int64
	for _, r := range rep.Results {
		if byEng[r.Engine] == nil {
			byEng[r.Engine] = map[string]cell{}
		}
		byEng[r.Engine][r.Workload] = cell{tput: r.Throughput}
		workloadSet[r.Workload] = true
		dropped += r.Dropped
	}
	workloads := make([]string, 0, len(workloadSet))
	for w := range workloadSet {
		workloads = append(workloads, w)
	}
	sort.Strings(workloads)

	t.Logf("setup: %s/%s %d CPU, go %s; %d keys, %d ops, conc %d, seed %d",
		rep.Results[0].Setup.GOOS, rep.Results[0].Setup.GOARCH, rep.Results[0].Setup.NumCPU,
		rep.Results[0].Setup.GoVersion, tmpl.KeyCount, tmpl.Ops, tmpl.Concurrency, tmpl.Seed)
	t.Logf("%-16s %12s %12s %12s   %10s", "workload", "btree o/s", "lsm o/s", "betree o/s", "betree vs best")
	for _, w := range workloads {
		bt := byEng["btree"][w].tput
		ls := byEng["lsm"][w].tput
		be := byEng["betree"][w].tput
		bestShipped := bt
		if ls > bestShipped {
			bestShipped = ls
		}
		ratio := 0.0
		if bestShipped > 0 {
			ratio = be / bestShipped
		}
		t.Logf("%-16s %12.0f %12.0f %12.0f   %9.2fx", w, bt, ls, be, ratio)
	}
	if dropped != 0 {
		t.Logf("WARNING: %d operations dropped across the suite", dropped)
	}
	t.Log("note: vs best-shipped only; external-competitor numbers are the out-of-env kvbench sweep")
}
