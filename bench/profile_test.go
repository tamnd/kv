package bench

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv"
)

// TestProfilingWritesProfiles runs a small workload with every profile enabled and checks the
// harness wrote each one, labelled for the run, non-empty, and in the gzip-wrapped pprof
// format the go tooling reads.
func TestProfilingWritesProfiles(t *testing.T) {
	dir := t.TempDir()
	profDir := filepath.Join(dir, "prof")
	cfg := smokeConfig(kv.BTree, dir)
	cfg.KeyCount = 500
	cfg.Ops = 500
	cfg.Profile = ProfileSet{Dir: profDir, CPU: true, Heap: true, Block: true, Mutex: true}
	w := Workload{Name: "ycsb-a", Dist: Uniform, ReadFraction: 0.5}

	if _, err := Run(cfg, w); err != nil {
		t.Fatalf("run: %v", err)
	}

	for _, kind := range []string{"cpu", "allocs", "block", "mutex"} {
		path := filepath.Join(profDir, "btree-ycsb-a."+kind+".pprof")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s profile: %v", kind, err)
		}
		if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
			t.Fatalf("%s profile is not gzip-wrapped pprof (%d bytes)", kind, len(data))
		}
	}
}

// TestProfilingDisabledWritesNothing confirms the zero-value ProfileSet is a true no-op: a
// run with profiling off creates no profile directory, so the gate and microbenchmarks pay
// nothing and leave no artifacts behind.
func TestProfilingDisabledWritesNothing(t *testing.T) {
	dir := t.TempDir()
	profDir := filepath.Join(dir, "prof")
	cfg := smokeConfig(kv.BTree, dir)
	cfg.KeyCount = 300
	cfg.Ops = 300
	cfg.Profile = ProfileSet{} // disabled: empty Dir

	if _, err := Run(cfg, Workload{Name: "ycsb-c", Dist: Uniform, ReadFraction: 1}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, err := os.Stat(profDir); !os.IsNotExist(err) {
		t.Fatalf("profile dir exists with profiling disabled: %v", err)
	}
}
