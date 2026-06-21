package bench

import (
	"encoding/json"
	"testing"

	"github.com/tamnd/kv"
)

// TestDurabilitySweepWalksTheLadder runs a write-heavy workload at every durability level and
// checks the sweep covers the ladder in order, each rung discloses its own level and did real
// work, and the report round-trips through JSON. It also checks the one directional fact that
// holds regardless of hardware: turning fsync off is not slower than the most conservative
// mode, since SyncOff skips the very syscalls SyncExtra adds.
func TestDurabilitySweepWalksTheLadder(t *testing.T) {
	cfg := smokeConfig(kv.BTree, t.TempDir())
	cfg.KeyCount = 2000
	cfg.Ops = 2000
	w := Workload{Name: "write-saturated", Dist: Uniform, ReadFraction: 0}

	rep, err := RunDurabilitySweep(cfg, w)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}

	wantOrder := []string{"off", "normal", "full", "extra"}
	if len(rep.Results) != len(wantOrder) {
		t.Fatalf("sweep has %d rungs, want %d", len(rep.Results), len(wantOrder))
	}
	byMode := map[string]Result{}
	for i, r := range rep.Results {
		if r.Setup.Synchronous != wantOrder[i] {
			t.Fatalf("rung %d disclosed %q, want %q", i, r.Setup.Synchronous, wantOrder[i])
		}
		if r.Ops <= 0 {
			t.Fatalf("rung %q measured no ops", r.Setup.Synchronous)
		}
		byMode[r.Setup.Synchronous] = r
	}

	// fsync off cannot be slower than the most conservative mode: it skips the syscalls extra
	// performs. This is the durability/throughput tradeoff in its safest, hardware-independent
	// form.
	if byMode["off"].Throughput < byMode["extra"].Throughput {
		t.Fatalf("SyncOff throughput %.0f < SyncExtra %.0f, which inverts the durability tradeoff",
			byMode["off"].Throughput, byMode["extra"].Throughput)
	}

	data, err := rep.JSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back Report
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(back.Results) != len(rep.Results) {
		t.Fatalf("JSON round trip changed the rung count")
	}
}
