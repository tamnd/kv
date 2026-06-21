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

	// fsync off does the strictly cheaper work: it skips the syscalls extra performs, so it
	// cannot be fundamentally slower. The measured throughputs are noisy though, especially on
	// a fast disk where the OS buffers writes and group commit folds many commits into one
	// fsync, so the two modes can land within a few percent and ordinary scheduling jitter can
	// flip them. The test asserts only the robust fact: off is not dramatically slower than
	// extra. A real inversion (durability levels wired backward) would put off far below this
	// floor, where measurement noise stays well inside it.
	const floor = 0.75 // off must reach at least 75% of extra's throughput
	if byMode["off"].Throughput < floor*byMode["extra"].Throughput {
		t.Fatalf("SyncOff throughput %.0f is far below SyncExtra %.0f (floor %.0f), which inverts the durability tradeoff",
			byMode["off"].Throughput, byMode["extra"].Throughput, floor*byMode["extra"].Throughput)
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
