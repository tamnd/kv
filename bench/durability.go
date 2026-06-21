package bench

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/tamnd/kv"
)

// durabilityModes is the durability ladder a sweep walks, from no fsync to the most
// conservative, in increasing-cost order. The order is meaningful and preserved in the report
// so the throughput-versus-durability tradeoff reads top to bottom.
var durabilityModes = []kv.Sync{kv.SyncOff, kv.SyncNormal, kv.SyncFull, kv.SyncExtra}

// RunDurabilitySweep runs the same workload at every durability level and gathers the results
// into one report, so the cost of durability is a measured curve rather than an assertion.
// Spec 21 §6 sets a durable-commit-throughput target (group commit sustaining thousands of
// commits per second at FULL, scaling with concurrency until fsync saturates); a sweep is how
// that target is read against the cheaper modes, the same workload priced at each rung of the
// ladder. The template's Sync is overridden per run; everything else (engine, sizing, seed,
// concurrency) is held fixed so durability is the only variable.
//
// The results are left in ladder order, not sorted, because the order is the point: each row
// is the same work at a stricter durability level, and a reader compares throughput down the
// column. Each result discloses its own Synchronous level in its Setup, so the report is
// self-describing.
func RunDurabilitySweep(tmpl Config, w Workload) (Report, error) {
	var rep Report
	for _, mode := range durabilityModes {
		cfg := tmpl
		cfg.Sync = mode
		dir := filepath.Join(tmpl.Dir, "sync-"+syncName(mode))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return Report{}, fmt.Errorf("sweep dir %s: %w", dir, err)
		}
		cfg.Dir = dir
		res, err := Run(cfg, w)
		if err != nil {
			return Report{}, fmt.Errorf("sweep %s: %w", syncName(mode), err)
		}
		rep.Results = append(rep.Results, res)
	}
	return rep, nil
}
