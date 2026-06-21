package bench

import (
	"encoding/json"
	"testing"

	"github.com/tamnd/kv"
)

// TestRecoveryReplaysAndVerifies runs the recovery measurement on both engines and checks it
// actually exercised replay (a positive backlog), produced a positive recovery time and rate,
// recovered the last committed value, and round-trips through its JSON form.
func TestRecoveryReplaysAndVerifies(t *testing.T) {
	for _, engine := range []kv.EngineKind{kv.BTree, kv.LSM} {
		t.Run(engineName(engine), func(t *testing.T) {
			cfg := smokeConfig(engine, t.TempDir())
			cfg.KeyCount = 2000

			res, err := RunRecovery(cfg)
			if err != nil {
				t.Fatalf("run recovery: %v", err)
			}
			if res.WALFramesReplayed == 0 {
				t.Fatalf("nothing was replayed: checkpointing was not actually disabled")
			}
			if res.Recover <= 0 {
				t.Fatalf("recover duration = %v, want positive", res.Recover)
			}
			if res.FramesPerSec <= 0 {
				t.Fatalf("frames/sec = %v, want positive", res.FramesPerSec)
			}
			if !res.Verified {
				t.Fatalf("recovered database did not return the last committed value")
			}
			if res.Setup.Concurrency != 1 {
				t.Fatalf("recovery disclosed concurrency %d, want 1", res.Setup.Concurrency)
			}

			data, err := res.JSON()
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var back RecoveryResult
			if err := json.Unmarshal(data, &back); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if back.WALFramesReplayed != res.WALFramesReplayed || back.Verified != res.Verified {
				t.Fatalf("JSON round trip changed the result")
			}
		})
	}
}
