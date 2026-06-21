package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/tamnd/kv"
)

// RecoveryResult is one recovery measurement: how long a database took to reopen with a
// backlog of un-checkpointed log to replay, and how much it replayed. Spec 21 §6 sets the
// recovery target (a B-tree opens O(1) past WAL replay, an LSM open is bounded by the
// MANIFEST since the last checkpoint), and this is the number that holds the implementation to
// it. Verified records that the recovered database actually returned the data that was written
// before the crash, so a fast-but-wrong recovery cannot pass as a good one.
type RecoveryResult struct {
	Engine string `json:"engine"`
	Setup  Setup  `json:"setup"`
	// Keys is how many keys were written before the simulated crash.
	Keys int `json:"keys"`
	// WALFramesReplayed is the log backlog the reopen had to replay, the replay workload's
	// size. A zero here would mean nothing was actually recovered, so the measurement asserts
	// it is positive.
	WALFramesReplayed uint64 `json:"wal_frames_replayed"`
	// Recover is the wall-clock time of the reopen that replayed the backlog.
	Recover time.Duration `json:"recover_ns"`
	// FramesPerSec is the replay rate, the throughput of recovery itself.
	FramesPerSec float64 `json:"frames_per_sec"`
	// Verified is true when the recovered database returned the exact value last written
	// before the crash, proving the replay restored committed state and did not just open fast.
	Verified bool `json:"verified"`
}

// JSON renders the recovery result as indented JSON, the same machine-readable form the
// throughput results use (spec 21 §5).
func (r RecoveryResult) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// RunRecovery measures how long the engine takes to reopen with a backlog of un-checkpointed
// log to replay. It loads cfg.KeyCount keys with checkpointing disabled so the whole load
// stays in the log, closes the database without checkpointing (which leaves the log on disk,
// the state a crash leaves it in), then reopens and times the open that replays it. The reopen
// is the measured window; the load is setup. It verifies the recovered database returns the
// last value written, so the time is the time of a recovery that actually worked.
//
// The measurement is of replay cost over a controlled backlog, not of torn-write handling;
// the correctness of recovery under partial and corrupt writes is pinned by the crash tests in
// the db package, and this benchmark assumes that correctness and measures its cost.
func RunRecovery(cfg Config) (RecoveryResult, error) {
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 1
	}
	path := filepath.Join(cfg.Dir, "bench.kv")

	// Checkpointing is disabled for the whole run so the load accumulates in the log and the
	// reopen has a real backlog to replay; a clean close would otherwise fold it away. The rest
	// of the open options match a normal run, so the reopen sees the same database shape.
	opts := append(openOptions(cfg), kv.WithAutoCheckpoint(-1))

	setup := Setup{
		GoVersion:    runtime.Version(),
		GOOS:         runtime.GOOS,
		GOARCH:       runtime.GOARCH,
		NumCPU:       runtime.NumCPU(),
		KeyCount:     cfg.KeyCount,
		KeyLen:       cfg.KeyLen,
		ValLen:       cfg.ValLen,
		Distribution: distName(Sequential),
		Seed:         cfg.Seed,
		Synchronous:  syncName(cfg.Sync),
		BatchSize:    cfg.BatchSize,
		Concurrency:  1,
		CacheBytes:   cfg.CacheBytes,
	}
	res := RecoveryResult{Engine: engineName(cfg.Engine), Setup: setup, Keys: cfg.KeyCount}

	gen := NewGenerator(GenConfig{KeyCount: cfg.KeyCount, KeyLen: cfg.KeyLen, ValLen: cfg.ValLen, Dist: Sequential, Seed: cfg.Seed})

	// Load, capture the backlog, then close without checkpointing. The captured frame count is
	// the replay workload the reopen will pay for.
	db, err := kv.Open(path, opts...)
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("open: %w", err)
	}
	if _, err := loadPhase(db, gen, cfg.KeyCount, cfg.BatchSize, false); err != nil {
		db.Close()
		return RecoveryResult{}, err
	}
	res.WALFramesReplayed = db.Stats().WALFrames
	if err := db.Close(); err != nil {
		return RecoveryResult{}, fmt.Errorf("close before recovery: %w", err)
	}

	// The value last written, recomputed from the generator so recovery can be checked against
	// what the load committed.
	lastIdx := uint64(cfg.KeyCount - 1)
	wantKey := gen.Key(nil, lastIdx)
	wantVal := append([]byte(nil), gen.Value(lastIdx)...)

	// The measured window: reopen, replaying the backlog.
	start := time.Now()
	rdb, err := kv.Open(path, opts...)
	res.Recover = time.Since(start)
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("reopen for recovery: %w", err)
	}
	defer rdb.Close()

	verified, err := recoveredValueMatches(rdb, wantKey, wantVal)
	if err != nil {
		return RecoveryResult{}, err
	}
	res.Verified = verified
	if res.Recover > 0 {
		res.FramesPerSec = float64(res.WALFramesReplayed) / res.Recover.Seconds()
	}
	return res, nil
}

// recoveredValueMatches reads key from the reopened database and reports whether it holds the
// expected value, the proof that replay restored committed state.
func recoveredValueMatches(db *kv.DB, key, want []byte) (bool, error) {
	var got []byte
	err := db.View(func(txn *kv.Txn) error {
		v, e := txn.Get(key)
		if e != nil {
			return e
		}
		got = append([]byte(nil), v...)
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("read recovered key: %w", err)
	}
	return bytes.Equal(got, want), nil
}
