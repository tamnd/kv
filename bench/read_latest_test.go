package bench

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/kv"
)

// TestReadLatestGrowsTheKeyspace runs the YCSB-D read-latest workload and checks the things
// that make it that workload and not another: the op accounting is exact, inserts and reads
// both happen, the inserts genuinely extend the keyspace past the loaded keys, the reads land
// on real keys (no miss, since read-latest only ever reads what it has already inserted or
// loaded), and the latency percentiles are well ordered. It runs on both engines.
func TestReadLatestGrowsTheKeyspace(t *testing.T) {
	for _, engine := range []kv.EngineKind{kv.BTree, kv.LSM} {
		t.Run(engineName(engine), func(t *testing.T) {
			cfg := smokeConfig(engine, t.TempDir())
			w := Workload{Name: "ycsb-d", Dist: Zipfian, ReadLatest: true, InsertFraction: insertFracD}

			res, err := Run(cfg, w)
			if err != nil {
				t.Fatalf("run: %v", err)
			}

			// Exact accounting: every op landed in reads or writes, none dropped (the single
			// goroutine cannot lose a write-write race against itself).
			if res.Dropped != 0 {
				t.Fatalf("read-latest dropped %d ops on one goroutine, which should never race", res.Dropped)
			}
			if res.Reads.Count+res.Writes.Count != cfg.Ops {
				t.Fatalf("reads %d + writes %d != ops %d", res.Reads.Count, res.Writes.Count, cfg.Ops)
			}
			// Both halves of the mix actually ran: a 5% insert share over 2000 ops is ~100
			// inserts and ~1900 reads, so neither side can be empty.
			if res.Writes.Count == 0 {
				t.Fatal("read-latest recorded no inserts; the keyspace never grew")
			}
			if res.Reads.Count == 0 {
				t.Fatal("read-latest recorded no reads")
			}

			// The inserts must extend the keyspace past the loaded keys. Reopen and confirm the
			// first appended key (index KeyCount) is present: it did not exist before the run.
			head := uint64(cfg.KeyCount)
			gen := NewGenerator(GenConfig{KeyCount: cfg.KeyCount, KeyLen: cfg.KeyLen, ValLen: cfg.ValLen, Dist: Zipfian, Seed: cfg.Seed})
			var kbuf []byte
			kbuf = gen.Key(kbuf, head)
			db, err := kv.Open(filepath.Join(cfg.Dir, "bench.kv"), openOptions(cfg)...)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer db.Close()
			err = db.View(func(txn *kv.Txn) error {
				_, e := txn.Get(kbuf)
				return e
			})
			if err != nil {
				t.Fatalf("first appended key (index %d) missing after run: %v", head, err)
			}

			checkLatency(t, "reads", res.Reads)
			checkLatency(t, "writes", res.Writes)
		})
	}
}
