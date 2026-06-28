package bench

import (
	"testing"

	"github.com/tamnd/kv"
)

// BenchmarkDirectionalScanE reproduces the directional ycsb-e path exactly: a Beta DB loaded and
// settled to a flushed tree with an empty tail, then scanOp (db.View, txn.NewIterator, SeekGE, a
// bounded run of Next, Value per key) driven through the public kv.DB stack with a fresh reader per
// op. This is the cell the directional bench reports at 0.64x of btree, and the microbenchmarks in
// the betree package miss it because they reuse one reader and read only the key. It exists to be
// profiled (go test -bench BenchmarkDirectionalScanE -cpuprofile ...) so the real per-op scan cost
// shows up instead of a hand-built proxy. The engine is parameterized so btree and betree run the
// identical path for a side-by-side read.
func benchmarkDirectionalScanE(b *testing.B, eng kv.EngineKind) {
	cfg := DefaultConfig(eng, b.TempDir())
	cfg.KeyCount = 20000
	cfg.Seed = 1

	db, err := kv.Open(cfg.Dir+"/bench.kv", openOptions(cfg)...)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	loadGen := NewGenerator(GenConfig{KeyCount: cfg.KeyCount, KeyLen: cfg.KeyLen, ValLen: cfg.ValLen, Dist: Sequential, Seed: cfg.Seed})
	if _, err := loadPhase(db, loadGen, cfg.KeyCount, cfg.BatchSize, false); err != nil {
		b.Fatalf("load: %v", err)
	}
	if err := settle(db); err != nil {
		b.Fatalf("settle: %v", err)
	}

	kbuf := make([]byte, 0, cfg.KeyLen)
	h := NewHistogram(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kbuf = loadGen.Key(kbuf, uint64((i*977)%(cfg.KeyCount-scanLenE)))
		if err := scanOp(db, kbuf, scanLenE, h); err != nil {
			b.Fatalf("scan: %v", err)
		}
	}
}

func BenchmarkDirectionalScanEBeta(b *testing.B)  { benchmarkDirectionalScanE(b, kv.Beta) }
func BenchmarkDirectionalScanEBtree(b *testing.B) { benchmarkDirectionalScanE(b, kv.BTree) }
