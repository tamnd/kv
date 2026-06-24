package db

import (
	"fmt"
	"testing"

	"runtime"

	"github.com/tamnd/kv/engine"
)

// BenchmarkScanOp mirrors one kvbench readseq operation rather than a full-keyspace walk: a fresh
// read transaction, a NewScanCursor at a lower bound, a bounded read of scanLen keys, then Close and
// Discard. The full-keyspace scan benchmarks amortize the per-op setup (txn begin, snapshot
// register, reader alloc, leaf descent, teardown) over thousands of keys and so hide it; the kvbench
// readseq op reads only 1..100 keys per scan (avg ~50), so the setup is a large fraction of the op.
// This benchmark exposes that fraction directly: compare ns/op here against scanLen*per-key from the
// full-scan benchmark to see how much of a readseq op is fixed setup versus per-key work.
func BenchmarkScanOp(b *testing.B) {
	const seedKeys = 20000
	for _, scanLen := range []int{1, 10, 50, 100} {
		b.Run(fmt.Sprintf("scanLen=%d", scanLen), func(b *testing.B) {
			d := seedScanDB(b, seedKeys)
			defer d.Close()
			// Precompute start keys so the loop measures the scan op, not fmt.Sprintf.
			const nstart = 512
			starts := make([][]byte, nstart)
			for j := range starts {
				starts[j] = []byte(fmt.Sprintf("k%08d", (j*97)%(seedKeys-scanLen)))
			}
			// Settle the background MVCC-GC and checkpoint loop so the measurement is the read
			// path, not the maintenance still digesting the seed writes.
			if err := d.Checkpoint(); err != nil {
				b.Fatalf("checkpoint: %v", err)
			}
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				// Spread start keys across the space the way a uniform readseq does.
				start := starts[i%nstart]
				txn := d.Begin(false)
				sc, err := txn.NewScanCursor(engine.IterOptions{Lower: start})
				if err != nil {
					b.Fatalf("cursor: %v", err)
				}
				n := 0
				for sc.Next() && n < scanLen {
					_ = sc.Key()
					_ = sc.Value()
					n++
				}
				if err := sc.Error(); err != nil {
					b.Fatalf("scan: %v", err)
				}
				sc.Close()
				txn.Discard()
			}
		})
	}
}
