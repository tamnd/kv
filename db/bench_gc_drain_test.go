package db

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/tamnd/kv/vfs"
)

// BenchmarkMaintainLatchHold measures how long one Maintain call holds the writer latch
// (d.rl) exclusive, as a function of the per-call page budget (perf/12 F1). A point read
// takes that same latch shared, so a Maintain call's hold time is the ceiling on how long a
// read that arrives mid-drain can stall. The budget is the lever: a fat batch reclaims more
// pages under one exclusive hold and blocks readers for longer.
//
// This benchmark measures the cause directly, not the reader-side tail, on purpose. A
// concurrent-reader benchmark could not show the effect cleanly: the drain windows are rare
// relative to the millions of reads in a timed run, so the reads unlucky enough to open on
// top of one sit above p999 and are swamped by unrelated tail noise (a checkpoint flush, a
// Go GC pause). The latch-hold time isolates exactly what F1 changes. The measured numbers
// on an M4 are roughly:
//
//	batch=512: meanHold ~5.1ms  p99 ~6.4ms  maxHold ~6.9ms   (the old default)
//	batch=64:  meanHold ~0.65ms p99 ~1.0ms  maxHold ~1.05ms  (the new default)
//
// So F1 cuts the worst-case read stall behind a drain by about 6.5x. It does NOT improve read
// throughput or typical (p50/p99) read latency, which the drain windows are too rare to
// touch; it bounds the worst case. Total GC work is unchanged: drainGC loops to completion
// either way, just in narrower windows, at the cost of more (cheap) latch acquire/release
// cycles. The seed tree is several thousand pages so 512 pages is a fraction of the leaf
// chain, not the whole of it.
func BenchmarkMaintainLatchHold(b *testing.B) {
	const (
		keys   = 80000
		value  = "value-payload-for-a-realistic-cell-size-padding-0000"
		stride = 8 // overwrite every Nth key per pressure round, creating dead versions
	)

	for _, batch := range []int{512, 64} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			fs := vfs.NewOS()
			path := filepath.Join(b.TempDir(), "bench.kv")
			// AutoCheckpoint -1: the benchmark drives checkpoints and the drain by hand so
			// the page budget under test is the only variable, not the worker's schedule.
			d, err := Open(fs, path, Options{PageSize: 4096, AutoCheckpoint: -1})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer d.Close()

			// Seed the working set in chunks so the leaf chain is many thousands of pages,
			// larger than either batch under test.
			for base := 0; base < keys; base += 5000 {
				if err := d.Update(func(txn *Txn) error {
					for i := base; i < base+5000 && i < keys; i++ {
						txn.Set([]byte(fmt.Sprintf("key%07d", i)), []byte(value))
					}
					return nil
				}); err != nil {
					b.Fatalf("seed: %v", err)
				}
			}

			holds := make([]time.Duration, 0, b.N)
			round := 0
			b.ResetTimer()
			for len(holds) < b.N {
				// Create a backlog of dead versions, then advance the GC watermark with a
				// checkpoint, so the drain that follows has real work to reclaim. This is
				// untimed setup; only the Maintain hold time is recorded.
				b.StopTimer()
				if err := d.Update(func(txn *Txn) error {
					for i := 0; i < keys; i += stride {
						txn.Set([]byte(fmt.Sprintf("key%07d", i)),
							[]byte(fmt.Sprintf("%s-r%d", value, round)))
					}
					return nil
				}); err != nil {
					b.Fatalf("pressure update: %v", err)
				}
				if err := d.Checkpoint(); err != nil {
					b.Fatalf("checkpoint: %v", err)
				}
				round++
				b.StartTimer()

				for len(holds) < b.N {
					t0 := time.Now()
					rep, err := d.Maintain(batch)
					hold := time.Since(t0)
					if err != nil {
						b.Fatalf("maintain: %v", err)
					}
					holds = append(holds, hold)
					if !rep.More {
						break
					}
				}
			}
			b.StopTimer()

			sort.Slice(holds, func(i, j int) bool { return holds[i] < holds[j] })
			var sum time.Duration
			for _, h := range holds {
				sum += h
			}
			b.ReportMetric(float64(sum.Nanoseconds())/float64(len(holds)), "meanHold-ns")
			b.ReportMetric(float64(holds[len(holds)*99/100].Nanoseconds()), "p99Hold-ns")
			b.ReportMetric(float64(holds[len(holds)-1].Nanoseconds()), "maxHold-ns")
		})
	}
}
