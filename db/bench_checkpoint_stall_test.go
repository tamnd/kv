package db

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tamnd/kv/vfs"
)

// BenchmarkWriteUnderCheckpoint measures write throughput while periodic background
// checkpoints are running concurrently (perf/02 F5). Before this slice, CheckpointMode
// held d.mu for the entire page-writeback+fsync, so each checkpoint stalled all writers
// for its full duration. After this slice, d.mu is released between prepare and finalize,
// so foreground commits overlap with the checkpoint's I/O.
//
// The benchmark runs a fixed number of writer goroutines and a checkpoint goroutine
// that fires after every 20 commits. The ns/op metric is the per-commit latency under
// checkpoint pressure: lower is better, and the improvement should be visible on
// workloads where checkpoint I/O time dominates.
func BenchmarkWriteUnderCheckpoint(b *testing.B) {
	for _, writers := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			fs := vfs.NewOS()
			path := filepath.Join(b.TempDir(), "bench.kv")
			// AutoCheckpoint -1: the benchmark drives checkpoints manually.
			d, err := Open(fs, path, Options{PageSize: 4096, AutoCheckpoint: -1})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer d.Close()

			var (
				total atomic.Int64
				stop  = make(chan struct{})
				wg    sync.WaitGroup
			)

			// Checkpoint goroutine fires after every 20 commits.
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					if total.Load()%20 == 0 {
						_ = d.Checkpoint()
					}
				}
			}()

			b.ResetTimer()
			var writersWG sync.WaitGroup
			perWriter := b.N / writers
			if perWriter == 0 {
				perWriter = 1
			}
			for w := range writers {
				writersWG.Add(1)
				go func(w int) {
					defer writersWG.Done()
					for i := range perWriter {
						if err := d.Update(func(txn *Txn) error {
							txn.Set([]byte(fmt.Sprintf("k%d-%d", w, i)), []byte("v"))
							return nil
						}); err != nil {
							b.Errorf("write: %v", err)
							return
						}
						total.Add(1)
					}
				}(w)
			}
			writersWG.Wait()
			b.StopTimer()
			close(stop)
			wg.Wait()
		})
	}
}
