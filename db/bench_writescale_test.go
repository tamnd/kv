package db

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// BenchmarkWriteScaleClean is the write-path sibling of BenchmarkReadScaleClean: it measures how
// commit throughput scales with writer count without the confounds BenchmarkConcurrentWriteFull
// carries. It pre-builds every writer's key band so no fmt.Sprintf runs inside the timed loop, it
// gives each writer its own private band of pre-encoded keys so the writers neither share an atomic
// counter nor overwrite each other's keys, and it runs at SyncOff so what is measured is the
// engine's in-memory commit path (WAL append, group-commit handoff, engine Apply, version bump) and
// not a disk fsync that would dominate the number and hide the path's own scaling.
//
// The writers overwrite a seeded key band rather than inserting fresh keys, so the tree structure is
// steady and a write measures the steady-state commit path, not a split-heavy insert, the same way
// the read benchmark measures steady-state lookups. b.N total commits are split across the writers,
// so ns/op is wall_time/b.N: under perfect parallel scaling ns/op at writers=N approaches the
// writers=1 figure divided by N, and a figure that stays flat (or climbs) as writers rise is the
// commit path serializing the writes instead of scaling.
func BenchmarkWriteScaleClean(b *testing.B) {
	const keys = 10000
	fs := vfs.NewMem()
	d, err := Open(fs, "bench.kv", Options{PageSize: 4096, Sync: wal.SyncOff})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer d.Close()

	keyBytes := make([][]byte, keys)
	val := []byte("value-payload")
	for i := range keys {
		keyBytes[i] = []byte(fmt.Sprintf("k%08d", i))
		if _, err := d.Write(func(wb *engine.WriteBatch) {
			wb.Set(keyBytes[i], val)
		}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	for _, writers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			perWriter := b.N / writers
			if perWriter == 0 {
				perWriter = 1
			}
			band := keys / writers
			var wg sync.WaitGroup
			var failed atomic.Bool
			b.ResetTimer()
			for w := range writers {
				wg.Add(1)
				go func(start int) {
					defer wg.Done()
					for i := 0; i < perWriter; i++ {
						k := keyBytes[start+(i%band)]
						if _, err := d.Write(func(wb *engine.WriteBatch) {
							wb.Set(k, val)
						}); err != nil {
							failed.Store(true)
							return
						}
					}
				}(w * band)
			}
			wg.Wait()
			if failed.Load() {
				b.Fatal("write failed")
			}
		})
	}
}
