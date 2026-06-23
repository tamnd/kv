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

// BenchmarkReadScaleClean measures how point-read throughput scales with reader count without
// the confounds the older BenchmarkConcurrentGet carries: it pre-builds every reader's key set so
// no fmt.Sprintf runs inside the timed loop, and it gives each reader its own private slice of
// pre-encoded keys so there is no shared atomic counter for the readers to contend on. What is
// left in the timed region is the engine's Get and nothing else, so the ns/op at readers=N over
// the ns/op at readers=1 is the engine's real parallel scaling, not the harness's.
func BenchmarkReadScaleClean(b *testing.B) {
	const keys = 10000
	fs := vfs.NewMem()
	d, err := Open(fs, "bench.kv", Options{PageSize: 4096, Sync: wal.SyncOff})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer d.Close()

	keyBytes := make([][]byte, keys)
	for i := range keys {
		keyBytes[i] = []byte(fmt.Sprintf("k%08d", i))
		if _, err := d.Write(func(wb *engine.WriteBatch) {
			wb.Set(keyBytes[i], []byte("value-payload"))
		}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	for _, readers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			perReader := b.N / readers
			if perReader == 0 {
				perReader = 1
			}
			var wg sync.WaitGroup
			var failed atomic.Bool
			b.ResetTimer()
			for r := range readers {
				wg.Add(1)
				go func(start int) {
					defer wg.Done()
					for i := 0; i < perReader; i++ {
						k := keyBytes[(start+i)%keys]
						if _, err := d.Get(k); err != nil {
							failed.Store(true)
							return
						}
					}
				}(r * (keys / readers))
			}
			wg.Wait()
			if failed.Load() {
				b.Fatal("get failed")
			}
			// b.N total ops are split across the readers, so ns/op is wall_time/b.N. Under
			// perfect parallel scaling the wall time falls with the reader count, so ns/op at
			// readers=N should approach the readers=1 figure divided by N; a figure that stays
			// flat as readers rise is the engine serializing the reads instead of scaling.
		})
	}
}
