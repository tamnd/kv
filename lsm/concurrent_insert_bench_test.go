package lsm

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tamnd/kv/format"
)

// BenchmarkSkiplistConcurrentInsert measures the true scaling ceiling of concurrent insert
// into one shared lock-free skip list, with persistent worker goroutines rather than a fresh
// fan-out per group, so the per-group spawn and join cost that the apply path pays is out of
// the picture. Each worker owns a disjoint slice of a pre-shuffled key stream and inserts its
// share into the same list; ns/op is per inserted key. Comparing workers=1 to workers=N is
// the honest answer to "does spreading memtable inserts across cores help", separate from how
// the group-apply path schedules them (perf/03 W1, perf/07).
func BenchmarkSkiplistConcurrentInsert(b *testing.B) {
	val := []byte("value-payload-1234567890")
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			// Pre-encode every internal key once, off the clock, so the measurement is the
			// insert alone and not key formatting. Keys are shuffled so inserts splice all
			// over the keyspace instead of appending in order.
			keys := make([][]byte, b.N)
			for i := 0; i < b.N; i++ {
				k := (i * 2654435761) % b.N
				keys[i] = format.EncodeInternalKey([]byte(fmt.Sprintf("key%010d", k)), 1, format.KindSet)
			}
			sl := newSkiplist(b.N * 64)

			b.ResetTimer()
			var next atomic.Int64
			const stride = 256 // each worker grabs a contiguous run to keep cache lines warm
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						start := int(next.Add(stride)) - stride
						if start >= b.N {
							return
						}
						end := start + stride
						if end > b.N {
							end = b.N
						}
						for i := start; i < end; i++ {
							sl.insert(keys[i], val)
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}
