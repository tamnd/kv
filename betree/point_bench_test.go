package betree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// BenchmarkPointGet measures the betree reader's point-read path (snapshotPoint: the optimistic
// gen-validation, the interior descent, the on-page version-group seek, and the fold) on a flushed,
// cache-resident keyspace, reusing one reader across reads. It is the ycsb-c shape isolated at the
// betree layer, so its allocs/op and ns/op say where the remaining point-read gap lives after the
// on-page seek (M8.10) removed the whole-leaf decode.
func BenchmarkPointGet(b *testing.B) {
	const n = 20000
	tr := benchTree(b, n)
	rd, err := tr.NewReader(engine.Snapshot{Version: 1})
	if err != nil {
		b.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%08d", i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rd.Get(keys[i%n]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

// BenchmarkPointGetFreshReader is BenchmarkPointGet with a reader opened and closed around each
// Get instead of one reader reused across them, the shape the bench harness drives through the
// public stack (every readOp is a fresh db.View that opens a reader, gets, and closes it). The
// gap between this and BenchmarkPointGet is the per-op reader setup and teardown the reused-reader
// benchmark hides: the epoch-guard register and unregister the reclaimer runs on every read.
func BenchmarkPointGetFreshReader(b *testing.B) {
	const n = 20000
	tr := benchTree(b, n)
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%08d", i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rd, err := tr.NewReader(engine.Snapshot{Version: 1})
		if err != nil {
			b.Fatalf("reader: %v", err)
		}
		if _, err := rd.Get(keys[i%n]); err != nil {
			b.Fatalf("get: %v", err)
		}
		rd.Close()
	}
}
