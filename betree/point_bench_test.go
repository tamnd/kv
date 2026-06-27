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
