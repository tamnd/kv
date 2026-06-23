package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// BenchmarkGetZeroCopy contrasts the copying Get with GetZeroCopy on the B-tree engine over
// a warm, fully resident keyspace, so what it isolates is the per-read value copy the
// zero-copy path drops: Get folds the group and copies the value into a fresh caller-owned
// slice, GetZeroCopy folds and returns the decoded leaf's slice. The gap is one allocation
// and one memcpy per read removed from the hot point-read path (perf/09 N3 part 2).
func BenchmarkGetZeroCopy(b *testing.B) {
	fs := vfs.NewMem()
	d, err := Open(fs, "bench.kv", Options{PageSize: 4096})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer d.Close()

	const n = 4096
	val := []byte("value-payload-1234567890-abcdefghij")
	if _, err := d.Write(func(wb *engine.WriteBatch) {
		for i := 0; i < n; i++ {
			wb.Set([]byte(fmt.Sprintf("key%010d", i)), val)
		}
	}); err != nil {
		b.Fatalf("write: %v", err)
	}
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%010d", i))
	}

	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := d.Get(keys[i&(n-1)]); err != nil {
				b.Fatalf("get: %v", err)
			}
		}
	})
	b.Run("GetZeroCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := d.GetZeroCopy(keys[i&(n-1)]); err != nil {
				b.Fatalf("get: %v", err)
			}
		}
	})
}
