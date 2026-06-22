package db

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// BenchmarkCommitAlloc measures the per-commit allocation of the write path with the
// fsync taken out (SyncOff), so the batch-encode and WAL-append allocations that perf/02
// Finding 4 targets show undiluted instead of hiding under syscall latency. A single
// writer drives one Set per commit with a realistic value. Before the encode/frame fuse
// the path made a throwaway Encode buffer per commit and copied it into the frame; after
// it serializes the batch straight into the reused frame buffer, so that per-commit
// allocation and copy are gone. Runs against a real os-backed file so the pager and WAL
// behave as in production.
func BenchmarkCommitAlloc(b *testing.B) {
	d, err := Open(vfs.NewOS(), filepath.Join(b.TempDir(), "bench.kv"),
		Options{PageSize: 4096, Sync: wal.SyncOff, AutoCheckpoint: -1})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer d.Close()

	value := make([]byte, 256)
	for i := range value {
		value[i] = byte(i)
	}
	var key [16]byte
	copy(key[:4], "key")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key[4:12], uint64(i))
		if _, err := d.Write(func(wb *engine.WriteBatch) {
			wb.Set(key[:], value)
		}); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}
