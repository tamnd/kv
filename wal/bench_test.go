package wal

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/kv/vfs"
)

// BenchmarkAppendFrame measures the WAL append hot path (one batch frame plus its commit
// frame per commit) in isolation of the fsync, so the per-frame allocation and copy that
// Finding 6 targets show undiluted. SyncOff makes Sync a no-op; the commit path is then
// pure encode + WriteAt. It runs against a real os-backed file rather than the mem VFS on
// purpose: the mem file reallocates its whole backing slice as it grows, which would add
// its own per-write allocation and hide the WAL's. On disk the steady-state append path is
// now allocation-free (the reused frame and checksum scratch), so this reports 0 allocs/op;
// before the pooling it reported 4 (a frame make and a chain make for each of the two
// frames). The single fsync the durability levels add back is amortized by group commit.
func BenchmarkAppendFrame(b *testing.B) {
	w, err := Create(vfs.NewOS(), filepath.Join(b.TempDir(), "bench.kv-wal"),
		Options{PageSize: 4096, Sync: SyncOff, Salt: 1})
	if err != nil {
		b.Fatalf("create: %v", err)
	}
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.LogBatch(uint64(i+1), payload); err != nil {
			b.Fatalf("log batch: %v", err)
		}
		if _, err := w.AppendCommit(uint64(i + 1)); err != nil {
			b.Fatalf("append commit: %v", err)
		}
		if err := w.Sync(); err != nil {
			b.Fatalf("sync: %v", err)
		}
	}
}
