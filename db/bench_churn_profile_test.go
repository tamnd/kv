package db

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// BenchmarkChurnProfile reproduces the kvbench ycsb-a update-churn collapse on the f2 core
// so a CPU profile can name the cost. It hammers a small hot key band with auto-commit
// overwrites at SyncOff, the same engine the kvbench "kv" adapter selects (EngineF2). With a
// tiny band, every write lands on a key that already carries many committed versions, so the
// per-commit cost is dominated by the version-group read-modify-write path, not by fresh
// inserts. Run with:
//
//	GOWORK=off go test ./db -run x -bench BenchmarkChurnProfile -benchtime 200000x \
//	    -cpuprofile /tmp/churn-cpu.prof -memprofile /tmp/churn-mem.prof
func BenchmarkChurnProfile(b *testing.B) {
	const hot = 64 // a small hot set so version groups grow deep, like a Zipfian head
	fs := vfs.NewMem()
	d, err := Open(fs, "churn.kv", Options{PageSize: 4096, Engine: format.EngineF2, Sync: wal.SyncOff})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer d.Close()

	keys := make([][]byte, hot)
	val := make([]byte, 64)
	for i := range hot {
		keys[i] = []byte(fmt.Sprintf("hot%08d", i))
		if _, err := d.Write(func(wb *engine.WriteBatch) { wb.Set(keys[i], val) }); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keys[i%hot]
		if _, err := d.Write(func(wb *engine.WriteBatch) { wb.Set(k, val) }); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

// BenchmarkChurnProfileFile is the same churn workload over a real OS-backed file instead of the
// in-memory VFS. The mem VFS reallocs and copies the whole file image on every append, an O(n
// squared) artifact that dominates the mem-VFS profile (vfs.growBytes, memFile.WriteAt) and hides
// the real commit-pipeline cost. On a real file the writes are syscalls, not heap growth, so the
// profile shows the genuine per-commit work: the apply latch, the oracle, and the engine apply.
// Run with:
//
//	GOWORK=off go test ./db -run x -bench BenchmarkChurnProfileFile -benchtime 200000x \
//	    -cpuprofile /tmp/churn-cpu.prof -memprofile /tmp/churn-mem.prof
func BenchmarkChurnProfileFile(b *testing.B) {
	const hot = 64
	dir := b.TempDir()
	d, err := Open(vfs.NewOS(), filepath.Join(dir, "churn.kv"), Options{PageSize: 4096, Engine: format.EngineF2, Sync: wal.SyncOff})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer d.Close()

	keys := make([][]byte, hot)
	val := make([]byte, 64)
	for i := range hot {
		keys[i] = []byte(fmt.Sprintf("hot%08d", i))
		if _, err := d.Write(func(wb *engine.WriteBatch) { wb.Set(keys[i], val) }); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keys[i%hot]
		if _, err := d.Write(func(wb *engine.WriteBatch) { wb.Set(k, val) }); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}
