package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/vfs"
)

// BenchmarkAutoGCSpaceAmp measures the file size (space amplification proxy) of a
// B-tree database under an overwrite-heavy workload for two cases: auto-GC enabled
// (auto-checkpoint triggers GC after each fold) vs. no-GC (explicit checkpoint only,
// Maintain never called). The space difference is the dead-version history the GC step
// reclaims automatically (perf/05 F3c). Both databases are settled and checkpointed so
// the WAL does not inflate the comparison.
func BenchmarkAutoGCSpaceAmp(b *testing.B) {
	const (
		keys       = 500
		overwrites = 8
	)

	write := func(fs *vfs.Mem, autoChkpt int) {
		b.Helper()
		opts := Options{PageSize: 4096, AutoCheckpoint: autoChkpt}
		d, err := Open(fs, "bench.kv", opts)
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		for round := range overwrites {
			if err := d.Update(func(txn *Txn) error {
				for i := range keys {
					txn.Set([]byte(fmt.Sprintf("key%04d", i)),
						[]byte(fmt.Sprintf("value-round-%d-%04d", round, i)))
				}
				return nil
			}); err != nil {
				b.Fatalf("write: %v", err)
			}
		}
		if err := d.Checkpoint(); err != nil {
			b.Fatalf("checkpoint: %v", err)
		}
		if err := d.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}

	cases := []struct {
		name     string
		autoChkt int
	}{
		{"auto-gc", 1},
		{"no-gc", -1},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			fs := vfs.NewMem()
			// The outer b.N loop just re-runs to stabilise the timer; what matters is the
			// size metric, which we re-measure only once (it's deterministic).
			for range b.N {
				fs = vfs.NewMem()
				write(fs, c.autoChkt)
			}
			b.StopTimer()
			size := memFileSize(b, fs, "bench.kv")
			b.ReportMetric(float64(size), "file-bytes")
			// Space amp: file bytes / (keys * value size after final overwrite). Only the last
			// round's values survive logically; the GC reclaims the dead 7 overwrite rounds.
			liveBytes := int64(keys) * int64(len(fmt.Sprintf("value-round-%d-%04d", overwrites-1, 0)))
			b.ReportMetric(float64(size)/float64(liveBytes), "space-amp")
		})
	}
}

// memFileSize reads the size of a file from an in-memory fs, returning 0 on error.
func memFileSize(b *testing.B, fs *vfs.Mem, path string) int64 {
	b.Helper()
	f, err := fs.Open(path, 0) // read flags
	if err != nil {
		return 0
	}
	defer f.Close()
	sz, err := f.Size()
	if err != nil {
		return 0
	}
	return sz
}
