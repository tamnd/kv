package db

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
)

// buildCompressedDB loads a compressible workload large enough to overflow L1 (8 MiB) into
// the cold deep levels, settles it, and folds it into the main file, so the measured file
// size reflects the durable compressed footprint and reads hit real on-disk segments. It
// returns the open database and its on-disk size in bytes. The value carries a long shared
// prefix so a page of them compresses well, the same shape a real low-entropy column has.
func buildCompressedDB(b *testing.B, mode engine.CompressionMode, n int) (*DB, int64) {
	b.Helper()
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.kv")
	// A small memtable so flushes are frequent and the tree cascades into L2+, where cold-only
	// does its compression; the default 64 MiB memtable would hold the whole load and never
	// build a cold level.
	opts := Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 1 << 20, CompressionMode: mode}
	d, err := Open(vfs.NewOS(), path, opts)
	if err != nil {
		b.Fatalf("open (mode=%d): %v", mode, err)
	}
	const batch = 500
	for lo := 0; lo < n; lo += batch {
		hi := lo + batch
		if hi > n {
			hi = n
		}
		if err := d.Update(func(txn *Txn) error {
			for i := lo; i < hi; i++ {
				txn.Set([]byte(fmt.Sprintf("key%08d", i)),
					[]byte(fmt.Sprintf("payload-field-value-shared-prefix-%08d", i)))
			}
			return nil
		}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
	for {
		rep, err := d.Maintain(0)
		if err != nil {
			b.Fatalf("maintain: %v", err)
		}
		if rep.PagesCompacted == 0 {
			break
		}
	}
	if err := d.Checkpoint(); err != nil {
		b.Fatalf("checkpoint: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		b.Fatalf("stat: %v", err)
	}
	return d, info.Size()
}

// BenchmarkColdCompression measures the cold-only compression policy against the off and
// heat-tiered baselines on a workload deep enough to populate the cold levels (perf/05 F4d).
// For each mode it reports two numbers: the settled on-disk file size (the space win) and the
// hot point-read latency (the read-path cost). The slice's claim is that cold-only buys most
// of heat-tiered's space win while keeping the hot read path as cheap as no compression at
// all, because the hot shallow levels stay raw and only the cold deep levels decompress.
func BenchmarkColdCompression(b *testing.B) {
	const n = 200000
	modes := []struct {
		name string
		mode engine.CompressionMode
	}{
		{"off", engine.CompressOff},
		{"cold-only", engine.CompressColdOnly},
		{"heat-tiered", engine.CompressHeatTiered},
	}
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			d, size := buildCompressedDB(b, m.mode, n)
			defer d.Close()

			rng := rand.New(rand.NewSource(1))
			b.ResetTimer()
			for range b.N {
				k := fmt.Sprintf("key%08d", rng.Intn(n))
				if _, ok := txnGetB(b, d, k); !ok {
					b.Fatalf("missing key %s", k)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(size)/(1<<20), "MiB-on-disk")
		})
	}
}

// txnGetB is the benchmark-side point read: it resolves one key in a read transaction and
// reports presence and value, the same path txnGet uses for tests but bound to *testing.B.
func txnGetB(b *testing.B, d *DB, key string) (string, bool) {
	b.Helper()
	var val string
	var ok bool
	if err := d.View(func(txn *Txn) error {
		v, e := txn.Get([]byte(key))
		if e == nil {
			val, ok = string(v), true
		}
		return nil
	}); err != nil {
		b.Fatalf("view: %v", err)
	}
	return val, ok
}
