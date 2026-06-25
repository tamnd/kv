package kv_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv"
)

// seedForGet opens a database and writes n cache-resident keys for the point-read
// benchmarks, returning the database and the key set in insertion order.
func seedForGet(b *testing.B, n int) (*kv.DB, [][]byte) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "data.kv")
	d, err := kv.Open(path)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { d.Close() })
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%08d", i))
		if err := d.Update(func(txn *kv.Txn) error {
			return txn.Set(keys[i], []byte("value-payload"))
		}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
	return d, keys
}

// BenchmarkPointGet measures the top-level Get convenience: one cache-resident point
// read with no transaction to begin, no snapshot watermark to register, and one owned
// value copy. It is the lightest public point read.
func BenchmarkPointGet(b *testing.B) {
	const n = 10000
	d, keys := seedForGet(b, n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := d.Get(keys[i%n]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

// BenchmarkPointViewGetCopy measures the same read through a View transaction and
// GetCopy, the shape callers had to write before Get existed. It begins and discards a
// transaction and registers and releases a snapshot watermark per read on top of the
// same lookup and copy BenchmarkPointGet does, so the gap between the two is the
// transaction machinery Get skips.
func BenchmarkPointViewGetCopy(b *testing.B) {
	const n = 10000
	d, keys := seedForGet(b, n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := d.View(func(txn *kv.Txn) error {
			_, err := txn.GetCopy(keys[i%n])
			return err
		}); err != nil {
			b.Fatalf("view: %v", err)
		}
	}
}
