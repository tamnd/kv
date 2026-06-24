package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

func seedScanDB(b *testing.B, keys int) *DB {
	b.Helper()
	fs := vfs.NewMem()
	d, err := Open(fs, "bench.kv", Options{PageSize: 4096, Sync: wal.SyncOff})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	for i := range keys {
		if _, err := d.Write(func(wb *engine.WriteBatch) {
			wb.Set([]byte(fmt.Sprintf("k%08d", i)), []byte("value-payload"))
		}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
	return d
}

// BenchmarkFullScanIter walks the whole keyspace through the general Iterator, the path the kv
// adapter used before the zero-copy cursor: it copies every key and value and accumulates the
// walked range. This is the readseq baseline the zero-copy cursor is measured against.
func BenchmarkFullScanIter(b *testing.B) {
	for _, keys := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			d := seedScanDB(b, keys)
			defer d.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := d.View(func(txn *Txn) error {
					it, err := txn.NewIterator(engine.IterOptions{})
					if err != nil {
						return err
					}
					defer it.Close()
					n := 0
					for ok := it.First(); ok; ok = it.Next() {
						_ = it.Key()
						if _, err := it.Value(); err != nil {
							return err
						}
						n++
					}
					if n != keys {
						b.Fatalf("scanned %d, want %d", n, keys)
					}
					return it.Error()
				}); err != nil {
					b.Fatalf("scan: %v", err)
				}
			}
		})
	}
}

// BenchmarkFullScanZeroCopy walks the whole keyspace through the zero-copy ScanCursor: no per-key
// copy, a recycled bounded buffer, views read transiently. Same work as BenchmarkFullScanIter, so
// the difference between the two is what the zero-copy path saves on the readseq shape.
func BenchmarkFullScanZeroCopy(b *testing.B) {
	for _, keys := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			d := seedScanDB(b, keys)
			defer d.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := d.View(func(txn *Txn) error {
					sc, err := txn.NewScanCursor(engine.IterOptions{})
					if err != nil {
						return err
					}
					defer sc.Close()
					n := 0
					for sc.Next() {
						_ = sc.Key()
						_ = sc.Value()
						n++
					}
					if n != keys {
						b.Fatalf("scanned %d, want %d", n, keys)
					}
					return sc.Error()
				}); err != nil {
					b.Fatalf("scan: %v", err)
				}
			}
		})
	}
}
