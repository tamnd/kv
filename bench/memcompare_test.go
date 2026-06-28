package bench

import (
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/tamnd/kv"
	"github.com/tamnd/kv/hashlog"
)

// This file pits the in-memory hashlog engine (resident hash index over a hybrid
// log, the FASTER/Garnet model adopted from aki v2) head to head against the three
// shipped kv tree cores (btree, lsm, betree) on the exact shape the aki spike used:
// 1,000,000 keys, 64-byte values, b.RunParallel with an inline xorshift uniform-
// random key pick. It answers the question that drove the pivot: how far is a
// single resident hash probe ahead of a tree descend-plus-decode-plus-fold read.
//
// Fairness: the tree cores run in their fastest in-memory configuration (Sync off,
// so no fsync floor), read through kv's lightest public point path (db.Get) and
// write through its real transactional path (db.Update + Set). hashlog reads and
// writes through its own Get/Set. Each engine is measured on the read and write
// path it actually has; the gap is the cost of the ordered/MVCC/transactional
// machinery the tree cores carry and hashlog does not. Build pinned for the 8-core
// aggregate: go test -c, then taskset/GOMAXPROCS=8 -test.benchtime=2s.
//
// Run one table with, e.g.:
//
//	go test -run x -bench 'BenchmarkMemGet|BenchmarkMemSet' -benchmem ./bench/

const (
	memKeys   = 1_000_000
	memValLen = 64
)

// memKeySet returns memKeys keys formatted once, so the benchmark loop indexes
// precomputed keys rather than formatting per op.
func memKeySet() [][]byte {
	keys := make([][]byte, memKeys)
	for i := range keys {
		keys[i] = []byte(strconv.Itoa(i))
	}
	return keys
}

// openTreeDB opens a kv database on eng in its fastest in-memory config and
// preloads it with the key set at a 64-byte value.
func openTreeDB(b *testing.B, eng kv.EngineKind, keys [][]byte) *kv.DB {
	b.Helper()
	path := filepath.Join(b.TempDir(), "mem.kv")
	db, err := kv.Open(path, kv.WithEngine(eng), kv.WithSynchronous(kv.SyncOff))
	if err != nil {
		b.Fatalf("open %v: %v", eng, err)
	}
	b.Cleanup(func() { db.Close() })
	val := make([]byte, memValLen)
	// Load in batches so the preload itself is not dominated by per-op commit.
	const batch = 1000
	for i := 0; i < len(keys); i += batch {
		end := min(i+batch, len(keys))
		if err := db.Update(func(txn *kv.Txn) error {
			for j := i; j < end; j++ {
				if err := txn.Set(keys[j], val); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatalf("preload %v: %v", eng, err)
		}
	}
	return db
}

func openHashlog(b *testing.B, keys [][]byte) *hashlog.Store {
	b.Helper()
	s, err := hashlog.New(hashlog.DefaultTunables())
	if err != nil {
		b.Fatalf("hashlog new: %v", err)
	}
	b.Cleanup(func() { s.Close() })
	val := make([]byte, memValLen)
	for _, k := range keys {
		if err := s.Set(k, val); err != nil {
			b.Fatalf("hashlog preload: %v", err)
		}
	}
	return s
}

// BenchmarkMemGet measures point-read throughput at saturation for each engine.
func BenchmarkMemGet(b *testing.B) {
	keys := memKeySet()

	b.Run("hashlog", func(b *testing.B) {
		s := openHashlog(b, keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			var x uint32 = 2463534242
			for pb.Next() {
				x ^= x << 13
				x ^= x >> 17
				x ^= x << 5
				if _, _, err := s.Get(keys[x%memKeys]); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	for _, eng := range []struct {
		name string
		kind kv.EngineKind
	}{
		{"btree", kv.BTree},
		{"lsm", kv.LSM},
		{"betree", kv.Beta},
	} {
		b.Run(eng.name, func(b *testing.B) {
			db := openTreeDB(b, eng.kind, keys)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var x uint32 = 2463534242
				for pb.Next() {
					x ^= x << 13
					x ^= x >> 17
					x ^= x << 5
					if _, err := db.Get(keys[x%memKeys]); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkMemSet measures write throughput at saturation for each engine.
func BenchmarkMemSet(b *testing.B) {
	keys := memKeySet()
	val := make([]byte, memValLen)

	b.Run("hashlog", func(b *testing.B) {
		s := openHashlog(b, keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			var x uint32 = 88172645
			for pb.Next() {
				x ^= x << 13
				x ^= x >> 17
				x ^= x << 5
				if err := s.Set(keys[x%memKeys], val); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	for _, eng := range []struct {
		name string
		kind kv.EngineKind
	}{
		{"btree", kv.BTree},
		{"lsm", kv.LSM},
		{"betree", kv.Beta},
	} {
		b.Run(eng.name, func(b *testing.B) {
			db := openTreeDB(b, eng.kind, keys)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var x uint32 = 88172645
				for pb.Next() {
					x ^= x << 13
					x ^= x >> 17
					x ^= x << 5
					k := keys[x%memKeys]
					if err := db.Update(func(txn *kv.Txn) error {
						return txn.Set(k, val)
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkMemMixed95Get5Set measures the 95/5 read-heavy mix for each engine.
func BenchmarkMemMixed95Get5Set(b *testing.B) {
	keys := memKeySet()
	val := make([]byte, memValLen)

	b.Run("hashlog", func(b *testing.B) {
		s := openHashlog(b, keys)
		var sets int64
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			var x uint32 = 747796405
			for pb.Next() {
				x ^= x << 13
				x ^= x >> 17
				x ^= x << 5
				k := keys[x%memKeys]
				if x%20 == 0 {
					s.Set(k, val)
					atomic.AddInt64(&sets, 1)
				} else {
					s.Get(k)
				}
			}
		})
		_ = sets
	})

	for _, eng := range []struct {
		name string
		kind kv.EngineKind
	}{
		{"btree", kv.BTree},
		{"lsm", kv.LSM},
		{"betree", kv.Beta},
	} {
		b.Run(eng.name, func(b *testing.B) {
			db := openTreeDB(b, eng.kind, keys)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var x uint32 = 747796405
				for pb.Next() {
					x ^= x << 13
					x ^= x >> 17
					x ^= x << 5
					k := keys[x%memKeys]
					if x%20 == 0 {
						db.Update(func(txn *kv.Txn) error { return txn.Set(k, val) })
					} else {
						db.Get(k)
					}
				}
			})
		})
	}
}
