package bench

import (
	"errors"
	"strconv"
	"testing"

	"github.com/tamnd/kv"
)

// This file is the fast-feedback companion to memcompare_test.go. It shrinks the
// keyspace to 10,000 keys so every engine preloads in well under a second, and it
// adds a redis-benchmark-style single-key path: every operation touches the same
// hot key, the way redis-benchmark runs with the default keyspacelen of 1. That
// isolates the per-operation cost of the read and write path with the index/cache
// fully warm and no key-distribution noise, so the four drivers report quickly.
//
//	go test -run x -bench 'BenchmarkQuick' -benchmem -benchtime=1s ./bench/

const quickKeys = 10_000

func quickKeySet() [][]byte {
	keys := make([][]byte, quickKeys)
	for i := range keys {
		keys[i] = []byte(strconv.Itoa(i))
	}
	return keys
}

type quickEngine struct {
	name string
	kind kv.EngineKind
}

var quickEngines = []quickEngine{
	{"btree", kv.BTree},
	{"lsm", kv.LSM},
	{"betree", kv.Beta},
}

// BenchmarkQuickGetSingle hammers one hot key, redis-benchmark default style.
func BenchmarkQuickGetSingle(b *testing.B) {
	keys := quickKeySet()
	hot := keys[len(keys)/2]

	b.Run("hashlog", func(b *testing.B) {
		s := openHashlog(b, keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, _, err := s.Get(hot); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	for _, eng := range quickEngines {
		b.Run(eng.name, func(b *testing.B) {
			db := openTreeDB(b, eng.kind, keys)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := db.Get(hot); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkQuickSetSingle overwrites one hot key repeatedly.
func BenchmarkQuickSetSingle(b *testing.B) {
	keys := quickKeySet()
	val := make([]byte, memValLen)
	hot := keys[len(keys)/2]

	b.Run("hashlog", func(b *testing.B) {
		s := openHashlog(b, keys)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := s.Set(hot, val); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	for _, eng := range quickEngines {
		b.Run(eng.name, func(b *testing.B) {
			db := openTreeDB(b, eng.kind, keys)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					// Concurrent optimistic writers to one key race; retry on
					// conflict so this mirrors a serialized redis-style writer
					// rather than counting lost races as failures.
					for {
						err := db.Update(func(txn *kv.Txn) error {
							return txn.Set(hot, val)
						})
						if err == nil {
							break
						}
						if !errors.Is(err, kv.ErrConflict) {
							b.Fatal(err)
						}
					}
				}
			})
		})
	}
}

// BenchmarkQuickGet reads uniformly at random across the 10k keyspace.
func BenchmarkQuickGet(b *testing.B) {
	keys := quickKeySet()
	n := uint32(len(keys))

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
				if _, _, err := s.Get(keys[x%n]); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	for _, eng := range quickEngines {
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
					if _, err := db.Get(keys[x%n]); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkQuickSet writes uniformly at random across the 10k keyspace.
func BenchmarkQuickSet(b *testing.B) {
	keys := quickKeySet()
	val := make([]byte, memValLen)
	n := uint32(len(keys))

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
				if err := s.Set(keys[x%n], val); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	for _, eng := range quickEngines {
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
					k := keys[x%n]
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
