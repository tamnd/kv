package cache

import (
	"hash/maphash"
	"testing"
)

const cacheBenchCells = 1 << 14
const cacheBenchVal = "a-cached-value-roughly-the-size-of-a-small-record-payload-for-the-read-cache-bench"
const benchKeyLen = 16

var hashSeed = maphash.MakeSeed()

func hashKeys(n, keyLen int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		k := make([]byte, keyLen)
		v := uint64(i) * 0x9e3779b97f4a7c15
		for j := range k {
			k[j] = byte(v)
			v >>= 8
			if v == 0 {
				v = uint64(i+j) * 0x100000001b3
			}
		}
		keys[i] = k
	}
	return keys
}

func cacheBenchKeys(n int) ([][]byte, []uint64) {
	keys := hashKeys(n, benchKeyLen)
	fps := make([]uint64, n)
	for i, k := range keys {
		fps[i] = maphash.Bytes(hashSeed, k)
	}
	return keys, fps
}

func BenchmarkReadCacheCOW(b *testing.B) {
	keys, fps := cacheBenchKeys(cacheBenchCells)
	mask := uint64(len(keys) - 1)
	c := NewCOWCache(cacheBenchCells)
	val := []byte(cacheBenchVal)
	for i := range keys {
		c.Put(fps[i], keys[i], val)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var scratch []byte
		var i uint64
		for pb.Next() {
			j := i & mask
			if i&15 == 0 {
				c.Put(fps[j], keys[j], val)
			} else {
				scratch, _ = c.Get(fps[j], keys[j], scratch)
			}
			i++
		}
	})
}

func BenchmarkReadCacheMutex(b *testing.B) {
	keys, fps := cacheBenchKeys(cacheBenchCells)
	mask := uint64(len(keys) - 1)
	c := NewMutexCache(cacheBenchCells)
	val := []byte(cacheBenchVal)
	for i := range keys {
		c.Put(fps[i], keys[i], val)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var scratch []byte
		var i uint64
		for pb.Next() {
			j := i & mask
			if i&15 == 0 {
				c.Put(fps[j], keys[j], val)
			} else {
				scratch, _ = c.Get(fps[j], keys[j], scratch)
			}
			i++
		}
	})
}
