package index

import "testing"

const indexKeys = 1 << 16

// fillFP returns the fingerprints both candidates read and write, nonzero and deterministic.
func fillFP(n int) []uint64 {
	fps := make([]uint64, n)
	for i := range fps {
		fps[i] = uint64(i)*0x9e3779b97f4a7c15 | 1
	}
	return fps
}

// BenchmarkIndexLockFree runs the read-heavy mix the hot index actually sees, one write per
// sixteen reads, against the lock-free table.
func BenchmarkIndexLockFree(b *testing.B) {
	fps := fillFP(indexKeys)
	ix := NewIndex(indexKeys)
	for i, fp := range fps {
		ix.Put(fp, int64(i))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var n uint64
		for pb.Next() {
			i := n & (indexKeys - 1)
			if n&15 == 0 {
				ix.Put(fps[i], int64(n))
			} else {
				ix.Get(fps[i])
			}
			n++
		}
	})
}

// BenchmarkIndexShardedMap runs the same mix against the latched baseline.
func BenchmarkIndexShardedMap(b *testing.B) {
	fps := fillFP(indexKeys)
	sm := NewShardedMap(8)
	for i, fp := range fps {
		sm.Put(fp, int64(i))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var n uint64
		for pb.Next() {
			i := n & (indexKeys - 1)
			if n&15 == 0 {
				sm.Put(fps[i], int64(n))
			} else {
				sm.Get(fps[i])
			}
			n++
		}
	})
}
