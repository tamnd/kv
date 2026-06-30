package hlog

import (
	"sync"
	"testing"
)

// This file is the step-two index decision: the slot-claim protocol. The index is read
// far more than it is written under skew, and reads run concurrently across cores, so
// the question is how a lookup and an insert coordinate on a slot.
//
//   - lock-free open addressing (Index): a lookup is atomic loads with no lock, an
//     insert claims its slot with one CAS. Readers never block and never touch a lock.
//   - sharded latched map (shardedMap below): the obvious alternative, a Go map per
//     shard under an RWMutex. Readers take a read lock, writers a write lock.
//
// The benchmark runs a read-heavy mix (the index's real load: many Get, few Put) in
// parallel and reads the two side by side. The claim is that the lock-free table holds
// flat as readers scale because a read is pure loads, while the sharded map pays the
// RWMutex's read-lock atomics and cache-line traffic on every Get. The verdict and the
// board are in impl note 175. The losing candidate stays here so the choice is
// reproducible.

// shardedMap is the latched baseline the lock-free Index replaces: a fingerprint-to-
// address map split into RWMutex-guarded shards. It is the design most engines reach for
// first, kept here only as the benchmark's comparison point.
type shardedMap struct {
	shards []mapShard
	mask   uint64
}

type mapShard struct {
	mu sync.RWMutex
	m  map[uint64]int64
}

func newShardedMap(shardBits int) *shardedMap {
	n := 1 << shardBits
	s := &shardedMap{shards: make([]mapShard, n), mask: uint64(n - 1)}
	for i := range s.shards {
		s.shards[i].m = make(map[uint64]int64)
	}
	return s
}

func (s *shardedMap) Put(fp uint64, addr int64) {
	sh := &s.shards[fp&s.mask]
	sh.mu.Lock()
	sh.m[fp] = addr
	sh.mu.Unlock()
}

func (s *shardedMap) Get(fp uint64) (int64, bool) {
	sh := &s.shards[fp&s.mask]
	sh.mu.RLock()
	addr, ok := sh.m[fp]
	sh.mu.RUnlock()
	return addr, ok
}

const indexKeys = 1 << 16

// fillFP returns the fingerprints the index benchmarks read and write, nonzero and
// deterministic so both candidates see the same keys.
func fillFP(n int) []uint64 {
	fps := make([]uint64, n)
	for i := range fps {
		fps[i] = uint64(i)*0x9e3779b97f4a7c15 | 1
	}
	return fps
}

// BenchmarkIndexLockFree runs the read-heavy mix against the lock-free table. One write
// per sixteen reads, the read-dominated shape the hot index actually sees.
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
	sm := newShardedMap(8)
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
