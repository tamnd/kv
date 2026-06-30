// Package index is a frozen experiment: how should a lookup and an insert coordinate on a
// slot in the in-memory index? The index is read far more than written under skew and reads
// run concurrently, so the slot-claim protocol is the question.
//
// Verdict: lock-free open addressing. A lookup is a walk of atomic loads with no lock; an
// insert claims its slot with one compare-and-swap. It beats a sharded RWMutex map by 14x to
// 18x on the read-heavy mix because a read is pure loads while the map pays the read-lock
// atomics and cache-line traffic on every Get. The full board is in impl note 175.
//
// Index here is a self-contained copy of the engine's index, frozen as the candidate it was
// when the comparison ran.
package index

import (
	"sync"
	"sync/atomic"
)

const emptyFP = 0

type slot struct {
	fp   atomic.Uint64
	addr atomic.Int64
}

// Index is the winner: a flat open-addressed table, latch-free on every path.
type Index struct {
	slots []slot
	mask  uint64
}

func NewIndex(capacity int) *Index {
	n := 1
	for n < capacity*2 {
		n <<= 1
	}
	return &Index{slots: make([]slot, n), mask: uint64(n - 1)}
}

func forceFP(fp uint64) uint64 {
	if fp == emptyFP {
		return 1
	}
	return fp
}

func (ix *Index) Put(fp uint64, addr int64) bool {
	fp = forceFP(fp)
	i := fp & ix.mask
	for probe := uint64(0); probe <= ix.mask; probe++ {
		cur := ix.slots[i].fp.Load()
		if cur == fp {
			ix.slots[i].addr.Store(addr)
			return true
		}
		if cur == emptyFP {
			if ix.slots[i].fp.CompareAndSwap(emptyFP, fp) {
				ix.slots[i].addr.Store(addr)
				return true
			}
			if ix.slots[i].fp.Load() == fp {
				ix.slots[i].addr.Store(addr)
				return true
			}
		}
		i = (i + 1) & ix.mask
	}
	return false
}

func (ix *Index) Get(fp uint64) (int64, bool) {
	fp = forceFP(fp)
	i := fp & ix.mask
	for probe := uint64(0); probe <= ix.mask; probe++ {
		cur := ix.slots[i].fp.Load()
		if cur == fp {
			return ix.slots[i].addr.Load(), true
		}
		if cur == emptyFP {
			return 0, false
		}
		i = (i + 1) & ix.mask
	}
	return 0, false
}

// ShardedMap is the loser kept for the comparison: a fingerprint-to-address map split into
// RWMutex-guarded shards, the design most engines reach for first.
type ShardedMap struct {
	shards []mapShard
	mask   uint64
}

type mapShard struct {
	mu sync.RWMutex
	m  map[uint64]int64
}

func NewShardedMap(shardBits int) *ShardedMap {
	n := 1 << shardBits
	s := &ShardedMap{shards: make([]mapShard, n), mask: uint64(n - 1)}
	for i := range s.shards {
		s.shards[i].m = make(map[uint64]int64)
	}
	return s
}

func (s *ShardedMap) Put(fp uint64, addr int64) {
	sh := &s.shards[fp&s.mask]
	sh.mu.Lock()
	sh.m[fp] = addr
	sh.mu.Unlock()
}

func (s *ShardedMap) Get(fp uint64) (int64, bool) {
	sh := &s.shards[fp&s.mask]
	sh.mu.RLock()
	addr, ok := sh.m[fp]
	sh.mu.RUnlock()
	return addr, ok
}
