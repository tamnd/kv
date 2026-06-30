// Package hotindex is a frozen experiment that isolates the central claim of the hot/cold
// split, the F2 flaw fix: an index bounded to the working set stays cache-resident and a write
// into it is far cheaper than a write into an index sized to the whole keyspace, which the
// profiler found is the write tax (impl note 177).
//
// Verdict: bounding the index wins 4.1x on the M4, 3.2x on the i9, and 10.9x on the
// memory-latency-bound EPYC, the box that needs it most. The worse the memory subsystem, the
// bigger the win, which is the right shape: the split pays its keep exactly where scatter
// hurts most. The full board is in impl note 179.
//
// Index here is a self-contained copy of the engine's index so the experiment stands alone.
package hotindex

import "sync/atomic"

const emptyFP = 0

type slot struct {
	fp   atomic.Uint64
	addr atomic.Int64
}

// Index is the structure under test; the experiment varies only its size.
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

// hashKeys returns n distinct deterministic keys, the same generator the hash experiment uses.
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
