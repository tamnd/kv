package kv

import (
	"sync/atomic"
)

// Index is the in-memory hash index of the hot tier: it maps a key fingerprint to the
// logical address of that key's most recent record in the log. It is a single flat
// open-addressed table with linear probing, and it is latch-free on every path. A lookup
// is a sequence of atomic loads with no lock; an insert or update claims its slot with
// one compare-and-swap. This is the index shape the F2 paper calls for, a latch-free
// in-memory hash table over only the hot records, and the slot-claim benchmark in
// index_bench_test.go is why it is built this way rather than as a sharded latched map.
//
// The table holds fingerprints, not keys. A fingerprint is the maphash of the key (see
// impl note 174), so two distinct keys can in principle land on the same fingerprint.
// The index does not resolve that; the engine does, by reading the record at the address
// and comparing the stored key. The index's job is to get the read path to the right
// address in one probe in the common case, and the record's own key is the final
// authority. That division keeps the index a tight array of fixed-size slots with no
// pointers and no per-entry allocation.
//
// This step sizes the table to the run and does not grow. Resize is a later step with
// its own migration protocol; sizing to the working set keeps this step honest about
// what it measures, the same way the log buffer is sized to the run in step one.

// emptyFP marks a free slot. A real key whose fingerprint hashes to zero is folded to
// this sentinel's neighbor by forceFP, so zero can mean empty without losing a key.
const emptyFP = 0

// slot is one open-addressed entry: the key fingerprint and the logical address of its
// record. Both are atomic so a lookup reads them without a lock and an insert publishes
// them with release ordering. The two words pack into 16 bytes, so a slot is half a
// cache line and a short probe stays local.
type slot struct {
	fp   atomic.Uint64
	addr atomic.Int64
}

// Index is the flat slot table. mask is len(slots)-1 with len a power of two, so the
// home slot is fp&mask and probing wraps with a mask, no modulo on the hot path.
type Index struct {
	slots []slot
	mask  uint64
}

// NewIndex returns an index with at least capacity slots, rounded up to a power of two
// and then doubled so the table stays below the load factor where linear probing's
// probe chains grow. The caller sizes capacity to the expected key count.
func NewIndex(capacity int) *Index {
	n := 1
	for n < capacity*2 {
		n <<= 1
	}
	return &Index{slots: make([]slot, n), mask: uint64(n - 1)}
}

// forceFP maps a fingerprint onto the nonzero range so emptyFP stays an unambiguous free
// marker. Only the single value zero is remapped, to one, so the collision it introduces
// is one extra fingerprint pairing in 2^64, far below the maphash collision floor the
// engine already tolerates by verifying keys against the record.
func forceFP(fp uint64) uint64 {
	if fp == emptyFP {
		return 1
	}
	return fp
}

// Put records that the key with fingerprint fp now lives at logical address addr. If the
// fingerprint is already in the table it updates the address in place with a single
// atomic store, which is the hot-key overwrite path. Otherwise it claims the first free
// slot in the probe chain with a compare-and-swap. It returns true on success and false
// only if the table is full, which the power-of-two oversizing in NewIndex is meant to
// prevent for the sized working set.
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
			// Lost the race for this slot. Re-read it: if the winner claimed it with our
			// own fingerprint it is now an update, so do not advance past it.
			if ix.slots[i].fp.Load() == fp {
				ix.slots[i].addr.Store(addr)
				return true
			}
		}
		i = (i + 1) & ix.mask
	}
	return false
}

// Get returns the logical address recorded for fingerprint fp, and whether it was found.
// It is latch-free: a walk of atomic loads down the probe chain, stopping at a matching
// fingerprint (hit) or the first free slot (miss, since an insert never leaves a gap
// before its slot). The address it returns is the most recent one Put stored for the
// fingerprint; the engine reads the record there and verifies the key.
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
