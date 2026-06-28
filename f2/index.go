package f2

import "sync/atomic"

// The index is the memory win over hashlog. Each slot is one atomic 64-bit word,
// no key bytes and no per-key struct, so the resident cost is a flat 8 bytes per
// slot whatever the keys look like. The word packs three fields:
//
//	bit  63      tombstone: the key was deleted, the slot still occupies its
//	            probe chain but resolves to nothing
//	bits 39..62 24-bit fingerprint: the low bits of the key's home hash. It lets a
//	            reader reject the overwhelming majority of non-matching slots
//	            without touching the log, and because it is the home hash's own low
//	            bits it also lets a grow recompute an entry's new home position from
//	            the slot alone, with no log read (see shard.grow).
//	bits 0..38  log address plus one: the byte offset of the record in the shard's
//	            log, biased by one so an all-zero word reads as an empty slot.
//
// A 39-bit address covers 512 GiB of log per shard per generation, which a store
// with compaction keeps bounded; an append past it returns errLogFull rather than
// truncating an address. The fingerprint plus the key-verify-from-log is what lets
// the key bytes leave RAM: a reader matches the fingerprint, then confirms the full
// key against the record, so a rare fingerprint collision costs one extra log read,
// never a wrong answer.
const (
	slotTombstone uint64 = 1 << 63

	// slotFPBits is the fingerprint width and slotFPShift its position, which is
	// also the address width below it. The fingerprint is the low slotFPBits of the
	// home hash, so for any table no wider than 2^slotFPBits slots the home position
	// is fingerprint & mask: a grow reads it straight from the slot.
	slotFPBits             = 24
	slotFPShift            = 39
	slotFPValueMask uint64 = (1 << slotFPBits) - 1
	slotAddrMask    uint64 = (1 << slotFPShift) - 1

	// minIndexSlots is the smallest table a shard starts with, a power of two. A
	// store with thousands of shards keeps this modest so an empty store stays
	// cheap; it doubles on demand.
	minIndexSlots = 1024

	// loadNum/loadDen is the grow threshold as a fraction, kept in integer math to
	// avoid a float compare on the write path. It is 0.8: the index never evicts, so
	// resident RAM is bound by slot count, and a higher fill packs more keys into the
	// same slots. 0.8 trades a longer probe chain (cheaper than it sounds, since the
	// 24-bit fingerprint rejects a non-matching slot without a log read) for about an
	// eighth less index RAM than 0.7 across a doubling cycle, the cheap interim RAM
	// cut before a future evictable index. Above ~0.85 linear-probe chains lengthen
	// faster than the RAM saved, so this is the practical ceiling.
	loadNum = 8
	loadDen = 10
)

// mixOf folds a key hash into the value that places its home slot. Both the home
// position (mix & mask) and the stored fingerprint (its low bits) derive from this
// one value, which is why a grow can relocate an entry from the fingerprint without
// rehashing the key. The fold spreads the home away from the high bits the shard
// selector uses.
func mixOf(h uint64) uint64 { return h ^ (h >> 15) }

// fpOf is the fingerprint stored in a slot: the low slotFPBits of the mix.
func fpOf(mix uint64) uint64 { return mix & slotFPValueMask }

func makeSlot(fp uint64, off int64) uint64 {
	return (fp << slotFPShift) | (uint64(off+1) & slotAddrMask)
}

func slotFP(slot uint64) uint64  { return (slot >> slotFPShift) & slotFPValueMask }
func slotAddr(slot uint64) int64 { return int64(slot&slotAddrMask) - 1 }

// index is one shard's open-addressing table. slots is read lock-free with atomic
// loads; live and used are write-side counters maintained under the shard lock.
// used counts every occupied slot including tombstones and drives the grow
// decision, because a chain clogged with tombstones probes as slowly as a full
// one; live counts only resolvable keys and feeds Stats.
//
// log is the log this table's addresses point into. It is immutable once the
// table is published and is what makes a compaction's swap atomic for a lock-free
// reader: a reader loads the index pointer once and resolves addresses through
// that same index's log, so it sees a whole generation (this table and its log)
// or the next one, never a new table reading an old log. A grow keeps the log
// (only the slots move); a compaction publishes a new table whose log is the
// freshly rewritten generation.
type index struct {
	slots []atomic.Uint64
	mask  uint64
	live  int
	used  int
	log   *log
}

func newIndex(n int) *index {
	sz := minIndexSlots
	for sz < n {
		sz <<= 1
	}
	return &index{slots: make([]atomic.Uint64, sz), mask: uint64(sz - 1)}
}

// shouldGrow reports whether adding one more entry would push the table past its
// load factor. It is checked before every insert.
func (ix *index) shouldGrow() bool {
	return (ix.used+1)*loadDen > len(ix.slots)*loadNum
}
