package f2

import "sync/atomic"

// The index is the memory win over hashlog. Each slot is one atomic 64-bit word,
// no key bytes and no per-key struct, so the resident cost is a flat 8 bytes per
// slot whatever the keys look like. The word packs three fields:
//
//	bit  63      tombstone: the key was deleted, the slot still occupies its
//	            probe chain but resolves to nothing
//	bits 48..62 15-bit tag: a cheap hash fingerprint that lets a reader reject
//	            the overwhelming majority of non-matching slots without touching
//	            the log at all
//	bits 0..47  log address plus one: the byte offset of the record in the
//	            shard's log, biased by one so an all-zero word reads as empty
//
// A 48-bit address covers 256 TiB of log per shard, far past any resident store.
// The tag plus the key-verify-from-log is what lets the key bytes leave RAM: a
// reader matches the tag, then confirms the full key against the record, so a
// rare tag collision costs one extra log read, never a wrong answer.
const (
	slotTombstone uint64 = 1 << 63
	slotTagShift         = 48
	slotTagMask   uint64 = 0x7fff << slotTagShift
	slotAddrMask  uint64 = (1 << slotTagShift) - 1

	// minIndexSlots is the smallest table a shard starts with, a power of two. A
	// store with thousands of shards keeps this modest so an empty store stays
	// cheap; it doubles on demand.
	minIndexSlots = 1024

	// loadNum/loadDen is the 0.7 grow threshold as a fraction, kept in integer
	// math to avoid a float compare on the write path.
	loadNum = 7
	loadDen = 10
)

// tagOf extracts the 15-bit fingerprint from a key's hash. It takes middle bits,
// clear of the high byte the shard selector uses and the low bits the home slot
// uses, so the tag is statistically independent of both.
func tagOf(h uint64) uint64 { return (h >> 24) & 0x7fff }

func makeSlot(tag uint64, off int64) uint64 {
	return (tag << slotTagShift) | (uint64(off+1) & slotAddrMask)
}

func slotTag(slot uint64) uint64 { return (slot >> slotTagShift) & 0x7fff }
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
