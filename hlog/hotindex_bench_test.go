package hlog

import (
	"hash/maphash"
	"testing"
)

// This file measures the central claim of the hot/cold split, the F2 flaw fix, before the
// tiers are built: an index bounded to the working set stays cache-resident and a write into
// it is far cheaper than a write into an index sized to the whole keyspace. Note 177's
// profile found the write tax is exactly that scatter, Index.Put at 70 percent on amd64
// because every insert lands at a random slot in a table sized to all keys, missing cache and
// TLB. The split's whole purpose is to keep the hot index small, so this benchmark is the
// measured justification for the architecture, and the loser, the keyspace-sized index, stays
// checked in next to the winner.
//
// Both benchmarks do the identical per-op work, hash a key and Put it. They differ only in
// the index size and the key stream:
//   - WorkingSet: a small index, hotSlots entries, with keys that cycle within the hot set,
//     so every Put lands in a table that fits cache. This is the hot tier under skew.
//   - Keyspace: a large index, keyspaceSlots entries, with keys spread across the whole
//     space, so every Put scatters into a table far larger than cache. This is the note 177
//     worst case the split removes.

const hotSlots = 1 << 14       // 16K hot keys, the resident working set
const keyspaceSlots = 1 << 24  // 16M keys, the whole space

func BenchmarkHotIndexPutWorkingSet(b *testing.B) {
	ix := NewIndex(hotSlots)
	keys := hashKeys(hotSlots, benchKeyLen)
	mask := uint64(len(keys) - 1)
	seed := hashSeed
	b.ReportAllocs()
	b.ResetTimer()
	for i := range uint64(b.N) {
		fp := forceFP(maphash.Bytes(seed, keys[i&mask]))
		ix.Put(fp, int64(i))
	}
}

func BenchmarkHotIndexPutKeyspace(b *testing.B) {
	ix := NewIndex(keyspaceSlots)
	keys := hashKeys(keyspaceSlots, benchKeyLen)
	mask := uint64(len(keys) - 1)
	seed := hashSeed
	b.ReportAllocs()
	b.ResetTimer()
	for i := range uint64(b.N) {
		fp := forceFP(maphash.Bytes(seed, keys[i&mask]))
		ix.Put(fp, int64(i))
	}
}
