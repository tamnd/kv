package hotindex

import (
	"hash/maphash"
	"testing"
)

const benchKeyLen = 16
const hotSlots = 1 << 14      // 16K hot keys, the resident working set
const keyspaceSlots = 1 << 24 // 16M keys, the whole space

var hashSeed = maphash.MakeSeed()

// BenchmarkHotIndexPutWorkingSet puts into a small index with keys that cycle within the hot
// set, so every Put lands in a table that fits cache. This is the hot tier under skew.
func BenchmarkHotIndexPutWorkingSet(b *testing.B) {
	ix := NewIndex(hotSlots)
	keys := hashKeys(hotSlots, benchKeyLen)
	mask := uint64(len(keys) - 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range uint64(b.N) {
		fp := forceFP(maphash.Bytes(hashSeed, keys[i&mask]))
		ix.Put(fp, int64(i))
	}
}

// BenchmarkHotIndexPutKeyspace puts into a large index with keys spread across the whole space,
// so every Put scatters into a table far larger than cache. This is the note 177 worst case.
func BenchmarkHotIndexPutKeyspace(b *testing.B) {
	ix := NewIndex(keyspaceSlots)
	keys := hashKeys(keyspaceSlots, benchKeyLen)
	mask := uint64(len(keys) - 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range uint64(b.N) {
		fp := forceFP(maphash.Bytes(hashSeed, keys[i&mask]))
		ix.Put(fp, int64(i))
	}
}
