package hlog

import (
	"hash/fnv"
	"hash/maphash"
	"testing"
)

// This file is the step-two technique decision: how the in-memory index fingerprints a
// key. The index maps a key to a logical address in the log, so every Get and every Set
// hashes the key at least once. On the read path that hash is a large share of the
// per-op cost, since the rest is one index probe and one slice index into the log. So
// the hash function is the second thing to settle with numbers.
//
// The candidates are the realistic ways to hash a byte key to a uint64 in Go.
//
//   - maphash.Bytes: the runtime's own hash, the one Go maps use, AES-accelerated on
//     amd64 and arm64, seeded so it resists collision attacks.
//   - maphash.Hash (streaming): the same hash through the Write/Sum64 struct API, to
//     show what the streaming interface costs versus the one-shot call.
//   - fnv via hash/fnv: FNV-1a behind the hash.Hash64 interface, the portable textbook
//     choice, to show the interface dispatch and per-call setup cost.
//   - inline FNV-1a: the same algorithm hand-rolled with no interface, to separate the
//     interface cost from the algorithm cost and isolate why FNV loses.
//
// The verdict and the board are in notes/Spec/2059/implementation/174-index-hash.md.
// Keys are sized to the engine's target: short, like the 8 to 16 byte keys a kv index
// holds, where per-call overhead dominates and a byte-at-a-time hash has the least room
// to amortize.

var hashSeed = maphash.MakeSeed()

// hashKeys returns n distinct keys of the given length, deterministic so every benchmark
// hashes the same input and the comparison is apples to apples.
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

// inlineFNV1a is FNV-1a 64 with no interface, the algorithm candidate stripped of the
// hash.Hash64 dispatch so the benchmark can attribute FNV's cost to the algorithm rather
// than the interface.
func inlineFNV1a(key []byte) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	h := uint64(offset64)
	for _, b := range key {
		h ^= uint64(b)
		h *= prime64
	}
	return h
}

const benchKeyLen = 16

func benchHash(b *testing.B, h func(key []byte) uint64) {
	keys := hashKeys(4096, benchKeyLen)
	b.ResetTimer()
	var sink uint64
	for i := 0; i < b.N; i++ {
		sink ^= h(keys[i&4095])
	}
	runtimeSink = sink
}

var runtimeSink uint64

func BenchmarkHashMaphashBytes(b *testing.B) {
	benchHash(b, func(key []byte) uint64 { return maphash.Bytes(hashSeed, key) })
}

func BenchmarkHashMaphashStream(b *testing.B) {
	benchHash(b, func(key []byte) uint64 {
		var h maphash.Hash
		h.SetSeed(hashSeed)
		h.Write(key)
		return h.Sum64()
	})
}

func BenchmarkHashFNVInterface(b *testing.B) {
	benchHash(b, func(key []byte) uint64 {
		h := fnv.New64a()
		h.Write(key)
		return h.Sum64()
	})
}

func BenchmarkHashFNVInline(b *testing.B) {
	benchHash(b, inlineFNV1a)
}
