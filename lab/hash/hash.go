// Package hash is a frozen experiment: how should the in-memory index fingerprint a key?
// Every Get and Set hashes the key at least once, and on the read path that hash is a large
// share of the per-op cost, so the hash function is settled with numbers, not opinion.
//
// Verdict: maphash.Bytes, the runtime's own AES-accelerated seeded hash. It beats FNV by 2x
// to 3x on the short keys an index holds, and the streaming maphash.Hash struct and the
// hash.Hash64 interface both lose to per-call overhead. The full board and the chi-square
// spread gate are in impl note 174.
package hash

import "hash/maphash"

// HashSeed is fixed for a run so every candidate hashes the same input.
var HashSeed = maphash.MakeSeed()

// BenchKeyLen sizes the keys to the engine's target: short, where per-call overhead dominates
// and a byte-at-a-time hash has the least room to amortize.
const BenchKeyLen = 16

// HashKeys returns n distinct deterministic keys of the given length.
func HashKeys(n, keyLen int) [][]byte {
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

// InlineFNV1a is FNV-1a 64 with no interface, the algorithm candidate stripped of the
// hash.Hash64 dispatch so the benchmark attributes FNV's cost to the algorithm, not the
// interface.
func InlineFNV1a(key []byte) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	h := uint64(offset64)
	for _, b := range key {
		h ^= uint64(b)
		h *= prime64
	}
	return h
}
