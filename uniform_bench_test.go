package kv

import (
	"path/filepath"
	"testing"
)

// This file mirrors the kvbench point workloads (fillrandom, readrandom) so a profile taken
// here is taken on the same shape the published board measures: uniform random access over the
// whole keyspace, not the skewed hot set the tier_bench file uses, and a 1 KiB value. Uniform
// access is the hard case for a tiered store, there is no hot set, so it is the access pattern
// the 5x board lives or dies on and the one worth profiling.

const uniKeys = 100000 // matches the default kvbench --cardinality
const uniValBytes = 1024

// openUniform opens a store sized the way the kvbench hlog adapter sizes it for this cell: the
// cold index to the key count, the resident window to the whole dataset so a cache-resident read
// never leaves memory. Keeping the bench and the adapter in lockstep is what makes the profile
// representative of the board.
func openUniform(b *testing.B) *TieredDB {
	path := filepath.Join(b.TempDir(), "uni.hlog")
	// Mirror the kvbench hlog adapter in the cache-resident regime: the resident window covers
	// the whole dataset (the harness budgets working-set x1.25) and the read cache stays small
	// because the ring already serves a cold read from memory.
	budget := int64(uniKeys) * int64(uniValBytes+16)
	budget += budget / 4
	// Size the hot tier to the value the way the adapter does: a 32 MiB segment holds ~32k 1 KiB
	// records, so a 100k fill seals ~3 times instead of ~12, and HotKeys sizes the index to those
	// records (~40k slots, well under a megabyte) rather than the 8 MiB/32 heuristic that
	// over-allocates a million slots and drove the fill-throughput variance note 182 chased.
	o := Options{
		KeyCapacity:    uniKeys,
		HotBytes:       32 << 20,
		HotKeys:        40000,
		ResidentBytes:  budget,
		ReadCacheCells: 4096,
	}.withDefaults()
	d, err := OpenTiered(path, o.HotBytes, o.hotKeys(), o.ResidentBytes, o.KeyCapacity, o.ReadCacheCells)
	if err != nil {
		b.Fatal(err)
	}
	return d
}

func uniValue() []byte {
	v := make([]byte, uniValBytes)
	for i := range v {
		v[i] = byte(i)
	}
	return v
}

// uniformKey maps a counter to a key id spread across the whole keyspace with no rand, the same
// uniform spread the readrandom generator produces.
func uniformKey(dst []byte, i uint64) {
	fixedKey(dst, (i*2654435761)%uniKeys)
}

// BenchmarkUniformFill is fillrandom: write every key once, uniform order. This is the write
// number the board compares against badger and pebble.
func BenchmarkUniformFill(b *testing.B) {
	d := openUniform(b)
	defer d.Close()
	key := make([]byte, 16)
	val := uniValue()
	b.SetBytes(uniValBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range uint64(b.N) {
		uniformKey(key, i)
		d.Set(key, val)
	}
}

// BenchmarkUniformGet is readrandom: fill the keyspace, then read uniformly at random. With the
// resident window sized to the dataset every read is served from memory, so this measures the
// in-memory read path, the cascade and the copy, which is what the board's readrandom cell does.
func BenchmarkUniformGet(b *testing.B) {
	d := openUniform(b)
	defer d.Close()
	key := make([]byte, 16)
	val := uniValue()
	for i := range uint64(uniKeys) {
		fixedKey(key, i)
		d.Set(key, val)
	}
	if err := d.Sync(); err != nil { // drain the hot tier to cold so reads exercise the cold path
		b.Fatal(err)
	}
	b.SetBytes(uniValBytes)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		k := make([]byte, 16)
		scratch := make([]byte, 0, uniValBytes)
		var i uint64
		for pb.Next() {
			uniformKey(k, i)
			d.Get(k, scratch)
			i++
		}
	})
}
