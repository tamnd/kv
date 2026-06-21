package bench

import (
	"testing"

	"github.com/tamnd/kv"
)

// TestOutOfCacheRaisesReadAmplification runs the same uniform read-only workload over a
// keyspace larger than a small buffer pool and then again with a pool big enough to hold it,
// and checks the out-of-cache run pays more physical page reads per logical read. That is the
// read axis of the RUM triple becoming visible: cache-resident, read amplification is near
// zero because every page is a hit; under cache pressure it rises because reads miss to disk.
func TestOutOfCacheRaisesReadAmplification(t *testing.T) {
	// A read-only uniform workload spreads reads across the whole keyspace, so a small pool
	// cannot hold the working set and reads miss.
	w := Workload{Name: "ycsb-c", Dist: Uniform, ReadFraction: 1}

	base := smokeConfig(kv.BTree, t.TempDir())
	base.KeyCount = 8000
	base.Ops = 4000
	base.PageSize = 4096

	small := base
	small.Dir = t.TempDir()
	small.CacheBytes = 64 * 1024 // 16 pages: far below the working set

	large := base
	large.Dir = t.TempDir()
	large.CacheBytes = 16 * 1024 * 1024 // holds the whole database

	smallRes, err := Run(small, w)
	if err != nil {
		t.Fatalf("small-cache run: %v", err)
	}
	largeRes, err := Run(large, w)
	if err != nil {
		t.Fatalf("large-cache run: %v", err)
	}

	if smallRes.Setup.CacheBytes != small.CacheBytes {
		t.Fatalf("cache size not disclosed: got %d", smallRes.Setup.CacheBytes)
	}
	// Both runs measured read amplification (they read), so neither is the not-measured
	// sentinel.
	if smallRes.Amplification.Read < 0 || largeRes.Amplification.Read < 0 {
		t.Fatalf("read amplification not measured: small %v large %v",
			smallRes.Amplification.Read, largeRes.Amplification.Read)
	}
	if smallRes.Amplification.Read <= largeRes.Amplification.Read {
		t.Fatalf("out-of-cache read amp %.3f should exceed in-cache %.3f",
			smallRes.Amplification.Read, largeRes.Amplification.Read)
	}
}
