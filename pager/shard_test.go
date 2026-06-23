package pager

import (
	"sync"
	"testing"

	"github.com/tamnd/kv/format"
)

// TestShardCount checks the pool partitioning math: a power-of-two shard count, capped at
// maxShards, that never drops a shard below minFramesPerShard frames, and a single shard
// once the pool is too small to split (the tiny-pool regime the streaming flush depends on).
func TestShardCount(t *testing.T) {
	cases := []struct {
		frames int
		want   int
	}{
		{1, 1},
		{16, 1},
		{31, 1},
		{32, 2},
		{64, 4},
		{128, 8},
		{256, 16},
		{2000, 64},   // capped at maxShards
		{100000, 64}, // still capped
	}
	for _, c := range cases {
		if got := shardCount(c.frames); got != c.want {
			t.Errorf("shardCount(%d) = %d, want %d", c.frames, got, c.want)
		}
		// Whatever the count, every shard must clear the frame floor (unless the whole
		// pool is below it, which collapses to one shard).
		n := shardCount(c.frames)
		if n > 1 && c.frames/n < minFramesPerShard {
			t.Errorf("shardCount(%d)=%d leaves %d frames/shard, below floor %d",
				c.frames, n, c.frames/n, minFramesPerShard)
		}
	}
}

// TestShardFramePartition checks every frame is owned by exactly one shard and the frame
// total matches the requested pool, so the arena is fully and disjointly distributed.
func TestShardFramePartition(t *testing.T) {
	_, p := newTestPager(t, Options{PageSize: 4096, CacheFrames: 200})
	seen := make(map[int]bool)
	total := 0
	for _, sh := range p.shards {
		for _, fr := range sh.frames {
			if seen[fr.slot] {
				t.Fatalf("frame slot %d owned by more than one shard", fr.slot)
			}
			seen[fr.slot] = true
			total++
		}
	}
	if total != 200 {
		t.Fatalf("shards own %d frames total, want 200", total)
	}
}

// TestConcurrentShardedReads hammers the pool from many goroutines reading distinct page
// ranges that spread across shards, all under the race detector. It proves the sharded
// Get/Unpin path is data-race free and that pages read back the bytes they were written.
// A pre-sharding global-lock bug or a shard-routing mistake would surface here as a race
// report or a content mismatch.
func TestConcurrentShardedReads(t *testing.T) {
	_, p := newTestPager(t, Options{PageSize: 4096, CacheFrames: 256, Engine: format.EngineBTree})
	const npages = 2000

	// Lay down npages distinct pages, each stamped with its own pattern.
	nums := make([]uint32, npages)
	for i := 0; i < npages; i++ {
		pgno, fr, err := p.Allocate()
		if err != nil {
			t.Fatalf("allocate %d: %v", i, err)
		}
		writePattern(fr.Data(), pgno)
		p.Unpin(fr, true)
		nums[i] = pgno
	}
	if err := p.Checkpoint(0, 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for round := 0; round < 4; round++ {
				for i := range nums {
					pgno := nums[(i+seed*37)%len(nums)]
					fr, err := p.Get(pgno, Read)
					if err != nil {
						errs <- err
						return
					}
					ok := checkPattern(fr.Data(), pgno)
					p.Unpin(fr, false)
					if !ok {
						errs <- errMismatch(pgno)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

type errMismatch uint32

func (e errMismatch) Error() string {
	return "page content mismatch after concurrent read of page " + itoa(uint32(e))
}

func itoa(v uint32) string {
	if v == 0 {
		return "0"
	}
	var b [10]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
