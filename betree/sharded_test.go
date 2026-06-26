package betree

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// newSharded opens a sharded Bε-tree core over a fresh in-memory database, partitioned the given way.
// It mirrors newTree, so the same conformance batches drive either core.
func newShardedCore(t *testing.T, part partitioner, pageSize int) (*Sharded, *pager.Pager, vfs.FS) {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "sharded.kv", pager.Options{
		PageSize:    pageSize,
		CacheFrames: 64,
		Engine:      format.EngineBeta,
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	s := newSharded(p, part)
	if err := s.Open(&engine.Env{}); err != nil {
		t.Fatalf("open sharded: %v", err)
	}
	return s, p, fs
}

// TestShardedConformanceBasic drives the same small set/delete/merge mix the single-shard core answers
// to through the sharded core, so the routing and the cross-shard merge are proven against the oracle.
func TestShardedConformanceBasic(t *testing.T) {
	for _, n := range []int{1, 2, 4, 7} {
		t.Run(fmt.Sprintf("hash%d", n), func(t *testing.T) {
			s, _, _ := newShardedCore(t, newHashPartitioner(n), 4096)

			var batches []*engine.WriteBatch

			b1 := engine.NewWriteBatch(10)
			b1.Set([]byte("apple"), []byte("red"))
			b1.Set([]byte("banana"), []byte("yellow"))
			b1.Set([]byte("cherry"), []byte("dark"))
			batches = append(batches, b1)

			b2 := engine.NewWriteBatch(20)
			b2.Set([]byte("apple"), []byte("green"))
			b2.Delete([]byte("banana"))
			b2.Merge([]byte("cherry"), []byte("!"))
			batches = append(batches, b2)

			b3 := engine.NewWriteBatch(30)
			b3.Merge([]byte("cherry"), []byte("?"))
			b3.Set([]byte("date"), []byte("brown"))
			batches = append(batches, b3)

			if err := engine.CheckEngine(s, batches, concatMerge); err != nil {
				t.Fatalf("conformance: %v", err)
			}
		})
	}
}

// TestShardedConformanceRangeDelete proves the replicate-to-every-shard rule for range deletes: a
// DeleteRange must shadow keys no matter which shard they were routed to, which only holds if the marker
// reaches every sub-tree.
func TestShardedConformanceRangeDelete(t *testing.T) {
	for _, n := range []int{1, 3, 5} {
		t.Run(fmt.Sprintf("hash%d", n), func(t *testing.T) {
			s, _, _ := newShardedCore(t, newHashPartitioner(n), 4096)

			var batches []*engine.WriteBatch

			b1 := engine.NewWriteBatch(10)
			for _, k := range []string{"a", "b", "c", "d", "e", "f", "g"} {
				b1.Set([]byte(k), []byte("v1-"+k))
			}
			batches = append(batches, b1)

			b2 := engine.NewWriteBatch(20)
			b2.DeleteRange([]byte("b"), []byte("f")) // covers b, c, d, e across whatever shards own them
			batches = append(batches, b2)

			b3 := engine.NewWriteBatch(30)
			b3.Set([]byte("c"), []byte("v3-c")) // newer than the range delete, must survive
			batches = append(batches, b3)

			if err := engine.CheckEngine(s, batches, concatMerge); err != nil {
				t.Fatalf("conformance: %v", err)
			}
		})
	}
}

// TestShardedConformanceRandom is the differential check that matters most: a randomized stream of sets,
// deletes, merges, and range deletes across many versions, driven through the sharded core at several
// shard counts and at a small page size that forces the sub-trees multi-level, so the routing and merge
// are exercised against the oracle over a real paged layout, not just a single tail.
func TestShardedConformanceRandom(t *testing.T) {
	parts := []struct {
		name string
		part partitioner
	}{
		{"hash1", newHashPartitioner(1)},
		{"hash3", newHashPartitioner(3)},
		{"hash8", newHashPartitioner(8)},
		{"range", newRangePartitioner([][]byte{[]byte("k08"), []byte("k16")})},
	}
	for _, pc := range parts {
		t.Run(pc.name, func(t *testing.T) {
			for seed := int64(1); seed <= 6; seed++ {
				s, _, _ := newShardedCore(t, pc.part, 512)
				batches := randomBatches(rand.New(rand.NewSource(seed)))
				if err := engine.CheckEngine(s, batches, concatMerge); err != nil {
					t.Fatalf("conformance (seed %d): %v", seed, err)
				}
			}
		})
	}
}

// TestShardedReopen writes a dense key space through the sharded core, flushes and persists the
// directory, reopens the file, and reads every key back, proving the partitioning and the per-shard
// roots survive a reopen through the directory rather than the single header root. It also checks a key
// reaches the same shard after the reopen (its value is intact), which is the routing-determinism the
// directory exists to guarantee.
func TestShardedReopen(t *testing.T) {
	s, p, fs := newShardedCore(t, newHashPartitioner(4), 512)

	const nkeys = 3000
	b := engine.NewWriteBatch(100)
	for i := 0; i < nkeys; i++ {
		b.Set([]byte(fmt.Sprintf("key%06d", i)), []byte(fmt.Sprintf("val%06d", i)))
	}
	if err := s.Apply(b, 100); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := p.Checkpoint(0, 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: the engine root names the directory page, so Open rebuilds the partitioner and remounts the
	// sub-trees at their recorded roots.
	p2, err := pager.Open(fs, "sharded.kv", pager.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()
	s2 := newSharded(p2, nil) // a reopen ignores the constructor partitioner; the directory supplies it
	if err := s2.Open(&engine.Env{}); err != nil {
		t.Fatalf("reopen sharded: %v", err)
	}
	if s2.part.shards() != 4 {
		t.Fatalf("reopened shard count = %d, want 4", s2.part.shards())
	}

	rd, err := s2.NewReader(engine.Snapshot{Version: 100})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	for i := 0; i < nkeys; i++ {
		k := []byte(fmt.Sprintf("key%06d", i))
		v, err := rd.Get(k)
		if err != nil {
			t.Fatalf("key %q missing after reopen: %v", k, err)
		}
		if want := fmt.Sprintf("val%06d", i); string(v) != want {
			t.Fatalf("key %q = %q, want %q", k, v, want)
		}
	}
}

// TestShardedSubTreesAreDisjoint checks the structural invariant the merge rests on: with more than one
// shard, the sub-trees hold disjoint key sets, every shard gets some of a dense uniform key space, and a
// per-shard reader sees only its own shard's keys. It reaches under the SPI to the sub-trees so a
// routing bug that doubled a key into two shards, which the merge would still order correctly and hide,
// is caught directly.
func TestShardedSubTreesAreDisjoint(t *testing.T) {
	s, _, _ := newShardedCore(t, newHashPartitioner(4), 4096)

	const nkeys = 4000
	b := engine.NewWriteBatch(1)
	for i := 0; i < nkeys; i++ {
		b.Set([]byte(fmt.Sprintf("k%06d", i)), []byte("v"))
	}
	if err := s.Apply(b, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}

	counts := make([]int, len(s.subs))
	total := 0
	for i, sub := range s.subs {
		rd, err := sub.NewReader(engine.Snapshot{Version: 1})
		if err != nil {
			t.Fatalf("sub reader %d: %v", i, err)
		}
		cur, err := rd.NewIter(engine.IterOptions{})
		if err != nil {
			t.Fatalf("sub iter %d: %v", i, err)
		}
		for ok := cur.First(); ok; ok = cur.Next() {
			// Every key this sub-tree holds must route to this shard, or the fan-out misrouted it.
			if got := s.part.route(cur.Key()); got != i {
				t.Fatalf("sub-tree %d holds key %q that routes to shard %d", i, cur.Key(), got)
			}
			counts[i]++
			total++
		}
		cur.Close()
		rd.Close()
	}

	if total != nkeys {
		t.Fatalf("sub-trees hold %d keys total, want %d (a key was dropped or duplicated)", total, nkeys)
	}
	for i, c := range counts {
		if c == 0 {
			t.Fatalf("shard %d got no keys; the hash spread is broken", i)
		}
	}
}

// TestShardedConcurrentReadersFrozenSnapshot stresses the parallel cross-shard gather under reader
// contention: many readers pin version 1 and scan the whole key space in a loop while a writer churns
// higher versions across every shard and periodically flushes, retiring pages a scanning reader may be
// mid-gather on. Each scanning reader fans its gather out to one goroutine per shard, so this drives the
// new concurrent gather path against the optimistic read protocol and epoch reclamation at once: a frozen
// snapshot must keep reading exactly its base values no matter what the writer does. It is the sharded
// analogue of TestConcurrentReadersFrozenSnapshot, which proves the same property for the single-shard
// Tree, and would fault under -race if a gather goroutine read a page the writer freed.
func TestShardedConcurrentReadersFrozenSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency stress in -short")
	}
	s, _, _ := newShardedCore(t, newHashPartitioner(4), 4096)
	s.SetMergeFunc(concatMerge)

	const nkeys = 48
	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("k%03d", i)) }
	// Pad the base so a few churn batches cross the tail budget and the writer rolls real pages over,
	// retiring frames the readers may be gathering rather than resting entirely in the hot tail.
	baseVal := func(i int) []byte { return []byte(fmt.Sprintf("base-%03d-%0300d", i, i)) }

	// Version 1: the frozen snapshot every reader reads at, spread across all four shards by the hash.
	b0 := engine.NewWriteBatch(1)
	for i := 0; i < nkeys; i++ {
		b0.Set(keyOf(i), baseVal(i))
	}
	if err := s.Apply(b0, b0.Version()); err != nil {
		t.Fatalf("seed apply: %v", err)
	}

	var wg sync.WaitGroup
	var writerDone atomic.Bool

	// The writer: a bounded run of higher versions that churns every shard, with periodic flushes that
	// drain the tails onto pages and retire the old ones, then it stops.
	const nbatches = 150
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer writerDone.Store(true)
		rng := rand.New(rand.NewSource(7))
		ver := uint64(1)
		for b := 0; b < nbatches; b++ {
			ver++
			wb := engine.NewWriteBatch(ver)
			used := map[int]bool{}
			for n := 0; n < 16; n++ {
				i := rng.Intn(nkeys)
				if used[i] {
					continue
				}
				used[i] = true
				if rng.Intn(6) == 0 {
					wb.Delete(keyOf(i))
				} else {
					wb.Set(keyOf(i), []byte(fmt.Sprintf("v%d-%03d-%0300d", ver, i, i)))
				}
			}
			if err := s.Apply(wb, wb.Version()); err != nil {
				t.Errorf("writer apply v%d: %v", ver, err)
				return
			}
			if ver%16 == 0 {
				if err := s.Flush(); err != nil {
					t.Errorf("writer flush v%d: %v", ver, err)
					return
				}
			}
		}
	}()

	// The readers: each pins version 1 and reads the whole universe until the writer is done, asserting
	// every key always resolves to its base. Even readers take the point path, odd readers the scanning
	// cursor, so both the routed Get and the parallel cross-shard gather run under contention. Each reader
	// does at least one full pass even if the writer finishes first.
	const nreaders = 6
	for r := 0; r < nreaders; r++ {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			rd, err := s.NewReader(engine.Snapshot{Version: 1})
			if err != nil {
				t.Errorf("reader %d: new reader: %v", r, err)
				return
			}
			defer rd.Close()
			for pass := 0; ; pass++ {
				if r%2 == 0 {
					for i := 0; i < nkeys; i++ {
						got, err := rd.Get(keyOf(i))
						if err != nil {
							t.Errorf("reader %d: get k%03d pass %d: %v", r, i, pass, err)
							return
						}
						if string(got) != string(baseVal(i)) {
							t.Errorf("reader %d: get k%03d pass %d = %q, want base", r, i, pass, got)
							return
						}
					}
				} else {
					cur, err := rd.NewIter(engine.IterOptions{})
					if err != nil {
						t.Errorf("reader %d: iter pass %d: %v", r, pass, err)
						return
					}
					seen := 0
					for ok := cur.First(); ok; ok = cur.Next() {
						lv, verr := cur.Value()
						if verr != nil {
							t.Errorf("reader %d: value pass %d: %v", r, pass, verr)
							cur.Close()
							return
						}
						v, verr := lv.Value()
						if verr != nil {
							t.Errorf("reader %d: lazy value pass %d: %v", r, pass, verr)
							cur.Close()
							return
						}
						i := seen
						if string(cur.Key()) != string(keyOf(i)) || string(v) != string(baseVal(i)) {
							t.Errorf("reader %d: scan pos %d pass %d = (%q,%q), want base", r, seen, pass, cur.Key(), v)
							cur.Close()
							return
						}
						seen++
					}
					cur.Close()
					if seen != nkeys {
						t.Errorf("reader %d: scan pass %d saw %d keys, want %d", r, pass, seen, nkeys)
						return
					}
				}
				if writerDone.Load() && pass > 0 {
					return
				}
			}
		}()
	}

	wg.Wait()
}

// TestShardedPersistDirRewritesInPlace checks that flushing twice does not allocate a second directory
// page: the directory rewrites in place so its page number is stable across flushes, which is what keeps
// repeated checkpoints from orphaning a directory page each time.
func TestShardedPersistDirRewritesInPlace(t *testing.T) {
	s, _, _ := newShardedCore(t, newHashPartitioner(3), 4096)

	b := engine.NewWriteBatch(1)
	b.Set([]byte("a"), []byte("1"))
	if err := s.Apply(b, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("first flush: %v", err)
	}
	first := s.dirPgno
	if first == format.NoPage {
		t.Fatalf("first flush left dirPgno at NoPage")
	}

	b2 := engine.NewWriteBatch(2)
	b2.Set([]byte("b"), []byte("2"))
	if err := s.Apply(b2, 2); err != nil {
		t.Fatalf("apply 2: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if s.dirPgno != first {
		t.Fatalf("second flush moved the directory page from %d to %d", first, s.dirPgno)
	}
}
