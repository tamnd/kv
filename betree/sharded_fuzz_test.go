package betree

import (
	"math/rand"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// openFuzzSharded opens a sharded core at a chosen page size over a fresh in-memory database,
// partitioned the given way. It mirrors openFuzzTree so the same byte-programmed batch stream drives
// either the single-shard or the sharded core.
func openFuzzSharded(t *testing.T, part partitioner, pageSize int) *Sharded {
	t.Helper()
	p, err := pager.Create(vfs.NewMem(), "shardfuzz.kv", pager.Options{
		PageSize:    pageSize,
		CacheFrames: 32,
		Engine:      format.EngineBeta,
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	s := newSharded(p, part)
	if err := s.Open(&engine.Env{}); err != nil {
		t.Fatalf("open sharded: %v", err)
	}
	return s
}

// fuzzPartitioners is the set of shardings every fuzz program is checked against, so one input
// exercises a single-shard degenerate case, a few hash widths (where adjacent keys scatter and the
// cross-shard merge does real work), and a range partitioner (where the routing is by band and a range
// delete still has to shadow keys across the bands it spans). The range splits sit inside the k%02d
// keyspace programToBatches writes so all three bands get traffic.
var fuzzPartitioners = []struct {
	name string
	make func() partitioner
}{
	{"hash1", func() partitioner { return newHashPartitioner(1) }},
	{"hash3", func() partitioner { return newHashPartitioner(3) }},
	{"hash8", func() partitioner { return newHashPartitioner(8) }},
	{"range", func() partitioner { return newRangePartitioner([][]byte{[]byte("k08"), []byte("k16")}) }},
}

// FuzzShardedConformance drives a byte-programmed mix of sets, deletes, merges, and range deletes
// through the sharded core and the conformance oracle, at a page size and now a sharding the corpus
// explores, so the routing fan-out and the cross-shard merge are checked against the model over flush
// timings and shard counts the fuzz chooses rather than ones the test fixes. It reuses programToBatches,
// so the same corpus that exercises the single-shard buffered path also exercises the sharded one, and
// a divergence the single-shard fuzz would catch as a buffering bug surfaces here as a routing or merge
// bug instead. The seed corpus alone runs under a plain go test, so a regression a past run found stays
// caught without -fuzz.
func FuzzShardedConformance(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0x09, 0x04, 0x05})
	f.Add([]byte{1, 0xff, 0x02, 0x07, 0x13, 0x00, 0x09})
	for seed := int64(1); seed <= 6; seed++ {
		rng := rand.New(rand.NewSource(seed))
		prog := make([]byte, 1+rng.Intn(400))
		for i := range prog {
			prog[i] = byte(rng.Intn(256))
		}
		f.Add(prog)
	}

	f.Fuzz(func(t *testing.T, prog []byte) {
		batches, pageSize := programToBatches(prog)
		if len(batches) == 0 {
			return
		}
		for _, pc := range fuzzPartitioners {
			// Each sharding gets its own copy of the batches: CheckEngine replays the same stream into a
			// fresh oracle per call, and the batches are read-only here, so the slice is shared safely, but
			// a fresh core per sharding keeps one input's flush state from leaking into the next.
			s := openFuzzSharded(t, pc.make(), pageSize)
			if err := engine.CheckEngine(s, batches, concatMerge); err != nil {
				t.Fatalf("sharded stream diverged (%s, page %d): %v", pc.name, pageSize, err)
			}
		}
	})
}

// FuzzShardedReopen drives a byte-programmed stream through the sharded core, flushes and reopens the
// file, and checks every key the final snapshot should hold reads back, so the directory persistence and
// the per-shard root remount are fuzzed over shardings and flush timings the corpus chooses, not just the
// one shape TestShardedReopen fixes. It checks the reopened core against the oracle's final-snapshot
// view, which is the property a reopen must preserve: the same data, routed to the same shards, reachable
// through the directory rather than the single header root.
func FuzzShardedReopen(f *testing.F) {
	f.Add([]byte{0, 0x11, 0x22, 0x05, 0x09, 0x33})
	f.Add([]byte{2, 0xa0, 0x01, 0x40, 0x07, 0x12, 0x00, 0x09, 0x80})
	for seed := int64(1); seed <= 4; seed++ {
		rng := rand.New(rand.NewSource(seed))
		prog := make([]byte, 8+rng.Intn(200))
		for i := range prog {
			prog[i] = byte(rng.Intn(256))
		}
		f.Add(prog)
	}

	f.Fuzz(func(t *testing.T, prog []byte) {
		batches, pageSize := programToBatches(prog)
		if len(batches) == 0 {
			return
		}
		// Use the byte after the page selector to pick a sharding, so the reopen is fuzzed across hash
		// widths and the range partitioner too. A single-shard core would route the directory through the
		// degenerate one-slot path, which is worth covering as much as the multi-shard one.
		pc := fuzzPartitioners[int(prog[0])%len(fuzzPartitioners)]

		fs := vfs.NewMem()
		p, err := pager.Create(fs, "shardreopen.kv", pager.Options{PageSize: pageSize, CacheFrames: 32, Engine: format.EngineBeta})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		s := newSharded(p, pc.make())
		if err := s.Open(&engine.Env{}); err != nil {
			t.Fatalf("open: %v", err)
		}
		s.SetMergeFunc(concatMerge)

		// Build the oracle alongside so the reopen can be checked against the model's final-snapshot view.
		oracle := engine.NewOracle(concatMerge)
		var maxVer uint64
		for _, b := range batches {
			if err := s.Apply(b, b.Version()); err != nil {
				t.Fatalf("apply v%d: %v", b.Version(), err)
			}
			oracle.Apply(b, b.Version())
			if b.Version() > maxVer {
				maxVer = b.Version()
			}
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

		p2, err := pager.Open(fs, "shardreopen.kv", pager.Options{})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer p2.Close()
		s2 := newSharded(p2, nil) // the directory supplies the partitioner on reopen
		if err := s2.Open(&engine.Env{}); err != nil {
			t.Fatalf("reopen open: %v", err)
		}
		s2.SetMergeFunc(concatMerge)

		// The reopened core's final-snapshot scan must equal the oracle's: same keys, same values, in
		// order. A lost root, a misrebuilt partitioner, or a dropped directory split would diverge here.
		snap := engine.Snapshot{Version: maxVer}
		want := oracle.Scan(nil, nil, snap)
		rd, err := s2.NewReader(snap)
		if err != nil {
			t.Fatalf("reader: %v", err)
		}
		defer rd.Close()
		cur, err := rd.NewIter(engine.IterOptions{})
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		var got int
		for ok := cur.First(); ok; ok = cur.Next() {
			if got >= len(want) {
				t.Fatalf("%s: reopened core has more keys than the oracle (%d+ vs %d)", pc.name, got, len(want))
			}
			lv, verr := cur.Value()
			if verr != nil {
				t.Fatalf("value: %v", verr)
			}
			v, verr := lv.Value()
			if verr != nil {
				t.Fatalf("lazy value: %v", verr)
			}
			if string(cur.Key()) != string(want[got].Key) || string(v) != string(want[got].Value) {
				t.Fatalf("%s: key %d after reopen = (%q,%q), want (%q,%q)", pc.name, got, cur.Key(), v, want[got].Key, want[got].Value)
			}
			got++
		}
		cur.Close()
		if got != len(want) {
			t.Fatalf("%s: reopened core has %d keys, oracle has %d", pc.name, got, len(want))
		}
	})
}
