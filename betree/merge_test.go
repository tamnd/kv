package betree

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

// This file gates the M7.2 cross-shard ordered merge (merge.go). The central property is the one doc
// 08 names for the milestone: the merged order matches a single-domain scan of the same data. The test
// shape is to take a single sorted view (the oracle a single-shard engine would return), partition it
// across shards by the real partition function, merge the per-shard views back, and assert the result
// is the oracle key for key and value for value. A correct merge cannot reorder, drop, or duplicate a
// key; a broken heap would do one of those and the equality fails.

// makeResolved builds a sorted, distinct []resolved from a set of key/value pairs, the single-domain
// view an unsharded scan produces. Duplicate keys keep the last value, matching last-write-wins
// resolution, so the oracle has one entry per key.
func makeResolved(pairs map[string]string) []resolved {
	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]resolved, 0, len(keys))
	for _, k := range keys {
		out = append(out, resolved{uk: []byte(k), val: []byte(pairs[k])})
	}
	return out
}

// partitionView splits a sorted single-domain view into one sorted sub-view per shard, routing each
// entry by the partitioner. Iterating the sorted view in order keeps every shard sub-view sorted, which
// is the precondition the merge relies on. This is the read-side analogue of how the sharded reader
// will gather: each shard resolves its own portion of the range in order.
func partitionView(view []resolved, p partitioner) [][]resolved {
	views := make([][]resolved, p.shards())
	for _, r := range view {
		s := p.route(r.uk)
		views[s] = append(views[s], r)
	}
	return views
}

// assertResolvedEqual fails if got is not want, entry for entry, comparing both user key and value.
func assertResolvedEqual(t *testing.T, got, want []resolved) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("merged length %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].uk, want[i].uk) {
			t.Fatalf("entry %d key %q, want %q", i, got[i].uk, want[i].uk)
		}
		if !bytes.Equal(got[i].val, want[i].val) {
			t.Fatalf("entry %d (key %q) value %q, want %q", i, got[i].uk, got[i].val, want[i].val)
		}
	}
}

// TestMergeEqualsSingleDomainHash is the gate under hash partitioning: the merged order reproduces the
// single-domain sorted view. Hash partitioning scatters adjacent keys across shards, so the merge does
// real interleaving work here, which is exactly the case the heap has to get right.
func TestMergeEqualsSingleDomainHash(t *testing.T) {
	pairs := make(map[string]string)
	for i := 0; i < 5000; i++ {
		pairs[fmt.Sprintf("key-%05d", i)] = fmt.Sprintf("val-%d", i*7)
	}
	oracle := makeResolved(pairs)
	for _, n := range []int{1, 2, 4, 8, 16, 32} {
		views := partitionView(oracle, newHashPartitioner(n))
		merged := mergeShardViews(views)
		assertResolvedEqual(t, merged, oracle)
	}
}

// TestMergeEqualsSingleDomainRange is the same gate under range partitioning. Range partitioning keeps
// contiguous keys together, so each shard view is a contiguous slice of the oracle and the merge mostly
// concatenates, but the boundaries between bands still have to splice in order.
func TestMergeEqualsSingleDomainRange(t *testing.T) {
	pairs := make(map[string]string)
	for c := byte('a'); c <= byte('z'); c++ {
		for i := 0; i < 200; i++ {
			pairs[fmt.Sprintf("%c%04d", c, i)] = fmt.Sprintf("v-%c-%d", c, i)
		}
	}
	oracle := makeResolved(pairs)
	splits := [][]byte{[]byte("f"), []byte("k"), []byte("p"), []byte("u")}
	views := partitionView(oracle, newRangePartitioner(splits))
	merged := mergeShardViews(views)
	assertResolvedEqual(t, merged, oracle)
}

// TestMergeReverseViaCursor checks that the merged view, walked by the existing reverse cursor, yields
// descending order, so the sharded reader gets reverse iteration for free over the merged order.
func TestMergeReverseViaCursor(t *testing.T) {
	pairs := make(map[string]string)
	for i := 0; i < 1000; i++ {
		pairs[fmt.Sprintf("k%04d", i)] = fmt.Sprintf("%d", i)
	}
	oracle := makeResolved(pairs)
	merged := mergeShardViews(partitionView(oracle, newHashPartitioner(8)))

	c := newViewCursor(merged, true)
	var got []string
	for ok := c.First(); ok; ok = c.Next() {
		got = append(got, string(c.Key()))
	}
	if len(got) != len(oracle) {
		t.Fatalf("reverse walk emitted %d keys, want %d", len(got), len(oracle))
	}
	for i := range got {
		want := string(oracle[len(oracle)-1-i].uk)
		if got[i] != want {
			t.Fatalf("reverse position %d key %q, want %q", i, got[i], want)
		}
	}
}

func TestMergeEdgeCases(t *testing.T) {
	// No views, all-empty views, and a single view all degenerate cleanly.
	if got := mergeShardViews(nil); got != nil {
		t.Fatalf("merge of nil views = %v, want nil", got)
	}
	if got := mergeShardViews([][]resolved{nil, {}, nil}); got != nil {
		t.Fatalf("merge of empty views = %v, want nil", got)
	}
	single := []resolved{{uk: []byte("a"), val: []byte("1")}, {uk: []byte("b"), val: []byte("2")}}
	assertResolvedEqual(t, mergeShardViews([][]resolved{single}), single)

	// One shard holds everything (the hot-key funnel degenerate), others empty: the merge is the one
	// non-empty view unchanged.
	full := makeResolved(map[string]string{"x": "1", "y": "2", "z": "3"})
	assertResolvedEqual(t, mergeShardViews([][]resolved{nil, full, nil, nil}), full)
}

// TestMergeInterleavesValuesNotJustKeys plants a different value per key and checks the merge carries
// each key's own value, not a neighbor's: a heap that emitted the right key order but read the value
// from the wrong view would pass a keys-only check and fail this one.
func TestMergeInterleavesValuesNotJustKeys(t *testing.T) {
	pairs := make(map[string]string)
	for i := 0; i < 3000; i++ {
		pairs[fmt.Sprintf("key-%05d", i)] = fmt.Sprintf("payload-for-%05d", i)
	}
	oracle := makeResolved(pairs)
	merged := mergeShardViews(partitionView(oracle, newHashPartitioner(7)))
	assertResolvedEqual(t, merged, oracle)
	for _, r := range merged {
		want := "payload-for-" + string(r.uk[len("key-"):])
		if string(r.val) != want {
			t.Fatalf("key %q carried value %q, want %q", r.uk, r.val, want)
		}
	}
}

// FuzzMergeEqualsSorted programs a random key/value set and shard count from the corpus bytes, builds
// the sorted oracle, partitions it by the hash router, merges, and asserts the merge reproduces the
// oracle. It explores key collisions, value lengths, and shard counts the table tests fix.
func FuzzMergeEqualsSorted(f *testing.F) {
	f.Add([]byte("abc\x00def\x00ghi"), uint8(4))
	f.Add([]byte(""), uint8(8))
	f.Fuzz(func(t *testing.T, raw []byte, shardByte uint8) {
		n := int(shardByte%32) + 1
		// Build a key/value set from the raw bytes: split on a separator into tokens, alternate
		// key/value. Keep it bounded so a giant input cannot wedge a worker.
		if len(raw) > 4096 {
			raw = raw[:4096]
		}
		tokens := bytes.Split(raw, []byte{0x00})
		pairs := make(map[string]string)
		for i := 0; i+1 < len(tokens); i += 2 {
			if len(tokens[i]) == 0 {
				continue
			}
			pairs[string(tokens[i])] = string(tokens[i+1])
		}
		oracle := makeResolved(pairs)
		merged := mergeShardViews(partitionView(oracle, newHashPartitioner(n)))
		assertResolvedEqual(t, merged, oracle)
	})
}
