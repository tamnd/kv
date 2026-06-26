package betree

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// This file gates the M7.3 cross-shard commit coordinator (shard.go). It holds the coordinator to the
// two properties doc 08 names for the milestone: cross-shard atomicity (a reader sees all of a
// transaction spanning shards or none of it) and snapshot consistency (a reader at the frontier sees a
// consistent cut of the commit history). The deterministic test and the fuzz compare the coordinator
// against a serial oracle, the LWW replay an unsharded single-domain engine would produce, so any
// reordering, tearing, or visibility slip diverges from the oracle. The concurrent test asserts
// atomicity under the race detector without an oracle, by checking no transaction is ever half-visible.

// txn is one transaction for the test driver: a set of writes that may span shards.
type txn struct {
	writes []shardWrite
}

// serialOracle replays txns in version order (version i+1 for txns[i]) into a last-write-wins map,
// applying deletes, and returns the resolved view at the given snapshot as a sorted slice. This is the
// single-domain answer the sharded coordinator must reproduce.
func serialOracle(txns []txn, snapshot uint64) []resolved {
	state := make(map[string][]byte)
	for i, tx := range txns {
		if uint64(i+1) > snapshot {
			break
		}
		for _, w := range tx.writes {
			if w.del {
				delete(state, string(w.key))
			} else {
				state[string(w.key)] = append([]byte(nil), w.val...)
			}
		}
	}
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]resolved, 0, len(keys))
	for _, k := range keys {
		out = append(out, resolved{uk: []byte(k), val: state[k]})
	}
	return out
}

func assertResolvedEqualShard(t *testing.T, got, want []resolved) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("view length %d, want %d", len(got), len(want))
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

// TestShardCoordSerialMatchesOracle commits a sequence of transactions one at a time, including
// cross-shard transactions, overwrites of the same key from a later transaction, and deletes, and after
// each commit reads at the frontier and compares to the serial oracle of the transactions so far. This
// is the deterministic atomicity and snapshot-consistency check: the sharded view equals the
// single-domain view at every prefix.
func TestShardCoordSerialMatchesOracle(t *testing.T) {
	c := newShardCoord(newHashPartitioner(8))
	var txns []txn

	mk := func(ws ...shardWrite) txn { return txn{writes: ws} }
	set := func(k, v string) shardWrite { return shardWrite{key: []byte(k), val: []byte(v)} }
	del := func(k string) shardWrite { return shardWrite{key: []byte(k), del: true} }

	program := []txn{
		mk(set("apple", "1"), set("banana", "2"), set("cherry", "3")), // spans shards by hashing
		mk(set("apple", "10")), // overwrite
		mk(set("date", "4"), set("elder", "5")),
		mk(del("banana")),                                         // delete one
		mk(set("banana", "22"), set("fig", "6")),                  // resurrect banana, add fig
		mk(del("nonexistent")),                                    // delete of an absent key
		mk(set("apple", "100"), del("cherry"), set("grape", "7")), // cross-shard mix of set and delete
	}

	for _, tx := range program {
		c.Commit(tx.writes)
		txns = append(txns, tx)
		snap := c.ReadFrontier()
		if snap != uint64(len(txns)) {
			t.Fatalf("after %d serial commits frontier is %d, want %d", len(txns), snap, len(txns))
		}
		got := c.Read(snap, nil, nil)
		want := serialOracle(txns, snap)
		assertResolvedEqualShard(t, got, want)
	}

	// A bounded read matches the oracle clipped to the same bound.
	full := serialOracle(txns, c.ReadFrontier())
	lower, upper := []byte("b"), []byte("f")
	gotBounded := c.Read(c.ReadFrontier(), lower, upper)
	var wantBounded []resolved
	for _, r := range full {
		if bytes.Compare(r.uk, lower) >= 0 && bytes.Compare(r.uk, upper) < 0 {
			wantBounded = append(wantBounded, r)
		}
	}
	assertResolvedEqualShard(t, gotBounded, wantBounded)
}

// TestShardCoordPastSnapshotStable checks snapshot consistency over time: a snapshot taken at an early
// frontier keeps resolving to the same view even as later commits land, because a read at version V
// sees only transactions with version <= V.
func TestShardCoordPastSnapshotStable(t *testing.T) {
	c := newShardCoord(newHashPartitioner(4))
	for i := 0; i < 50; i++ {
		c.Commit([]shardWrite{{key: []byte(fmt.Sprintf("k%03d", i)), val: []byte(strconv.Itoa(i))}})
	}
	mid := c.ReadFrontier()
	midView := c.Read(mid, nil, nil)
	for i := 50; i < 100; i++ {
		c.Commit([]shardWrite{{key: []byte(fmt.Sprintf("k%03d", i)), val: []byte(strconv.Itoa(i))}})
	}
	// The old snapshot still resolves to exactly the first 50 keys.
	again := c.Read(mid, nil, nil)
	assertResolvedEqualShard(t, again, midView)
	if len(again) != 50 {
		t.Fatalf("old snapshot view has %d keys, want 50", len(again))
	}
}

// TestShardCoordConcurrentAtomicity is the concurrent cross-shard atomicity gate. Many committers run
// transactions whose keys are globally unique (so no transaction's write is ever hidden by a later
// overwrite) and which span multiple shards. Each write's value encodes its transaction id and the
// transaction's total write count. Readers snapshot at the frontier and group the visible writes by
// transaction id; a correct coordinator never shows a transaction half-applied, so every transaction id
// that appears at all must appear with its full write count. A torn cross-shard commit would show a
// partial count and fail. The reader also asserts the merged view is globally sorted.
func TestShardCoordConcurrentAtomicity(t *testing.T) {
	const shards = 8
	const workers = 8
	const txnsPerWorker = 400
	const writesPerTxn = 5

	c := newShardCoord(newHashPartitioner(shards))
	var nextID atomic.Uint64
	var crossShard atomic.Uint64 // count of genuinely multi-shard transactions, to prove the path ran
	part := newHashPartitioner(shards)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < txnsPerWorker; i++ {
				id := nextID.Add(1)
				writes := make([]shardWrite, writesPerTxn)
				touched := make(map[int]bool)
				for k := 0; k < writesPerTxn; k++ {
					key := []byte(fmt.Sprintf("t%d-k%d", id, k)) // globally unique per (txn, k)
					writes[k] = shardWrite{key: key, val: []byte(fmt.Sprintf("%d#%d", id, writesPerTxn))}
					touched[part.route(key)] = true
				}
				if len(touched) >= 2 {
					crossShard.Add(1)
				}
				c.Commit(writes)
			}
		}(w)
	}

	// Reader goroutines run concurrently with the committers, repeatedly checking atomicity.
	var readers sync.WaitGroup
	stop := make(chan struct{})
	checkView := func() {
		snap := c.ReadFrontier()
		view := c.Read(snap, nil, nil)
		// Global sort order.
		for i := 1; i < len(view); i++ {
			if bytes.Compare(view[i-1].uk, view[i].uk) >= 0 {
				t.Errorf("merged view not strictly ascending at %d: %q then %q", i, view[i-1].uk, view[i].uk)
				return
			}
		}
		// Atomicity: every transaction id present appears with its full write count.
		counts := make(map[string]int)
		totals := make(map[string]int)
		for _, r := range view {
			parts := bytes.SplitN(r.val, []byte("#"), 2)
			if len(parts) != 2 {
				t.Errorf("malformed value %q", r.val)
				return
			}
			id := string(parts[0])
			total, _ := strconv.Atoi(string(parts[1]))
			counts[id]++
			totals[id] = total
		}
		for id, n := range counts {
			if n != totals[id] {
				t.Errorf("transaction %s half-visible: %d of %d writes", id, n, totals[id])
				return
			}
		}
	}
	for r := 0; r < 3; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					checkView()
				}
			}
		}()
	}

	wg.Wait()
	close(stop)
	readers.Wait()

	// Final check after every commit is visible.
	checkView()
	if crossShard.Load() == 0 {
		t.Fatal("no cross-shard transaction occurred; the cross-shard path was not exercised")
	}
	total := uint64(workers * txnsPerWorker)
	if c.ReadFrontier() != total {
		t.Fatalf("frontier %d after all commits, want %d", c.ReadFrontier(), total)
	}
}

// FuzzShardCoord programs a transaction sequence from the corpus bytes, commits it serially through the
// coordinator, and asserts the resolved view matches the serial oracle at the final frontier and at a
// mid snapshot. It explores key collisions, cross-shard footprints, deletes, and shard counts.
func FuzzShardCoord(f *testing.F) {
	f.Add([]byte("a1b2c3"), uint8(4))
	f.Add([]byte(""), uint8(1))
	f.Fuzz(func(t *testing.T, raw []byte, shardByte uint8) {
		if len(raw) > 2048 {
			raw = raw[:2048]
		}
		n := int(shardByte%16) + 1
		c := newShardCoord(newHashPartitioner(n))

		// Decode the raw bytes into transactions: each byte pair is one write, the first byte choosing a
		// key from a small alphabet (so collisions and overwrites happen), the second its value or a
		// delete. A zero second byte ends the current transaction.
		var txns []txn
		var cur []shardWrite
		flush := func() {
			if len(cur) > 0 {
				txns = append(txns, txn{writes: cur})
				cur = nil
			}
		}
		for i := 0; i+1 < len(raw); i += 2 {
			key := []byte{'k', raw[i] % 16} // 16 distinct keys
			if raw[i+1] == 0 {
				flush()
				continue
			}
			if raw[i+1]%5 == 0 {
				cur = append(cur, shardWrite{key: key, del: true})
			} else {
				cur = append(cur, shardWrite{key: key, val: []byte{raw[i+1]}})
			}
		}
		flush()

		for _, tx := range txns {
			c.Commit(tx.writes)
		}
		if len(txns) == 0 {
			return
		}
		snap := c.ReadFrontier()
		assertResolvedEqualShard(t, c.Read(snap, nil, nil), serialOracle(txns, snap))
		mid := snap / 2
		assertResolvedEqualShard(t, c.Read(mid, nil, nil), serialOracle(txns, mid))
	})
}
