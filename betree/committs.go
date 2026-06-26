package betree

// This file is the second slice of M5.3, the decentralized commit-timestamp assigner (doc 05
// section 3, decision D8), built on the hybrid logical clock the first slice landed. It is the
// thing that actually replaces the global oracle's commit-version counter: instead of a single
// shared counter every committer increments under one mutex, a committer derives its commit
// timestamp from the timestamps it observed on the keys in its own conflict footprint, by the
// max-plus-one rule. It is built alongside the shipped oracle (db/oracle.go) and off the live
// commit path until the M8 flip; the btree and lsm cores keep assigning versions from the
// oracle, and nothing references this assigner yet.
//
// The rule. A committing transaction's commit timestamp is one past the highest timestamp it
// observed on any key it read or wrote. It then stamps the keys it wrote with that timestamp.
// Two transactions on disjoint keys observe disjoint footprints and impose no order on each
// other; two transactions that share a key read each other's stamp through that key, and the
// max-plus-one rule threads them into a consistent relative order. The order over all committed
// transactions emerges from these pairwise constraints, never materialized in one place, which
// is what removes the oracle's global counter.
//
// How max-plus-one is computed. The footprint's highest observed timestamp is fed through the
// HLC's Update once. Update returns a timestamp strictly greater than both the clock's prior
// value and the value fed in, so the result dominates every observed stamp in the footprint
// (the highest, hence all of them) and every timestamp the clock issued before. Feeding the
// single running maximum is equivalent to feeding every observed stamp one by one, because
// Update is monotone in its argument and the result only needs to exceed the maximum.
//
// Why writers serialize per key, and only per key. Two transactions that both write the same key
// must order against each other, so the second has to observe the first's stamp. If both merely
// loaded the key's stamp lock-free, computed timestamps, and then stored, they could both read
// the pre-commit stamp and miss each other, landing two unordered commits on one key. The OLC
// protocol prevents this in the tree by making a writer take the per-record write lock before it
// touches the record, so two writers to one key serialize on that lock and the second observes
// the first's published stamp. This slice models that with a per-key write lock: a committer
// locks the keys it writes (in sorted order, so overlapping write sets cannot deadlock), and
// observes-and-stamps under those locks. The lock is per key, never global, so disjoint writers
// never contend, which is the whole point. Readers in the footprint are still observed lock-free;
// catching a conflict a lock-free read missed is the separate job of read-set validation, layered
// on at integration the same way the shipped serializable path validates its read set.
//
// What this is not. This assigns and orders commit timestamps; it does not by itself decide
// whether a transaction aborts. First-committer-wins write-conflict detection and serializable
// read-set validation are a separate concern the integration layer runs (the oracle does today),
// and they compose with this: the commit timestamp orders the transactions that do commit, and
// validation rejects the ones that must not. Keeping the two apart is what lets this slice be
// proved on its own. The single-node honest frame: the HLC word is still one word every committer
// on this node CASes, so on one unsharded node this trades the oracle's mutex (and its
// under-lock conflict-record scan) for a lock-free CAS and per-key locks, already cheaper but not
// contention-free; the full decentralization, where each shard owns its own clock and stamp space
// so disjoint committers never share even the clock word, arrives with the logical sharding of M7.

import (
	"sort"
	"sync"
	"sync/atomic"
)

// keyStamp is the per-key commit-timestamp cell: the timestamp of the last transaction to write
// the key, plus the write lock that serializes writers to it. The timestamp is atomic so a
// footprint read can load it lock-free; the mutex is the per-key write lock that models the OLC
// per-record write lock, taken by a committer before it observes-and-stamps a key it writes. The
// zero value is a valid never-written key at timestamp zero.
type keyStamp struct {
	mu sync.Mutex
	ts atomic.Uint64
}

// stampTable is the pre-integration home of the per-key commit timestamps. In the integrated
// engine each key's commit timestamp lives on its record in the tree (stamped into the message
// the writer applies, observed through the OLC version word during a descent); until that
// integration lands, the stamps live here, keyed by the user key, exactly as olc.go's
// versionTable is the pre-arena home of the per-node version words. It is read-mostly under
// steady traffic (every footprint observation is a Load, a new key is the only write), so it uses
// sync.Map.
type stampTable struct {
	m sync.Map // string(key) -> *keyStamp
}

// of returns the stamp cell for a key, creating it on first touch, so every committer that
// touches the key couples on the one cell.
func (t *stampTable) of(key []byte) *keyStamp { return t.ofString(string(key)) }

// ofString is of for a key already materialized as a string, so the lock path does not
// re-allocate the map key for every write.
func (t *stampTable) ofString(key string) *keyStamp {
	if v, ok := t.m.Load(key); ok {
		return v.(*keyStamp)
	}
	v, _ := t.m.LoadOrStore(key, &keyStamp{})
	return v.(*keyStamp)
}

// assigner derives decentralized commit timestamps from a per-node hybrid logical clock and the
// per-key stamp table. It is safe for concurrent use: the clock is lock-free and the stamp table
// is per-key locked, so two committers coordinate only through keys they actually share.
type assigner struct {
	clock  *hlc
	stamps *stampTable
}

// newAssigner builds an assigner over a clock. A nil clock gets a fresh system-clock HLC, so a
// caller that just wants the default behavior need not build one.
func newAssigner(clock *hlc) *assigner {
	if clock == nil {
		clock = newHLC(nil)
	}
	return &assigner{clock: clock, stamps: &stampTable{}}
}

// Commit assigns a commit timestamp to a transaction that read the keys in reads and writes the
// keys in writes, stamps the written keys with it, and returns it. The timestamp is one past the
// highest timestamp observed over the whole footprint (the written keys, locked so writers
// serialize, and the read keys, observed lock-free), so it dominates every transaction this one
// conflicts with. A transaction with no writes does not stamp anything and only needs a read
// timestamp; callers take that from readTimestamp instead.
func (a *assigner) Commit(reads, writes [][]byte) hlcTime {
	locks := a.lockWrites(writes)
	defer func() {
		// Unlock in reverse so the release order mirrors the acquire order, which keeps the lock
		// discipline easy to reason about even though any order is correct once we hold them all.
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].mu.Unlock()
		}
	}()

	// The highest timestamp anywhere in the footprint. Written keys are read under their write
	// locks, so no concurrent writer can change them between this observation and the stamp below.
	var footprint hlcTime
	for _, ks := range locks {
		if t := hlcTime(ks.ts.Load()); t > footprint {
			footprint = t
		}
	}
	for _, k := range reads {
		if t := hlcTime(a.stamps.of(k).ts.Load()); t > footprint {
			footprint = t
		}
	}

	// max-plus-one: one past the highest observed, and past everything the clock issued before.
	commitTs := a.clock.Update(footprint)

	// Stamp every written key. commitTs strictly exceeds footprint, which is at least each locked
	// key's current stamp, so the store is a monotone advance; the write lock makes it race-free
	// against another writer, and the atomic store publishes it to lock-free footprint readers.
	for _, ks := range locks {
		ks.ts.Store(uint64(commitTs))
	}
	return commitTs
}

// readTimestamp returns a snapshot read timestamp: a fresh clock value at or above every commit
// timestamp issued so far, so the reader's snapshot ("every commit at or below this") is a real
// point in the commit order and never arbitrarily stale. It does not touch the stamp table; a
// read does not stamp anything.
func (a *assigner) readTimestamp() hlcTime { return a.clock.Now() }

// lockWrites takes the per-key write lock on each distinct written key, in sorted key order. The
// sort gives a global lock-acquisition order, so two committers with overlapping write sets
// acquire the shared keys in the same order and cannot deadlock. Duplicate keys in writes are
// collapsed so a key is locked once. The returned cells are held locked; the caller releases
// them.
func (a *assigner) lockWrites(writes [][]byte) []*keyStamp {
	if len(writes) == 0 {
		return nil
	}
	keys := make([]string, 0, len(writes))
	for _, w := range writes {
		keys = append(keys, string(w))
	}
	sort.Strings(keys)

	locks := make([]*keyStamp, 0, len(keys))
	for i, k := range keys {
		if i > 0 && k == keys[i-1] {
			continue // a key written more than once is locked and stamped once
		}
		ks := a.stamps.ofString(k)
		ks.mu.Lock()
		locks = append(locks, ks)
	}
	return locks
}

// stampOf reports the current commit timestamp recorded for a key, or zero if it was never
// written. It is the lock-free observation a footprint read and a test use; it never blocks.
func (a *assigner) stampOf(key []byte) hlcTime {
	return hlcTime(a.stamps.of(key).ts.Load())
}
