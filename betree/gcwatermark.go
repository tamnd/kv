package betree

// This file is the fourth slice of M5.3, the split OLTP/OLAP garbage-collection watermark (doc 05
// section "Watermark GC without one global high-watermark"). The commit-timestamp assigner and the
// read frontier handle when a commit becomes visible; this handles when a dead version becomes
// reclaimable. They are two ends of the same MVCC machinery: one decides what a reader can see, the
// other decides what the collector can drop, and both have to avoid the single global coordination
// point the shipped oracle uses. It is built alongside the shipped engine, off the live path, and
// nothing references it yet.
//
// The problem with one watermark. A key overwritten ten times leaves nine dead versions that have
// to be reclaimed. The textbook design keeps one global low-watermark, the oldest read timestamp any
// open transaction holds, and reclaims any version superseded at or before it, because no reader can
// still need a version every reader has already advanced past. That single watermark stalls behind
// the one oldest reader: a long analytical scan holding a read timestamp from an hour ago pins every
// version newer than that hour against GC across the whole keyspace, even though the OLTP traffic has
// moved a billion versions past it. The shipped engine's single watermark has exactly this stall.
//
// The split. Following the eager-pruning lineage (the HyPer/Umbra split-watermark MVCC GC work,
// Böttcher et al.), there are two watermarks rather than one, and the long readers are made to pin
// narrowly instead of globally.
//
//   - The OLTP watermark is the oldest read timestamp among the short, latency-sensitive
//     transactions. Because they are short it advances quickly, and a dead version superseded at or
//     before it is reclaimable as far as OLTP traffic is concerned: every short reader has already
//     advanced past the superseding write, so none of them needs the old version. When there are no
//     short readers at all, nothing on the OLTP side constrains GC, so the watermark is the maximum
//     timestamp and every dead version clears the OLTP gate.
//
//   - The OLAP readers, the long analytical scans, are tagged as such when they begin and carry the
//     key range they read. They do not fold into the OLTP minimum; instead each one pins old versions
//     only inside its own range, and only the versions it actually still needs (those superseded by a
//     write the scan has not advanced to). A scan over [a, b) pins old versions in [a, b) and nowhere
//     else, for only as long as it runs.
//
// The reclaim rule. A dead version of a key, superseded by a write that committed at succTs, is
// reclaimable when succTs is at or below the OLTP watermark (so every short reader sees the
// superseding write) and no open OLAP reader pins it (no long scan whose range covers the key still
// sits below succTs, which would mean the scan has not advanced to the superseding write and so still
// needs this version or an older one in its range). The common case, all readers short, reclaims
// aggressively and never stalls behind a long reader because there are none in the OLTP set; a long
// scan freezes only the slice of versions its range touches, for only as long as it runs, which is
// the irreducible cost of giving it a consistent snapshot and nothing more.
//
// The single-node honest frame, the same one the assigner and the read frontier carry. This registry
// is one per-node structure that readers register and release against under one mutex, and the GC
// scan consults it; on a single unsharded node it is a coordination point, cheap relative to the work
// it gates but not free. The decentralization here is not in removing the structure, it is in what it
// computes: per-range pins instead of one global minimum, so a long reader no longer serializes the
// whole version store behind itself. Per-shard registries, where disjoint shards never share even
// this structure, arrive with the logical sharding of M7.

import (
	"bytes"
	"sync"
)

// maxHLCTime is the sentinel the OLTP watermark takes when no short reader is open: nothing on the
// OLTP side constrains GC, so every real commit timestamp is at or below it and clears the gate.
const maxHLCTime = hlcTime(^uint64(0))

// readerClass distinguishes the two reader populations the split watermark tracks: short
// latency-sensitive transactions that fold into the fast-advancing OLTP minimum, and long analytical
// scans that pin per range instead.
type readerClass uint8

const (
	classOLTP readerClass = iota
	classOLAP
)

// keyRange is the half-open span [lo, hi) an OLAP reader scans and therefore pins old versions
// within. A nil lo is unbounded below (the start of the keyspace) and a nil hi is unbounded above
// (the end), so the zero value is the whole keyspace, which is the correct conservative default for a
// scan that did not declare a narrower range.
type keyRange struct {
	lo, hi []byte
}

// contains reports whether key falls in the half-open range, with nil bounds read as unbounded.
func (r keyRange) contains(key []byte) bool {
	if r.lo != nil && bytes.Compare(key, r.lo) < 0 {
		return false
	}
	if r.hi != nil && bytes.Compare(key, r.hi) >= 0 {
		return false
	}
	return true
}

// gcReader is one registered open transaction: its class, the read timestamp it holds its snapshot
// at, and, for an OLAP reader, the range it pins. The range field is unused for an OLTP reader.
type gcReader struct {
	class readerClass
	rt    hlcTime
	rng   keyRange
}

// gcWatermark is the split-watermark reclamation registry. It holds the open readers and answers, for
// a dead version, whether the collector may drop it. It is safe for concurrent use under its own
// mutex: readers register and release as transactions begin and end, and the GC scan queries
// reclaimability, all serialized on the one lock.
type gcWatermark struct {
	mu      sync.Mutex
	readers map[uint64]*gcReader
	nextID  uint64
}

// newGCWatermark builds an empty registry. With no readers open, the OLTP watermark is the maximum
// and nothing is pinned, so every dead version is reclaimable, which is the correct idle state.
func newGCWatermark() *gcWatermark {
	return &gcWatermark{readers: make(map[uint64]*gcReader)}
}

// registerOLTP records a short transaction holding read timestamp rt and returns its handle. While it
// is open it holds the OLTP watermark down to at most rt, so no version superseded after rt is
// reclaimed out from under it.
func (g *gcWatermark) registerOLTP(rt hlcTime) uint64 {
	return g.register(&gcReader{class: classOLTP, rt: rt})
}

// registerOLAP records a long analytical scan holding read timestamp rt over the half-open range
// [lo, hi) and returns its handle. It does not pull the OLTP watermark down; it pins old versions
// only within its range, and only those it has not advanced past. A nil lo or hi is unbounded on that
// side.
func (g *gcWatermark) registerOLAP(rt hlcTime, lo, hi []byte) uint64 {
	return g.register(&gcReader{class: classOLAP, rt: rt, rng: keyRange{lo: lo, hi: hi}})
}

func (g *gcWatermark) register(rd *gcReader) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nextID++
	id := g.nextID
	g.readers[id] = rd
	return id
}

// release drops a reader's registration when its transaction ends, removing its hold on the OLTP
// watermark or its OLAP range pin. After it returns, versions the reader was pinning become
// reclaimable, which is how a long scan freezes its range for only as long as it runs.
func (g *gcWatermark) release(id uint64) {
	g.mu.Lock()
	delete(g.readers, id)
	g.mu.Unlock()
}

// oltpWatermark returns the oldest read timestamp among the open OLTP readers, or maxHLCTime if there
// are none. A dead version superseded at or before this is past every short reader.
func (g *gcWatermark) oltpWatermark() hlcTime {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.oltpWatermarkLocked()
}

func (g *gcWatermark) oltpWatermarkLocked() hlcTime {
	w := maxHLCTime
	for _, rd := range g.readers {
		if rd.class == classOLTP && rd.rt < w {
			w = rd.rt
		}
	}
	return w
}

// reclaimable reports whether a dead version of key, superseded by a write that committed at succTs,
// may be collected. It is reclaimable when succTs is at or below the OLTP watermark (every short
// reader has advanced past the superseding write) and no open OLAP reader pins it (no long scan whose
// range covers key still sits below succTs and so might still need this version). Both halves are
// evaluated under one lock so the answer is consistent with a single registry state.
func (g *gcWatermark) reclaimable(key []byte, succTs hlcTime) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if succTs > g.oltpWatermarkLocked() {
		return false
	}
	for _, rd := range g.readers {
		if rd.class == classOLAP && rd.rt < succTs && rd.rng.contains(key) {
			return false
		}
	}
	return true
}

// pinnedByOLAP reports whether any open OLAP reader pins a dead version of key superseded at succTs:
// a long scan whose range covers key and whose read timestamp is below succTs, so it has not advanced
// to the superseding write. It is the OLAP half of reclaimable on its own, for tests and diagnostics.
func (g *gcWatermark) pinnedByOLAP(key []byte, succTs hlcTime) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, rd := range g.readers {
		if rd.class == classOLAP && rd.rt < succTs && rd.rng.contains(key) {
			return true
		}
	}
	return false
}
