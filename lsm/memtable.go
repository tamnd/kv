package lsm

import (
	"sync"

	"github.com/tamnd/kv/format"
)

// memtable is one in-memory sorted run: the active write target of the LSM core
// (spec 06 §2). Writes land here first, as pure in-memory inserts, because
// durability was already secured by the WAL commit above the engine seam; that is
// where the LSM's low write latency comes from. A memtable is sealed when it
// reaches its size cap and later flushed to an on-disk L0 segment; this slice
// builds the memtable and its read path, and leaves sealing and flush to the
// segment slices that follow.
//
// Alongside the skip list the memtable carries the live range-delete intervals it
// has seen, so a read can fold range deletes exactly as the B-tree core and the
// oracle do (spec 11 §4). A range-delete marker is stored in the skip list as an
// ordinary cell (so it shadows and sorts like any other) and is additionally
// recorded here as an interval.
type memtable struct {
	sl *skiplist
	// rdmu guards rangeDels against the parallel group apply, where several workers insert
	// into one active memtable at once and a range-delete marker on each would append to the
	// same slice concurrently. The skip list itself is lock-free, so the common point write
	// takes no lock here; only a range-delete marker does. A reader gathering the live set
	// runs under the engine's l.mu, which the apply holds for its duration, so this guards
	// worker against worker, not worker against reader.
	rdmu      sync.Mutex
	rangeDels []format.RangeDel
}

// newMemtable returns an empty memtable with an arena pre-sized to a fraction of
// the eventual cap, so a small memtable does not over-allocate while a large one
// still grows geometrically.
func newMemtable(arenaCap int) *memtable {
	return &memtable{sl: newSkiplist(arenaCap)}
}

// set installs one committed entry. The internal key already carries its version
// and kind, so this is a single ordered insert plus, for a range-delete marker,
// recording the interval for read-time folding.
func (m *memtable) set(internalKey, value []byte) {
	m.sl.insert(internalKey, value)
	if format.KindOf(internalKey) == format.KindRangeBegin {
		m.rdmu.Lock()
		m.rangeDels = append(m.rangeDels, format.RangeDel{
			Lo:      append([]byte(nil), format.UserKey(internalKey)...),
			Hi:      append([]byte(nil), value...),
			Version: format.Version(internalKey),
		})
		m.rdmu.Unlock()
	}
}

// size reports the memtable's in-memory footprint, used to decide when to seal it.
func (m *memtable) size() int { return m.sl.a.size() }

// count reports how many distinct internal-key cells the memtable holds.
func (m *memtable) count() int { return int(m.sl.count.Load()) }

// getGroup calls fn for every cell of userKey's version group, in ascending
// internal-key order (newest version first), seeking the skip list to the group
// rather than scanning from the head. The slices alias the arena and are valid only
// for the duration of the call, so fn copies anything it retains. It stops early if
// fn returns false.
func (m *memtable) getGroup(userKey []byte, fn func(internalKey, value []byte) bool) {
	// The smallest internal key for a user key inverts the largest version to zero
	// and uses the lowest kind, so a forward seek lands on the group's newest cell.
	seekKey := format.EncodeInternalKey(userKey, format.MaxVersion, format.KindDelete)
	for off := m.sl.seek(seekKey); off != 0; off = m.sl.next(off) {
		ik := m.sl.nodeKey(off)
		if format.CompareUser(format.UserKey(ik), userKey) != 0 {
			return
		}
		if !fn(ik, m.sl.nodeValue(off)) {
			return
		}
	}
}

// scan calls fn for every (internalKey, value) cell in ascending internal-key
// order. The slices alias the arena and are valid only for the duration of the
// call, so fn copies anything it retains. It stops early if fn returns false.
func (m *memtable) scan(fn func(internalKey, value []byte) bool) {
	for off := m.sl.first(); off != 0; off = m.sl.next(off) {
		if !fn(m.sl.nodeKey(off), m.sl.nodeValue(off)) {
			return
		}
	}
}
