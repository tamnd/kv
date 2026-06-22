package lsm

import (
	"sort"
	"sync/atomic"

	"github.com/tamnd/kv/format"
)

// The segment tree is held in an immutable, reference-counted snapshot rather than mutated
// in place (perf/03 R3, the pebble versionSet model). Before this, a read held l.mu.RLock
// across its whole fold, from the memtable gather through every segment Bloom probe, data-page
// read, and value-log dereference, and a compaction install took l.mu.Lock to swap the live
// set and free the inputs' pages. With the background compactor (W4) now merging continuously,
// that put read latency and compaction in constant contention over the one lock.
//
// A version is one immutable snapshot of the levels. A read loads the current version under a
// brief l.mu.RLock (the same lock it already takes to gather the active memtable, which is not
// lock-free against a concurrent Apply), takes a reference, and then releases the lock and
// probes the version's segments with no lock held: the segment I/O, the slow part, no longer
// blocks or is blocked by compaction. A flush or compaction builds a new version by copy-on-
// write and swaps it in under l.mu. Because a reader may still hold an older version after a
// compaction has removed some of its segments from the current one, a segment's pages are not
// freed when the compaction installs but when the last version that names it is released, so a
// reader walking a retired version never reads a page a concurrent allocate has reused.

// version is an immutable snapshot of the on-disk segment tree: levels[0] is L0, the
// overlapping run of flushed memtables, and levels[i>=1] hold key-range-disjoint segments
// (leveled) or stacked runs (the tiered bottom). A read folds every segment regardless of
// level; the level structure exists so compaction can bound read fan-in and reclaim the space
// shadowed versions waste. A version is never mutated after publishVersionLocked installs it;
// the next install builds a fresh one.
type version struct {
	levels [][]*segment

	// refs counts the live holders of this version: one implicit reference while it is the
	// engine's current version, plus one per in-flight reader that loaded it. When refs falls
	// to zero (which can only happen after the version has been retired, since the current
	// version keeps its implicit reference) the version is dropped: every segment it names
	// loses a version reference, and a segment whose last version is gone has its pages freed.
	refs atomic.Int32
}

// levelsLocked returns the current version's levels, the snapshot every under-lock reader and
// the compaction planner walk. The caller holds l.mu (read or write); the returned slices
// belong to an immutable version, so a caller that then releases l.mu must not assume they
// stay current, but their contents never change. The lock-free point read instead holds a
// referenced version directly (acquireVersionLocked) and reads version.levels.
func (l *LSM) levelsLocked() [][]*segment { return l.cur.levels }

// acquireVersionLocked returns the current version with a fresh reference, so the caller may
// release l.mu and still read the version's segments: the reference keeps every page the
// version names off the freelist until the matching releaseVersion, even if a compaction
// retires the version meanwhile. The caller holds l.mu (read is enough); the reference is
// taken before the lock is dropped, so a publish that retires this version cannot free its
// segments before the reference lands.
func (l *LSM) acquireVersionLocked() *version {
	v := l.cur
	v.refs.Add(1)
	return v
}

// releaseVersion drops a reference a reader took with acquireVersionLocked. The common case,
// the still-current version, only decrements an atomic counter and returns, taking no lock: the
// current version keeps its implicit reference, so a reader's release never brings it to zero.
// Only a reader that held a version a compaction has since retired, and is the last to let go,
// takes l.mu to free the segments that version was keeping alive. The caller holds no lock.
func (l *LSM) releaseVersion(v *version) {
	if v.refs.Add(-1) != 0 {
		return
	}
	l.mu.Lock()
	l.dropVersionLocked(v)
	l.mu.Unlock()
}

// releaseVersionLocked is releaseVersion for a caller that already holds l.mu: it drops a
// reference and, if that was the last, frees the version's now-unreferenced segments inline.
// publishVersionLocked uses it to release the implicit reference the retiring version held.
func (l *LSM) releaseVersionLocked(v *version) {
	if v.refs.Add(-1) == 0 {
		l.dropVersionLocked(v)
	}
}

// dropVersionLocked retires a version whose last reference is gone: every segment it named
// loses a version reference, a segment whose last version is now gone has its pages returned
// to the freelist, and the version leaves the live set. A page-free I/O error is stuck on
// flushErr, the same sticky channel a background flush or compaction failure surfaces through,
// since a reader's release has no return path. The caller holds l.mu.
func (l *LSM) dropVersionLocked(v *version) {
	for _, lvl := range v.levels {
		for _, s := range lvl {
			s.vrefs--
			if s.vrefs <= 0 {
				if err := l.freeSegmentPages(s); err != nil && l.flushErr == nil {
					l.flushErr = err
				}
			}
		}
	}
	for i, lv := range l.live {
		if lv == v {
			l.live = append(l.live[:i], l.live[i+1:]...)
			break
		}
	}
}

// publishVersionLocked installs levels as the engine's current segment tree, retiring the
// version it replaces. Each segment in the new version gains a version reference; the retiring
// version's implicit reference is then released, which drops it (and frees the segments only it
// named) once no reader still holds it. The swap is a single pointer store under l.mu, so a
// reader loads either the whole old tree or the whole new one, never a mix. The caller holds
// l.mu, and (because the new version may free the old one's segments inline) flushMu, the
// page-write serializer freeSegmentPages runs under. The new levels slices must be freshly
// owned (cloneLevelsLocked or a recovery-time build), since the version keeps them.
func (l *LSM) publishVersionLocked(levels [][]*segment) {
	nv := &version{levels: levels}
	nv.refs.Store(1) // the implicit reference the current slot holds
	for _, lvl := range levels {
		for _, s := range lvl {
			s.vrefs++
		}
	}
	old := l.cur
	l.cur = nv
	l.live = append(l.live, nv)
	l.releaseVersionLocked(old)
}

// cloneLevelsLocked returns a copy of the current version's level slices for copy-on-write
// mutation: the outer slice and each per-level slice are fresh, the *segment elements shared.
// The caller mutates the copy with addSegment/removeSegments and installs it with
// publishVersionLocked, leaving the current version untouched for any reader still folding it.
// The caller holds l.mu.
func (l *LSM) cloneLevelsLocked() [][]*segment {
	old := l.cur.levels
	nl := make([][]*segment, len(old))
	for i := range old {
		nl[i] = append([]*segment(nil), old[i]...)
	}
	return nl
}

// allLiveSegmentsLocked returns every segment any live version still names, deduplicated. The
// value-log GC marks liveness against this set rather than just the current version, so a
// separated value a lock-free point read holding a retired version may still dereference is
// never reclaimed out from under it: the value's segment stays in that reader's version until
// the reader releases it, keeping the value marked live for as long as it can be read (perf/03
// R3). With no slow reader spanning a compaction, the live set is just the current version, so
// the GC scans exactly what it always did. The caller holds l.mu.
func (l *LSM) allLiveSegmentsLocked() []*segment {
	seen := make(map[*segment]bool)
	var segs []*segment
	for _, v := range l.live {
		for _, lvl := range v.levels {
			for _, s := range lvl {
				if !seen[s] {
					seen[s] = true
					segs = append(segs, s)
				}
			}
		}
	}
	return segs
}

// flattenSegments returns every segment across every level, the flat view the read paths fold
// (resolution depends on the version in each key, not the level a segment sits at, so a read
// need not care about levels). It is applied both to the current version under l.mu and to a
// referenced version a lock-free read holds.
func flattenSegments(levels [][]*segment) []*segment {
	var segs []*segment
	for _, lvl := range levels {
		segs = append(segs, lvl...)
	}
	return segs
}

// addSegment inserts seg into the given level of a copy-on-write level set, growing the outer
// slice as needed and keeping every level below L0 sorted by first key so the non-overlapping
// run reads in order. It mutates and returns levels, which must be a caller-owned copy
// (cloneLevelsLocked or a recovery build), never a published version's slices.
func addSegment(levels [][]*segment, level int, seg *segment) [][]*segment {
	for len(levels) <= level {
		levels = append(levels, nil)
	}
	levels[level] = append(levels[level], seg)
	if level >= 1 {
		sort.Slice(levels[level], func(i, j int) bool {
			return format.CompareUser(levels[level][i].minKey, levels[level][j].minKey) < 0
		})
	}
	return levels
}

// removeSegments drops every segment in set from the given level of a copy-on-write level set.
// It mutates levels in place (reusing the level's backing array), which must be a caller-owned
// copy, never a published version's slices.
func removeSegments(levels [][]*segment, level int, set map[*segment]bool) {
	if level >= len(levels) || len(set) == 0 {
		return
	}
	kept := levels[level][:0]
	for _, s := range levels[level] {
		if !set[s] {
			kept = append(kept, s)
		}
	}
	levels[level] = kept
}
