package betree

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/tamnd/kv/format"
)

// This file is M4.1, the resident learned point index (doc 03 section 2, decision D6): a
// small model of the leaf run that a point read consults before the interior descent, so a
// read can jump near the leaf that holds its key instead of comparing its way down the tree.
// The bare descent is compare-bound and cache-bound at the B-tree floor (doc 00), so the only
// point-read headroom left is the cache misses a side index removes, and that is what this
// lands: the model replaces the interior node touches with one model evaluation plus a tiny
// local search, and the leaf the read was going to decode anyway is where it lands.
//
// What it models, and why that is enough. The model maps a user key to a leaf in the
// immutable on-disk run: it stores, per leaf, that leaf's smallest user key and its page,
// and fits a spline over those keys so a lookup predicts the leaf index in one evaluation.
// The model is not the authority on where a key lives; it is a hint for where to start the
// bounded right-sibling walk collectRange already does. That is what makes it safe under a
// stale model, which is the whole correctness story below.
//
// Why a stale model is still correct. The leaf run in this core only grows: a leaf split
// keeps the original page number on the left piece with its original smallest key and chains
// right to the new piece, and no leaf page is ever freed or reused or merged (the M0 page
// lifecycle). So a leaf whose recorded smallest key is at or before the read's lower bound is
// still, after any amount of splitting, a live leaf on the run from which a right-sibling walk
// reaches every key at or above lower. The walk filters out keys below lower and stops past
// upper, so starting it one or several leaves too far left only costs a few extra leaf decodes
// and never returns a wrong answer. The model can therefore be arbitrarily behind the live
// tree and stay correct; being behind only costs locality, never an answer. The read path
// (startLeafFor in paged.go) verifies the predicted leaf actually starts at or before lower
// and falls back to the proven leafForKey descent if it does not, so even a future change that
// breaks the grow-only invariant (a node merge, page reuse) degrades to descent speed rather
// than a wrong read.
//
// The data-dependent worst case (doc 03). A spline cannot fit a pathological key distribution
// without a large error window, and a large window degrades the local search toward a scan. So
// the local search is capped: past the cap it falls back to a binary search of the leaf array,
// which is the descent's own log cost, so the locate is never worse than the descent it
// replaces. That cap is the bounded-window guarantee D6 makes, made concrete.

// locatorMaxErr is the spline's target maximum error in leaf-index units: the build keeps the
// linear interpolation within this many leaves of every leaf's true index, so a lookup's local
// search is a handful of array probes rather than a descent. It is a hint, not a contract; the
// local search self-corrects past it and the cap below bounds the worst case regardless.
const locatorMaxErr = 4

// minLocatorLeaves is the leaf count below which the model is not worth building: a run of a
// few leaves is a one or two step descent already, so the model is built only once the run is
// deep enough that skipping the descent saves real node touches.
const minLocatorLeaves = 4

// leafEntry is one leaf in the model: that leaf's smallest key, kept both as the user key (for
// the spline, which is monotonic in user-key order) and as the full internal key (for the
// at-or-before comparison the locate makes). Both are copied out so the entry owns them and
// stays valid as pages are later decoded and evicted.
//
// Why the entry must carry the internal key, not just the user key. Under MVCC a single user
// key's versions can straddle a leaf boundary: a split can leave the newest version of a key as
// the last record of one leaf and an older version of the same user key as the first record of
// the next. A point read routes on (key, MaxVersion), the smallest internal key for that user
// key, so it must start at the leaf the descent would reach, which is the one holding the newest
// version. Comparing only user keys would accept the next leaf (its smallest user key still
// equals the probe) and the right walk would start past the newest version and miss it. The
// locate therefore compares internal keys, so it lands on the same leaf the descent does.
type leafEntry struct {
	first   []byte
	firstIK []byte
	page    format.PageNo
}

// splinePoint is one vertex of the piecewise-linear spline: the key as a uint, and the leaf
// index the spline maps it to. Between two consecutive points the model interpolates linearly.
type splinePoint struct {
	x uint64
	y float64
}

// leafLocator is the resident model: the per-leaf entries in run order (ascending smallest
// key) and the spline over their keys. It is immutable once built and published through an
// atomic pointer, so a reader loads a whole consistent model with no latch and a rebuild swaps
// in a fresh one without mutating the one a reader holds.
type leafLocator struct {
	entries []leafEntry
	spline  []splinePoint
	maxErr  int
}

// keyToU64 maps a user key to a uint for the spline by taking its first eight bytes big-endian,
// zero-padded on the right. The mapping is monotonic with lexicographic order (a key and a
// longer key that extends it map to the same or a larger uint), so the spline sees the keys in
// the order the leaves are in. Keys that share an eight-byte prefix collide to one uint, which
// only widens the local search a little because the leaf array, not the uint, is the source of
// truth for the exact slot.
func keyToU64(key []byte) uint64 {
	var b [8]byte
	copy(b[:], key)
	return binary.BigEndian.Uint64(b[:])
}

// buildSpline fits a piecewise-linear spline over the ascending key uints xs so that linear
// interpolation between consecutive spline points stays within maxErr of every point's index.
// It is the greedy spline corridor (the construction RadixSpline uses): it walks the points
// keeping the cone of slopes from the last emitted spline point that holds every point so far
// within maxErr, and emits a new spline point the moment a point would leave the cone. The
// result is a small set of vertices, kilobytes for a large run, that a lookup interpolates in
// one step. A run of two or fewer leaves needs no spline and returns its endpoints.
func buildSpline(xs []uint64, maxErr float64) []splinePoint {
	n := len(xs)
	if n <= 2 {
		pts := make([]splinePoint, n)
		for i := 0; i < n; i++ {
			pts[i] = splinePoint{x: xs[i], y: float64(i)}
		}
		return pts
	}
	pts := []splinePoint{{x: xs[0], y: 0}}
	base := pts[0]
	prev := splinePoint{x: xs[1], y: 1}
	var loSlope, hiSlope float64
	haveCone := false

	setCone := func(cur splinePoint) {
		dx := float64(cur.x - base.x)
		if dx == 0 {
			haveCone = false
			return
		}
		hiSlope = (cur.y + maxErr - base.y) / dx
		loSlope = (cur.y - maxErr - base.y) / dx
		haveCone = true
	}

	for i := 1; i < n; i++ {
		cur := splinePoint{x: xs[i], y: float64(i)}
		dx := float64(cur.x - base.x)
		if dx == 0 {
			// Same uint as the base (a prefix collision): no slope to add, carry it as the
			// candidate spline point and move on.
			prev = cur
			continue
		}
		if !haveCone {
			setCone(cur)
			prev = cur
			continue
		}
		s := (cur.y - base.y) / dx
		if s < loSlope || s > hiSlope {
			// cur would leave the cone, so the segment must end at the previous point. Emit it
			// as a spline vertex, restart the cone from there, and reseed it with cur.
			pts = append(pts, prev)
			base = prev
			setCone(cur)
			prev = cur
			continue
		}
		// cur stays in the cone: tighten the cone by the slopes to cur plus and minus maxErr.
		if hi := (cur.y + maxErr - base.y) / dx; hi < hiSlope {
			hiSlope = hi
		}
		if lo := (cur.y - maxErr - base.y) / dx; lo > loSlope {
			loSlope = lo
		}
		prev = cur
	}
	last := splinePoint{x: xs[n-1], y: float64(n - 1)}
	if tail := pts[len(pts)-1]; tail.x != last.x || tail.y != last.y {
		pts = append(pts, last)
	}
	return pts
}

// predictIndex returns the leaf index the spline predicts for key. It binary-searches the small
// spline-vertex array for the segment that brackets the key's uint and interpolates within it,
// so the prediction costs a search over the vertices (far fewer than the leaves) plus one
// interpolation. The result is clamped to the leaf array and is only a starting point; locate
// corrects it to the exact slot.
func (lo *leafLocator) predictIndex(key []byte) int {
	pts := lo.spline
	if len(pts) == 0 {
		return 0
	}
	x := keyToU64(key)
	if x <= pts[0].x {
		return 0
	}
	if x >= pts[len(pts)-1].x {
		return len(lo.entries) - 1
	}
	j := sort.Search(len(pts), func(m int) bool { return pts[m].x > x }) - 1
	a, b := pts[j], pts[j+1]
	yi := a.y
	if b.x != a.x {
		yi = a.y + (b.y-a.y)*float64(x-a.x)/float64(b.x-a.x)
	}
	idx := int(yi + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(lo.entries) {
		idx = len(lo.entries) - 1
	}
	return idx
}

// locate returns the page to begin the bounded right-sibling walk at for the internal key lik
// (the read's lower bound encoded at MaxVersion): the leaf whose smallest internal key is the
// largest one still at or before lik, or the leftmost leaf when lik is below the whole run. It
// predicts the index from lik's user key with the spline, then self-corrects with a short local
// search to the exact slot, comparing internal keys so a user key whose versions span a leaf
// boundary lands on the leaf the descent would, falling back to a binary search of the entries
// if the local search runs past its cap (the bounded-window guarantee: the locate is never
// slower than a descent's log search). The returned page's smallest internal key is at or before
// lik except in the below-all case, which is the at-or-before property the right walk rests on.
func (lo *leafLocator) locate(lik []byte) format.PageNo {
	n := len(lo.entries)
	if n == 0 {
		return format.NoPage
	}
	p := lo.predictIndex(format.UserKey(lik))
	window := 2*lo.maxErr + 8
	steps := 0
	for p > 0 && format.CompareInternal(lo.entries[p].firstIK, lik) > 0 {
		p--
		if steps++; steps > window {
			return lo.binarySearch(lik)
		}
	}
	for p+1 < n && format.CompareInternal(lo.entries[p+1].firstIK, lik) <= 0 {
		p++
		if steps++; steps > window {
			return lo.binarySearch(lik)
		}
	}
	return lo.entries[p].page
}

// binarySearch is the bounded-window fallback: the largest entry whose smallest internal key is
// at or before lik, found by a plain binary search of the entries. When lik is below every leaf
// it returns the leftmost leaf, which is a safe start because the right walk from it filters
// out nothing it should keep. It is the locate's descent-cost floor, taken only when the spline
// is so far off that the local search would scan, which is the adversarial distribution D6
// names.
func (lo *leafLocator) binarySearch(lik []byte) format.PageNo {
	n := len(lo.entries)
	j := sort.Search(n, func(m int) bool { return format.CompareInternal(lo.entries[m].firstIK, lik) > 0 }) - 1
	if j < 0 {
		j = 0
	}
	return lo.entries[j].page
}

// buildLeafLocator walks the leaf run once and builds the model over it: one entry per
// non-empty leaf holding that leaf's smallest user key and page, then a spline over those keys.
// It returns a nil model for a run too small to be worth one (descent is already cheap there),
// so the caller stores nil and the read path simply descends. The caller holds the write latch
// (Open before any concurrent use, or a rollover under wmu), so the run is fixed across the
// walk; the cycle guard turns a corrupt sibling loop into an error rather than a hang.
func (t *Tree) buildLeafLocator() (*leafLocator, error) {
	head, err := t.leftmostLeaf()
	if err != nil {
		return nil, err
	}
	var entries []leafEntry
	seen := map[format.PageNo]bool{}
	for pgno := head; pgno != format.NoPage; {
		if seen[pgno] {
			return nil, fmt.Errorf("betree: leaf run cycles at page %d", pgno)
		}
		seen[pgno] = true
		lf, derr := t.viewLeaf(pgno)
		if derr != nil {
			return nil, derr
		}
		if len(lf.records) > 0 {
			entries = append(entries, leafEntry{
				first:   append([]byte(nil), format.UserKey(lf.records[0].key)...),
				firstIK: append([]byte(nil), lf.records[0].key...),
				page:    pgno,
			})
		}
		pgno = lf.right
	}
	if len(entries) < minLocatorLeaves {
		return nil, nil
	}
	xs := make([]uint64, len(entries))
	for i := range entries {
		xs[i] = keyToU64(entries[i].first)
	}
	return &leafLocator{entries: entries, spline: buildSpline(xs, locatorMaxErr), maxErr: locatorMaxErr}, nil
}

// maybeRebuildLocator rebuilds the model on an amortized schedule from the rollover that just
// settled the run. A build walks the whole run, which is O(leaves), so building it every
// rollover would load every rollover with the whole run's cost. Instead it rebuilds once the
// number of rollovers since the last build reaches the model's leaf count, which spreads the
// O(leaves) build over that many rollovers and keeps the per-rollover cost amortized constant.
// Between rebuilds the model goes stale, which the read path absorbs as a few extra leaf
// decodes (the grow-only invariant above), so trading freshness for a cheap write path is
// sound. The caller holds wmu, so locRebuildCount is single-writer state; the model is
// published atomically for the lock-free readers. A build error leaves the existing model in
// place, and a nil result (the run shrank below the threshold) disables the model.
func (t *Tree) maybeRebuildLocator() {
	t.locRebuildCount++
	threshold := uint64(1)
	if cur := t.locator.Load(); cur != nil {
		threshold = uint64(len(cur.entries))
	}
	if t.locRebuildCount < threshold {
		return
	}
	loc, err := t.buildLeafLocator()
	if err != nil {
		return
	}
	t.locator.Store(loc)
	t.locRebuildCount = 0
}
