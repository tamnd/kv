package betree

// This file is the first slice of M5.3, the hybrid logical clock (doc 05 section 3,
// decision D8). It is built alongside the shipped global oracle (db/oracle.go) and off the
// live commit path until the M8 flip, the same alongside-then-flip discipline the leaderless
// WAL followed: the btree and lsm cores keep taking their commit and read timestamps from the
// shared oracle's single mutex-guarded counter, and this clock is proved in isolation before
// the decentralized commit-timestamp assigner (the next slice) is built on it.
//
// Why a clock at all. D8 removes the global timestamp oracle because a single shared counter
// every committer increments is a cache line that bounces between cores and a total order
// imposed on transactions that have no logical reason to order against each other. The
// replacement derives each commit timestamp from the conflict footprint by the max-plus-one
// rule: a committer's timestamp is one past the highest timestamp it observed on any record or
// node it touched. That rule needs two things from a clock. It needs a way to absorb an
// observed timestamp so the next timestamp this writer issues exceeds it (the max-plus-one
// step, Update below), and it needs a source of fresh timestamps for a reader's snapshot that
// tracks real time so a fresh snapshot is not arbitrarily stale (Now below). A hybrid logical
// clock is exactly that: a physical-time component that keeps timestamps anchored to the wall
// clock, and a logical counter that breaks ties and carries causality when many events land in
// the same physical instant or when an observed timestamp runs ahead of the local wall clock.
//
// The construction follows the HLC of Kulkarni et al.: one monotonic timestamp built from the
// physical clock and a logical counter, never running backward, never more than the logical
// counter's worth ahead of the true wall clock once the wall clock catches up. On a single node
// there is no clock skew between machines, so the only job the logical counter does here is
// tie-breaking within a physical instant and threading causality through observed timestamps,
// which is precisely what max-plus-one needs.
//
// The packing. One 64-bit word holds the whole timestamp: the high 48 bits are the physical
// component in milliseconds since the Unix epoch, and the low 16 bits are the logical counter (0
// through 65535). Milliseconds, not microseconds, is what makes 48 bits the right width: 2^48
// milliseconds is roughly 8900 years past the epoch, so the physical component never overflows the
// field in any realistic deployment, whereas 48 bits of microseconds spans only about 8.9 years
// and the current epoch is already well past that, which would silently truncate the high bits of
// the shifted word and collapse distinct instants onto one timestamp. A millisecond holds 65536
// logical ticks before it has to borrow from the next millisecond, which is 65 million events per
// second of headroom, far past any commit rate, so the carry path exists for correctness but is
// effectively never taken, and the logical counter is what separates events within a millisecond.
// A millisecond is fine grain for a read snapshot's staleness, the only thing the physical
// component bounds. The whole word is monotonic, so two timestamps compare as plain uint64s, which
// is what lets a reader's "all commits at or below ts_r" be a single integer comparison.

import (
	"sync/atomic"
	"time"
)

const (
	// hlcLogicalBits is the width of the logical counter, the low bits of the packed word.
	hlcLogicalBits = 16
	// hlcLogicalMask isolates the logical counter.
	hlcLogicalMask = (uint64(1) << hlcLogicalBits) - 1
	// hlcMaxLogical is the largest logical value before a tick borrows from the physical
	// component. At this point the next event in the same physical millisecond advances the
	// physical component by one instead of overflowing the logical field into it.
	hlcMaxLogical = hlcLogicalMask
)

// hlcTime is one packed hybrid-logical timestamp: physical milliseconds in the high 48 bits,
// logical counter in the low 16. Because the word is monotonic, a plain uint64 comparison is
// the timestamp order, which is exactly the comparison a snapshot read ("commit timestamp at or
// below my read timestamp") and the max-plus-one rule ("one past the highest observed") need.
type hlcTime uint64

// physical returns the physical (wall-clock-anchored) component, in milliseconds since epoch.
func (t hlcTime) physical() uint64 { return uint64(t) >> hlcLogicalBits }

// logical returns the logical tie-breaking counter.
func (t hlcTime) logical() uint64 { return uint64(t) & hlcLogicalMask }

// packHLC builds a timestamp from a physical millisecond value and a logical counter. A logical
// value at or past the max borrows into the physical component so the packed word stays
// well-formed and still strictly greater than the timestamp it advanced from; the caller below
// only ever passes a logical value one past a valid one, so a single carry is always enough.
func packHLC(physMillis, logical uint64) hlcTime {
	if logical > hlcMaxLogical {
		physMillis += logical >> hlcLogicalBits
		logical &= hlcLogicalMask
	}
	return hlcTime(physMillis<<hlcLogicalBits | logical)
}

// hlc is the hybrid logical clock: one atomic packed timestamp and the physical-time source it
// reads. It is safe for concurrent use; every transition is a compare-and-swap loop on the one
// word, so many goroutines can issue and absorb timestamps without a mutex, which is the whole
// point of replacing the oracle's single lock. The clock never runs backward and every issued
// timestamp is strictly greater than every timestamp issued or absorbed before it.
type hlc struct {
	state atomic.Uint64
	// nowMillis returns the current physical time in milliseconds since the Unix epoch. It is a
	// field rather than a direct time.Now call so a test can drive the physical component
	// deterministically, including holding it still to force the logical counter to do the work
	// or moving it backward to prove the clock still never regresses.
	nowMillis func() uint64
}

// newHLC builds a clock reading the system wall clock. A nil now uses time.Now; a test passes a
// controllable source.
func newHLC(now func() uint64) *hlc {
	if now == nil {
		now = func() uint64 { return uint64(time.Now().UnixNano() / 1e6) }
	}
	return &hlc{nowMillis: now}
}

// Now issues a fresh timestamp strictly greater than every timestamp this clock has issued or
// absorbed, and at least the current physical time. A reader samples it for a snapshot read
// timestamp (doc 05 section 3): its snapshot is every commit at or below the returned value, and
// because the physical component tracks the wall clock the snapshot is never arbitrarily stale.
// It is a CAS loop, so concurrent callers each get a distinct, monotonically increasing value
// without a lock.
func (h *hlc) Now() hlcTime {
	for {
		old := hlcTime(h.state.Load())
		phys := h.nowMillis()
		var next hlcTime
		if phys > old.physical() {
			// Wall clock moved past the stored physical instant: adopt it and reset the logical
			// counter. This is the common path and keeps the timestamp anchored to real time.
			next = packHLC(phys, 0)
		} else {
			// Wall clock has not advanced past the stored instant (same millisecond, or a clock that
			// stalled or stepped back): hold the physical component and advance the logical counter,
			// so the issued timestamp still strictly increases without depending on the wall clock.
			next = packHLC(old.physical(), old.logical()+1)
		}
		if h.state.CompareAndSwap(uint64(old), uint64(next)) {
			return next
		}
	}
}

// Update absorbs an observed timestamp and returns a fresh local timestamp strictly greater than
// both the clock's prior value and the observed one. This is the max-plus-one step of the
// decentralized commit rule (doc 05 section 3): a committer feeds every timestamp it observed on
// the records and nodes it touched through Update, and the value it ends on is one past the
// highest of them, which is its commit timestamp. Threading observed timestamps through Update is
// what gives conflicting transactions a consistent relative order without a shared counter, since
// each reads the other's stamp through the records they both touch and advances past it. It is
// the HLC receive rule of Kulkarni et al., specialized to a single node where the only causality
// to carry is the observed stamp.
func (h *hlc) Update(observed hlcTime) hlcTime {
	for {
		old := hlcTime(h.state.Load())
		phys := h.nowMillis()
		// The new physical component is the largest of the wall clock, the stored instant, and the
		// observed instant, so the result dominates real time, the clock's history, and the thing
		// just observed.
		maxPhys := old.physical()
		if p := observed.physical(); p > maxPhys {
			maxPhys = p
		}
		var next hlcTime
		switch {
		case maxPhys < phys:
			// The wall clock leads everything: adopt it, reset logical. Neither the stored nor the
			// observed instant constrains the logical counter because both are strictly behind.
			next = packHLC(phys, 0)
		case old.physical() == observed.physical() && old.physical() == maxPhys:
			// Stored and observed share the leading physical instant: take one past the higher of the
			// two logical counters, so the result exceeds both.
			l := old.logical()
			if observed.logical() > l {
				l = observed.logical()
			}
			next = packHLC(maxPhys, l+1)
		case old.physical() == maxPhys:
			// The stored instant leads: advance its logical counter.
			next = packHLC(maxPhys, old.logical()+1)
		default:
			// The observed instant leads: advance past its logical counter.
			next = packHLC(maxPhys, observed.logical()+1)
		}
		if h.state.CompareAndSwap(uint64(old), uint64(next)) {
			return next
		}
	}
}

// Peek returns the clock's current value without advancing it, for tests and metrics. It is not
// a snapshot source: a reader uses Now so its read timestamp dominates every already-issued
// commit timestamp.
func (h *hlc) Peek() hlcTime {
	return hlcTime(h.state.Load())
}
