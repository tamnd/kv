package betree

// This file is the third slice of M5.3, the durability-coupled read frontier (doc 05 sections on
// preserving snapshot isolation and on the completion watermark for out-of-order completion). The
// HLC slice gave a reader a way to sample a timestamp and the assigner slice gave each commit a
// timestamp from its conflict footprint, but neither tied a read to durability. A reader that
// samples the raw clock can pick a read timestamp above a commit whose WAL frame has not yet been
// synced and whose apply is still in flight, and then it would observe a write that a crash could
// still lose. The rule the design states is that a reader takes its snapshot read timestamp at or
// below a frontier that never includes a commit whose durability is still in flight. This slice is
// that frontier in timestamp space.
//
// The shape mirrors the M5.2 completion watermark, which does the same job in LSN space: leaderless
// committers finish out of order, so the engine needs one monotonic point that says everything at
// or below it is durable and applied. There the point is the highest LSN whose whole prefix has
// completed; here it is the highest commit timestamp below which no commit is still in flight. The
// two are the same idea on the two axes the commit path runs on, and at integration a commit
// resolves here exactly when the completion watermark covers its LSN, so the LSN watermark drives
// this one. Until that wiring lands this tracker stands alone, off the live path, as the assigner
// does.
//
// Admission first, then resolution. A committer registers here at the start of its commit, before
// it computes its commit timestamp, and that registration hands back an admission timestamp: a fresh
// clock sample taken under this tracker's lock. The admission timestamp is a lower bound on the
// commit's eventual commit timestamp, because the commit timestamp comes from the same clock later
// and the clock only moves forward. Registering before the commit timestamp exists is what closes
// the window a naive design leaves open: if a commit only joined the in-flight set after its
// timestamp were assigned, a newer commit could be admitted, committed, and resolved in the gap,
// draining the set empty and letting the frontier advance past the older commit that had a timestamp
// but was not yet registered. Sampling the admission timestamp under the same lock that registers it
// removes the gap entirely, so the in-flight set always contains a lower bound for every commit that
// holds a timestamp.
//
// The two values the frontier is built from. While any commit is in flight, the frontier is one
// below the oldest admission timestamp still active. No in-flight commit can have a commit timestamp
// at or below that, because each commit's timestamp is at least its own admission timestamp, which
// is at least the oldest. So a reader at the frontier never names an in-flight commit. When the
// in-flight set drains empty, every commit ever admitted has resolved, and the frontier becomes the
// highest commit timestamp any of them actually committed at, so a reader sees every durable commit
// up to the latest. The frontier uses the admission timestamp for the in-flight floor (a lower
// bound, all that is known while a commit runs) and the real commit timestamp for the resolved
// ceiling (known once it lands), which is what makes it both safe against in-flight commits and
// complete for durable ones.
//
// Monotonicity. The frontier never goes backward. While the in-flight set stays non-empty it is the
// oldest admission timestamp minus one, and that oldest only rises, because admissions are issued in
// increasing order under the lock and the minimum leaves only by resolving. When the last in-flight
// commit resolves the frontier jumps to the highest resolved commit timestamp, which is at least the
// just-resolved commit's timestamp, itself above the admission-minus-one the frontier just held. And
// a commit admitted after the set was empty samples a clock value above every timestamp issued so
// far, so its admission-minus-one is at or above the resolved ceiling the frontier reported. Every
// transition is non-decreasing.
//
// The single-node honest frame, the same one the assigner carries. This tracker is one per-node
// structure that every committer touches at admit and resolve under one mutex, so on a single
// unsharded node it is a coordination point, cheap relative to the fsync it gates but not free. It
// is the timestamp-space twin of the completion watermark, the one place the out-of-order leaderless
// commits are reconciled into a single ordered frontier. Full decentralization, where each shard
// owns its own frontier so disjoint committers never share it, arrives with the logical sharding of
// M7; this slice is the per-node half of it.

import "sync"

// tsHeap is a tiny min-heap of timestamps, used here for the admission timestamps of the active
// committers so the oldest is always at index 0. It is hand-rolled over a slice of hlcTime rather
// than built on container/heap so the elements stay unboxed uint64s and the push and pop read
// plainly. It is not safe for concurrent use; the visibility mutex guards it.
type tsHeap []hlcTime

func (h tsHeap) len() int { return len(h) }

// min returns the oldest timestamp in the heap. The caller checks len first.
func (h tsHeap) min() hlcTime { return h[0] }

// push adds a timestamp and sifts it up to restore the heap order.
func (h *tsHeap) push(ts hlcTime) {
	*h = append(*h, ts)
	s := *h
	i := len(s) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if s[parent] <= s[i] {
			break
		}
		s[parent], s[i] = s[i], s[parent]
		i = parent
	}
}

// pop removes and returns the oldest timestamp, sifting the moved tail element down to restore the
// heap order. The caller checks len first.
func (h *tsHeap) pop() hlcTime {
	s := *h
	top := s[0]
	last := len(s) - 1
	s[0] = s[last]
	s = s[:last]
	*h = s
	i := 0
	for {
		l, r := 2*i+1, 2*i+2
		small := i
		if l < len(s) && s[l] < s[small] {
			small = l
		}
		if r < len(s) && s[r] < s[small] {
			small = r
		}
		if small == i {
			break
		}
		s[small], s[i] = s[i], s[small]
		i = small
	}
	return top
}

// visibility is the durability-coupled read frontier. It tracks the admission timestamps of the
// commits in flight (admitted but not resolved) and reports the frontier a reader takes its read
// timestamp from. It is safe for concurrent use under its own mutex.
type visibility struct {
	mu          sync.Mutex
	clock       *hlc
	adm         tsHeap           // min-heap of admission timestamps of active committers
	resolved    map[hlcTime]bool // admission timestamps resolved but still in the heap, awaiting drain
	maxResolved hlcTime          // highest commit timestamp resolved; the frontier when nothing is in flight
}

// newVisibility builds an empty tracker over a clock. A nil clock gets a fresh system-clock HLC, so
// a caller that just wants the default need not build one. A reader on an empty tracker sees the
// frontier at zero, which is the empty store.
func newVisibility(clock *hlc) *visibility {
	if clock == nil {
		clock = newHLC(nil)
	}
	return &visibility{clock: clock, resolved: make(map[hlcTime]bool)}
}

// admit registers a new committer and returns its admission timestamp, a lower bound on the commit
// timestamp it will later compute. The sample is taken under the lock so the registration has no
// gap: the moment a commit holds a timestamp lower bound, that lower bound is already in the
// in-flight set. The returned value is the handle passed back to resolve.
func (v *visibility) admit() hlcTime {
	v.mu.Lock()
	defer v.mu.Unlock()
	ts := v.clock.Now()
	v.adm.push(ts)
	return ts
}

// resolve marks the commit admitted at adm durable and applied, recording the commit timestamp it
// actually committed at, then advances the frontier as far as the contiguous run of resolved
// admissions now allows. Resolution is out of order: a commit resolved while an older one is still
// in flight is recorded and only leaves the heap when the older admission ahead of it resolves and
// the heap drains forward through it, exactly as the LSN completion watermark drains its sorted set
// forward when the next-expected LSN lands.
//
// The contract on commitTs. It must be at or above adm (a commit never commits before it was
// admitted), and it must be a timestamp the same clock issued, which in the integrated engine it is:
// the commit timestamp is clock.Update over the conflict footprint, a real point the clock handed
// out. That is what makes the empty-set frontier safe and monotonic. When the in-flight set drains,
// the frontier becomes the highest resolved commit timestamp, and the next commit's admission samples
// the same clock with Now, which returns a value strictly past every timestamp the clock issued, so
// the new admission exceeds that resolved ceiling and the frontier never regresses. A commitTs
// fabricated outside the clock (a value the clock never issued, that a later admission could collide
// with) would break that and is not a valid input.
func (v *visibility) resolve(adm, commitTs hlcTime) {
	v.mu.Lock()
	if commitTs > v.maxResolved {
		v.maxResolved = commitTs
	}
	v.resolved[adm] = true
	for v.adm.len() > 0 && v.resolved[v.adm.min()] {
		delete(v.resolved, v.adm.min())
		v.adm.pop()
	}
	v.mu.Unlock()
}

// frontier returns the read timestamp a snapshot reader takes: the highest timestamp at or below
// which every commit has resolved. When something is in flight it is one below the oldest admission
// timestamp, so no in-flight commit (whose commit timestamp is at least its admission timestamp, at
// least the oldest) is at or below it; when nothing is in flight it is the highest resolved commit
// timestamp, the latest durable point. It never regresses across calls.
func (v *visibility) frontier() hlcTime {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.adm.len() == 0 {
		return v.maxResolved
	}
	return v.adm.min() - 1
}

// inFlight reports how many commits are admitted but not yet drained. It exists for tests and
// diagnostics; the live path never needs it.
func (v *visibility) inFlight() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.adm.len()
}
