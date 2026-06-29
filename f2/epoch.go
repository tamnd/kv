package f2

import (
	"math"
	"runtime"
	"sync/atomic"
)

// Epoch-based reclamation (spec 2070 docs 06 and 07), the foundation f2's
// compactor rests on. f2's lock-free read loads the page directory and slices a
// value out of a page (resident) or preads it from a file block (evicted),
// holding no shard lock. The memory-only profile never frees a page, so a reader
// can hold a slice forever and the bytes never change. The durable profile is
// different once compaction lands: compaction rewrites a shard into a fresh
// generation and retires the old generation's file blocks back to the allocator,
// which may hand a block to another shard that overwrites it. A reader that loaded
// an old block offset before the rewrite and is about to pread it would then read
// a different shard's bytes. Reusing a block under a lock-free reader is a
// use-after-free unless something guarantees the reader has let go first.
//
// The guarantee is QSBR: a reader publishes the current global epoch into a
// striped per-participant slot before it loads the directory and clears it after
// its read (two atomic stores, not a read-modify-write on a shared word), and a
// reclaimer defers every block free behind the safe epoch (the minimum epoch any
// active reader is inside), so a block retired at epoch r is reused only once
// every reader that could hold an offset into it has left. This is FASTER's epoch
// framework, the same mechanism hashlog uses, specialized to what f2 frees: file
// blocks.
//
// This file is the mechanism. It frees nothing yet: the compactor (a later
// increment) is what retires blocks behind the frontier this establishes. Adding
// it now cannot change correctness, and the memory-only hot path the benchmark
// measures never enters an epoch, so it is untouched.

// cacheLine is the padding that keeps two participants' epoch slots off the same
// cache line, so a store into one slot does not bounce another's line.
const cacheLine = 64

// paddedEpoch is one participant slot, padded to a full cache line so two active
// readers on different stripes never false-share.
type paddedEpoch struct {
	e atomic.Uint64
	_ [cacheLine - 8]byte
}

// slotPool is a fixed striped array of epoch slots. A reader enters by storing the
// current global epoch into one striped slot and leaves by storing the sentinel
// zero. The stripe count is a power of two so stripe selection is a single AND,
// and it is sized to a small multiple of GOMAXPROCS so two active readers rarely
// land on the same stripe. It is fixed at store open and never grows.
type slotPool struct {
	stripes []paddedEpoch
	mask    uint64
}

// newSlotPool builds a slot pool with at least n stripes, rounded up to a power of
// two.
func newSlotPool(n int) *slotPool {
	size := 1
	for size < n {
		size <<= 1
	}
	return &slotPool{stripes: make([]paddedEpoch, size), mask: uint64(size - 1)}
}

// epochGuard pins one participant slot for the duration of a protected read. The
// zero value pins nothing; obtain one from enter and release it with leave. It is
// a one-word value held on the reader's stack, so the read path allocates nothing.
type epochGuard struct {
	slot *atomic.Uint64
}

// enter pins the current global epoch into a striped slot and returns a guard. The
// store into the slot is the release that publishes the pin before the reader's
// loads that touch freeable memory: the global epoch is read, then stored into the
// slot, then the caller loads the directory, sequenced after this store, so a
// reclaimer that observes the reader's later effects also observes the pin.
func (p *slotPool) enter(ge *atomic.Uint64, stripe uint64) epochGuard {
	s := &p.stripes[stripe&p.mask].e
	s.Store(ge.Load())
	return epochGuard{slot: s}
}

// leave clears the slot, ending the participant's pin. After it the reader pins
// nothing and any object it might have held is reclaimable, subject to other
// readers.
func (g epochGuard) leave() {
	if g.slot != nil {
		g.slot.Store(0)
	}
}

// safeEpoch is the minimum epoch any active reader is currently inside, or
// math.MaxUint64 if no reader is inside any epoch. A slot holding the sentinel
// zero is skipped: that participant pins nothing. A block retired at epoch r is
// safe to reuse once safeEpoch is strictly greater than r, because then every
// active reader is inside an epoch later than r, so no active reader could have
// obtained an offset into a block retired at r.
func (p *slotPool) safeEpoch() uint64 {
	var safe uint64 = math.MaxUint64
	for i := range p.stripes {
		e := p.stripes[i].e.Load()
		if e != 0 && e < safe {
			safe = e
		}
	}
	return safe
}

// epochs is the per-store epoch state, shared by every shard. It is nil in the
// memory-only profile, which never frees and so never needs a grace period.
type epochs struct {
	global     atomic.Uint64 // current global epoch, starts at 1 (0 is the no-pin sentinel)
	slots      *slotPool
	nextStripe atomic.Uint64 // round-robin stripe selector for the bare Get path
}

// newEpochs builds the shared epoch state with a slot pool sized to the machine.
// The global epoch starts at 1 so a reader that pins it never stores the sentinel
// zero (which would read as "pins nothing").
func newEpochs() *epochs {
	e := &epochs{slots: newSlotPool(defaultSlotStripes())}
	e.global.Store(1)
	return e
}

// advance bumps the global epoch and returns the new value. A reclaimer calls it
// to open a fresh epoch before scanning the safe epoch, so a block retired in the
// old epoch becomes reusable once every reader has moved past it. It is a single
// atomic add, called from the background compactor, never the hot path.
func (e *epochs) advance() uint64 { return e.global.Add(1) }

// Reader is an optional handle for callers doing many reads in a tight loop. It
// caches a stripe so each Get enters on a stable, low-contention slot, amortising
// the stripe selection to once per handle instead of paying the shared round-robin
// counter per read. The bare Store.Get stays for convenience and draws a fresh
// stripe per call; a hot durable read loop should hold a Reader. In the
// memory-only mode neither path enters an epoch, so the handle costs nothing in
// the mode the benchmark measures.
type Reader struct {
	s      *Store
	stripe uint64
}

// NewReader returns a Reader that caches a low-contention stripe. It is safe to
// hold across many Get calls and is not safe for concurrent use by multiple
// goroutines; give each goroutine its own Reader.
func (s *Store) NewReader() *Reader {
	var stripe uint64
	if s.ep != nil {
		stripe = s.ep.nextStripe.Add(1)
	}
	return &Reader{s: s, stripe: stripe}
}

// Get returns the value stored under key, entering the epoch on the Reader's
// cached stripe in durable mode. It is the zero-overhead read path for hot loops;
// see Store.Get for the returned-slice ownership contract.
func (r *Reader) Get(key []byte) (value []byte, found bool, err error) {
	if r.s.closed.Load() {
		return nil, false, errClosed
	}
	h := hash64(key)
	sh := r.s.shardFor(h)
	if sh.budgeted {
		return sh.getLocked(h, key)
	}
	if sh.ep == nil {
		return sh.get(h, key)
	}
	return sh.getGuarded(h, key, r.stripe)
}

// EpochStats reports the epoch observability counters: the current global epoch,
// the safe epoch (the minimum epoch any active reader is inside, math.MaxUint64
// when no reader is active), and the total deferred-free queue depth across all
// shards. A safe epoch that never advances or a deferred depth that only grows
// signals a stuck reader leaking deferred frees. It is the zero value with a
// math.MaxUint64 safe epoch for a memory-only store, which never retires.
type EpochStats struct {
	GlobalEpoch   uint64
	SafeEpoch     uint64
	DeferredFrees int
}

// EpochStats returns the current epoch counters.
func (s *Store) EpochStats() EpochStats {
	if s.ep == nil {
		return EpochStats{SafeEpoch: math.MaxUint64}
	}
	depth := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		depth += len(sh.deferred)
		sh.mu.Unlock()
	}
	return EpochStats{
		GlobalEpoch:   s.ep.global.Load(),
		SafeEpoch:     s.ep.slots.safeEpoch(),
		DeferredFrees: depth,
	}
}

// defaultSlotStripes sizes the slot pool: a small multiple of GOMAXPROCS so active
// readers spread across stripes, with a floor so a single-P run still has a few
// slots. newSlotPool rounds it up to a power of two.
func defaultSlotStripes() int {
	n := runtime.GOMAXPROCS(0) * 4
	if n < 16 {
		n = 16
	}
	return n
}

// deferredFree is one retired file block waiting behind the safe epoch. block is
// the data block id to return to the allocator; retireEpoch is the global epoch at
// the moment it was retired, so it is freed only once the safe epoch passes it.
// The compactor (a later increment) populates these; this increment defines the
// type so the shard carries the queue and EpochStats can report its depth.
type deferredFree struct {
	block       int64
	retireEpoch uint64
}
