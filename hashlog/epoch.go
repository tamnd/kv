package hashlog

import (
	"math"
	"runtime"
	"sync/atomic"
)

// Epoch-based reclamation (spec 2070 doc 07, decisions D12 and D13).
//
// The in-memory full-resident read is lock-free because pages are never freed: a
// reader can hold a slice into a resident page forever, the bytes are immutable and
// the backing array is never reclaimed. The durable engine breaks that property,
// because it frees memory the in-memory engine never freed: eviction recycles a
// page buffer (hands it to the next tail page, which overwrites it), and compaction
// (M8) frees an extent back to the allocator. Freeing memory under a lock-free
// reader is a use-after-free unless something guarantees the reader has let go
// first.
//
// The in-memory engine's answer to exactly this hazard is the evicting GET's shard
// read lock, which reintroduces on the larger-than-memory read the reader-count
// cache-line ping-pong the lock-free rewrite removed to win the hot-key read. Epoch
// reclamation keeps the read lock-free while making the free safe: a reader
// publishes the current global epoch into a striped per-participant slot before its
// lookup-and-slice and clears it after (a couple of atomic stores, not a
// read-modify-write on a shared word), and a reclaimer defers every free behind the
// safe epoch (the minimum epoch any active reader is inside) so an object retired at
// epoch r is freed only once every reader that could hold a reference into it has
// left. This is FASTER's epoch framework (prior-art 1) and the betree redesign's
// epoch reclamation, specialised to what hashlog frees: page buffers and (at M8)
// extents.

// cacheLine is the padding used to keep two participants' epoch slots off the same
// cache line, so a store into one slot does not bounce another's line.
const cacheLine = 64

// paddedEpoch is one participant slot, padded to a full cache line so two active
// readers on different stripes never false-share.
type paddedEpoch struct {
	e atomic.Uint64
	_ [cacheLine - 8]byte
}

// slotPool is a fixed striped array of epoch slots (doc 07 section 4.4). A reader
// enters by storing the current global epoch into one striped slot and leaves by
// storing the sentinel zero. The stripe count is a power of two so stripe selection
// is a single AND, and it is sized to a small multiple of GOMAXPROCS so two active
// readers rarely land on the same stripe. It is fixed at store open and never grows.
type slotPool struct {
	stripes []paddedEpoch
	mask    uint64
}

// newSlotPool builds a slot pool with at least n stripes, rounded up to a power of
// two. n is a small multiple of GOMAXPROCS, so under normal concurrency two active
// readers rarely collide on a stripe.
func newSlotPool(n int) *slotPool {
	size := 1
	for size < n {
		size <<= 1
	}
	return &slotPool{stripes: make([]paddedEpoch, size), mask: uint64(size - 1)}
}

// epochGuard pins one participant slot for the duration of a protected read. The
// zero value pins nothing; obtain one from enter and release it with leave. It is a
// two-word value held on the reader's stack, so the read path allocates nothing.
type epochGuard struct {
	slot *atomic.Uint64
}

// enter pins the current global epoch into a striped slot and returns a guard (doc 07
// section 2.3, 4.4). The store into the slot is the release that publishes the pin
// before the reader's loads that touch freeable memory: the global epoch is read,
// then stored into the slot, then the caller loads the index and the page directory,
// all sequenced after this store, so a reclaimer that observes the reader's later
// effects also observes the pin (doc 07 section 3).
func (p *slotPool) enter(ge *atomic.Uint64, stripe uint64) epochGuard {
	s := &p.stripes[stripe&p.mask].e
	s.Store(ge.Load())
	return epochGuard{slot: s}
}

// leave clears the slot, ending the participant's pin. After it the reader pins
// nothing and any object it might have held is reclaimable, subject to other readers.
func (g epochGuard) leave() {
	if g.slot != nil {
		g.slot.Store(0)
	}
}

// safeEpoch is the minimum epoch any active reader is currently inside, or
// math.MaxUint64 if no reader is inside any epoch (doc 07 section 2.3, the reclaim
// scan). A slot holding the sentinel zero is skipped: that participant pins nothing.
// An object retired at epoch r is safe to free once safeEpoch is strictly greater
// than r, because then every active reader is inside an epoch later than r, so no
// active reader could have obtained a reference to an object retired at r.
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

// retireKind tags what a deferred-free entry reclaims. M6 retires page buffers;
// retireExtent is reserved for M8's compaction, which frees extents behind the same
// epoch frontier this milestone establishes.
type retireKind uint8

const (
	retirePageBuf retireKind = iota
	retireExtent
)

// deferredFree is one retired object waiting behind the safe epoch (doc 07 section
// 2.2, the deferred-free list). buf is the page buffer to recycle for retirePageBuf;
// extent is the extent id to free for retireExtent (M8). retireEpoch is the global
// epoch at the moment the object was retired: it is freed only once the safe epoch
// passes it.
type deferredFree struct {
	kind        retireKind
	buf         []byte
	extent      int64
	retireEpoch uint64
}

// Reader is an optional handle for callers doing many reads in a tight loop (doc 07
// section 4.6). It caches a stripe so each Get enters on a stable, low-contention
// slot, amortising the stripe selection to once per handle instead of paying a
// shared round-robin counter per read. The bare Store.Get stays for convenience and
// draws a fresh stripe per call; a hot read loop should hold a Reader. In the
// full-resident memory-only mode neither path enters an epoch, so the handle costs
// nothing in the mode the benchmark measures.
type Reader struct {
	s      *Store
	stripe uint64
}

// NewReader returns a Reader that caches a low-contention stripe. It is safe to hold
// across many Get calls and is not safe for concurrent use by multiple goroutines;
// give each goroutine its own Reader (the point of the per-Reader stripe is that two
// goroutines use different stripes).
func (s *Store) NewReader() *Reader {
	return &Reader{s: s, stripe: s.nextStripe.Add(1)}
}

// Get returns the value stored under key, entering the epoch on the Reader's cached
// stripe. It is the zero-overhead read path for hot loops; see Store.Get for the
// returned-slice ownership contract.
func (r *Reader) Get(key []byte) (value []byte, found bool, err error) {
	return r.s.shardFor(key).getGuarded(key, r.stripe)
}

// EpochStats reports the epoch observability counters (doc 08 section 1, the M6
// frontier and deferred-free depth): the current global epoch, the safe epoch (the
// minimum epoch any active reader is inside, math.MaxUint64 when no reader is
// active), and the total deferred-free queue depth across all shards. A safe epoch
// that never advances or a deferred depth that only grows signals a stuck reader
// leaking deferred frees. It is the zero value with a math.MaxUint64 safe epoch for a
// memory-only store, which never evicts and so never retires.
type EpochStats struct {
	GlobalEpoch   uint64
	SafeEpoch     uint64
	DeferredFrees int
}

// EpochStats returns the current epoch counters.
func (s *Store) EpochStats() EpochStats {
	depth := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		depth += len(sh.deferred)
		sh.mu.RUnlock()
	}
	return EpochStats{
		GlobalEpoch:   s.globalEpoch.Load(),
		SafeEpoch:     s.slots.safeEpoch(),
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
