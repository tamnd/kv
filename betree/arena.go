package betree

// This file is the first slice of M6, the off-heap memory substrate (doc 05 section 5, D10). It is
// the raw arena: a region of anonymous memory the Go garbage collector does not scan, manage, or
// free, with a bump allocator that carves integer-addressed allocations out of it. It is built
// alongside the engine and off the live path; nothing carves nodes or buffer-pool frames out of it
// yet. The fixed-size slab free list the epoch reclaimer returns slabs to, and the sync.Pool for
// hot-path transients, are the next M6 slices on top of this.
//
// Why off-heap. Go's collector, left to manage the engine's large long-lived buffers, drives
// resident memory to a multiple of the live set (at GOGC=100 the heap is allowed to roughly double
// the live set before a collection) and walks every live pointer on each cycle. For an engine whose
// whole job is to hold a large buffer pool and large node buffers resident, that multiplier is paid
// on the single biggest allocation in the process and the per-cycle scan walks gigabytes of cache
// pages hunting pointers that are not there. Moving those buffers into anonymous mmap memory the
// runtime knows nothing about removes both costs: the region does not count against the heap goal
// that triggers GC, and it is never scanned. This is the move Pebble made for its block cache and
// memtables (docs/memory.md). The Go arenas experiment that would have given a stdlib allocator for
// exactly this is paused (golang/go#51317), so the engine rolls its own, which is fine because the
// mechanism it needs is just anonymous mmap, pure Go through the stdlib syscall package, no cgo and
// no dependency.
//
// The pointer-free rule, which has teeth. The arena holds opaque bytes and never a Go pointer. Every
// reference into arena memory is an integer offset, never an unsafe.Pointer the GC would track, so
// the collector sees nothing to trace from the heap into the arena and nothing to keep alive there.
// That is the decoupling D10 buys: arena lifetime is not managed by GC reachability at all. The cost
// of the escape is that the GC also will not free an arena node for an optimistic reader still inside
// it, which is exactly why reclamation is manual and epoch-based (section 2, the M2 epoch machinery).
// A single Go pointer stored in arena bytes would break both halves: the GC could not trace it (the
// arena is not scanned), so its target could be freed under a live arena reference, a use-after-free;
// and freeing an arena slab the GC believed referenced would corrupt the heap. So the allocator's API
// is integer offsets and byte slices only, and callers copy keys and values in as bytes rather than
// retaining a Go pointer into caller memory.
//
// The honest fallback. Anonymous mmap is reached through the stdlib syscall package on Unix
// (arena_mmap_unix.go); on platforms without it the arena falls back to a plain Go byte slice
// (arena_mmap_other.go). The fallback keeps the pointer-free property (a []byte of bytes is a noscan
// allocation the GC does not walk for pointers) but loses the escape from the heap-size multiplier,
// because a heap slice still counts against the GC goal. offHeap reports which backing a given arena
// got, so a test or a diagnostic can tell the real off-heap arena from the fallback rather than
// assume.

import (
	"sync/atomic"
)

// arenaNil is the offset sentinel for "no allocation", returned alongside ok=false. It is the maximum
// uint64, which a real offset (bounded by the arena size) can never equal, so it is unambiguous.
const arenaNil = arenaOff(^uint64(0))

// arenaAlign is the allocation alignment. Every allocation starts at a multiple of 8 bytes so a node
// header's 64-bit version word, accessed through atomics, lands on an 8-byte boundary. The mmap base
// is page-aligned, so base+offset is 8-aligned whenever offset is.
const arenaAlign = 8

// arenaOff is a handle into an arena: a byte offset from the region base, stored as a plain integer
// so the GC sees no pointer to trace. It is meaningful only against the arena it was allocated from.
type arenaOff uint64

// arena is a region of pointer-free memory with a bump allocator. Allocation is a lock-free atomic
// add on the bump cursor, so many goroutines can carve allocations out concurrently without a mutex;
// the region itself is fixed at construction and never grows, so a full arena returns ok=false rather
// than reallocating (growth, when needed, is a new arena, the slab free list's job in the next slice).
type arena struct {
	base    []byte        // the backing region: anonymous mmap, or a heap slice on the fallback
	size    uint64        // len(base), the fixed capacity
	next    atomic.Uint64 // bump cursor: the offset of the next free byte
	offHeap bool          // true if base is real off-heap mmap memory, false on the heap fallback
}

// newArena builds an arena of at least size bytes. The size is rounded up to a whole page so the mmap
// covers entire pages and the region is page-aligned. A zero or tiny size still gets one page, so the
// arena always has room for at least a few small allocations. It returns an error only if the mmap
// itself fails; the fallback never errors.
func newArena(size uint64) (*arena, error) {
	const page = 4096
	if size == 0 {
		size = page
	}
	size = (size + page - 1) &^ (page - 1)
	base, off, err := mmapAnon(int(size))
	if err != nil {
		return nil, err
	}
	return &arena{base: base, size: uint64(len(base)), offHeap: off}, nil
}

// alloc carves n bytes out of the arena and returns the offset of the start, or (arenaNil, false) if
// the arena does not have n bytes left. The request is rounded up to the alignment so the next
// allocation also starts aligned. It is safe to call concurrently: the bump cursor advances by a
// lock-free compare-and-swap, and a loser simply re-reads and retries, so no caller ever waits on
// another, which is the same wait-free-claim shape the WAL region claim uses.
func (a *arena) alloc(n uint64) (arenaOff, bool) {
	if n == 0 {
		n = 1
	}
	n = (n + arenaAlign - 1) &^ (arenaAlign - 1)
	for {
		start := a.next.Load()
		end := start + n
		if end < start || end > a.size { // overflow or past the end: the arena is full
			return arenaNil, false
		}
		if a.next.CompareAndSwap(start, end) {
			return arenaOff(start), true
		}
	}
}

// bytesAt returns a slice over the n arena bytes at off. The slice's capacity is capped to n (a
// three-index slice) so a caller cannot accidentally append past its allocation into a neighbor's
// bytes. The returned slice aliases the arena directly; it is valid until the arena is closed, and
// writes through it land in the arena. The caller is responsible for not reading or writing outside
// an allocation it actually made.
func (a *arena) bytesAt(off arenaOff, n uint64) []byte {
	start := uint64(off)
	return a.base[start : start+n : start+n]
}

// used reports how many bytes have been handed out, the bump cursor. It is a point-in-time read under
// concurrency; it never exceeds size.
func (a *arena) used() uint64 { return a.next.Load() }

// cap reports the arena's fixed capacity in bytes.
func (a *arena) cap() uint64 { return a.size }

// reset rewinds the bump cursor to empty so the whole region can be reused. It does not zero the
// bytes. It is not safe to call while any allocation from this arena is still in use or while another
// goroutine is allocating; it exists for tearing an arena down to a clean state for reuse, not for
// live reclamation, which is the slab free list's job.
func (a *arena) reset() { a.next.Store(0) }

// close returns the region to the operating system (munmap) on the off-heap path, or drops the heap
// slice for the GC on the fallback. After close the arena must not be used; bytesAt over a closed
// off-heap region would fault. It returns the munmap error, if any.
func (a *arena) close() error {
	b := a.base
	a.base = nil
	a.size = 0
	return munmapAnon(b)
}
