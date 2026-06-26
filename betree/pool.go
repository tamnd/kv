package betree

// This file is the third M6 slice, the pooled transients half of the memory substrate (doc 05
// section 5, D10). The arena and slab allocator handle the big long-lived buffers; this handles the
// other half, the small short-lived per-operation objects the hot path allocates and drops on every
// request: message-encode scratch buffers, gather scratch, the per-operation bookkeeping structs that
// hold Go pointers and so cannot live in a pointer-free arena. Pooling these is the first-line
// GC-pressure reduction, the one Pebble's experience says to do before going off-heap, because it
// drives the hot path's transient allocation rate toward zero so a collection has almost nothing to
// trace. Built alongside the engine, off the live path; nothing on a request path pools through it yet
// (the call sites in the encode and gather paths are wired at the M8 flip). This closes the M6
// substrate: arena, slabs, and now pooled transients.
//
// Two disciplines, both load-bearing, from the doc.
//
// Pool pointers, not values. A sync.Pool stores its items in an interface, and putting a value type
// into an interface boxes it, which heap-allocates on every Put and defeats the entire purpose. So
// the pool stores a pointer: *[]byte, not []byte, and *T, not T. Putting a pointer into the interface
// stores the pointer word with no per-Put allocation. The generic pool here only ever holds *T, so a
// caller cannot accidentally pool a value; the type makes the discipline structural rather than a
// rule to remember.
//
// Reset on put, not on get. A pooled object carries its previous contents until reset, and a scratch
// buffer reused without resetting would leak stale bytes into the next operation. The pool resets on
// Put (truncate the buffer to length zero, keeping its capacity so the next user writes into the same
// backing array) so an object on the free list is always clean and a Get hands back something ready
// to use. Resetting on Put rather than Get also means the reset cost is paid by the goroutine
// releasing the object, off the path of the goroutine that needs it.
//
// Lean on the victim cache. Go's sync.Pool keeps a victim cache: an object not retrieved during one
// GC cycle is not freed immediately but moved to a victim list and given one more cycle, which smooths
// the pool across collections so it is not fully drained on every GC and then thrashing to refill.
// The engine sizes its hot-path pooling to lean on this, so a steady-state operation almost never
// allocates a fresh transient: it recycles one from the pool or the victim cache. That is the
// near-zero hot-path allocation rate pooling delivers, and it is why D10 does this before the arenas.

import "sync"

// pool is a typed wrapper over sync.Pool that holds pointers only and resets on put. T is the pooled
// value type; the pool stores *T, so the pointer-not-value discipline is enforced by construction.
type pool[T any] struct {
	sp    sync.Pool
	reset func(*T)
}

// newPool builds a pool. alloc constructs a fresh *T when the pool and its victim cache are both
// empty; reset returns an item to a clean state on put and may be nil if T needs none. alloc must
// return a non-nil pointer.
func newPool[T any](alloc func() *T, reset func(*T)) *pool[T] {
	p := &pool[T]{reset: reset}
	p.sp.New = func() any { return alloc() }
	return p
}

// get returns a ready-to-use *T, recycled from the pool or its victim cache when one is available and
// freshly allocated through alloc only when both are empty. The item is already reset (it was reset on
// the put that returned it, or freshly allocated clean), so the caller can use it immediately.
func (p *pool[T]) get() *T { return p.sp.Get().(*T) }

// put resets x and returns it to the pool for reuse. A nil x is ignored so a caller need not guard.
// After put the caller must not touch x: it belongs to the pool and another goroutine may take it.
func (p *pool[T]) put(x *T) {
	if x == nil {
		return
	}
	if p.reset != nil {
		p.reset(x)
	}
	p.sp.Put(x)
}

// scratchPool pools reusable byte scratch buffers of a fixed capacity, the message-encode and gather
// scratch the hot path would otherwise allocate per operation. It pools *[]byte, the pointer the
// discipline requires, and resets each buffer to length zero on put while keeping its capacity, so a
// reused buffer writes into the same backing array with no fresh allocation.
type scratchPool struct {
	p    *pool[[]byte]
	size int
}

// newScratchPool builds a scratch pool whose buffers start with capacity size and length zero. A get
// hands back a *[]byte the caller appends into; a buffer that grows past size on some operation is
// reset back to a zero-length slice over its (now larger) backing array on put, so the pool naturally
// keeps whatever capacity its users have needed.
func newScratchPool(size int) *scratchPool {
	if size < 0 {
		size = 0
	}
	return &scratchPool{
		size: size,
		p: newPool(
			func() *[]byte { b := make([]byte, 0, size); return &b },
			func(b *[]byte) { *b = (*b)[:0] },
		),
	}
}

// get returns a *[]byte with length zero, ready to append into. The caller writes through the
// pointer (buf := *ref; buf = append(buf, ...); *ref = buf) so a reallocation on growth is carried
// back into the pooled pointer and kept on put.
func (sp *scratchPool) get() *[]byte { return sp.p.get() }

// put resets the buffer to length zero and returns it for reuse. The caller must not touch the buffer
// after put.
func (sp *scratchPool) put(b *[]byte) { sp.p.put(b) }

// capHint reports the capacity fresh buffers start at, for diagnostics.
func (sp *scratchPool) capHint() int { return sp.size }
