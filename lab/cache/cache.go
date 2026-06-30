// Package cache is a frozen experiment: how should a read-cache cell be read and written
// concurrently? The cache is read far more than written, a cold miss fills it once and many
// reads serve from it, so the read path is what matters.
//
// The decision driver here is correctness, not raw speed. The textbook answer, a per-cell
// seqlock with in-place rewrites, is disqualified outright: Go's memory model does not sanction
// its non-atomic payload reads and the race detector flags them, so it cannot ship. That leaves
// the atomic-pointer copy-on-write cell and a per-cell read-write mutex, and between those two
// the board is a platform-dependent near-tie: COW wins on the M4 (1.75x) and the EPYC (1.6x),
// where its lock-free read pays off, but the mutex wins on the 32-core i9 (1.5x), where COW's
// per-fill allocation turns into GC pressure the many-core box feels.
//
// The engine carries COW because its lock-free read composes with the rest of the lock-free
// design and the per-fill allocation lands on a cold miss only, off the hot read path, and
// because the cache earns its keep only when its hit rate is high, which is exactly when fills
// are rare. The mutex stays here as a live alternative for an allocation-sensitive,
// churn-heavy deployment. The full board is in impl note 180.
package cache

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// COWCache is the winner: a direct-mapped cache whose cells are atomic pointers to immutable
// entries.
type COWCache struct {
	cells []atomic.Pointer[cowEntry]
	mask  uint64
}

type cowEntry struct {
	key []byte
	val []byte
}

func NewCOWCache(cells int) *COWCache {
	n := pow2(cells)
	return &COWCache{cells: make([]atomic.Pointer[cowEntry], n), mask: uint64(n - 1)}
}

func (c *COWCache) Put(fp uint64, key, value []byte) {
	e := &cowEntry{
		key: append([]byte(nil), key...),
		val: append([]byte(nil), value...),
	}
	c.cells[fp&c.mask].Store(e)
}

func (c *COWCache) Get(fp uint64, key, scratch []byte) ([]byte, bool) {
	e := c.cells[fp&c.mask].Load()
	if e == nil || !bytes.Equal(e.key, key) {
		return nil, false
	}
	return append(scratch[:0], e.val...), true
}

// MutexCache is the loser kept for the comparison: a per-cell read-write mutex over in-place
// key and value buffers. It avoids the per-fill allocation, at the cost of a lock on every read.
type MutexCache struct {
	cells []mutexCell
	mask  uint64
}

type mutexCell struct {
	mu  sync.RWMutex
	key []byte
	val []byte
}

func NewMutexCache(cells int) *MutexCache {
	n := pow2(cells)
	return &MutexCache{cells: make([]mutexCell, n), mask: uint64(n - 1)}
}

func (c *MutexCache) Put(fp uint64, key, value []byte) {
	cell := &c.cells[fp&c.mask]
	cell.mu.Lock()
	cell.key = append(cell.key[:0], key...)
	cell.val = append(cell.val[:0], value...)
	cell.mu.Unlock()
}

func (c *MutexCache) Get(fp uint64, key, scratch []byte) ([]byte, bool) {
	cell := &c.cells[fp&c.mask]
	cell.mu.RLock()
	if !bytes.Equal(cell.key, key) {
		cell.mu.RUnlock()
		return nil, false
	}
	scratch = append(scratch[:0], cell.val...)
	cell.mu.RUnlock()
	return scratch, true
}

func pow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
