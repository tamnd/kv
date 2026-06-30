package hlog

import (
	"bytes"
	"sync/atomic"
)

// readCache is a direct-mapped cache over cold reads: a key maps to one cell by its
// fingerprint, and a new entry for that cell overwrites whatever was there, so the collision
// is the eviction and there is no policy to maintain. A repeated cold key is served from RAM
// instead of the file, which also sidesteps the Windows file-handle serialization note 178
// measured on the cold read path.
//
// Each cell is an atomic pointer to an immutable entry, copy-on-write. A put builds a fresh
// entry and atomically swaps it in; a get atomically loads the current entry and reads it,
// and because the entry is never mutated after it is published the reader can read it without
// a lock and without tearing. This is the race-clean Go idiom for a concurrently read and
// written cell. The textbook answer, a per-cell seqlock with in-place rewrites, is rejected
// for cause: Go's memory model does not sanction the non-atomic payload reads a seqlock makes,
// and the race detector flags them, so it cannot ship. Against a per-cell mutex the board is a
// platform-dependent near-tie (lab/cache, note 180); COW is chosen because its lock-free read
// composes with the rest of the design and its only cost, a per-fill allocation, lands on a
// cold miss, off the hot read path, and is rare exactly when the cache is doing its job.
type readCache struct {
	cells []atomic.Pointer[cacheEntry]
	mask  uint64
}

// cacheEntry is immutable once stored, which is what lets a reader read it lock-free. key and
// value are copied in at put time so they do not alias the caller's buffers.
type cacheEntry struct {
	key []byte
	val []byte
}

func newReadCache(cells int) *readCache {
	n := 1
	for n < cells {
		n <<= 1
	}
	return &readCache{cells: make([]atomic.Pointer[cacheEntry], n), mask: uint64(n - 1)}
}

// put installs key and value into the cell the fingerprint maps to, overwriting the previous
// occupant with one atomic store. It copies key and value into the entry so the entry owns its
// bytes and stays valid for any reader after the caller reuses its buffers.
func (c *readCache) put(fp uint64, key, value []byte) {
	e := &cacheEntry{
		key: append([]byte(nil), key...),
		val: append([]byte(nil), value...),
	}
	c.cells[fp&c.mask].Store(e)
}

// invalidate clears the cell the fingerprint maps to with one atomic store, so a later read of
// that cell misses and refills from the live tier. A write calls it after appending a newer
// record for the key, because the cached value, read from cold, goes stale the moment that newer
// record exists. Clearing by cell is correct even on a fingerprint collision: it may evict an
// unrelated key that shares the slot, which only costs that key one cold read.
func (c *readCache) invalidate(fp uint64) {
	c.cells[fp&c.mask].Store(nil)
}

// get returns the cached value for key, copied into scratch, if the cell holds it. It loads
// the cell's current entry atomically and verifies the key, since the cell is direct-mapped
// and may hold a different key that hashed to the same slot.
func (c *readCache) get(fp uint64, key, scratch []byte) ([]byte, bool) {
	e := c.cells[fp&c.mask].Load()
	if e == nil || !bytes.Equal(e.key, key) {
		return nil, false
	}
	return append(scratch[:0], e.val...), true
}
