package f2

import (
	"sync"
	"sync/atomic"
)

// A shard owns one index and one log. Reads take no lock; writes take mu. The
// index is published behind an atomic pointer so a reader always sees a complete
// table even while a writer is swapping in a larger one during a grow.
//
// The cache-line pad keeps adjacent shards' write state off the same line, so a
// writer on shard i does not invalidate the line a reader on shard i+1 is
// touching. This matters because the store packs shards in a slice.
type shard struct {
	mu    sync.RWMutex
	index atomic.Pointer[index]
	log   *log

	// ep is the shared epoch state, set in durable mode and nil in memory-only. A
	// lock-free reader enters an epoch through it so a later compaction never reuses
	// a file block out from under the read; a memory-only shard frees nothing and
	// leaves it nil, keeping the hot path free of the guard.
	ep *epochs

	// budgeted is true when this shard's log evicts and recycles page buffers. A
	// budgeted read must not alias a buffer a writer could recycle, so it reads under
	// the shard read lock (getLocked) and copies its value out, excluding the evictor,
	// which holds the write lock. The memory-only and unbudgeted-durable profiles never
	// recycle and stay lock-free. It is fixed at construction (the budget is invariant
	// across compaction generations), so reading it lock-free to pick the path is
	// race-free.
	budgeted bool

	// inPlace is true on the durable evicting non-Full profile, the only one where a
	// hot same-size overwrite would otherwise grow a real durable log. There a SET that
	// repoints a key whose record still sits in the resident, unflushed tail page rewrites
	// the value over its existing bytes (FASTER's in-place update) instead of appending a
	// fresh record and stranding the old one. It is safe to mutate live bytes here because
	// a budgeted read takes the shard read lock (getLocked), which the write lock excludes,
	// so no reader can alias the bytes mid-rewrite. It is off on the memory-only and
	// unbudgeted-durable profiles, whose lock-free aliasing reads are the benchmarked
	// ceiling and must not see a byte change underfoot, and off under Full, which flushes
	// the tail per write so its bytes are already durable and in-place would be the ARIES
	// in-place durable mutation the append-only log exists to avoid.
	inPlace      bool
	inPlaceCount int64 // count of overwrites taken in place, read under mu by InPlaceUpdates

	// deferred holds file blocks a compaction retired, each waiting behind the safe
	// epoch before it returns to the allocator. Guarded by mu. Empty until the
	// compactor (a later increment) populates it.
	deferred []deferredFree

	// logBytes and deadBytes are write-side accounting, read under mu by Stats.
	logBytes  int64
	deadBytes int64

	_ [24]byte // pad the struct toward a cache line
}

// newShard builds a shard. df is nil for the memory-only core and the shared
// durable file in single-file mode; shardID names this shard's blocks in that
// file; budget is the resident page cap (zero unbounded); ep is the shared epoch
// state in durable mode, nil in memory-only.
func newShard(pageSize int, df *durableFile, shardID, budget int, ep *epochs) *shard {
	s := &shard{log: newLog(pageSize, df, shardID, budget), ep: ep, budgeted: budget > 0}
	s.inPlace = s.budgeted && df != nil && df.dial != DurabilityFull
	idx := newIndex(minIndexSlots)
	idx.log = s.log
	s.index.Store(idx)
	return s
}

// get is the read path. A budgeted shard recycles page buffers, so it reads under
// the shard read lock and copies the value out (getLocked), excluding the evictor.
// A memory-only shard probes directly with no lock. An unbudgeted durable shard pins
// an epoch first so a concurrent compaction never reuses a file block while this read
// is resolving an address or preading it; the bare path draws a fresh stripe per
// call, so a hot durable read loop should hold a Reader, which caches one.
func (s *shard) get(h uint64, key []byte) ([]byte, bool, error) {
	if s.budgeted {
		return s.getLocked(h, key)
	}
	if s.ep == nil {
		return s.lookup(h, key)
	}
	return s.getGuarded(h, key, s.ep.nextStripe.Add(1))
}

// getLocked reads a budgeted shard under the shard read lock and returns an owned
// copy of the value. The read lock excludes the evictor (it holds the write lock),
// so the page buffer the value is read from cannot be recycled mid-read; copying the
// value out before releasing the lock means the returned bytes never alias a buffer
// a later eviction could wipe and reuse. This mirrors the sibling hashlog engine's
// durable evicting profile, which reads the same way for the same reason. An evicted
// hit's value is already an owned copy from readEvicted, so the copy here is redundant
// for it, but a single branch-free copy on every budgeted hit is simpler than tracking
// which hits aliased a resident buffer, and it costs a few hundred nanoseconds against
// the microseconds of a pread.
func (s *shard) getLocked(h uint64, key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	v, ok, err := s.lookup(h, key)
	if ok && v != nil {
		v = append([]byte(nil), v...)
	}
	s.mu.RUnlock()
	return v, ok, err
}

// getGuarded pins the epoch on the given stripe for the duration of the lookup,
// then leaves. The guard must span the directory load and any pread inside lookup:
// an evicted read returns an owned copy made while the guard was held, and a
// resident read returns a slice into page RAM. The guard keeps a file block from
// being reused while lookup is computing or reading it. This path serves only the
// unbudgeted durable profile, which never evicts, so a resident hit's slice into page
// RAM stays valid (the garbage collector keeps the buffer alive) and is returned as
// is, preserving the zero-copy resident read.
func (s *shard) getGuarded(h uint64, key []byte, stripe uint64) ([]byte, bool, error) {
	g := s.ep.slots.enter(&s.ep.global, stripe)
	v, ok, err := s.lookup(h, key)
	g.leave()
	return v, ok, err
}

// lookup loads the current index without a lock, probes by fingerprint, and for
// each fingerprint match reads the candidate record from the log and compares the
// full key. A hit returns a slice aliasing the log page, which is immutable in the
// full-resident profile, so no copy and no lock are needed.
func (s *shard) lookup(h uint64, key []byte) ([]byte, bool, error) {
	idx := s.index.Load()
	mix := mixOf(h)
	fp := fpOf(mix)
	mask := idx.mask
	i := mix & mask
	for {
		slot := idx.slots[i].Load()
		if slot == 0 {
			return nil, false, nil // empty slot ends the probe chain
		}
		if slot&slotTombstone == 0 && slotFP(slot) == fp {
			off := slotAddr(slot)
			rkey, rval := idx.log.read(off)
			if bytesEqual(rkey, key) {
				return rval, true, nil
			}
		}
		i = (i + 1) & mask
	}
}

// scan visits every live key in this shard, calling fn with the key and value of
// each. It is lock-free, like get: it loads the current index once and walks its
// slots, and because a grow publishes a complete new table atomically and never
// mutates the old one, the walk sees a consistent snapshot of whatever table was
// current when it started. A concurrent overwrite or delete may make a key show
// its old or its new state, the same visibility get gives. fn returning false
// stops the walk; scan then returns false so the store can stop the whole scan.
// The key and value alias the log page and must not be mutated or retained.
func (s *shard) scan(fn func(key, value []byte) bool) bool {
	idx := s.index.Load()
	for i := range idx.slots {
		slot := idx.slots[i].Load()
		if slot == 0 || slot&slotTombstone != 0 {
			continue
		}
		key, val := idx.log.read(slotAddr(slot))
		if key == nil {
			continue // a torn or unreadable record, skip rather than report garbage
		}
		if !fn(key, val) {
			return false
		}
	}
	return true
}

// set appends a record and publishes its address in the index. It runs under the
// shard write lock so two writers never race on the same probe chain or the tail.
// An overwrite of an existing key repoints the slot with a single atomic store
// (read-copy-update): a reader either sees the old address or the new one, never
// a torn slot, and the old record's bytes stay valid for any reader mid-probe.
func (s *shard) set(h uint64, key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.inPlace && s.tryInPlace(h, key, value) {
		s.inPlaceCount++
		return nil
	}

	off, n, err := s.log.append(key, value, false)
	if err != nil {
		return err
	}
	s.logBytes += int64(n)

	idx := s.index.Load()
	if idx.shouldGrow() {
		idx = s.grow(idx)
	}
	s.put(idx, h, key, off, n)
	return nil
}

// tryInPlace handles a SET in place when the key is present and its record still sits
// in the resident, unflushed tail page with the same value size, and reports whether it
// did. It runs under the shard write lock, before the append path, on the in-place
// profile only. A hit rewrites the value over the record's existing bytes, leaving the
// log length, the dead-byte count, and the index slot untouched: the address does not
// move, so there is nothing to repoint. A miss (absent key, a size change, or a record
// that has rolled out of the tail or been flushed) returns false and SET appends as
// usual. The read of the old record here is the same read the append path's put would
// make to size the stranded bytes, so a miss does not read the log a second time on the
// common hot-key path, where the next overwrite hits.
func (s *shard) tryInPlace(h uint64, key, value []byte) bool {
	idx := s.index.Load()
	mix := mixOf(h)
	fp := fpOf(mix)
	mask := idx.mask
	i := mix & mask
	for {
		slot := idx.slots[i].Load()
		if slot == 0 {
			return false // key absent: nothing to overwrite in place, SET appends
		}
		if slot&slotTombstone == 0 && slotFP(slot) == fp {
			off := slotAddr(slot)
			rkey, rval := s.log.read(off)
			if bytesEqual(rkey, key) {
				if len(rval) != len(value) {
					return false // size change: the record span would differ, must append
				}
				return s.log.overwriteInPlace(off, key, value)
			}
		}
		i = (i + 1) & mask
	}
}

// put installs the address for h/key into idx, either claiming a fresh slot for a
// new key or repointing the slot an existing key already holds. The caller holds
// mu. It runs after any needed grow, so a free slot is guaranteed.
//
// A fingerprint match on a live slot is confirmed against the key before the slot
// is repointed. A fingerprint match on a tombstone is only a candidate home, not a
// confirmed one: the key may still hold a live slot further down the chain (it was
// placed past this slot while this slot was occupied by another key, which was then
// deleted). So the probe remembers the first tombstone but keeps going, reusing it
// only on reaching an empty slot. Stopping at the tombstone would leave the live
// slot behind as a duplicate, and a later delete of the new slot would resurrect
// the old value from it.
func (s *shard) put(idx *index, h uint64, key []byte, off int64, n int) {
	mix := mixOf(h)
	fp := fpOf(mix)
	mask := idx.mask
	i := mix & mask
	firstTomb := -1
	for {
		slot := idx.slots[i].Load()
		if slot == 0 {
			if firstTomb >= 0 {
				idx.slots[firstTomb].Store(makeSlot(fp, off))
				idx.live++ // a tombstone slot coming back to life; used already counts it
				return
			}
			idx.slots[i].Store(makeSlot(fp, off))
			idx.live++
			idx.used++
			return
		}
		if slotFP(slot) == fp {
			if slot&slotTombstone != 0 {
				if firstTomb < 0 {
					firstTomb = int(i)
				}
			} else {
				// Read the old record once: it both confirms the key and gives the
				// stranded-byte count, so an overwrite touches the log a single time.
				rkey, rval := s.log.read(slotAddr(slot))
				if bytesEqual(rkey, key) {
					idx.slots[i].Store(makeSlot(fp, off))
					s.deadBytes += int64(s.log.recordLenKV(rkey, rval))
					return
				}
			}
		}
		i = (i + 1) & mask
	}
}

// del marks the key's slot as a tombstone if present. The record stays in the log
// (it is reclaimed only by compaction), but the slot no longer resolves to it.
// A tombstone is not an empty slot, so it does not break a probe chain that runs
// through it. In durable mode a delete of a present key also appends a tombstone
// record, so recovery sees the deletion and does not resurrect the key from its
// earlier value record. An absent key logs nothing.
func (s *shard) del(h uint64, key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.delLocked(s.index.Load(), h, key) {
		return nil // absent: nothing to mark, nothing to log
	}
	if s.log.df != nil {
		_, n, err := s.log.append(key, nil, true)
		if err != nil {
			return err
		}
		s.logBytes += int64(n)
	}
	return nil
}

// delLocked tombstones the slot for h/key in idx, returning whether the key was
// present and live. It is the index half of del, shared with recovery replay. The
// caller holds mu.
func (s *shard) delLocked(idx *index, h uint64, key []byte) bool {
	mix := mixOf(h)
	fp := fpOf(mix)
	mask := idx.mask
	i := mix & mask
	for {
		slot := idx.slots[i].Load()
		if slot == 0 {
			return false // not present
		}
		if slot&slotTombstone == 0 && slotFP(slot) == fp {
			// One read confirms the key and sizes its stranded bytes.
			rkey, rval := s.log.read(slotAddr(slot))
			if bytesEqual(rkey, key) {
				idx.slots[i].Store(slot | slotTombstone)
				idx.live--
				s.deadBytes += int64(s.log.recordLenKV(rkey, rval))
				return true
			}
		}
		i = (i + 1) & mask
	}
}

// recoverApply replays one already-logged record into the index during recovery:
// a value record installs or repoints the key's slot, a tombstone marks it
// deleted. The record bytes are already in the log at off, so this only edits the
// index, growing it first when the load factor demands. The caller holds mu (or
// runs single-threaded during open).
func (s *shard) recoverApply(h uint64, key []byte, off int64, n int, tombstone bool) {
	idx := s.index.Load()
	if idx.shouldGrow() {
		idx = s.grow(idx)
	}
	if tombstone {
		s.delLocked(idx, h, key)
		return
	}
	s.put(idx, h, key, off, n)
}

// grow doubles the index and replays every live slot into the new table, dropping
// tombstones so they do not accumulate. The new table is published with an atomic
// store, so a concurrent reader either finishes against the old table (still
// valid, it is not mutated during the replay) or sees the complete new one. The
// caller holds mu.
//
// The replay needs no log read. The slot's fingerprint is the low bits of the home
// hash, so for any table no wider than 2^slotFPBits slots the new home position is
// fingerprint & mask, taken straight from the slot. On a budgeted shard this turns
// what was a pread per live key under the write lock, a rehash latency cliff, into
// pure arithmetic. Past 2^slotFPBits slots the fingerprint no longer spans the mask
// and the replay falls back to rehashing the key from the log; that is beyond the
// documented per-shard key ceiling and effectively unreached in a supported store.
func (s *shard) grow(old *index) *index {
	ni := newIndex(len(old.slots) * 2)
	ni.log = old.log // a grow moves the slots, never the log they point into
	readFree := ni.mask <= slotFPValueMask
	for i := range old.slots {
		slot := old.slots[i].Load()
		if slot == 0 || slot&slotTombstone != 0 {
			continue
		}
		var j uint64
		if readFree {
			j = slotFP(slot) & ni.mask
		} else {
			rkey, _ := s.log.read(slotAddr(slot))
			j = mixOf(hash64(rkey)) & ni.mask
		}
		for ni.slots[j].Load() != 0 {
			j = (j + 1) & ni.mask
		}
		ni.slots[j].Store(slot)
		ni.live++
		ni.used++
	}
	s.index.Store(ni)
	return ni
}
