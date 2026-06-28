package f2

import (
	"sync"
	"sync/atomic"
)

// shardShift selects the bits of a hash used to pick a shard. We take the high
// byte so the shard choice is independent of the low bits the index uses to pick
// a slot, which keeps both distributions clean.
const shardShift = 56

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

	// logBytes and deadBytes are write-side accounting, read under mu by Stats.
	logBytes  int64
	deadBytes int64

	_ [24]byte // pad the struct toward a cache line
}

// newShard builds a shard. df is nil for the memory-only core and the shared
// durable file in single-file mode; shardID names this shard's blocks in that
// file; budget is the resident page cap (zero unbounded).
func newShard(pageSize int, df *durableFile, shardID, budget int) *shard {
	s := &shard{log: newLog(pageSize, df, shardID, budget)}
	s.index.Store(newIndex(minIndexSlots))
	return s
}

// get is the lock-free read path. It loads the current index without a lock,
// probes by tag, and for each tag match reads the candidate record from the log
// and compares the full key. A hit returns a slice aliasing the log page, which
// is immutable in the full-resident profile, so no copy and no lock are needed.
func (s *shard) get(h uint64, key []byte) ([]byte, bool, error) {
	idx := s.index.Load()
	tag := tagOf(h)
	mask := idx.mask
	i := (h ^ (h >> 15)) & mask // spread the home slot away from the shard bits
	for {
		slot := idx.slots[i].Load()
		if slot == 0 {
			return nil, false, nil // empty slot ends the probe chain
		}
		if slot&slotTombstone == 0 && slotTag(slot) == tag {
			off := slotAddr(slot)
			rkey, rval := s.log.read(off)
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
		key, val := s.log.read(slotAddr(slot))
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

// put installs the address for h/key into idx, either claiming a fresh slot for a
// new key or repointing the slot an existing key already holds. The caller holds
// mu. It runs after any needed grow, so a free slot is guaranteed.
func (s *shard) put(idx *index, h uint64, key []byte, off int64, n int) {
	tag := tagOf(h)
	mask := idx.mask
	i := (h ^ (h >> 15)) & mask
	for {
		slot := idx.slots[i].Load()
		if slot == 0 {
			idx.slots[i].Store(makeSlot(tag, off))
			idx.live++
			idx.used++
			return
		}
		// A tombstone slot or a tag+key match is the same key's old home: reuse it.
		if slotTag(slot) == tag {
			if slot&slotTombstone != 0 {
				idx.slots[i].Store(makeSlot(tag, off))
				idx.live++ // a tombstone slot coming back to life
				return
			}
			// Read the old record once: it both confirms the key and gives the
			// stranded-byte count, so an overwrite touches the log a single time.
			rkey, rval := s.log.read(slotAddr(slot))
			if bytesEqual(rkey, key) {
				idx.slots[i].Store(makeSlot(tag, off))
				s.deadBytes += int64(s.log.recordLenKV(rkey, rval))
				return
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
	tag := tagOf(h)
	mask := idx.mask
	i := (h ^ (h >> 15)) & mask
	for {
		slot := idx.slots[i].Load()
		if slot == 0 {
			return false // not present
		}
		if slot&slotTombstone == 0 && slotTag(slot) == tag {
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
func (s *shard) grow(old *index) *index {
	ni := newIndex(len(old.slots) * 2)
	for i := range old.slots {
		slot := old.slots[i].Load()
		if slot == 0 || slot&slotTombstone != 0 {
			continue
		}
		// Recover the home slot from the record's key hash, since the tag alone is
		// not enough to recompute the home position in the larger table.
		rkey, _ := s.log.read(slotAddr(slot))
		h := hash64(rkey)
		j := (h ^ (h >> 15)) & ni.mask
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
