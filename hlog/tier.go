package hlog

import (
	"bytes"
	"encoding/binary"
	"hash/maphash"
	"runtime"
	"sync"
	"sync/atomic"
)

// TieredDB is the F2 flaw fix: it splits a small mutable hot tier from the compact cold tier
// so the index the write path touches stays bounded to the working set, which note 179
// measured is a 4x to 11x write win over an index sized to the whole keyspace. The cold tier
// is the step-three hybrid log, larger than memory in one file; the hot tier lives in RAM.
//
// The shape is the one a log-structured store uses to keep its in-memory index bounded by
// construction, a memtable and an immutable memtable, applied to FASTER's address space:
//   - active: the segment writes land in, an in-memory append region with its own small
//     index. Lock-free append, fetch-add the tail.
//   - sealed: a full segment being drained to cold by a background migrator. At most one
//     exists at a time. When the drain finishes the whole segment and its index are dropped,
//     so the hot index never accumulates dead keys; it is thrown away, not garbage-collected
//     entry by entry. This is what keeps the hot index small without a delete or a rebuild.
//   - cold: the step-three DB, the spill-to-disk tier the sealed records migrate into.
//   - cache: a read cache over cold reads, so a repeated cold key is served from RAM instead
//     of the file, which also removes the Windows file-handle serialization note 178 found.
//
// Reads check active, then sealed, then the cache, then cold. Each hot segment verifies the
// stored key, so a stale index entry, an entry whose record was superseded by a newer write,
// is a clean miss that falls through to the right tier. Because a segment is only ever sealed
// after the previous sealed segment has fully drained, version order is preserved: the older
// value reaches cold before the newer one can, so the newest write always wins.
type TieredDB struct {
	mu       sync.Mutex // guards the seal-and-swap only; the read and write fast paths are lock-free
	active   atomic.Pointer[segment]
	sealed   atomic.Pointer[segment]
	cold     *DB
	cache    *readCache
	segBytes int64
	segKeys  int
	seed     maphash.Seed

	migrate chan *segment
	closed  chan struct{}
	wg      sync.WaitGroup
}

// segment is an in-memory append region plus its own index. It is write-once per record: an
// update appends a new record and repoints the index, so a reader that observes an index
// entry sees a fully written record, the index store being the publish. Its capacity is
// fixed; put reports full instead of overflowing so the caller seals and retries.
type segment struct {
	buf  []byte
	cap  int64
	tail atomic.Int64
	ix   *Index
}

func newSegment(capBytes int64, capKeys int) *segment {
	return &segment{buf: make([]byte, capBytes), cap: capBytes, ix: NewIndex(capKeys)}
}

// put writes the record for key and value and indexes it at its address. It returns false if
// the segment cannot fit the record, in which case nothing was indexed and the caller should
// seal this segment and retry on a fresh one. The tail may overshoot cap on a failed put,
// which is harmless because a failed put indexes nothing and the segment is about to be sealed.
func (s *segment) put(fp uint64, key, value []byte) bool {
	payload := keyLenSize + len(key) + len(value)
	total := int64(hdrLen + payload)
	off := s.tail.Add(total) - total
	if off+total > s.cap {
		return false
	}
	binary.LittleEndian.PutUint32(s.buf[off:], uint32(payload))
	p := off + hdrLen
	binary.LittleEndian.PutUint16(s.buf[p:], uint16(len(key)))
	copy(s.buf[p+keyLenSize:], key)
	copy(s.buf[p+keyLenSize+int64(len(key)):], value)
	s.ix.Put(fp, off) // the index store publishes the record: a reader that sees it sees the bytes
	return true
}

// get returns the value for key if this segment holds the live version, copied into scratch so
// it stays valid after the segment is dropped. It verifies the stored key, so a stale index
// entry whose record was superseded is a clean miss.
func (s *segment) get(fp uint64, key, scratch []byte) ([]byte, bool) {
	addr, ok := s.ix.Get(fp)
	if !ok {
		return nil, false
	}
	payload := int64(binary.LittleEndian.Uint32(s.buf[addr:]))
	p := addr + hdrLen
	klen := int64(binary.LittleEndian.Uint16(s.buf[p:]))
	if klen+keyLenSize > payload {
		return nil, false
	}
	if !bytes.Equal(s.buf[p+keyLenSize:p+keyLenSize+klen], key) {
		return nil, false
	}
	v := s.buf[p+keyLenSize+klen : p+payload]
	return append(scratch[:0], v...), true
}

// rangeRecords calls fn for every record in the segment in address order, up to the committed
// tail. The migrator uses it to walk a sealed segment; fn gets the address so it can ask the
// index whether this record is still the live version for its key.
func (s *segment) rangeRecords(fn func(addr int64, key, value []byte)) {
	end := min(s.tail.Load(), s.cap)
	for off := int64(0); off+hdrLen <= end; {
		payload := int64(binary.LittleEndian.Uint32(s.buf[off:]))
		if payload <= 0 || off+hdrLen+payload > end {
			return
		}
		p := off + hdrLen
		klen := int64(binary.LittleEndian.Uint16(s.buf[p:]))
		if klen+keyLenSize > payload {
			return
		}
		key := s.buf[p+keyLenSize : p+keyLenSize+klen]
		value := s.buf[p+keyLenSize+klen : p+payload]
		fn(off, key, value)
		off += hdrLen + payload
	}
}

// OpenTiered creates a tiered store at path. segBytes sizes one hot segment and segKeys its
// index, so the hot index is bounded to one segment's worth of keys, the small resident table
// note 179 wants. ringBytes and coldKeys size the cold hybrid log and its index; cacheBytes
// sizes the read cache.
func OpenTiered(path string, segBytes int64, segKeys int, ringBytes int64, coldKeys int, cacheCells int) (*TieredDB, error) {
	cold, err := OpenDB(path, ringBytes, coldKeys)
	if err != nil {
		return nil, err
	}
	t := &TieredDB{
		cold:     cold,
		cache:    newReadCache(cacheCells),
		segBytes: segBytes,
		segKeys:  segKeys,
		seed:     maphash.MakeSeed(),
		migrate:  make(chan *segment, 1),
		closed:   make(chan struct{}),
	}
	t.active.Store(newSegment(segBytes, segKeys))
	t.wg.Add(1)
	go t.migrateLoop()
	return t, nil
}

// Set stores value under key in the hot tier. The common path is one lock-free append into the
// active segment. When the active segment fills, Set seals it, hands it to the background
// migrator, and installs a fresh active segment, then retries; this is the only path that
// takes the lock, and it runs once per segment, not once per write.
func (t *TieredDB) Set(key, value []byte) {
	fp := forceFP(maphash.Bytes(t.seed, key))
	for {
		a := t.active.Load()
		if a.put(fp, key, value) {
			return
		}
		t.seal(a)
	}
}

// seal swaps the full segment out for a fresh one and queues it for migration. It re-checks
// under the lock so that only the first caller to see a given full segment seals it; the rest
// observe the already-swapped active and loop back to retry their put. If a previous sealed
// segment is still draining, seal waits for it, which is the backpressure that bounds hot
// memory to two segments and preserves version order across seals.
func (t *TieredDB) seal(full *segment) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active.Load() != full {
		return // another writer already sealed this one
	}
	for t.sealed.Load() != nil {
		t.mu.Unlock()
		runtime.Gosched()
		t.mu.Lock()
		if t.active.Load() != full {
			return
		}
	}
	t.sealed.Store(full)
	t.active.Store(newSegment(t.segBytes, t.segKeys))
	t.migrate <- full
}

// Get returns the value for key. It checks the hot tiers first, active then sealed, then the
// read cache, then cold, populating the cache on a cold hit. scratch is a caller buffer the
// value is copied into and may be reused across calls.
func (t *TieredDB) Get(key, scratch []byte) ([]byte, bool, error) {
	fp := forceFP(maphash.Bytes(t.seed, key))
	if v, ok := t.active.Load().get(fp, key, scratch); ok {
		return v, true, nil
	}
	if s := t.sealed.Load(); s != nil {
		if v, ok := s.get(fp, key, scratch); ok {
			return v, true, nil
		}
	}
	if v, ok := t.cache.get(fp, key, scratch); ok {
		return v, true, nil
	}
	v, ok, err := t.cold.Get(key, scratch)
	if err != nil || !ok {
		return nil, false, err
	}
	t.cache.put(fp, key, v)
	return v, true, nil
}

// migrateLoop drains sealed segments into cold in the background. For each sealed segment it
// walks the records and migrates the ones still live, the ones whose index entry still points
// at the record, dropping superseded records as compaction. When a segment is fully drained
// it clears sealed, which releases any writer waiting in seal, and the segment and its index
// become garbage.
func (t *TieredDB) migrateLoop() {
	defer t.wg.Done()
	for {
		select {
		case seg := <-t.migrate:
			t.drain(seg)
			t.sealed.Store(nil)
		case <-t.closed:
			// Drain anything still queued so Close persists every set.
			for {
				select {
				case seg := <-t.migrate:
					t.drain(seg)
					t.sealed.Store(nil)
				default:
					return
				}
			}
		}
	}
}

// drain migrates the live records of one sealed segment into cold. A record is live when the
// segment's index still maps its key to this record's address; a record superseded by a later
// write in the same segment points elsewhere and is dropped.
func (t *TieredDB) drain(seg *segment) {
	seg.rangeRecords(func(addr int64, key, value []byte) {
		fp := forceFP(maphash.Bytes(t.seed, key))
		if cur, ok := seg.ix.Get(fp); ok && cur == addr {
			t.cold.Set(key, value)
		}
	})
}

// Sync forces a durability barrier: it seals and drains the hot tier into cold, then fsyncs
// cold, so every set so far is on disk and recoverable. It is the crash-safe checkpoint point.
// The normal path leaves the hot tier in memory and relies on Close to migrate it; between
// syncs a crash loses the un-migrated hot records, bounded to at most two segments, which is
// the cost of keeping the hot write path lock-free and allocation-light. Callers that need a
// tighter window call Sync more often.
func (t *TieredDB) Sync() error {
	a := t.active.Load()
	if a.tail.Load() > 0 {
		t.seal(a)
	}
	t.drainPending()
	return t.cold.Sync()
}

// drainPending blocks until the migrator has emptied the seal queue and cleared the sealed
// slot, so every sealed record has reached cold. It is the wait a durability barrier needs.
func (t *TieredDB) drainPending() {
	for {
		if len(t.migrate) == 0 && t.sealed.Load() == nil {
			return
		}
		runtime.Gosched()
	}
}

// Close stops the migrator after draining queued segments, then closes the cold tier so every
// set is on disk. A segment still in active at Close is migrated first so its records are not lost.
func (t *TieredDB) Close() error {
	// Seal whatever is in active so its records are queued for the final drain.
	a := t.active.Load()
	if a.tail.Load() > 0 {
		t.seal(a)
	}
	close(t.closed)
	t.wg.Wait()
	return t.cold.Close()
}
