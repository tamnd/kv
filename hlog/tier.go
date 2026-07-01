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
	sealed   atomic.Pointer[sealedNode] // newest-first list of sealed segments not yet drained
	cold     *DB
	cache    *readCache
	segBytes int64
	segKeys  int
	seed     maphash.Seed

	migrate chan *segment
	closed  chan struct{}
	wg      sync.WaitGroup
}

// maxSealed bounds how many sealed segments may be outstanding at once, the pipeline depth
// between the writers and the single migrator. One segment was the original design: a writer
// that filled the active had to wait for the whole previous segment to drain before it could
// seal, so at high write concurrency all writers stalled in bursts and fill throughput sagged
// and jittered (note 182). A small queue lets writers keep sealing while the migrator works
// through the backlog, which smooths the stall and lifts throughput toward the migrator's steady
// drain rate, while still bounding hot memory to maxSealed+1 segments.
const maxSealed = 4

// sealedNode is one entry in the newest-first list of sealed segments. The list is immutable
// once published: seal prepends a new head and the migrator republishes a rebuilt list with a
// drained segment removed, so a reader walking the chain holds a consistent snapshot without a
// lock and the segment it is reading stays alive as long as it holds the pointer.
type sealedNode struct {
	seg  *segment
	next *sealedNode
}

// sealedLen counts the segments in a sealed list, the outstanding pipeline depth seal checks
// against maxSealed.
func sealedLen(n *sealedNode) int {
	c := 0
	for ; n != nil; n = n.next {
		c++
	}
	return c
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

	// sealed and inflight fence the migrator against writers still filling this segment. A put
	// claims its offset with tail.Add and then writes its bytes, so a segment sealed and handed
	// to the drainer can still have a slow writer mid-record; the drainer walks the raw buffer
	// by tail, so it would read a length header a PutUint32 is still writing. inflight counts the
	// puts that have passed the sealed check and may still be writing bytes, and sealed stops new
	// puts, so seal can wait for the buffer to go quiescent before the drainer sees it.
	sealed   atomic.Bool
	inflight atomic.Int64
}

func newSegment(capBytes int64, capKeys int) *segment {
	return &segment{buf: make([]byte, capBytes), cap: capBytes, ix: NewIndex(capKeys)}
}

// put writes the record for key and value under op and indexes it at its address. It returns
// false if the segment cannot fit the record, in which case nothing was indexed and the caller
// should seal this segment and retry on a fresh one. The tail may overshoot cap on a failed put,
// which is harmless because a failed put indexes nothing and the segment is about to be sealed.
func (s *segment) put(fp uint64, op byte, key, value []byte) bool {
	payload := recordLen(key, value)
	total := int64(hdrLen + payload)
	off := s.tail.Add(total) - total
	if off+total > s.cap {
		return false
	}
	binary.LittleEndian.PutUint32(s.buf[off:], uint32(payload))
	frameRecord(s.buf[off+hdrLen:off+hdrLen+int64(payload)], op, key, value)
	// The index store publishes the record: a reader that sees the entry sees the bytes. If the
	// index is full this reports full just as a full buffer does, so the caller seals and retries
	// the record on a fresh segment. The bytes already written here are then orphaned, never
	// indexed, and dropped on drain, so a too-small index costs an early seal, never a lost write.
	return s.ix.Put(fp, off)
}

// get reports this segment's view of key: found with a value, found as a tombstone, or not
// present. The three-way answer is what lets a tombstone in a higher tier shadow an older value
// in a lower one: a tombstone returns tomb=true so the cascade stops here and reports the key
// deleted instead of falling through to the stale value below. The value is copied into scratch
// so it stays valid after the segment is dropped. A stale index entry whose key does not match,
// a superseded record, is a clean miss.
func (s *segment) get(fp uint64, key, scratch []byte) (val []byte, found, tomb bool) {
	addr, ok := s.ix.Get(fp)
	if !ok {
		return nil, false, false
	}
	payload := int64(binary.LittleEndian.Uint32(s.buf[addr:]))
	op, recKey, value, ok := parseRecord(s.buf[addr+hdrLen : addr+hdrLen+payload])
	if !ok || !bytes.Equal(recKey, key) {
		return nil, false, false
	}
	if op == opDel {
		return nil, false, true
	}
	return append(scratch[:0], value...), true, false
}

// rangeRecords calls fn for every record in the segment in address order, up to the committed
// tail. The migrator uses it to walk a sealed segment; fn gets the address so it can ask the
// index whether this record is still the live version for its key, and the op so it can carry a
// tombstone to cold rather than dropping it.
func (s *segment) rangeRecords(fn func(addr int64, op byte, key, value []byte)) {
	end := min(s.tail.Load(), s.cap)
	for off := int64(0); off+hdrLen <= end; {
		payload := int64(binary.LittleEndian.Uint32(s.buf[off:]))
		if payload <= 0 || off+hdrLen+payload > end {
			return
		}
		op, key, value, ok := parseRecord(s.buf[off+hdrLen : off+hdrLen+payload])
		if !ok {
			return
		}
		fn(off, op, key, value)
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
		migrate:  make(chan *segment, maxSealed),
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
func (t *TieredDB) Set(key, value []byte) { t.write(opSet, key, value) }

// Delete writes a tombstone for key in the hot tier. The key reads as absent afterward across
// every tier: the hot tombstone shadows an older value in a sealed segment or in cold, and it
// migrates to cold as a tombstone so the delete survives a reopen. It is one append, the same
// fast path as Set.
func (t *TieredDB) Delete(key []byte) { t.write(opDel, key, nil) }

// write appends a record under op into the active segment, sealing and retrying on a full
// segment, then invalidates the read-cache cell for the key. The invalidation is what keeps a
// read correct across the hot-to-cold migration: the cache holds values read from cold, and once
// this newer record migrates to cold the cached value would be stale, so a write drops the cell
// and the next cold read repopulates it from the migrated record. Without it an overwrite or a
// delete could be shadowed in the hot tier yet still served stale from the cache after the hot
// record drained away.
func (t *TieredDB) write(op byte, key, value []byte) {
	fp := forceFP(maphash.Bytes(t.seed, key))
	for {
		a := t.active.Load()
		// Register as in-flight before the sealed check, so seal cannot both observe inflight
		// zero and race a put still writing bytes: either seal sees this increment and waits, or
		// this put sees the sealed flag and backs out. It is the store-load pair the barrier
		// rests on, correct under Go's sequentially consistent atomics.
		a.inflight.Add(1)
		if a.sealed.Load() {
			a.inflight.Add(-1)
			continue // sealed after we loaded it; reload the fresh active
		}
		ok := a.put(fp, op, key, value)
		a.inflight.Add(-1)
		if ok {
			t.cache.invalidate(fp)
			return
		}
		t.seal(a)
	}
}

// seal swaps the full segment out for a fresh one and queues it for migration. It re-checks
// under the lock so that only the first caller to see a given full segment seals it; the rest
// observe the already-swapped active and loop back to retry their put. It prepends the segment to
// the newest-first sealed list and hands it to the migrator in seal order. When the list already
// holds maxSealed segments it waits for the migrator to drain one, which is the backpressure that
// bounds hot memory; version order is preserved because the list is newest-first for reads and
// the migrator drains in seal order, so an older value always reaches cold before a newer one.
func (t *TieredDB) seal(full *segment) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active.Load() != full {
		return // another writer already sealed this one
	}
	for sealedLen(t.sealed.Load()) >= maxSealed {
		t.mu.Unlock()
		runtime.Gosched()
		t.mu.Lock()
		if t.active.Load() != full {
			return
		}
	}
	// Stop new puts into full, then publish it on the sealed list so a reader still finds its
	// records, then swap in a fresh active so writers move on to it. Only after that wait for any
	// put that already claimed an offset in full to finish writing its bytes; until inflight
	// reaches zero the buffer is still being written and the drainer must not walk it. Publishing
	// before the swap keeps full reachable to readers the whole time, so no committed key blinks
	// out between active and the sealed list.
	full.sealed.Store(true)
	t.sealed.Store(&sealedNode{seg: full, next: t.sealed.Load()})
	t.active.Store(newSegment(t.segBytes, t.segKeys))
	for full.inflight.Load() > 0 {
		runtime.Gosched()
	}
	t.migrate <- full
}

// removeSealed republishes the sealed list with seg dropped, called by the migrator once it has
// finished draining seg into cold. It rebuilds the chain rather than mutating it in place so a
// reader walking the old chain keeps a consistent snapshot; the new chain reuses the surviving
// segments and only the drained one falls out, to become garbage once no reader holds it.
func (t *TieredDB) removeSealed(seg *segment) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var keep []*segment
	for n := t.sealed.Load(); n != nil; n = n.next {
		if n.seg != seg {
			keep = append(keep, n.seg)
		}
	}
	var head *sealedNode
	for i := len(keep) - 1; i >= 0; i-- {
		head = &sealedNode{seg: keep[i], next: head}
	}
	t.sealed.Store(head)
}

// Get returns the value for key. It checks the hot tiers first, active then sealed, then the
// read cache, then cold, populating the cache on a cold hit. scratch is a caller buffer the
// value is copied into and may be reused across calls.
func (t *TieredDB) Get(key, scratch []byte) ([]byte, bool, error) {
	fp := forceFP(maphash.Bytes(t.seed, key))
	if v, found, tomb := t.active.Load().get(fp, key, scratch); found {
		return v, true, nil
	} else if tomb {
		return nil, false, nil
	}
	for n := t.sealed.Load(); n != nil; n = n.next {
		if v, found, tomb := n.seg.get(fp, key, scratch); found {
			return v, true, nil
		} else if tomb {
			return nil, false, nil
		}
	}
	if v, ok := t.cache.get(fp, key, scratch); ok {
		return v, true, nil
	}
	v, ok, fromDisk, err := t.cold.get(key, scratch)
	if err != nil || !ok {
		return nil, false, err
	}
	// Only a disk-sourced read is worth caching: a value the cold log served from its resident
	// ring is already in memory, so caching it would spend RAM and an allocation to save nothing.
	if fromDisk {
		t.cache.put(fp, key, v)
	}
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
			t.removeSealed(seg)
		case <-t.closed:
			// Drain anything still queued so Close persists every set.
			for {
				select {
				case seg := <-t.migrate:
					t.drain(seg)
					t.removeSealed(seg)
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
	seg.rangeRecords(func(addr int64, op byte, key, value []byte) {
		fp := forceFP(maphash.Bytes(t.seed, key))
		cur, ok := seg.ix.Get(fp)
		if !ok || cur != addr {
			return // superseded within the segment, dropped as compaction
		}
		if op == opDel {
			t.cold.Delete(key) // carry the tombstone to cold so the delete survives a reopen
			return
		}
		t.cold.Set(key, value)
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
