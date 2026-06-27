package hashlog

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// recover.go is M5: it rebuilds every shard's resident index from the last valid
// checkpoint plus the durable log tail (spec 2070 doc 05 section 6, D9). It is the
// reader half of the two-artifact model (doc 05 section 1): M4 wrote the periodic
// index snapshot and the continuous per-shard frontier; this file joins them back into
// a live store. Recovery runs once, at open, before the store serves any request, and
// it is fail-closed: every length, offset, and address read off disk is bounded before
// it is used, so a corrupt file is rejected with an error, never a panic (doc 05
// section 8, FuzzRecover).
//
// The algorithm, per doc 05 section 6:
//
//   - The durableFile open already picked the valid highest-generation superblock slot
//     and restored the LSN high-water and the committed snapshot's location (the
//     pickNewer step a and the scalar half of step b). This file does the rest.
//   - Scan the file's extents to learn each shard's log chain from the bytes alone (the
//     on-disk extent header, doc 03 section 5, is what makes this possible), and
//     reconcile the allocator so no in-use extent is ever handed out again and no
//     orphaned half-checkpoint extent is leaked (the rest of step b).
//   - Per shard, load the snapshot section into a fresh index and replay the log delta
//     from the recorded frontier forward, CRC-stopping at the torn tail, applying
//     last-writer-wins by LSN (step c).
//   - Resume the store LSN counter past the highest LSN seen anywhere (step d).
//
// The shards share nothing (D2), so the per-shard rebuild runs a worker per shard; the
// allocator reconciliation and the LSN resume are the only cross-shard steps and they
// sit outside the parallel section (doc 05 section 9).

// recoveryStats records what recovery did, the observability doc 08 section 1.5 asks
// for: how many delta records were replayed across all shards, how many log bytes each
// shard replayed past its frontier, and where each shard's CRC-stop fired. A recovery
// that replayed too little or too much is then visible, not silent.
type recoveryStats struct {
	replayedRecords int64
	bytesReplayed   []int64
	tornTailOff     []int64
}

// extentRef is one of a shard's log extents as found by the header scan: its id and the
// logical base address of its first body byte (the header's baseAddr field). A shard's
// chain is the set of its extents ordered by base address (doc 03 section 5: recovery
// orders by base address and does not need the forward link, which the M8 compactor
// owns).
type extentRef struct {
	id       int64
	baseAddr int64
}

// recover rebuilds every shard's index from the durable file. It is called from New
// only when the file already held a superblock (df.existed). It never reads a value
// into memory: it reconstructs the index (keys and locations) and leaves the values on
// disk where the locations point, the larger-than-memory property carried into recovery
// (doc 05 section 9).
func (s *Store) recover() error {
	d := s.df
	pageSize := int64(s.t.PageSize)

	// Step b, part 1: learn the physical extent layout from the file. The file holds
	// whole extents past the superblock, so the count is the size past the superblock
	// divided by the on-disk stride.
	physCount := (d.fileEnd - d.sbSize) / d.stride
	if physCount < 0 {
		return errors.New("hashlog: durable file shorter than its superblock")
	}

	chains, inUse, err := d.scanExtents(physCount)
	if err != nil {
		return err
	}

	// Load the committed index snapshot, if any. A generation-0 file that never
	// checkpointed has no snapshot, so recovery replays each shard's whole log from the
	// start (frontier zero). The snapshot's generation must match the slot we recovered
	// from, or the file is inconsistent and we fail closed.
	var snap *decodedSnapshot
	if d.snapRoot >= 0 && d.snapBytes.Load() > 0 {
		snap, err = d.loadSnapshot()
		if err != nil {
			return err
		}
		if snap.generation != d.sb.generation {
			return fmt.Errorf("hashlog: snapshot generation %d does not match superblock %d",
				snap.generation, d.sb.generation)
		}
		if len(snap.sections) != s.t.Shards {
			return fmt.Errorf("hashlog: snapshot has %d sections, store has %d shards",
				len(snap.sections), s.t.Shards)
		}
		// The committed snapshot run is in use even though it carries no log header, so
		// fold it into the in-use set before reconciling the allocator.
		for k := int64(0); k < d.snapCount; k++ {
			inUse[d.snapRoot+k] = struct{}{}
		}
	}

	// Step b, part 2: reconcile the allocator with the physical file. Every log extent
	// and the committed snapshot run are in use; every other physical extent is free,
	// which reclaims any orphaned half-checkpoint extents from a crash before commit
	// (doc 05 section 4). This replaces the slot-derived allocator the open built,
	// because post-checkpoint log growth allocated extents the slot does not record.
	free := make([]int64, 0)
	for id := int64(0); id < physCount; id++ {
		if _, ok := inUse[id]; !ok {
			free = append(free, id)
		}
	}
	d.alloc = newAllocator(physCount, free)

	// Step c: per-shard, in parallel (the shards share nothing, D2). Each worker writes
	// only its own shard and its own stats slot, plus the shared max-LSN and
	// replayed-record atomics, so there is no cross-shard race.
	s.rec = recoveryStats{
		bytesReplayed: make([]int64, s.t.Shards),
		tornTailOff:   make([]int64, s.t.Shards),
	}
	var maxLSN atomic.Uint64
	maxLSN.Store(d.sb.lsnHighWater)
	var replayed atomic.Int64
	errs := make([]error, s.t.Shards)
	var wg sync.WaitGroup
	for i, sh := range s.shards {
		wg.Add(1)
		go func(i int, sh *shard) {
			defer wg.Done()
			var sec *snapSection
			if snap != nil {
				sec = &snap.sections[i]
			}
			r, err := sh.rebuild(d, pageSize, chains[i], sec, d.sb.frontiers[i])
			if err != nil {
				errs[i] = err
				return
			}
			s.rec.bytesReplayed[i] = r.bytes
			s.rec.tornTailOff[i] = r.tornAt
			replayed.Add(r.records)
			for {
				cur := maxLSN.Load()
				if r.maxLSN <= cur || maxLSN.CompareAndSwap(cur, r.maxLSN) {
					break
				}
			}
		}(i, sh)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	s.rec.replayedRecords = replayed.Load()

	// Step d: resume the store LSN counter past the highest LSN seen, so the first
	// post-recovery write gets an LSN strictly greater than any already on disk.
	d.lsn.Store(maxLSN.Load())
	return nil
}

// scanExtents reads every physical extent's header and groups the log extents by shard.
// It returns, per shard, the shard's log extents (unordered here, ordered by the
// rebuild), and the set of in-use extent ids (every extent that decodes as a log
// extent). A non-log extent (the snapshot run, or a torn or never-written extent)
// decodes to errBadExtentHeader and is simply not a log extent; the snapshot run is
// folded into the in-use set separately by the caller. The scan is fail-closed: it
// never indexes past the file and a shard id outside the configured range is rejected.
func (d *durableFile) scanExtents(physCount int64) (chains [][]extentRef, inUse map[int64]struct{}, err error) {
	chains = make([][]extentRef, d.shardCount)
	inUse = make(map[int64]struct{})
	// Extents the committed superblock records free are not part of any live chain: a
	// compaction retired them and a checkpoint freed them, but their on-disk log header is
	// left stale (the retire never rewrites it). Skip them so a freed-not-yet-reused extent
	// is not chained back in as a phantom page (M8, doc 06 section 7.3). A freed extent that
	// was since reused carries a fresh header at its new base, so it is not in this set.
	freeSet := make(map[int64]struct{}, len(d.sb.free))
	for _, id := range d.sb.free {
		freeSet[id] = struct{}{}
	}
	hdr := make([]byte, extentHeaderBytes)
	for id := int64(0); id < physCount; id++ {
		if _, ok := freeSet[id]; ok {
			continue
		}
		if _, err := d.f.ReadAt(hdr, d.extentOffset(id)); err != nil {
			return nil, nil, err
		}
		h, derr := decodeExtentHeader(hdr)
		if derr != nil {
			// Not a log extent: the snapshot run, or an orphaned half-written extent.
			// Recovery does not chain it; the allocator reconciliation frees whatever is
			// not in use.
			continue
		}
		if h.kind != extentKindLog {
			continue
		}
		if h.shardID < 0 || int(h.shardID) >= d.shardCount {
			return nil, nil, fmt.Errorf("hashlog: log extent %d names shard %d out of range", id, h.shardID)
		}
		chains[h.shardID] = append(chains[h.shardID], extentRef{id: id, baseAddr: h.baseAddr})
		inUse[id] = struct{}{}
	}
	return chains, inUse, nil
}

// loadSnapshot reads the committed snapshot stream off the file and decodes it. The
// stream is one contiguous region (M4 wrote it across a contiguous extent run), so it
// reads back with one ReadAt and the fail-closed decoder validates the rest.
func (d *durableFile) loadSnapshot() (*decodedSnapshot, error) {
	n := d.snapBytes.Load()
	buf := make([]byte, n)
	if _, err := d.f.ReadAt(buf, d.extentOffset(d.snapRoot)); err != nil {
		return nil, err
	}
	return decodeSnapshot(buf)
}

// shardRebuildResult is what a shard's rebuild reports to the cross-shard steps: the
// highest LSN it saw (snapshot frontier or a replayed record), the number of delta
// records it applied, the bytes it replayed, and the torn-tail offset (-1 if none).
type shardRebuildResult struct {
	maxLSN  uint64
	records int64
	bytes   int64
	tornAt  int64
}

// rebuild reconstructs one shard: it builds the page directory from the shard's log
// extents, seeds a fresh index from the snapshot section, and replays the log delta
// from the recorded frontier forward with a CRC-stop at the torn tail. It runs with the
// shard owned exclusively by recovery (no concurrent reader or writer), so it mutates
// the shard's fields directly without the lock.
func (sh *shard) rebuild(d *durableFile, pageSize int64, chain []extentRef, sec *snapSection, fr shardFrontier) (shardRebuildResult, error) {
	res := shardRebuildResult{tornAt: -1}

	// Order the shard's extents by logical base address: that is the page order, since
	// page p sits at base address p*pageSize (doc 03 section 5).
	sort.Slice(chain, func(i, j int) bool { return chain[i].baseAddr < chain[j].baseAddr })

	// The directory covers pages 0..maxPid, one page per extent. baseByExt maps an
	// extent id to its base address so the recorded frontier tail extent resolves to a
	// logical address below.
	maxPid := int64(-1)
	baseByExt := make(map[int64]int64, len(chain))
	pageOfExt := make(map[int64]int64, len(chain))
	for _, e := range chain {
		if e.baseAddr < 0 || e.baseAddr%pageSize != 0 {
			return res, fmt.Errorf("hashlog: shard %d extent %d has misaligned base %d", sh.shardID, e.id, e.baseAddr)
		}
		pid := e.baseAddr / pageSize
		baseByExt[e.id] = e.baseAddr
		pageOfExt[e.id] = pid
		if pid > maxPid {
			maxPid = pid
		}
	}

	// Build the per-page arrays. A gap in the page sequence is normal after compaction:
	// a retired extent leaves a permanent hole at its page id (pageExtent stays -1), which
	// the directory carries and no live key points at (M8, doc 06 section 7.3). So a missing
	// page is a hole, not corruption; the snapshot-tuple check below still fails closed if a
	// live key points into a hole, which would be a genuine inconsistency.
	npages := maxPid + 1
	if npages < 1 {
		npages = 1
	}
	pages := make([][]byte, npages)
	pageExtent := make([]int64, npages)
	diskOff := make([]int64, npages)
	pageFill := make([]int, npages)
	pageFlushed := make([]int, npages)
	pageMaxLSN := make([]int64, npages)
	deadBytes := make([]int64, npages)
	for pid := int64(0); pid < npages; pid++ {
		pageExtent[pid] = -1
		diskOff[pid] = -1
	}
	for _, e := range chain {
		pid := e.baseAddr / pageSize
		pageExtent[pid] = e.id
		diskOff[pid] = d.logBodyOffset(e.id)
	}

	// Seed a fresh index from the snapshot section. The tuples carry no per-key LSN (doc
	// 05 section 2, the implementation note): the section frontier LSN is the shared
	// version stamp, so every snapshot key is seeded at F_shard, and any delta record
	// with a higher LSN wins. A delta record at or below F_shard is already reflected in
	// the snapshot and is ignored.
	idx := newIdxTable(1)
	live := 0
	occ := 0
	appliedLSN := make(map[string]uint64)
	var fShard uint64
	if sec != nil {
		fShard = sec.frontierLSN
		idx = newIdxTable(len(sec.tuples)*2 + 1)
		for _, tup := range sec.tuples {
			pid := tup.loc.addr / pageSize
			if pid < 0 || pid >= npages || pageExtent[pid] < 0 {
				return res, fmt.Errorf("hashlog: snapshot key in shard %d points at unbacked page %d", sh.shardID, pid)
			}
			recoverInsert(idx, tup.key, tup.loc, &live, &occ)
			appliedLSN[string(tup.key)] = fShard
		}
	}
	res.maxLSN = fShard

	// Find where the delta replay starts. With a snapshot, the superblock recorded the
	// shard's log tail at the cut (tailExtent, tailOff); the records after it are the
	// delta. Without a snapshot, replay the whole log from address zero. If the recorded
	// tail extent is not in this shard's chain (it should always be), fall back to a full
	// replay, which is slower but correct.
	var startAddr int64
	if sec != nil {
		if base, ok := baseByExt[fr.tailExtent]; ok {
			startAddr = base + int64(fr.tailOff)
		}
	}
	startPage := startAddr / pageSize
	startOff := int(startAddr % pageSize)

	// Replay each page from the start page to the tail, applying the delta records.
	body := make([]byte, pageSize)
	lastPage := maxPid
	for pid := startPage; pid <= maxPid && pid < npages; pid++ {
		if pageExtent[pid] < 0 {
			continue // a compaction hole: no extent, no records, skip to the next page
		}
		if _, err := d.f.ReadAt(body, d.logBodyOffset(pageExtent[pid])); err != nil {
			return res, err
		}
		pos := 0
		if pid == startPage {
			pos = startOff
		}
		var pageMax uint64
		for pos < int(pageSize) {
			lsn, flags, key, value, n, derr := decodeDurableRecord(body[pos:])
			if derr != nil || n == 0 {
				// A clean page end is zero padding left when a record did not fit and the
				// log rolled to the next page; a genuine torn tail is non-zero bytes a crash
				// left mid-append. Only the latter is the CRC-stop the frontier names.
				if !allZero(body[pos:]) {
					res.tornAt = pid*pageSize + int64(pos)
					lastPage = pid
					pid = maxPid // stop the outer scan after this page
				}
				break
			}
			recStart := pid*pageSize + int64(pos)
			if lsn > appliedLSN[string(key)] {
				appliedLSN[string(key)] = lsn
				if flags&flagTombstone != 0 {
					recoverDelete(idx, key, &live)
				} else {
					valOff := durableValOff(key, value)
					recoverInsertGrow(&idx, key, valLoc{addr: recStart + int64(valOff), vlen: uint32(len(value))}, &live, &occ)
				}
			}
			if lsn > pageMax {
				pageMax = lsn
			}
			if lsn > res.maxLSN {
				res.maxLSN = lsn
			}
			res.records++
			res.bytes += int64(n)
			pos += n
		}
		pageFill[pid] = pos
		pageFlushed[pid] = pos
		pageMaxLSN[pid] = int64(pageMax)
		if res.tornAt >= 0 {
			break
		}
	}

	// Publish the rebuilt shard state. The tail page is held resident so appends
	// continue into it; older pages are left spilled (read back from disk on GET) up to
	// the resident budget, matching the live engine's resident set. With no budget
	// (residentCap zero, the unbounded mode) every page is resident so the lock-free read
	// path never meets a spilled page.
	tailPage := lastPage
	if tailPage < 0 {
		tailPage = 0
	}
	residentFrom := int64(0)
	if sh.residentCap > 0 {
		residentFrom = tailPage - int64(sh.residentCap) + 1
		if residentFrom < 0 {
			residentFrom = 0
		}
	}
	spilled := 0
	sh.residentOrder = sh.residentOrder[:0]
	for pid := int64(0); pid < npages; pid++ {
		if pageExtent[pid] < 0 {
			continue
		}
		if pid >= residentFrom && pid <= tailPage {
			if _, err := d.f.ReadAt(body, d.logBodyOffset(pageExtent[pid])); err != nil {
				return res, err
			}
			p := make([]byte, pageSize)
			copy(p, body)
			pages[pid] = p
			sh.residentOrder = append(sh.residentOrder, pid)
		} else {
			spilled++
		}
	}

	sh.pages.Store(&pageSet{pages: pages, diskOff: diskOff})
	sh.pageExtent = pageExtent
	sh.pageFill = pageFill
	sh.pageFlushed = pageFlushed
	sh.pageMaxLSN = pageMaxLSN
	sh.deadBytes = deadBytes
	sh.tailPage = tailPage
	sh.tailPos = pageFill[tailPage]
	sh.spilledPages = spilled
	sh.index.Store(idx)
	sh.idxLive = live
	sh.idxOcc = occ
	frontier := int64(fShard)
	if res.maxLSN > uint64(frontier) {
		frontier = int64(res.maxLSN)
	}
	sh.frontier.Store(frontier)
	// Record the committed checkpoint's frontier as the lower bound a tombstone discard
	// checks against, the same value the live store stamps at checkpoint commit (M8, doc 06
	// section 3.4). It is the snapshot's per-shard frontier, the highest LSN baked into the
	// recovered checkpoint.
	sh.ckptFrontier.Store(int64(fr.frontierLSN))
	// Recompute the per-page dead-byte tally exactly from the rebuilt index and the on-disk
	// records (M8, doc 06 section 2.2): a data record is dead when the index no longer points
	// at it, the same condition the live store credits on an overwrite or delete. This makes a
	// recovered store choose the same compaction targets a never-crashed one would.
	if err := sh.recomputeDeadBytes(d, pageSize, npages); err != nil {
		return res, err
	}
	return res, nil
}

// recomputeDeadBytes rebuilds the per-page dead-byte tally after recovery, so the in-memory
// counter the compactor reads matches what a never-crashed store would hold (doc 06 section
// 2.2). It walks each backed page's records against the just-rebuilt index: a data record is
// dead exactly when the index does not point at its value address (it was overwritten or
// deleted), the same predicate the live store credits incrementally. A tombstone is not
// counted dead, matching the live store, which credits the data record a delete kills but
// never the tombstone itself; a discardable tombstone is reclaimed only when its page is
// compacted for its dead data (doc 06 section 3.4). It runs while recovery owns the shard
// exclusively, so it reads the shard fields without the lock.
func (sh *shard) recomputeDeadBytes(d *durableFile, pageSize int64, npages int64) error {
	body := make([]byte, pageSize)
	for pid := int64(0); pid < npages; pid++ {
		if sh.pageExtent[pid] < 0 {
			continue
		}
		fill := sh.pageFill[pid]
		if fill <= 0 {
			continue
		}
		var src []byte
		if p := sh.pages.Load().pages[pid]; p != nil {
			src = p[:fill]
		} else {
			if _, err := d.f.ReadAt(body[:fill], d.logBodyOffset(sh.pageExtent[pid])); err != nil {
				return err
			}
			src = body[:fill]
		}
		dead := int64(0)
		pos := 0
		for pos < fill {
			_, flags, key, value, n, derr := decodeDurableRecord(src[pos:])
			if derr != nil || n == 0 {
				break
			}
			if flags&flagTombstone == 0 {
				valueAddr := pid*pageSize + int64(pos) + int64(durableValOff(key, value))
				if e := sh.index.Load().lookupEntry(tableHash(key), key); e == nil || e.loc.addr != valueAddr {
					dead += int64(durableRecordLenFor(len(key), len(value)))
				}
			}
			pos += n
		}
		sh.deadBytes[pid] = dead
	}
	return nil
}

// recoverInsert places a tuple into the rebuilt table without growing it. The table is
// pre-sized to the snapshot's live count, so the snapshot seed never grows, and the key
// is unique within a section, so the probe always lands on an empty slot. It maintains
// the live and occupancy counts the shard publishes.
func recoverInsert(t *idxTable, key []byte, loc valLoc, live, occ *int) {
	thash := tableHash(key)
	i := thash & t.mask
	for t.slots[i].Load() != nil {
		i = (i + 1) & t.mask
	}
	t.slots[i].Store(&entry{thash: thash, key: append([]byte(nil), key...), loc: loc})
	*live++
	*occ++
}

// recoverInsertGrow places or overwrites a key during delta replay, growing the table
// first when it is about to cross the load-factor threshold. Replay can introduce keys
// the snapshot did not have (writes after the cut), so unlike the seed it must size up.
func recoverInsertGrow(t **idxTable, key []byte, loc valLoc, live, occ *int) {
	tb := *t
	if *occ+1 > int((tb.mask+1)*7/10) {
		tb = recoverGrow(tb, *live)
		*occ = *live
		*t = tb
	}
	thash := tableHash(key)
	i := thash & tb.mask
	for {
		e := tb.slots[i].Load()
		if e == nil {
			tb.slots[i].Store(&entry{thash: thash, key: append([]byte(nil), key...), loc: loc})
			*live++
			*occ++
			return
		}
		if e != tombstone && e.thash == thash && bytes.Equal(e.key, key) {
			tb.slots[i].Store(&entry{thash: thash, key: e.key, loc: loc})
			return
		}
		i = (i + 1) & tb.mask
	}
}

// recoverDelete drops a key during delta replay (a tombstone record). It is a no-op for
// an absent key, the same last-writer-wins delete the live engine applies.
func recoverDelete(t *idxTable, key []byte, live *int) {
	thash := tableHash(key)
	i := thash & t.mask
	for {
		e := t.slots[i].Load()
		if e == nil {
			return
		}
		if e != tombstone && e.thash == thash && bytes.Equal(e.key, key) {
			t.slots[i].Store(tombstone)
			*live--
			return
		}
		i = (i + 1) & t.mask
	}
}

// recoverGrow rebuilds a table sized to the live key count, dropping tombstones, the
// same compacting rebuild growIndex does, but driven by recovery's local counts rather
// than the shard's.
func recoverGrow(old *idxTable, live int) *idxTable {
	nt := newIdxTable((live + 1) * 2)
	for j := range old.slots {
		e := old.slots[j].Load()
		if e == nil || e == tombstone {
			continue
		}
		i := e.thash & nt.mask
		for nt.slots[i].Load() != nil {
			i = (i + 1) & nt.mask
		}
		nt.slots[i].Store(e)
	}
	return nt
}

// allZero reports whether b is all zero bytes, the test that tells clean page padding
// (the log rolled, leaving the page tail zeroed) from a genuine torn tail record (a
// crash left non-zero bytes mid-append).
func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}
