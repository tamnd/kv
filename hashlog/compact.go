package hashlog

import (
	"bytes"
	"errors"
)

// compact.go is M8: it reclaims dead log space by copying the live records out of a
// mostly-dead sealed extent and retiring the extent (spec 2070 doc 06). It is the
// garbage collector the hybrid log needs once writes are durable: an overwrite or a
// delete does not erase the old record, it only drops the index pointer and credits the
// old record's bytes to its page's dead tally (store.go creditDeadLocked), so without
// compaction the file grows without bound under churn. Compaction turns that dead space
// back into free extents.
//
// It runs only on the durable eviction-possible profile (sh.inPlace), where a GET takes
// the shard read lock (getLocked). That is the load-bearing simplification: because
// readers are excluded by the lock, the compactor does its liveness probe, copy,
// repoint, and extent retire under the shard write lock with no epoch machinery at all.
// The epoch deferred-free path (epoch.go) stays reserved for a future lock-free-profile
// compactor; here the write lock already serialises the free against every reader (doc 06
// section 6). The full-resident durable profile (residentCap zero) does not compact: it
// keeps every page resident and aliases it on the lock-free read, so it has no inPlace
// shard and Compact skips it, accepting unbounded log growth the way doc 04 section 7.3
// describes.
//
// The space an extent frees is not handed back to the allocator the instant the extent
// is retired. A retired extent becomes a hole in its shard's page directory immediately
// (no live entry points at it), but it is recorded durably free, and pushed onto the
// allocator's in-memory free stack, only by the next checkpoint that also captures the
// repointed index (Checkpoint plus commitCheckpoint). That interlock is what makes a
// crash between compaction and checkpoint safe: recovery falls back to the prior
// checkpoint, whose snapshot still points into the not-yet-freed source extent, and the
// extent's bytes are intact because nothing reused it (doc 06 section 7).

// Compact runs one compaction pass over every shard that compacts (the durable
// eviction-possible profile). It selects each shard's sealed extents whose dead fraction
// has reached the threshold and copies their live records forward, retiring the emptied
// extents into the per-shard pending-free list the next checkpoint commits. It is a
// no-op error in memory-only mode and a no-op (no shard compacts) on the full-resident
// durable profile. M8 provides the explicit call; a throttled background scheduler that
// fires a pass when a shard's dead fraction crosses the threshold is a later milestone
// (doc 06 section 9.3), so at M8 a pass is taken by calling Compact.
func (s *Store) Compact() error {
	if s.df == nil {
		return errors.New("hashlog: Compact requires durable mode")
	}
	for _, sh := range s.shards {
		if !sh.inPlace {
			continue
		}
		if err := sh.compact(); err != nil {
			return err
		}
	}
	return nil
}

// compact runs one compaction pass over shard sh. It selects the compactable pages under
// the read lock (a sealed page, fully below ReadOnlyAddress, backed by an extent, whose
// dead fraction has reached the threshold), then compacts each. compactMu serialises
// passes on this shard so two never pick or retire the same extent; ordinary writes,
// reads, and checkpoints proceed concurrently between the per-record lock acquisitions.
func (sh *shard) compact() error {
	sh.compactMu.Lock()
	defer sh.compactMu.Unlock()

	sh.mu.RLock()
	ro := sh.readOnlyAddress()
	pageBytes := int64(sh.pageSize)
	var targets []int64
	for pid := int64(0); pid < int64(len(sh.pageExtent)); pid++ {
		if sh.pageExtent[pid] < 0 {
			continue // already a hole from a prior pass
		}
		// Only a wholly-sealed page: its last byte sits below ReadOnlyAddress, so it holds
		// no record still eligible for an in-place overwrite and copying it cannot race a
		// writer mutating its bytes (doc 06 section 4.2). The mutable tail window is never a
		// compaction target.
		if (pid+1)*pageBytes > ro {
			continue
		}
		fill := sh.pageFill[pid]
		if fill <= 0 {
			continue
		}
		if float64(sh.deadBytes[pid])/float64(fill) >= sh.compactionThreshold {
			targets = append(targets, pid)
		}
	}
	sh.mu.RUnlock()

	for _, pid := range targets {
		if err := sh.compactExtent(pid); err != nil {
			return err
		}
	}
	return nil
}

// compactExtent copies the live records out of page pid and retires its extent. It reads
// the page bytes once into a private buffer under the read lock, so the relocation walk
// is stable even as the page is later retired and its buffer recycled: the page is sealed
// below ReadOnlyAddress so its bytes never change under us, and compactMu keeps any other
// pass off it. It then walks the records, relocating each live one (keeping its original
// LSN), and retires the now-dead extent.
func (sh *shard) compactExtent(pid int64) error {
	pageBytes := int64(sh.pageSize)

	sh.mu.RLock()
	if pid >= int64(len(sh.pageExtent)) || sh.pageExtent[pid] < 0 {
		sh.mu.RUnlock()
		return nil // retired or evicted out from under the selection; nothing to do
	}
	fill := sh.pageFill[pid]
	src := make([]byte, fill)
	ref := sh.pages.Load().refs[pid].Load()
	if ref.mem != nil {
		copy(src, ref.mem[:fill])
		sh.mu.RUnlock()
	} else {
		dOff := ref.diskOff
		f := sh.df.f
		sh.mu.RUnlock()
		if _, err := f.ReadAt(src, dOff); err != nil {
			return err
		}
	}

	// Walk the source records. A copied record keeps its original LSN; the compactor does
	// not claim a fresh LSN from the per-store counter for a copied record (doc 06 section
	// 5.1), so a relocated record's version stays exactly what it was and last-writer-wins
	// on recovery is unchanged.
	pageBase := pid * pageBytes
	pos := 0
	for pos < fill {
		lsn, flags, key, value, n, derr := decodeDurableRecord(src[pos:])
		if derr != nil || n == 0 {
			break // clean end of the page's records
		}
		if flags&flagTombstone != 0 {
			if err := sh.relocateTombstone(lsn, key); err != nil {
				// A seal-flush failed copying a record forward. The source extent is not yet
				// retired, so every original still lives; abort without retiring it and the
				// data is intact for a later pass to compact (D5).
				return err
			}
		} else {
			valueAddr := pageBase + int64(pos) + int64(durableValOff(key, value))
			if err := sh.relocateData(lsn, flags, key, value, valueAddr); err != nil {
				return err
			}
		}
		pos += n
	}

	sh.retireCompactedExtent(pid)
	return nil
}

// relocateData copies one live data record forward with a compare-and-publish repoint
// (doc 06 section 3.2, 5.4). Phase 1 is a lock-free liveness probe: the record is live
// only if the index still points at this exact value address, and a dead record (an
// overwrite or delete moved the index off it) is skipped, to be discarded with the
// extent. Phase 2 takes the write lock, copies the record to the tail keeping its
// original LSN, then re-checks: if the key still points at the old address the copy is
// published (the index repoints to it, REPOINTED); if a racing overwrite or delete moved
// the key off the old address between the probe and the lock the copy is abandoned, its
// bytes credited dead in the output page so the accounting stays exact (ABANDONED).
func (sh *shard) relocateData(lsn uint64, flags byte, key, value []byte, oldValueAddr int64) error {
	thash := tableHash(key)
	if e := sh.index.Load().lookupEntry(thash, key); e == nil || e.loadLoc().addr != oldValueAddr {
		return nil
	}

	sh.mu.Lock()
	newValueAddr, newPid, encLen, err := sh.appendRelocated(lsn, flags, key, value)
	if err != nil {
		sh.mu.Unlock()
		return err
	}
	// An oversize home record carries the 24-byte descriptor as its value and the oversize
	// marker in its index entry. Relocating it copies the home record verbatim, descriptor
	// and all, so the moved record still points at the same in-place cont chain; the repoint
	// must carry the marker forward or the read would slice the descriptor as an inline value
	// (M9, doc 03 section 7). The cont extents are not moved here: a live value's chain stays
	// put and is freed only when the value is finally overwritten or deleted, so a home-page
	// compaction never rewrites the large value bytes.
	newVlen := uint32(len(value))
	if flags&flagOversize != 0 {
		newVlen = valLocOversizeBit | oversizeDescriptorLen
	}
	if e := sh.index.Load().lookupEntry(thash, key); e != nil && e.loadLoc().addr == oldValueAddr {
		sh.indexRepointLocked(thash, key, valLoc{addr: newValueAddr, vlen: newVlen})
		sh.store.relocatedRecords.Add(1)
		sh.store.copiedBytes.Add(int64(encLen))
	} else {
		// A concurrent writer superseded the original after the phase-1 probe. The copy we
		// just appended is immediately dead; credit it to its output page so a later pass
		// reclaims it, and leave the index pointing at the newer record.
		sh.deadBytes[newPid] += int64(encLen)
		sh.store.abandonedCopies.Add(1)
	}
	sh.mu.Unlock()
	return nil
}

// relocateTombstone decides whether a tombstone must be copied forward or can be dropped
// (doc 06 section 3.4). A tombstone is discardable when its LSN is at or below the last
// committed checkpoint's frontier (its deletion is baked into the snapshot recovery
// starts from) and its key is absent from the current index (nothing has re-added it):
// below the frontier the deletion is never replayed, so the tombstone is pure dead space.
// Otherwise it is copied forward with its original LSN so a future recovery still applies
// the delete. The frontier is read under the write lock together with the absence check
// so the discard decision is taken against one consistent moment; ckptFrontier only ever
// advances, and it only ever holds a committed value, so the predicate is a sound
// conservative lower bound (doc 06 section 3.4).
func (sh *shard) relocateTombstone(lsn uint64, key []byte) error {
	sh.mu.Lock()
	frontier := uint64(sh.ckptFrontier.Load())
	e := sh.index.Load().lookupEntry(tableHash(key), key)
	if lsn <= frontier && e == nil {
		sh.store.discardedTombstones.Add(1)
		sh.mu.Unlock()
		return nil
	}
	_, _, _, err := sh.appendRelocated(lsn, flagTombstone, key, nil)
	sh.mu.Unlock()
	return err
}

// appendRelocated appends a copied record to the tail with a caller-supplied LSN (the
// record's original, never a fresh one) and returns the copy's value address, its page
// id, and its encoded length. It is the append half of set without the LSN claim: it
// rolls a new tail page when the record does not fit, encodes the record with the given
// LSN and flags, points past the header and key to the value, and advances the page's
// fill and (by max, never assign) its highest LSN, since a relocated low LSN must not
// lower a page that already holds higher ones. It runs under the shard write lock.
func (sh *shard) appendRelocated(lsn uint64, flags byte, key, value []byte) (valueAddr int64, pid int64, encLen int, err error) {
	rl := durableRecordLen(key, value)
	if err := sh.rollFor(rl); err != nil {
		// A Normal seal-flush failed rolling the output page. Do not append the copy: the
		// original still lives in its un-retired source extent, so the caller aborts the
		// extent's compaction and the data is intact (D5).
		return 0, 0, 0, err
	}
	page := sh.pages.Load().refs[sh.tailPage].Load().mem
	recStart := sh.tailPage*int64(sh.pageSize) + int64(sh.tailPos)
	if err := addrInRange(recStart, rl); err != nil {
		return 0, 0, 0, err
	}
	n := encodeDurableRecord(page[sh.tailPos:], lsn, key, value, flags)
	sh.tailPos += n
	sh.pageFill[sh.tailPage] = sh.tailPos
	if int64(lsn) > sh.pageMaxLSN[sh.tailPage] {
		sh.pageMaxLSN[sh.tailPage] = int64(lsn)
	}
	sh.df.bytesSinceCkpt.Add(int64(n))
	return recStart + int64(durableValOff(key, value)), sh.tailPage, n, nil
}

// indexRepointLocked republishes key's index slot to point at a new value location,
// reusing the stored key, without touching the live or occupancy counts (the key already
// occupied a slot). It runs under the shard write lock. The caller has verified under the
// same lock that the key is present and still points at the record being relocated, so
// the probe always finds the slot; a missing slot would mean a concurrent delete the
// caller's re-check already ruled out, and is left as a no-op rather than a panic.
func (sh *shard) indexRepointLocked(thash uint64, key []byte, loc valLoc) {
	t := sh.index.Load()
	i := thash & t.mask
	for {
		e := t.slots[i].Load()
		if e == nil {
			return
		}
		if e != tombstone && e.thash == thash && bytes.Equal(e.key, key) {
			// Repoint the existing entry in place (L2): same key, new location, one atomic
			// store. A concurrent lock-free reader sees either the pre- or post-relocation
			// address; the old extent is retired only after this pass under the epoch guard,
			// so both name live bytes.
			e.loc.Store(packLoc(loc))
			return
		}
		i = (i + 1) & t.mask
	}
}

// retireCompactedExtent turns page pid into a hole and queues its extent for the next
// checkpoint to free (doc 06 section 5.5, 7.3). It runs under the shard write lock, so no
// reader (which holds the read lock on this profile) is mid-read of the page. By the time
// it runs every live record on the page has been copied off and the index repointed, so
// no live entry points into the extent. It publishes the page directory with the page
// nilled and its disk offset cleared, recycles the resident buffer straight back to the
// free pool (the write lock excludes every reader, so no epoch drain is needed) or drops
// the spilled-page count, zeroes the page's accounting, and appends the extent to the
// shard's pending-free list. The extent stays a hole, neither read nor reallocated, until
// the checkpoint that captured the repointed index commits its free.
func (sh *shard) retireCompactedExtent(pid int64) {
	sh.mu.Lock()
	ext := sh.pageExtent[pid]
	if ext < 0 {
		sh.mu.Unlock()
		return
	}
	d := sh.pages.Load()
	resident := d.refs[pid].Load().mem
	// Repoint the slot to a hole ref (mem nil, diskOff -1: neither resident nor on disk) in
	// one atomic store, no directory copy. The write lock excludes every reader on this
	// profile, so no reader is mid-read of the page.
	d.refs[pid].Store(&pageRef{diskOff: -1})

	if resident != nil {
		for k, rp := range sh.residentOrder {
			if rp == pid {
				sh.residentOrder = append(sh.residentOrder[:k], sh.residentOrder[k+1:]...)
				break
			}
		}
		sh.freeBufs = append(sh.freeBufs, resident)
	} else {
		sh.spilledPages--
	}

	sh.pageExtent[pid] = -1
	sh.deadBytes[pid] = 0
	sh.pageFill[pid] = 0
	sh.pageFlushed[pid] = 0
	sh.pageMaxLSN[pid] = 0
	sh.pendingFree = append(sh.pendingFree, ext)
	sh.mu.Unlock()
	sh.store.compactedExtents.Add(1)
}
