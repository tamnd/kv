package f2

// Compaction reclaims the dead bytes an append-only log strands. Every overwrite
// and delete leaves the old record behind (shard.go credits it to deadBytes), so a
// durable store under churn grows with the operation count, not the live-key
// count, until something rewrites the live records into a fresh log and drops the
// dead ones. This file is that rewrite.
//
// The shape is a generational whole-shard rewrite (the f2 design doc's "shape that
// fits f2", spec 2070 implementation/F2-compaction.md section 2). f2 couples a
// logical address to a page position (addr/pageSize is the page index), so it
// cannot free a middle page the way hashlog frees a middle extent: a hole in the
// page sequence makes every later address unreachable and recovery would truncate
// the shard there. So compaction rewrites a whole shard at once into a new
// generation of contiguous pages 0..m, stamps those pages with a higher generation
// number, and lets recovery prefer the higher generation. At 256 shards a rewrite
// touches one shard's live data, a 1/256 slice, which bounds the copy and the
// pause.
//
// Crash safety rests on one ordering: the new generation's pages 1..m are written
// and fsynced first, then page 0 last. A durable page 0 at generation G proves
// every page of G already reached disk, so recovery takes, per shard, the highest
// generation that has a page 0 (recovery.go). A crash mid-rewrite therefore
// recovers to the whole old generation (the new page 0 never landed) or the whole
// new one (it did), never a torn mix.
//
// Reader safety rests on the epoch gate: after the swap the old generation's file
// blocks are retired behind the safe epoch (epoch.go), so a lock-free reader still
// preading an evicted old page is never handed a reused block. Resident old pages
// need no gate, the garbage collector keeps them alive for any reader that still
// holds the old index.

// defaultCompactThreshold is the dead fraction at which a shard is worth
// rewriting when Tunables leaves CompactionThreshold zero. At 0.5 a shard is
// rewritten once half its log bytes are dead, the Bitcask-style midpoint that
// trades steady-state write amplification against how much dead space is tolerated
// (doc 06 section 4.2).
const defaultCompactThreshold = 0.5

// compactThreshold is the configured dead-fraction trigger, or the default.
func (s *Store) compactThreshold() float64 {
	if s.t.CompactionThreshold > 0 {
		return s.t.CompactionThreshold
	}
	return defaultCompactThreshold
}

// Compact rewrites every shard whose dead fraction is over the threshold, once,
// synchronously, reclaiming the dead bytes each holds. It is the operator and test
// entry point; a store can also run it in the background by setting
// CompactionInterval. In the memory-only profile nothing is durable and nothing is
// stranded, so it is a no-op.
func (s *Store) Compact() error {
	if s.closed.Load() {
		return errClosed
	}
	if s.df == nil {
		return nil
	}
	for _, sh := range s.shards {
		if err := s.maybeCompactShard(sh); err != nil {
			return err
		}
	}
	return nil
}

// maybeCompactShard rewrites one shard if it is over the dead-fraction threshold.
// It always drains the shard's deferred-free queue first, so blocks a past
// compaction retired return to the allocator as soon as the readers that could
// have held them are gone, whether or not this shard compacts again.
func (s *Store) maybeCompactShard(sh *shard) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.drainDeferredLocked(s.ep, s.df)
	if !sh.shouldCompact(s.compactThreshold()) {
		return nil
	}
	return s.compactLocked(sh)
}

// forceCompact rewrites a shard unconditionally, ignoring the trigger. It is the
// hook tests use to compact a shard regardless of how much is dead.
func (s *Store) forceCompact(sh *shard) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.drainDeferredLocked(s.ep, s.df)
	return s.compactLocked(sh)
}

// shouldCompact reports whether the shard's dead fraction is at or over the
// threshold. The caller holds the shard lock so the byte counters are stable.
func (sh *shard) shouldCompact(threshold float64) bool {
	if sh.logBytes <= 0 {
		return false
	}
	return float64(sh.deadBytes) >= threshold*float64(sh.logBytes)
}

// compactLocked is the whole-shard rewrite. The caller holds the shard write lock,
// so no writer races the build or the swap; readers stay lock-free throughout and
// see the old generation until the single index store publishes the new one.
func (s *Store) compactLocked(sh *shard) error {
	oldIdx := sh.index.Load()
	oldLog := sh.log

	// A fresh log one generation higher than the current one, built fully resident:
	// no disk IO during the build, so the shard lock is held only for an in-memory
	// copy plus the final bulk write.
	nl := newLog(int(oldLog.pageSize), s.df, oldLog.shardID, oldLog.budget)
	nl.gen = oldLog.gen + 1

	// Size the new index up front to hold the live set under the load factor, so the
	// build never grows it mid-copy. want is live / 0.7 rounded up; newIndex rounds
	// that to a power of two at or above it.
	want := oldIdx.live*loadDen/loadNum + 1
	ni := newIndex(want)
	ni.log = nl

	// Copy every live record forward. Tombstones and the records they shadow are
	// simply not copied: a whole-shard rewrite has no other generation an older
	// value could survive in, so dropping a tombstone here can never resurrect a key
	// (doc 06 section 3.4, made unconditional by the whole-shard shape).
	var liveBytes int64
	for i := range oldIdx.slots {
		slot := oldIdx.slots[i].Load()
		if slot == 0 || slot&slotTombstone != 0 {
			continue
		}
		key, value := oldLog.read(slotAddr(slot))
		if key == nil {
			continue // an unreadable record: drop it rather than copy garbage forward
		}
		off := nl.packResident(key, value)
		h := hash64(key)
		insertCompacted(ni, tagOf(h), h, off)
		liveBytes += int64(nl.recordLenKV(key, value))
	}

	// A shard that compacted to empty still needs a page 0 at the new generation, so
	// it carries a commit marker that supersedes the old generation's page 0.
	// Without it recovery would find no new page 0, fall back to the old generation,
	// and resurrect every deleted key.
	if nl.npages == 0 {
		nl.addPageResident()
	}

	// Make the new generation durable before anything can read it: pages 1..m
	// fsynced first, then page 0, so a crash recovers old-or-new (section 2).
	if err := nl.commitGeneration(); err != nil {
		return err
	}
	nl.evictToBudget()

	// Publish the new generation. ni.log already points at nl, so this one atomic
	// store flips the whole generation for a lock-free reader: it resolves through
	// either the old index and old log or the new index and new log, never a mix.
	sh.log = nl
	sh.index.Store(ni)
	sh.logBytes = liveBytes
	sh.deadBytes = 0

	// Retire the old generation's blocks behind the epoch gate. advance opens a
	// fresh epoch after the swap, so any reader still holding the old index pinned an
	// earlier epoch and keeps these blocks until it leaves; the drain returns the
	// ones already past the safe epoch (every block at once when no reader is
	// active, as in a test or a quiescent store).
	retire := s.ep.advance()
	for _, b := range oldLog.pageBlock {
		sh.deferred = append(sh.deferred, deferredFree{block: b, retireEpoch: retire})
	}
	sh.drainDeferredLocked(s.ep, s.df)
	return nil
}

// insertCompacted claims a slot for a key copied into a fresh index during a
// rewrite. The keys are the live set, each distinct, so it only ever lands on an
// empty slot, no tombstone or overwrite case to handle. The caller sized the table
// to stay under the load factor, so the probe is short.
func insertCompacted(ni *index, tag, h uint64, off int64) {
	mask := ni.mask
	j := (h ^ (h >> 15)) & mask
	for ni.slots[j].Load() != 0 {
		j = (j + 1) & mask
	}
	ni.slots[j].Store(makeSlot(tag, off))
	ni.live++
	ni.used++
}

// drainDeferredLocked returns every retired block whose retire epoch the safe
// epoch has passed to the allocator's free list, keeping the rest queued. A block
// retired at epoch r is reusable once safeEpoch is strictly greater than r, because
// then every active reader is inside a later epoch and none can hold an offset into
// it (epoch.go). The caller holds the shard write lock.
func (sh *shard) drainDeferredLocked(ep *epochs, df *durableFile) {
	if len(sh.deferred) == 0 {
		return
	}
	safe := ep.slots.safeEpoch()
	kept := sh.deferred[:0]
	for _, d := range sh.deferred {
		if d.retireEpoch < safe {
			df.freeBlock(d.block)
		} else {
			kept = append(kept, d)
		}
	}
	sh.deferred = kept
}
