package pager

import (
	"fmt"

	"github.com/tamnd/kv/crypto"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
)

// Get returns the frame for pgno, pinned, reading it from the main file if it is
// not already resident. The caller must Unpin exactly once. intent is advisory in
// this milestone (it documents read vs write at the call site); dirtiness is
// declared at Unpin. Get contends only on the shard that owns pgno, so reads of
// pages in different shards proceed in parallel.
func (p *Pager) Get(pgno uint32, intent Intent) (*Frame, error) {
	if pgno == 0 {
		return nil, fmt.Errorf("pager: page 0 is the null page")
	}
	sh := p.shardFor(pgno)
	// Fast path: a resident page is pinned under the shard's read lock, so concurrent
	// readers of the same hot frame (the root and upper interiors every lookup touches)
	// pin in parallel instead of serializing on an exclusive lock. The pin is an atomic
	// increment, and eviction needs the exclusive lock, so it cannot take this frame
	// while a reader holds the read lock and is mid-pin.
	sh.mu.RLock()
	if fr, ok := sh.index[pgno]; ok {
		p.cacheHits.Add(1)
		if intent == Write {
			// The caller is about to mutate the page bytes, so any decoded view cached
			// against the old bytes is now stale. Drop it before the writer can touch the
			// page so the next reader re-decodes the new bytes.
			// writePinned is set before the shard lock is released so that a concurrent
			// lock-free checkpoint sees it and skips this frame's writeback; the checkpoint
			// holds the exclusive shard lock, which waits for this RLock to drain, so the
			// store is visible to the checkpoint by the time it calls writeBack.
			fr.clearDecoded()
			fr.writePinned.Store(true)
		}
		fr.pins.Add(1)
		fr.ref.Store(true)
		sh.mu.RUnlock()
		return fr, nil
	}
	sh.mu.RUnlock()

	// Miss: take the exclusive lock to admit and read. Re-check first, since another
	// goroutine may have admitted the page between the read-lock release and here.
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if fr, ok := sh.index[pgno]; ok {
		p.cacheHits.Add(1)
		if intent == Write {
			fr.clearDecoded()
			fr.writePinned.Store(true)
		}
		fr.pins.Add(1)
		fr.ref.Store(true)
		return fr, nil
	}
	fr, err := p.admit(sh, pgno)
	if err != nil {
		return nil, err
	}
	// Read the page from disk if it is within the materialized file; a freshly
	// allocated page beyond the on-disk size reads as zeroes. Either way a physical
	// read was issued, so it counts toward read amplification.
	p.pageReads.Add(1)
	off := int64(pgno-1) * int64(p.pageSize)
	if sc := p.cryptoScheme(); sc != nil && pgno != 1 {
		// Encrypted data page: read the ciphertext envelope into the shard's staging
		// buffer, verify its checksum, and decrypt into the frame, which holds plaintext.
		if err := p.readEncrypted(sh, sc, fr, pgno, off); err != nil {
			delete(sh.index, pgno)
			fr.pgno = 0
			fr.dirty = false
			return nil, err
		}
	} else if _, err := p.file.ReadAt(fr.data, off); err != nil {
		// A short read at the tail of a just-grown file is not an error; zero-fill.
		for i := range fr.data {
			fr.data[i] = 0
		}
	} else if err := verifyPageChecksum(fr.data, p.header.Checksum); err != nil {
		// The page read whole but failed its checksum: a torn write or bit rot. Drop
		// the frame so a retry re-reads rather than trusting the cached bad bytes, and
		// surface ErrCorrupt to the caller (spec 02 §3.2).
		delete(sh.index, pgno)
		fr.pgno = 0
		fr.dirty = false
		return nil, fmt.Errorf("pager: page %d: %w", pgno, err)
	}
	if intent == Write {
		fr.writePinned.Store(true)
	}
	fr.pins.Add(1)
	fr.ref.Store(true)
	return fr, nil
}

// Unpin releases one pin. If dirty, the frame is marked for write-back at the
// next checkpoint. The caller still holds a pin when calling, so the frame cannot
// be evicted underneath this and fr.pgno still names its shard.
//
// It takes the shard's read lock, not the exclusive lock: the pin count is atomic,
// so the decrement needs no exclusion, and taking the read lock keeps Unpin off the
// path that eviction and admission serialize on, so it does not re-serialize the
// parallel cache-hit reads in Get. The dirty flag is only set true by the write
// path, which the DB serializes behind its single write lock, so the two writers of
// fr.dirty (this and the exclusive-locked evict/writeBack) never overlap.
func (p *Pager) Unpin(fr *Frame, dirty bool) {
	sh := p.shardFor(fr.pgno)
	sh.mu.RLock()
	if dirty {
		fr.dirty = true
	}
	// Clear writePinned before releasing the shard lock. The checkpoint holds the
	// exclusive shard lock while reading fr.data; it skips any frame where
	// writePinned is true (the B-tree may still be writing its bytes). Once this
	// Unpin runs, the write session is over and the frame is safe to flush.
	fr.writePinned.Store(false)
	fr.pins.Add(-1)
	sh.mu.RUnlock()
}

// BeginExternalWrite marks the start of a page-producing section that runs outside the
// host's single write lock, and EndExternalWrite marks its end. The LSM core's background
// flusher brackets its segment build and install with the pair so a checkpoint waits for
// the build to finish rather than reading a half-written frame. Producers share the gate, so
// two background writers never block each other; only a checkpoint, which holds it
// exclusively, waits. The matching EndExternalWrite must run on every path, so callers defer
// it.
func (p *Pager) BeginExternalWrite() { p.ckptGate.RLock() }

// EndExternalWrite ends a section opened by BeginExternalWrite.
func (p *Pager) EndExternalWrite() { p.ckptGate.RUnlock() }

// admit finds a free or evictable frame in sh, binds it to pgno, and indexes it.
// The caller must hold sh.mu. The returned frame is not yet pinned.
func (p *Pager) admit(sh *shard, pgno uint32) (*Frame, error) {
	fr := p.evict(sh)
	if fr == nil {
		return nil, fmt.Errorf("pager: buffer pool exhausted (all frames pinned)")
	}
	fr.pgno = pgno
	fr.dirty = false
	fr.ref.Store(false)
	// The frame is being rebound to a different page (or to a freshly allocated one),
	// so a decoded view left over from its previous page must not survive into this one.
	fr.clearDecoded()
	sh.index[pgno] = fr
	return fr, nil
}

// evict returns a reusable frame from sh via CLOCK: sweep its frames, clearing
// reference bits, and take the first unpinned frame whose bit is already clear. A
// dirty victim is written back to the main file first (its describing WAL batch is
// already durable by the time any page is dirtied, so this respects the WAL rule).
// The caller must hold sh.mu.
func (p *Pager) evict(sh *shard) *Frame {
	// Fast path: an unbound frame is immediately reusable.
	for _, fr := range sh.frames {
		if fr.pgno == 0 && fr.pins.Load() == 0 {
			return fr
		}
	}
	n := len(sh.frames)
	for i := 0; i < 2*n; i++ {
		fr := sh.frames[sh.hand]
		sh.hand = (sh.hand + 1) % n
		if fr.pins.Load() != 0 {
			continue
		}
		if fr.ref.Load() {
			fr.ref.Store(false)
			continue
		}
		// Victim found.
		if fr.dirty {
			if err := p.writeBack(sh, fr); err != nil {
				// If write-back fails, skip this victim and try another.
				continue
			}
		}
		delete(sh.index, fr.pgno)
		fr.pgno = 0
		fr.dirty = false
		return fr
	}
	return nil
}

// writeBack flushes one dirty frame to the main file. It stamps the page's checksum
// into the reserved trailer first, so every page reaches disk self-describing and a
// later read can detect a torn write or bit rot (spec 02 §3.2). The stamp lands in
// the trailer the engine never uses, so it does not disturb the cached node body.
// The caller must hold sh.mu (sh owns fr).
func (p *Pager) writeBack(sh *shard, fr *Frame) error {
	if sc := p.cryptoScheme(); sc != nil && fr.pgno != 1 {
		return p.writeBackEncrypted(sh, sc, fr)
	}
	format.StampPageChecksum(fr.data, p.header.Checksum)
	off := int64(fr.pgno-1) * int64(p.pageSize)
	if _, err := p.file.WriteAt(fr.data, off); err != nil {
		return err
	}
	fr.dirty = false
	return nil
}

// scratchBuf returns the shard's pageSize crypto staging buffer, allocating it on first
// use. The caller must hold the shard's mu; the encrypted read and write paths run under
// it and use the buffer for the span of a single page operation, so one buffer per shard
// serves all of that shard's encrypted traffic without sharing memory across cores.
func (sh *shard) scratchBuf(pageSize int) []byte {
	if cap(sh.scratch) < pageSize {
		sh.scratch = make([]byte, pageSize)
	}
	return sh.scratch[:pageSize]
}

// readEncrypted reads an encrypted data page into a frame: it loads the on-disk envelope
// into the shard's staging buffer, verifies the page checksum, and decrypts the envelope
// into the frame's plaintext window, zeroing the reserved tail. An all-zero page (a freshly
// grown hole never written) or a short read at the file tail yields a zero plaintext frame,
// the same as the cleartext path. A checksum mismatch or a failed decrypt is corruption and
// is returned as an error. The caller must hold sh.mu.
func (p *Pager) readEncrypted(sh *shard, sc *crypto.Scheme, fr *Frame, pgno uint32, off int64) error {
	buf := sh.scratchBuf(p.pageSize)
	if _, err := p.file.ReadAt(buf, off); err != nil {
		for i := range fr.data {
			fr.data[i] = 0
		}
		return nil
	}
	if allZero(buf) {
		for i := range fr.data {
			fr.data[i] = 0
		}
		return nil
	}
	if err := verifyPageChecksum(buf, p.header.Checksum); err != nil {
		return fmt.Errorf("pager: page %d: %w", pgno, err)
	}
	env := buf[:p.header.UsablePageSize()+crypto.Overhead]
	pt, err := sc.OpenPage(fr.data[:0], env, pgno)
	if err != nil {
		return fmt.Errorf("pager: page %d: decrypt: %w", pgno, err)
	}
	// OpenPage wrote the plaintext into the frame's backing array; clear any bytes past
	// it (the reserved trailer) so the engine never sees stale data there.
	for i := len(pt); i < len(fr.data); i++ {
		fr.data[i] = 0
	}
	return nil
}

// writeBackEncrypted flushes a dirty data page as ciphertext: it seals the frame's
// plaintext window into the shard's staging buffer, stamps the page checksum over the
// envelope, and writes the whole page. The plaintext usable area, the AEAD tag and nonce,
// and the checksum exactly fill the page, since the reserved trailer was widened to
// crypto.Overhead plus the checksum size at Create (spec 14 §3). The caller must hold sh.mu.
func (p *Pager) writeBackEncrypted(sh *shard, sc *crypto.Scheme, fr *Frame) error {
	buf := sh.scratchBuf(p.pageSize)
	env, err := sc.SealPage(buf[:0], fr.data[:p.header.UsablePageSize()], fr.pgno)
	if err != nil {
		return err
	}
	// Zero anything between the envelope and the checksum trailer; for a full page the two
	// meet exactly, so this only clears the trailer before the checksum is stamped over it.
	for i := len(env); i < len(buf); i++ {
		buf[i] = 0
	}
	format.StampPageChecksum(buf, p.header.Checksum)
	off := int64(fr.pgno-1) * int64(p.pageSize)
	if _, err := p.file.WriteAt(buf, off); err != nil {
		return err
	}
	fr.dirty = false
	return nil
}

// verifyPageChecksum checks a freshly read page against its stored checksum,
// skipping a page that is entirely zero: an uninitialized hole or a never-written
// allocation carries no checksum and is not corruption (spec 02 §3.2). A real
// written page always begins with a non-zero type byte.
func verifyPageChecksum(page []byte, algo format.ChecksumAlgo) error {
	if allZero(page) {
		return nil
	}
	return format.VerifyPageChecksum(page, algo)
}

// allZero reports whether every byte of b is zero.
func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// Allocate returns a fresh page, pinned with Write intent and zeroed. It reuses a
// page from the freelist if one is available, otherwise it grows the file by one
// page (high-water mark). The number is reserved under metaMu and the frame is bound
// under the owning shard, two separate locks taken in sequence; the reserved number
// is already off the freelist so nothing else can claim it in between.
func (p *Pager) Allocate() (uint32, *Frame, error) {
	pgno := p.AllocateNumber()
	fr, err := p.GetAllocated(pgno)
	if err != nil {
		return 0, nil, err
	}
	return pgno, fr, nil
}

// AllocateNumber reserves a fresh page number without binding a frame to it: it pops
// the freelist or grows the high-water mark and returns the number, unpinned and
// unread. The reservation is immediate, so the number is already off the freelist and
// will not be handed out twice. The caller pairs it with GetAllocated when it is ready
// to write that page, which lets a bulk writer reserve a whole run of page numbers (to
// chain their next-pointers) while holding at most one frame pinned at a time. That is
// what keeps a segment flush from pinning the entire segment's worth of frames at once
// and exhausting a pool smaller than the segment (perf/05 F2).
func (p *Pager) AllocateNumber() uint32 {
	p.metaMu.Lock()
	defer p.metaMu.Unlock()
	return p.allocateNumberLocked()
}

// allocateNumberLocked pops the freelist or bumps the high-water mark and returns the
// reserved page number. The caller must hold metaMu.
func (p *Pager) allocateNumberLocked() uint32 {
	if n := len(p.free); n > 0 {
		pgno := p.free[n-1]
		p.free = p.free[:n-1]
		return pgno
	}
	p.dbSize++
	return p.dbSize
}

// GetAllocated returns the frame for a page number that came from AllocateNumber,
// pinned with Write intent and zeroed, WITHOUT reading the old on-disk bytes. The page
// is about to be written in full, so its prior content (a hole past the high-water mark
// or a stale freed page) is dead and a read would be wasted I/O. The caller Unpins
// exactly once, as with Allocate; this is simply Allocate's second half, split out so a
// bulk writer can materialize reserved pages one at a time.
func (p *Pager) GetAllocated(pgno uint32) (*Frame, error) {
	sh := p.shardFor(pgno)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	fr, err := p.admit(sh, pgno)
	if err != nil {
		return nil, err
	}
	for i := range fr.data {
		fr.data[i] = 0
	}
	fr.dirty = true
	fr.pins.Add(1)
	fr.ref.Store(true)
	return fr, nil
}

// Free returns a page to the freelist. The page must not be pinned. It drops any cached
// frame under the owning shard, then appends to the freelist under metaMu (shard before
// meta, the global lock order).
func (p *Pager) Free(pgno uint32) {
	sh := p.shardFor(pgno)
	sh.mu.Lock()
	if fr, ok := sh.index[pgno]; ok {
		delete(sh.index, pgno)
		fr.pgno = 0
		fr.dirty = false
		fr.ref.Store(false)
		fr.clearDecoded()
	}
	sh.mu.Unlock()
	p.metaMu.Lock()
	p.free = append(p.free, pgno)
	p.metaMu.Unlock()
}

// lockAllShards takes every shard lock in ascending index order and returns a function
// that releases them in reverse. Heavy, infrequent operations (checkpoint, truncate,
// close) use it to make the whole pool exclusive; the hot paths take a single shard lock
// and so simply wait their turn rather than deadlock, because the order is always
// shards-ascending then metaMu.
func (p *Pager) lockAllShards() func() {
	for _, sh := range p.shards {
		sh.mu.Lock()
	}
	return func() {
		for i := len(p.shards) - 1; i >= 0; i-- {
			p.shards[i].mu.Unlock()
		}
	}
}

// Checkpoint writes every dirty frame to the main file, persists the freelist and
// header, and fsyncs. After it returns, the main file is a consistent image of
// all committed work and contains no torn pages. checkpointLSN is recorded in the
// header so recovery knows which WAL frames precede this checkpoint.
// Checkpoint writes all dirty frames back to the main file, updates the header, and
// fsyncs. lastCommitVersion is the oracle's committed version at the moment the
// checkpoint was prepared (under d.mu in the host); setting it here, under the pager's
// own shard and meta locks, avoids a data race with the commit path that also updates
// LastCommitVersion under d.mu, a lock independent of the pager's own locks.
func (p *Pager) Checkpoint(checkpointLSN, lastCommitVersion uint64) error {
	// Exclude any external page producer (the LSM background flusher) for the duration: it
	// writes frame buffers without the host write lock a checkpoint relies on, so without
	// this gate the writeBack below could read a frame the flusher is still filling.
	p.ckptGate.Lock()
	defer p.ckptGate.Unlock()
	unlock := p.lockAllShards()
	defer unlock()
	p.metaMu.Lock()
	defer p.metaMu.Unlock()

	// Flush dirty data frames across every shard. Skip frames that are write-pinned:
	// a concurrent commit's B-tree is still writing those bytes (it holds d.mu, the
	// checkpoint released d.mu for the lock-free I/O phase). Their WAL frames are at
	// LSN > foldedLSN, so finalizeCheckpointLocked will detect actualFoldedLSN >
	// foldedLSN and call Checkpoint again (under d.mu) to flush them. For callers of
	// checkpointLocked (which hold d.mu throughout), no concurrent write-intent pins
	// can exist and writePinned is always false, so the skip never fires there.
	for _, sh := range p.shards {
		for _, fr := range sh.frames {
			if fr.pgno != 0 && fr.dirty && !fr.writePinned.Load() {
				if err := p.writeBack(sh, fr); err != nil {
					return err
				}
			}
		}
	}
	if err := p.persistFreelistLocked(); err != nil {
		return err
	}
	// Update and write the header page. LastCommitVersion is stamped here (not by the
	// caller) so that the CheckpointMode lock-free path does not need to update the
	// shared header field before releasing d.mu — which would race with concurrent
	// commits that also write it under d.mu.
	p.header.DBSize = p.dbSize
	p.header.HighWaterMark = p.dbSize
	p.header.CheckpointLSN = checkpointLSN
	p.header.LastCommitVersion = lastCommitVersion
	p.header.ChangeCounter++
	p.header.VersionValidFor = p.header.ChangeCounter
	if err := p.flushHeaderLocked(); err != nil {
		return err
	}
	return p.file.Sync(vfs.SyncFull)
}

// flushHeaderLocked encodes the live header onto page 1 and writes it to the file,
// preserving any non-header bytes already on the page and keeping the resident
// frame, if any, in sync. It does not fsync; the caller decides when to make the
// write durable. The caller must hold the shard that owns page 1 (and, for the
// checkpoint/truncate paths that also touch the freelist and header, metaMu).
func (p *Pager) flushHeaderLocked() error {
	sh := p.shardFor(1)
	page1 := make([]byte, p.pageSize)
	// Preserve any non-header bytes already on page 1 by reading the resident
	// frame if present, else the file.
	if fr, ok := sh.index[1]; ok {
		copy(page1, fr.data)
	} else {
		_, _ = p.file.ReadAt(page1, 0)
	}
	p.header.Encode(page1)
	format.StampPageChecksum(page1, p.header.Checksum)
	if fr, ok := sh.index[1]; ok {
		copy(fr.data, page1)
		fr.dirty = false
	}
	if _, err := p.file.WriteAt(page1, 0); err != nil {
		return err
	}
	return nil
}

// Rekey swaps the encryption scheme new and rewritten pages are sealed under and rewrites
// the cleartext descriptor on page 1 to record the new epoch, the main-file half of a key
// rotation (spec 14 §5). It does not re-encrypt the pages already on disk: those keep the
// epoch their envelopes record and stay readable under the new scheme, which derives any
// earlier epoch's key from the same master. The new descriptor is written and fsynced before
// the scheme pointer is swapped, so a crash mid-rekey leaves either the old descriptor with
// old-epoch pages or the new descriptor with the same old-epoch pages, both of which open.
// The caller holds no pager lock; Rekey takes the page-1 shard then metaMu itself.
func (p *Pager) Rekey(newScheme *crypto.Scheme, newDescriptor []byte) error {
	sh := p.shardFor(1)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	p.metaMu.Lock()
	defer p.metaMu.Unlock()
	if p.cryptoScheme() == nil {
		return ErrNotEncrypted
	}
	if format.HeaderSize+len(newDescriptor) > p.pageSize-p.header.Checksum.ChecksumSize() {
		return fmt.Errorf("pager: rotated encryption descriptor does not fit on page 1")
	}
	page1 := make([]byte, p.pageSize)
	if fr, ok := sh.index[1]; ok {
		copy(page1, fr.data)
	} else {
		_, _ = p.file.ReadAt(page1, 0)
	}
	p.header.Encode(page1)
	copy(page1[format.HeaderSize:], newDescriptor)
	format.StampPageChecksum(page1, p.header.Checksum)
	if fr, ok := sh.index[1]; ok {
		copy(fr.data, page1)
		fr.dirty = false
	}
	if _, err := p.file.WriteAt(page1, 0); err != nil {
		return err
	}
	if err := p.file.Sync(vfs.SyncFull); err != nil {
		return err
	}
	p.crypto.Store(newScheme)
	return nil
}

// TruncateTail returns trailing free pages to the operating system by shrinking the
// file (spec 09 §3.1, incremental vacuum). It reclaims the maximal contiguous run of
// free pages at the very end of the file: as long as the highest page number is on
// the freelist it is dropped from the freelist and the high-water mark falls by one,
// stopping at the first reachable page or when budget pages have been freed (a
// non-positive budget reclaims the whole trailing run). It then persists the smaller
// freelist and header, fsyncs, truncates the file, and fsyncs again, so a clean run
// hands the freed space back to the filesystem. Page 1, the header, is never freed.
//
// Only pages physically at the tail can be returned to the OS; free pages buried in
// the middle of the file stay on the freelist for reallocation (spec 09 §3.1). The
// caller is expected to have folded the WAL with a checkpoint first so the freelist
// reflects all committed frees.
func (p *Pager) TruncateTail(budget int) (int, error) {
	unlock := p.lockAllShards()
	defer unlock()
	p.metaMu.Lock()
	defer p.metaMu.Unlock()

	// Index the freelist for O(1) tail membership tests.
	freeset := make(map[uint32]struct{}, len(p.free))
	for _, pg := range p.free {
		freeset[pg] = struct{}{}
	}
	freed := 0
	for p.dbSize > 1 {
		if _, ok := freeset[p.dbSize]; !ok {
			break
		}
		if budget > 0 && freed >= budget {
			break
		}
		delete(freeset, p.dbSize)
		// Drop any cached frame for the page being reclaimed.
		sh := p.shardFor(p.dbSize)
		if fr, ok := sh.index[p.dbSize]; ok {
			delete(sh.index, p.dbSize)
			fr.pgno = 0
			fr.dirty = false
			fr.ref.Store(false)
		}
		p.dbSize--
		freed++
	}
	if freed == 0 {
		return 0, nil
	}
	// Rebuild the in-memory freelist without the reclaimed tail pages, preserving
	// order of the survivors.
	survivors := p.free[:0:0]
	for _, pg := range p.free {
		if _, ok := freeset[pg]; ok {
			survivors = append(survivors, pg)
		}
	}
	p.free = survivors

	// Persist the smaller freelist and header before shrinking the file. The trunk
	// pages persistFreelistLocked reserves come from the surviving freelist, all
	// below the new high-water mark, so they live inside the truncated file.
	if err := p.persistFreelistLocked(); err != nil {
		return 0, err
	}
	p.header.DBSize = p.dbSize
	p.header.HighWaterMark = p.dbSize
	if err := p.flushHeaderLocked(); err != nil {
		return 0, err
	}
	if err := p.file.Sync(vfs.SyncFull); err != nil {
		return 0, err
	}
	if err := p.file.Truncate(int64(p.dbSize) * int64(p.pageSize)); err != nil {
		return 0, err
	}
	if err := p.file.Sync(vfs.SyncFull); err != nil {
		return 0, err
	}
	return freed, nil
}

// Close flushes nothing implicitly; it just releases the file. Callers checkpoint
// first for a clean shutdown.
func (p *Pager) Close() error {
	unlock := p.lockAllShards()
	defer unlock()
	p.metaMu.Lock()
	defer p.metaMu.Unlock()
	return p.file.Close()
}

// CheckpointLSN reports the WAL LSN recorded by the last checkpoint.
func (p *Pager) CheckpointLSN() uint64 {
	p.metaMu.Lock()
	defer p.metaMu.Unlock()
	return p.header.CheckpointLSN
}

// loadFreelist reads the freelist trunk chain into memory. The caller need not
// hold any lock (called during Open before the pager is shared).
func (p *Pager) loadFreelist() error {
	trunk := p.header.FreelistTrunk
	usable := p.header.UsablePageSize()
	buf := make([]byte, p.pageSize)
	for trunk != 0 {
		off := int64(trunk-1) * int64(p.pageSize)
		if _, err := p.file.ReadAt(buf, off); err != nil {
			return fmt.Errorf("pager: read freelist trunk %d: %w", trunk, err)
		}
		if err := verifyPageChecksum(buf, p.header.Checksum); err != nil {
			return fmt.Errorf("pager: freelist trunk %d: %w", trunk, err)
		}
		tp := format.DecodeTrunk(buf, usable)
		p.free = append(p.free, tp.Leafs...)
		// The trunk page itself is also a free page once drained.
		p.free = append(p.free, trunk)
		trunk = tp.Next
	}
	return nil
}

// persistFreelistLocked writes the in-memory freelist back as a trunk chain. The
// caller must hold metaMu. For simplicity this milestone packs the whole freelist
// into a single trunk chain rebuilt from scratch each checkpoint.
func (p *Pager) persistFreelistLocked() error {
	usable := p.header.UsablePageSize()
	cap := format.TrunkCapacity(usable)
	if len(p.free) == 0 || cap == 0 {
		p.header.FreelistTrunk = 0
		p.header.FreelistPages = 0
		return nil
	}
	// Reserve trunk pages from the freelist itself; each trunk page holds up to
	// cap leaf numbers. We need ceil(len/cap) trunks, but each trunk consumes one
	// free page, so iterate until stable.
	free := append([]uint32(nil), p.free...)
	var trunks []uint32
	// Pull trunk pages off the tail; remaining are leaves.
	// Number of trunks needed grows as we remove them; solve iteratively.
	for {
		nTrunks := len(trunks)
		leaves := len(free) - nTrunks
		need := (leaves + cap - 1) / cap
		if need <= nTrunks {
			break
		}
		// Take one more page to be a trunk.
		trunks = append(trunks, free[len(free)-1-nTrunks])
	}
	// Partition: the last len(trunks) pages are trunks, the rest are leaves.
	nTrunks := len(trunks)
	leaves := free[:len(free)-nTrunks]
	trunkPages := free[len(free)-nTrunks:]

	buf := make([]byte, p.pageSize)
	var next uint32
	li := 0
	for ti := 0; ti < len(trunkPages); ti++ {
		tp := format.TrunkPage{Next: next}
		end := li + cap
		if end > len(leaves) {
			end = len(leaves)
		}
		tp.Leafs = leaves[li:end]
		li = end
		for i := range buf {
			buf[i] = 0
		}
		format.EncodeTrunk(buf, tp)
		format.StampPageChecksum(buf, p.header.Checksum)
		off := int64(trunkPages[ti]-1) * int64(p.pageSize)
		if _, err := p.file.WriteAt(buf, off); err != nil {
			return err
		}
		next = trunkPages[ti]
	}
	p.header.FreelistTrunk = next
	p.header.FreelistPages = uint32(len(p.free))
	return nil
}
