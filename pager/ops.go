package pager

import (
	"fmt"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
)

// Get returns the frame for pgno, pinned, reading it from the main file if it is
// not already resident. The caller must Unpin exactly once. intent is advisory in
// this milestone (it documents read vs write at the call site); dirtiness is
// declared at Unpin.
func (p *Pager) Get(pgno uint32, intent Intent) (*Frame, error) {
	if pgno == 0 {
		return nil, fmt.Errorf("pager: page 0 is the null page")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if fr, ok := p.index[pgno]; ok {
		fr.pins.Add(1)
		fr.ref = true
		return fr, nil
	}
	fr, err := p.admit(pgno)
	if err != nil {
		return nil, err
	}
	// Read the page from disk if it is within the materialized file; a freshly
	// allocated page beyond the on-disk size reads as zeroes.
	off := int64(pgno-1) * int64(p.pageSize)
	if _, err := p.file.ReadAt(fr.data, off); err != nil {
		// A short read at the tail of a just-grown file is not an error; zero-fill.
		for i := range fr.data {
			fr.data[i] = 0
		}
	}
	fr.pins.Add(1)
	fr.ref = true
	return fr, nil
}

// Unpin releases one pin. If dirty, the frame is marked for write-back at the
// next checkpoint.
func (p *Pager) Unpin(fr *Frame, dirty bool) {
	p.mu.Lock()
	if dirty {
		fr.dirty = true
	}
	fr.pins.Add(-1)
	p.mu.Unlock()
}

// admit finds a free or evictable frame, binds it to pgno, and indexes it. The
// caller must hold p.mu. The returned frame is not yet pinned.
func (p *Pager) admit(pgno uint32) (*Frame, error) {
	fr := p.evict()
	if fr == nil {
		return nil, fmt.Errorf("pager: buffer pool exhausted (all frames pinned)")
	}
	fr.pgno = pgno
	fr.dirty = false
	fr.ref = false
	p.index[pgno] = fr
	return fr, nil
}

// evict returns a reusable frame via CLOCK: sweep, clearing reference bits, and
// take the first unpinned frame whose bit is already clear. A dirty victim is
// written back to the main file first (its describing WAL batch is already
// durable by the time any page is dirtied, so this respects the WAL rule). The
// caller must hold p.mu.
func (p *Pager) evict() *Frame {
	// Fast path: an unbound frame is immediately reusable.
	for _, fr := range p.pool {
		if fr.pgno == 0 && fr.pins.Load() == 0 {
			return fr
		}
	}
	n := len(p.pool)
	for i := 0; i < 2*n; i++ {
		fr := p.pool[p.hand]
		p.hand = (p.hand + 1) % n
		if fr.pins.Load() != 0 {
			continue
		}
		if fr.ref {
			fr.ref = false
			continue
		}
		// Victim found.
		if fr.dirty {
			if err := p.writeBack(fr); err != nil {
				// If write-back fails, skip this victim and try another.
				continue
			}
		}
		delete(p.index, fr.pgno)
		fr.pgno = 0
		fr.dirty = false
		return fr
	}
	return nil
}

// writeBack flushes one dirty frame to the main file. The caller must hold p.mu.
func (p *Pager) writeBack(fr *Frame) error {
	off := int64(fr.pgno-1) * int64(p.pageSize)
	if _, err := p.file.WriteAt(fr.data, off); err != nil {
		return err
	}
	fr.dirty = false
	return nil
}

// Allocate returns a fresh page, pinned with Write intent and zeroed. It reuses a
// page from the freelist if one is available, otherwise it grows the file by one
// page (high-water mark).
func (p *Pager) Allocate() (uint32, *Frame, error) {
	p.mu.Lock()
	var pgno uint32
	if n := len(p.free); n > 0 {
		pgno = p.free[n-1]
		p.free = p.free[:n-1]
	} else {
		p.dbSize++
		pgno = p.dbSize
	}
	fr, err := p.admit(pgno)
	if err != nil {
		p.mu.Unlock()
		return 0, nil, err
	}
	for i := range fr.data {
		fr.data[i] = 0
	}
	fr.dirty = true
	fr.pins.Add(1)
	fr.ref = true
	p.mu.Unlock()
	return pgno, fr, nil
}

// Free returns a page to the freelist. The page must not be pinned.
func (p *Pager) Free(pgno uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if fr, ok := p.index[pgno]; ok {
		delete(p.index, pgno)
		fr.pgno = 0
		fr.dirty = false
		fr.ref = false
	}
	p.free = append(p.free, pgno)
}

// Checkpoint writes every dirty frame to the main file, persists the freelist and
// header, and fsyncs. After it returns, the main file is a consistent image of
// all committed work and contains no torn pages. checkpointLSN is recorded in the
// header so recovery knows which WAL frames precede this checkpoint.
func (p *Pager) Checkpoint(checkpointLSN uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Flush dirty data frames.
	for _, fr := range p.pool {
		if fr.pgno != 0 && fr.dirty {
			if err := p.writeBack(fr); err != nil {
				return err
			}
		}
	}
	if err := p.persistFreelistLocked(); err != nil {
		return err
	}
	// Update and write the header page.
	p.header.DBSize = p.dbSize
	p.header.HighWaterMark = p.dbSize
	p.header.CheckpointLSN = checkpointLSN
	p.header.ChangeCounter++
	p.header.VersionValidFor = p.header.ChangeCounter
	page1 := make([]byte, p.pageSize)
	// Preserve any non-header bytes already on page 1 by reading the resident
	// frame if present, else the file.
	if fr, ok := p.index[1]; ok {
		copy(page1, fr.data)
	} else {
		_, _ = p.file.ReadAt(page1, 0)
	}
	p.header.Encode(page1)
	if fr, ok := p.index[1]; ok {
		copy(fr.data, page1)
		fr.dirty = false
	}
	if _, err := p.file.WriteAt(page1, 0); err != nil {
		return err
	}
	return p.file.Sync(vfs.SyncFull)
}

// Close flushes nothing implicitly; it just releases the file. Callers checkpoint
// first for a clean shutdown.
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.file.Close()
}

// CheckpointLSN reports the WAL LSN recorded by the last checkpoint.
func (p *Pager) CheckpointLSN() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.header.CheckpointLSN
}

// loadFreelist reads the freelist trunk chain into memory. The caller need not
// hold p.mu (called during Open before the pager is shared).
func (p *Pager) loadFreelist() error {
	trunk := p.header.FreelistTrunk
	usable := p.header.UsablePageSize()
	buf := make([]byte, p.pageSize)
	for trunk != 0 {
		off := int64(trunk-1) * int64(p.pageSize)
		if _, err := p.file.ReadAt(buf, off); err != nil {
			return fmt.Errorf("pager: read freelist trunk %d: %w", trunk, err)
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
// caller must hold p.mu. For simplicity this milestone packs the whole freelist
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
