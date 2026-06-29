package f2

import (
	"context"
	"sort"
	"time"
)

// recover rebuilds every shard's log structure and compact index from the file
// alone, after a crash or a clean reopen. It trusts nothing in RAM: the file's
// self-describing blocks (each header names its shard and page index) and CRC
// records are the whole truth.
//
// The walk is two passes so recovery RAM stays bounded by the resident budget
// rather than the file size, which is what keeps a larger-than-memory store
// recoverable on a machine smaller than the data:
//
//  1. A header-only scan reads the header of every block and groups the valid
//     ones by shard, noting each block's generation, so each shard has its page
//     blocks tagged by generation.
//  2. Per shard, the active generation is selected, its page directory is rebuilt
//     (the newest budget pages resident, the rest evicted refs pointing at their
//     blocks), then records are replayed in page order to install index slots,
//     reading one page at a time.
//
// A torn tail record fails its CRC and ends that shard's replay exactly where the
// crash cut the log. A block that was allocated but never headered (a crash mid
// page-create) reads as zero, fails the header check, and is skipped. A gap in a
// shard's page indices truncates it to the contiguous prefix, since a logical
// address past a missing page is unreachable.
//
// Generations make a crash mid-compaction recover to either the whole old log or
// the whole new one, never a torn mix of the two. A compaction writes a shard's
// live records into a fresh generation numbered from page 0, writing every page
// from 1 upward first and page 0 last, so a durable page 0 at generation G proves
// every page of generation G reached disk before it. Recovery therefore takes, per
// shard, the highest generation that has a page 0, and uses only that generation's
// pages: a half-written newer generation has no page 0 yet, so the complete older
// generation wins. Every block the chosen generation does not claim (a retired old
// generation, a gap-truncated tail, an unheadered block) is returned to the free
// list, so the space a crash mid-compaction stranded is reclaimed on reopen rather
// than leaked.
func (s *Store) recover(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()
	var replayed int64
	defer func() {
		s.recoverRecords.Store(replayed)
		s.recoverNanos.Store(int64(time.Since(start)))
	}()
	df := s.df
	nblocks, err := df.fileBlocks()
	if err != nil {
		return err
	}

	// Read the committed index snapshot, if the superblock points at one. A torn or
	// truncated chain, or one that does not cover this shard count, reads as no snapshot
	// and every shard full-replays, never a wrong index. The chain blocks are remembered
	// so the scan below conserves them (a committed snapshot is neither live data nor
	// free) and a later checkpoint can free them after writing the next chain.
	var snaps []shardSnap
	var snapBlocks []int64
	sb := readSuperblock(df.f)
	if sb.snapValid {
		if stream, chain, rerr := df.readSnapshot(sb.snapRoot, sb.snapSeq, nblocks); rerr == nil {
			if decoded, derr := decodeSnapStream(stream); derr == nil && len(decoded) == len(s.shards) {
				snaps = decoded
				snapBlocks = chain
				df.mu.Lock()
				df.snapRoot, df.snapSeq, df.snapShards, df.snapBlocks = sb.snapRoot, sb.snapSeq, sb.snapShards, chain
				df.mu.Unlock()
			}
		}
	}

	// Pass 1: header-only scan, group blocks by shard tagged with generation, and
	// track the highest generation that has a page 0 (the committed generation).
	type blk struct {
		block     int64
		pageIndex int
		gen       uint32
		hdrLen    int
	}
	perShard := make([][]blk, len(s.shards))
	activeGen := make([]int64, len(s.shards)) // -1 means the shard has no committed page 0
	for i := range activeGen {
		activeGen[i] = -1
	}
	survivor := make([]bool, nblocks) // block id to whether a surviving page or the snapshot claims it
	for _, b := range snapBlocks {
		if b >= 0 && b < nblocks {
			survivor[b] = true // the committed snapshot chain is occupied, not free
		}
	}
	hdr := make([]byte, blockHeaderSize)
	for b := int64(0); b < nblocks; b++ {
		n, _ := df.f.ReadAt(hdr, df.blockOffset(b))
		if n < blockHeaderV1 {
			continue
		}
		sid, pi, gen, hl, ok := parseBlockHeader(hdr[:n])
		if !ok || sid < 0 || sid >= len(s.shards) {
			continue
		}
		perShard[sid] = append(perShard[sid], blk{block: b, pageIndex: pi, gen: gen, hdrLen: hl})
		if pi == 0 && int64(gen) > activeGen[sid] {
			activeGen[sid] = int64(gen)
		}
	}

	// Pass 2: rebuild each shard's log and index from its active generation.
	for sid, all := range perShard {
		// Observe cancellation before each shard's replay: a cancel during recovery stops
		// the shards that have not started, so a deadline bounds the open rather than
		// replaying every shard's log tail regardless.
		if err := ctx.Err(); err != nil {
			return err
		}
		if activeGen[sid] < 0 {
			continue // no committed generation: no page 0 reached disk
		}
		gen := uint32(activeGen[sid])

		// Keep only the active generation's pages, one block per page index. Within a
		// single generation a page index is written once, so a duplicate would only be
		// a stale block a reused id left behind; taking the first is safe either way.
		byPage := make(map[int]blk, len(all))
		for _, bk := range all {
			if bk.gen != gen {
				continue
			}
			if _, dup := byPage[bk.pageIndex]; !dup {
				byPage[bk.pageIndex] = bk
			}
		}
		blocks := make([]blk, 0, len(byPage))
		for _, bk := range byPage {
			blocks = append(blocks, bk)
		}
		sort.Slice(blocks, func(i, j int) bool { return blocks[i].pageIndex < blocks[j].pageIndex })

		// Truncate at the first gap: a missing page makes later logical addresses
		// unreachable, so we keep only the contiguous prefix [0, n).
		n := 0
		for n < len(blocks) && blocks[n].pageIndex == n {
			n++
		}
		if n == 0 {
			continue
		}
		blocks = blocks[:n]

		sh := s.shards[sid]
		l := sh.log
		l.npages = n
		l.pageBlock = make([]int64, n)
		d := l.ensureCap(n)

		firstResident := 0
		if l.budget > 0 && n > l.budget {
			firstResident = n - l.budget
		}
		l.evict = firstResident

		for pi := 0; pi < n; pi++ {
			block := blocks[pi].block
			l.pageBlock[pi] = block
			survivor[block] = true
			if pi < firstResident {
				// Evicted: just a pointer at the block, reread on demand.
				d.refs[pi].Store(&pageRef{fileOff: df.blockOffset(block)})
			} else {
				// Resident: load the page into RAM.
				buf := make([]byte, l.pageSize)
				_, _ = df.f.ReadAt(buf, df.blockOffset(block))
				d.refs[pi].Store(&pageRef{mem: buf})
			}
		}

		// A recovered log keeps stamping new pages in the generation it recovered at,
		// so a later append never reuses a generation a compaction already retired.
		l.gen = gen

		// Choose the replay range. With a committed snapshot for this shard whose
		// generation still matches (a compaction after the checkpoint would have bumped
		// it and stranded the snapshot's addresses), install the index from the snapshot
		// and replay only the records past the frontier, the post-checkpoint delta. The
		// pre-frontier pages are not even decoded: the snapshot already holds their live
		// state, so the replay cost falls from the whole history to the delta. Otherwise
		// replay the whole generation from page 0, the original path, with an empty index.
		replayPage := 0
		replayWithin := int64(blocks[0].hdrLen)
		snapTail := int64(-1) // >= 0 once a snapshot frontier sits at or past the last page
		if snaps != nil && snaps[sid].gen == gen && installSnapshotIndex(sh, snaps[sid], l) {
			sec := snaps[sid]
			sh.logBytes = sec.logBytes
			sh.deadBytes = sec.deadBytes
			fp := int(sec.frontier / l.pageSize)
			if fp < n {
				replayPage = fp
				replayWithin = sec.frontier % l.pageSize
			} else {
				// The frontier names a page boundary no page was created past, so there
				// is no delta and the tail is exactly the frontier the checkpoint cut.
				replayPage = n
				snapTail = sec.frontier
			}
		}

		// Replay records in page order to rebuild (or extend) the index. The tail offset
		// falls out of where the last page's records end. Each page's records start past
		// its own header, whose length depends on the generation that wrote it; the
		// frontier page instead starts at the frontier, a record boundary inside it.
		lastWithin := int64(blocks[n-1].hdrLen)
		for pi := replayPage; pi < n; pi++ {
			// Observe cancellation per page so a long replay over a large delta honours a
			// deadline. One check per page is off the per-record path, so it costs nothing
			// on a normal recovery.
			if err := ctx.Err(); err != nil {
				return err
			}
			ref := d.refs[pi].Load()
			buf := ref.mem
			if buf == nil {
				buf = make([]byte, l.pageSize)
				_, _ = df.f.ReadAt(buf, ref.fileOff)
			}
			within := int64(blocks[pi].hdrLen)
			if pi == replayPage {
				within = replayWithin
			}
			base := int64(pi) * l.pageSize
			for {
				key, _, tomb, rn, ok := decodeDurable(buf[within:])
				if !ok {
					break
				}
				h := hash64(key)
				// recoverApply mirrors the live write path, so stranded-byte
				// accounting (an overwrite's old record, a delete's shadowed record)
				// is rebuilt exactly as it accrued. Every replayed record, value or
				// tombstone, counts toward logBytes just as its original append did.
				sh.recoverApply(h, key, base+within, rn, tomb)
				sh.logBytes += int64(rn)
				within += int64(rn)
				replayed++
			}
			if pi == n-1 {
				lastWithin = within
			}
		}
		if snapTail >= 0 {
			l.tail = snapTail
		} else {
			l.tail = int64(n-1)*l.pageSize + lastWithin
		}
	}

	// Resume allocation past every block the file physically spans, so a new page
	// never collides with a recovered one or a still-present retired generation.
	// Reconcile the free list against the survivors: any block in range that no
	// surviving page claims (a retired old generation, a gap-truncated tail, an
	// unheadered block) is reusable, so recording it now reclaims that space.
	df.mu.Lock()
	if nblocks > df.allocHigh {
		df.allocHigh = nblocks
	}
	df.free = df.free[:0]
	for b := int64(0); b < df.allocHigh; b++ {
		if b >= int64(len(survivor)) || !survivor[b] {
			df.free = append(df.free, b)
		}
	}
	df.mu.Unlock()
	return nil
}
