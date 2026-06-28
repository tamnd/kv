package f2

import "sort"

// recover rebuilds every shard's log structure and compact index from the file
// alone, after a crash or a clean reopen. It trusts nothing in RAM: the file's
// self-describing blocks (each header names its shard and page index) and CRC
// records are the whole truth.
//
// The walk is two passes so recovery RAM stays bounded by the resident budget
// rather than the file size, which is what keeps a larger-than-memory store
// recoverable on a machine smaller than the data:
//
//  1. A header-only scan reads the 16-byte header of every block and groups the
//     valid ones by shard, giving each shard its ordered list of page blocks.
//  2. Per shard, the page directory is rebuilt (the newest budget pages resident,
//     the rest evicted refs pointing at their blocks), then records are replayed
//     in page order to install index slots, reading one page at a time.
//
// A torn tail record fails its CRC and ends that shard's replay exactly where the
// crash cut the log. A block that was allocated but never headered (a crash mid
// page-create) reads as zero, fails the header check, and is skipped. A gap in a
// shard's page indices truncates it to the contiguous prefix, since a logical
// address past a missing page is unreachable.
func (s *Store) recover() error {
	df := s.df
	nblocks, err := df.fileBlocks()
	if err != nil {
		return err
	}

	// Pass 1: header-only scan, group blocks by shard.
	type blk struct {
		block     int64
		pageIndex int
	}
	perShard := make([][]blk, len(s.shards))
	hdr := make([]byte, blockHeaderSize)
	for b := int64(0); b < nblocks; b++ {
		n, _ := df.f.ReadAt(hdr, df.blockOffset(b))
		if n < blockHeaderSize {
			continue
		}
		sid, pi, ok := parseBlockHeader(hdr)
		if !ok || sid < 0 || sid >= len(s.shards) {
			continue
		}
		perShard[sid] = append(perShard[sid], blk{block: b, pageIndex: pi})
	}

	var maxBlock int64 = -1

	// Pass 2: rebuild each shard's log and index.
	for sid, blocks := range perShard {
		if len(blocks) == 0 {
			continue
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
			if block > maxBlock {
				maxBlock = block
			}
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

		// Replay records in page order to rebuild the index. The tail offset falls
		// out of where the last page's records end.
		lastWithin := int64(blockHeaderSize)
		for pi := 0; pi < n; pi++ {
			ref := d.refs[pi].Load()
			buf := ref.mem
			if buf == nil {
				buf = make([]byte, l.pageSize)
				_, _ = df.f.ReadAt(buf, ref.fileOff)
			}
			within := int64(blockHeaderSize)
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
			}
			if pi == n-1 {
				lastWithin = within
			}
		}
		l.tail = int64(n-1)*l.pageSize + lastWithin
	}

	// Resume allocation past the highest block any surviving page used, so new
	// pages never collide with recovered ones.
	df.mu.Lock()
	if maxBlock+1 > df.allocHigh {
		df.allocHigh = maxBlock + 1
	}
	df.mu.Unlock()
	return nil
}
