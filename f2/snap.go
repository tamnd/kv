package f2

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// The index snapshot is the S6 recovery bound. f2 recovery rebuilds each shard's index
// by replaying every record of the shard's active generation, so a reopen costs the
// operation history between compactions, not the live key count: a million overwrites
// since the last compaction means a million records decoded and reinserted at open. A
// checkpoint already flushes each shard's tail as a durability barrier, so it is the
// natural place to record the index as of that cut. With the cut on disk, recovery loads
// the index directly and replays only the records appended after the frontier, which the
// checkpoint cadence bounds.
//
// The snapshot stores only the live slot words, one 64-bit word per live key, neither the
// empty slots nor the tombstones. A grow already rebuilds the whole table from the slot
// words alone with no log reads, because for any table no wider than 2^slotFPBits the home
// position is fingerprint & mask, taken straight from the word. Recovery reuses exactly
// that: it inserts each stored word at its home with linear probing, pure arithmetic, no
// value ever read. So the snapshot is 8 bytes per live key and restoring it is O(live)
// arithmetic, the larger-than-memory property carried into recovery.
const (
	// snapMagic tags a snapshot block. It is distinct from bhMagic so the recovery page
	// scan, which keys on bhMagic, walks straight past a snapshot block.
	snapMagic   uint32 = 0x32504e53 // "SNP2"
	snapVersion uint32 = 1

	// snapBlockHdr is a snapshot block's header: magic(4) version(4) seq(8) next(8)
	// payloadLen(4) crc(4). The CRC covers the header through payloadLen plus the payload,
	// so a torn snapshot block fails it and recovery rejects the whole chain rather than
	// trust a partial index.
	snapBlockHdr = 32

	// snapStreamHdr is the whole-snapshot header at the front of the first block's
	// payload: magic(4) version(4) shards(4).
	snapStreamHdr = 12

	// snapShardHdr is one shard's fixed section header: gen(4) frontier(8) logBytes(8)
	// deadBytes(8) live(8). The live slot words follow, 8 bytes each.
	snapShardHdr = 36
)

// errSnapTorn rejects a snapshot whose magic, length, sequence, or CRC does not check
// out. Recovery treats it as no snapshot and falls back to a full replay.
var errSnapTorn = errors.New("f2: snapshot chain torn or truncated")

// shardSnap is one shard's captured index cut: the write-side accounting at the cut plus
// the live slot words. The addresses in the words are generation-relative, so gen pins
// which generation they belong to.
type shardSnap struct {
	gen       uint32
	frontier  int64
	logBytes  int64
	deadBytes int64
	slots     []uint64 // live slot words only, neither empty nor tombstone
}

// captureSnap copies the shard's live slots and write-side accounting, the consistent cut
// the snapshot stores. The caller holds the shard lock and has flushed the tail first, so
// the frontier names bytes already on disk and no concurrent writer can move the table out
// from under the copy.
func (sh *shard) captureSnap() shardSnap {
	idx := sh.index.Load()
	ss := shardSnap{
		gen:       sh.log.gen,
		frontier:  sh.log.tail,
		logBytes:  sh.logBytes,
		deadBytes: sh.deadBytes,
		slots:     make([]uint64, 0, idx.live),
	}
	for i := range idx.slots {
		w := idx.slots[i].Load()
		if w == 0 || w&slotTombstone != 0 {
			continue
		}
		ss.slots = append(ss.slots, w)
	}
	return ss
}

// encodeSnapStream serializes the per-shard sections into one contiguous byte stream, the
// payload the block chain then carries.
func encodeSnapStream(snaps []shardSnap) []byte {
	size := snapStreamHdr
	for i := range snaps {
		size += snapShardHdr + len(snaps[i].slots)*8
	}
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[0:], snapMagic)
	binary.LittleEndian.PutUint32(buf[4:], snapVersion)
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(snaps)))
	off := snapStreamHdr
	for i := range snaps {
		ss := &snaps[i]
		binary.LittleEndian.PutUint32(buf[off:], ss.gen)
		binary.LittleEndian.PutUint64(buf[off+4:], uint64(ss.frontier))
		binary.LittleEndian.PutUint64(buf[off+12:], uint64(ss.logBytes))
		binary.LittleEndian.PutUint64(buf[off+20:], uint64(ss.deadBytes))
		binary.LittleEndian.PutUint64(buf[off+28:], uint64(len(ss.slots)))
		off += snapShardHdr
		for _, w := range ss.slots {
			binary.LittleEndian.PutUint64(buf[off:], w)
			off += 8
		}
	}
	return buf
}

// decodeSnapStream parses a snapshot stream back into per-shard sections, returning
// errSnapTorn if the bytes are malformed. It is the inverse of encodeSnapStream.
func decodeSnapStream(buf []byte) ([]shardSnap, error) {
	if len(buf) < snapStreamHdr || binary.LittleEndian.Uint32(buf[0:]) != snapMagic {
		return nil, errSnapTorn
	}
	nshards := int(binary.LittleEndian.Uint32(buf[8:]))
	snaps := make([]shardSnap, nshards)
	off := snapStreamHdr
	for i := 0; i < nshards; i++ {
		if off+snapShardHdr > len(buf) {
			return nil, errSnapTorn
		}
		ss := &snaps[i]
		ss.gen = binary.LittleEndian.Uint32(buf[off:])
		ss.frontier = int64(binary.LittleEndian.Uint64(buf[off+4:]))
		ss.logBytes = int64(binary.LittleEndian.Uint64(buf[off+12:]))
		ss.deadBytes = int64(binary.LittleEndian.Uint64(buf[off+20:]))
		n := int(binary.LittleEndian.Uint64(buf[off+28:]))
		off += snapShardHdr
		if n < 0 || off+n*8 > len(buf) {
			return nil, errSnapTorn
		}
		ss.slots = make([]uint64, n)
		for j := 0; j < n; j++ {
			ss.slots[j] = binary.LittleEndian.Uint64(buf[off:])
			off += 8
		}
	}
	return snaps, nil
}

// installSnapshotIndex rebuilds a shard's index from a snapshot section's live slot
// words and publishes it. It is the grow path run from disk: each word's home is
// fingerprint & mask, so no key or value is read, the slots drop straight into place. It
// sizes the table for the live count at the load factor so the delta replay that follows
// does not immediately grow it. It returns false without publishing when that table would
// be wider than the fingerprint can address, the documented per-shard ceiling, where the
// home arithmetic no longer holds and the caller must full-replay the generation instead.
func installSnapshotIndex(sh *shard, sec shardSnap, l *log) bool {
	ni := newIndex(len(sec.slots)*loadDen/loadNum + 1)
	if ni.mask > slotFPValueMask {
		return false
	}
	ni.log = l
	for _, w := range sec.slots {
		j := slotFP(w) & ni.mask
		for ni.slots[j].Load() != 0 {
			j = (j + 1) & ni.mask
		}
		ni.slots[j].Store(w)
		ni.live++
		ni.used++
	}
	sh.index.Store(ni)
	return true
}

// writeSnapshot writes a byte stream across a freshly allocated chain of blocks and
// returns the chain root and the blocks it used. Each block carries the next block id, so
// the chain is self-describing on disk and recovery follows it without an index. The
// blocks come from the shared allocator, which reuses blocks a compaction freed, so the
// file stays bounded. Under a non-None dial it fsyncs the whole chain before returning, so
// a caller that then commits the superblock has the snapshot fully durable first: the
// superblock flip is the only thing that makes the new chain authoritative.
func (d *durableFile) writeSnapshot(stream []byte, seq uint64) (root int64, blocks []int64, err error) {
	payloadPer := int(d.pageSize) - snapBlockHdr
	if d.enc != nil {
		payloadPer -= cryptoOverhead // the sealed envelope takes the page tail
	}
	if payloadPer <= 0 {
		return -1, nil, errors.New("f2: page too small for a snapshot block")
	}
	nblocks := (len(stream) + payloadPer - 1) / payloadPer
	if nblocks == 0 {
		nblocks = 1 // an empty snapshot (no shard holds a key) still writes one block
	}
	blocks = make([]int64, nblocks)
	for i := range blocks {
		blocks[i] = d.allocBlock()
	}
	buf := make([]byte, d.pageSize) // reused across blocks: header is rewritten and the payload tail re-zeroed each pass
	for i := 0; i < nblocks; i++ {
		lo := i * payloadPer
		hi := lo + payloadPer
		if hi > len(stream) {
			hi = len(stream)
		}
		chunk := stream[lo:hi]
		next := int64(-1)
		if i+1 < nblocks {
			next = blocks[i+1]
		}
		binary.LittleEndian.PutUint32(buf[0:], snapMagic)
		binary.LittleEndian.PutUint32(buf[4:], snapVersion)
		binary.LittleEndian.PutUint64(buf[8:], seq)
		binary.LittleEndian.PutUint64(buf[16:], uint64(next))
		binary.LittleEndian.PutUint32(buf[24:], uint32(len(chunk)))
		copy(buf[snapBlockHdr:], chunk)
		for j := snapBlockHdr + len(chunk); j < len(buf); j++ {
			buf[j] = 0 // clear any tail a longer prior chunk left, so the block is deterministic
		}
		crc := crc32.Update(crc32.Checksum(buf[0:28], crcTable), crcTable, chunk)
		binary.LittleEndian.PutUint32(buf[28:], crc)
		// The header (through the CRC) stays plaintext; writeSealed seals only the
		// payload region when encryption is on, so the CRC over the plaintext chunk is
		// computed before the seal and checked after the open on read.
		if err := d.writeSealed(d.blockOffset(blocks[i]), buf, snapBlockHdr, uint32(blocks[i])); err != nil {
			return -1, blocks, err
		}
	}
	if d.dial != DurabilityNone {
		if err := d.sync(); err != nil {
			return -1, blocks, err
		}
	}
	return blocks[0], blocks, nil
}

// readSnapshot follows the chain from root, validates each block's magic, sequence, and
// CRC, and returns the concatenated payload stream together with the blocks it spanned, so
// recovery can both decode the snapshot and account its blocks as occupied. A wrong
// sequence, a bad CRC, or a chain longer than the file can hold reads as errSnapTorn,
// never a partial index. maxBlocks bounds the walk so a cyclic corrupt chain cannot spin.
func (d *durableFile) readSnapshot(root int64, seq uint64, maxBlocks int64) (stream []byte, blocks []int64, err error) {
	for b := root; b >= 0; {
		if int64(len(blocks)) > maxBlocks {
			return nil, nil, errSnapTorn
		}
		buf := make([]byte, d.pageSize)
		if _, err := d.readSealed(d.blockOffset(b), buf, snapBlockHdr, uint32(b)); err != nil {
			if d.enc != nil {
				return nil, nil, errSnapTorn // a failed open reads as a torn chain, full replay
			}
			return nil, nil, err
		}
		if binary.LittleEndian.Uint32(buf[0:]) != snapMagic {
			return nil, nil, errSnapTorn
		}
		if binary.LittleEndian.Uint64(buf[8:]) != seq {
			return nil, nil, errSnapTorn
		}
		plen := int(binary.LittleEndian.Uint32(buf[24:]))
		if plen > int(d.pageSize)-snapBlockHdr {
			return nil, nil, errSnapTorn
		}
		chunk := buf[snapBlockHdr : snapBlockHdr+plen]
		crc := crc32.Update(crc32.Checksum(buf[0:28], crcTable), crcTable, chunk)
		if crc != binary.LittleEndian.Uint32(buf[28:]) {
			return nil, nil, errSnapTorn
		}
		stream = append(stream, chunk...)
		blocks = append(blocks, b)
		b = int64(binary.LittleEndian.Uint64(buf[16:]))
	}
	return stream, blocks, nil
}

// commitSnapshot captures, writes, and commits a checkpoint's index snapshot. The caller
// has already flushed every shard tail and captured each section under its shard lock, so
// the sections are a consistent cut. The new chain is written and fsynced first; only then
// does the superblock flip to its root, the atomic commit. After the flip the prior chain
// is unreferenced and its blocks return to the free list for reuse.
func (d *durableFile) commitSnapshot(snaps []shardSnap) error {
	stream := encodeSnapStream(snaps)
	d.mu.Lock()
	seq := d.snapSeq + 1
	old := d.snapBlocks
	d.mu.Unlock()

	root, blocks, err := d.writeSnapshot(stream, seq)
	if err != nil {
		return err
	}

	d.mu.Lock()
	d.snapRoot = root
	d.snapSeq = seq
	d.snapShards = len(snaps)
	d.snapBlocks = blocks
	d.mu.Unlock()

	if err := d.writeSuperblock(); err != nil {
		return err
	}
	for _, b := range old {
		d.freeBlock(b)
	}
	return nil
}
