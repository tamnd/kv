package hashlog

import (
	"encoding/binary"
	"errors"
)

// oversize.go is M9: it stores a value too large for one extent as a chain of
// oversize-cont extents addressed through a descriptor in a home log record (spec 2070
// doc 03 section 7, D10). Most values fit one extent and take the inline path unchanged
// (store.go set); only a value that does not fit drops to this path, so the common small
// value pays none of the spanning machinery.
//
// The two size classes (doc 03 section 7):
//
//   - Inline: the record (header, key, value, CRC) is written inline in a log extent and
//     the index points straight at the value. The resident read slices it zero-copy.
//   - Oversize: the value does not fit one extent, so its bytes live in a chain of
//     oversize-cont extents and the home log record carries a 24-byte descriptor
//     {totalLen, headExtent, extentCnt} in place of inline value bytes, with flagOversize
//     set. The index entry marks the value oversize in one bit of its location.
//
// The home record is the atomic commit point (I6, doc 03 section 7): the cont extents are
// written to the file before the home record is appended, and the home record's
// durability is governed by the ordinary frontier (a seal or checkpoint barrier, or the
// Full dial's per-write sync). Because syncData is a whole-file barrier, any sync that
// makes the home record durable has already flushed the earlier cont writes, so an
// acknowledged oversize value is wholly durable; a crash before the home record syncs
// leaves it un-acknowledged and the half-written cont extents orphaned, which recovery
// reconciles back to the free stack. So an oversize value is all-or-nothing across a
// crash, exactly like an inline one.
//
// Oversize is supported only on the durable eviction-possible profile (sh.inPlace), where
// a GET copies the value out (getLocked). The full-resident lock-free profile aliases the
// page on read and cannot return a spanning value as a zero-copy slice, so it rejects an
// oversize value at SET rather than carry an oversize branch on its benchmarked read path;
// the memory-only profile rejects an over-page value as it always has.

const (
	// oversizeDescriptorLen is the fixed home-record descriptor size: three little-endian
	// uint64s (totalLen, headExtent, extentCnt). It is the home record's inline value
	// length, so a resident read of the home record's bytes recovers the descriptor.
	oversizeDescriptorLen = 24

	// valLocOversizeBit is the one-bit oversize marker carried in the index entry's value
	// length (doc 03 section 7). The high bit of vlen is free because an inline value never
	// approaches 2 GiB (it must fit one extent), so the marker never collides with a real
	// length. A non-oversize entry never sets it, so the full-resident read path uses vlen
	// directly with no mask and the evicting read checks one already-loaded bit before
	// taking the descriptor branch.
	valLocOversizeBit = uint32(1) << 31
)

// errBadOversize is the single sentinel for a corrupt or inconsistent oversize value: a
// descriptor that does not match its chain length, a cont extent that cannot be read, or
// a value whose trailing CRC does not verify. The read path is fail-closed: it returns
// this rather than hand back bytes it could not check.
var errBadOversize = errors.New("hashlog: oversize value corrupt")

// isOversize reports whether this location names an oversize value (its bytes span a cont
// chain) rather than an inline value. It is one bit test on an already-loaded word.
func (l valLoc) isOversize() bool { return l.vlen&valLocOversizeBit != 0 }

// length returns the inline length the location describes, masking off the oversize
// marker. For an oversize entry it is the home record's descriptor length (24), which is
// what the dead-byte accounting needs to size the home record; for an inline entry it is
// the value length unchanged.
func (l valLoc) length() uint32 { return l.vlen &^ valLocOversizeBit }

// oversizeDescriptor is the home record's pointer to a spanning value's bytes: the value's
// true length, the first cont extent's id, and how many cont extents the value spans. The
// cont extents are a contiguous run (allocRun), so headExtent..headExtent+extentCnt-1 are
// the whole chain and read or free as one region.
type oversizeDescriptor struct {
	totalLen   uint64
	headExtent int64
	extentCnt  int64
}

// encodeOversizeDescriptor serialises a descriptor into exactly oversizeDescriptorLen
// bytes, the home record's inline value.
func encodeOversizeDescriptor(d oversizeDescriptor) []byte {
	b := make([]byte, oversizeDescriptorLen)
	binary.LittleEndian.PutUint64(b[0:8], d.totalLen)
	binary.LittleEndian.PutUint64(b[8:16], uint64(d.headExtent))
	binary.LittleEndian.PutUint64(b[16:24], uint64(d.extentCnt))
	return b
}

// decodeOversizeDescriptor parses a descriptor from the front of b. It is fail-closed: a
// short buffer returns errBadOversize rather than indexing out of bounds. The descriptor's
// own integrity is already covered by the home record's CRC (the descriptor is the home
// record's value, inside the CRC span), so this never re-CRCs; the read path's separate
// chain-length and trailing-CRC checks catch a descriptor that is internally consistent
// but points at the wrong bytes.
func decodeOversizeDescriptor(b []byte) (oversizeDescriptor, error) {
	if len(b) < oversizeDescriptorLen {
		return oversizeDescriptor{}, errBadOversize
	}
	return oversizeDescriptor{
		totalLen:   binary.LittleEndian.Uint64(b[0:8]),
		headExtent: int64(binary.LittleEndian.Uint64(b[8:16])),
		extentCnt:  int64(binary.LittleEndian.Uint64(b[16:24])),
	}, nil
}

// oversizeExtentCount returns how many cont extents a value of valueLen bytes spans: the
// value bytes plus a trailing CRC32C, laid back to back across cont bodies of extentSize
// bytes each. It is the single place the write and the read agree on the chain length, so
// the read can reject a descriptor whose extentCnt does not match its totalLen.
func oversizeExtentCount(valueLen int64, contBody int64) int64 {
	payloadLen := valueLen + recordCRCSize
	cnt := (payloadLen + contBody - 1) / contBody
	if cnt < 1 {
		cnt = 1
	}
	return cnt
}

// setOversizeLocked stores a value too large for one extent (doc 03 section 7). It runs
// under the shard write lock and only on the durable eviction-possible profile. It writes
// the cont chain first, then appends the home record carrying the descriptor with
// flagOversize, then publishes the index entry with the oversize marker. An overwrite of
// an existing key supersedes its record first (and frees its cont chain if it too was
// oversize), the same supersede the inline path does, so last-writer-wins holds across the
// size classes.
func (sh *shard) setOversizeLocked(key, value []byte) error {
	thash := tableHash(key)
	if old := sh.index.Load().lookupEntry(thash, key); old != nil {
		sh.supersedeOldLocked(old)
	}

	head, cnt, err := sh.writeOversizeChain(value)
	if err != nil {
		return err
	}

	desc := encodeOversizeDescriptor(oversizeDescriptor{
		totalLen:   uint64(len(value)),
		headExtent: head,
		extentCnt:  cnt,
	})
	rl := durableRecordLen(key, desc)
	if rl > sh.pageSize {
		// The home record is key plus a 24-byte descriptor plus the fixed overhead, so it
		// fits any sane page; a key so long the home record overflows a page is rejected
		// rather than silently truncated. The cont chain is already written and will be
		// reconciled free on the next recovery, since no home record will ever reference it.
		return errors.New("hashlog: oversize home record larger than page size")
	}
	sh.rollFor(rl)
	ps := sh.pages.Load()
	page := ps.pages[sh.tailPage]
	recStart := sh.tailPage*int64(sh.pageSize) + int64(sh.tailPos)
	lsn := sh.df.nextLSN()
	n := encodeDurableRecord(page[sh.tailPos:], lsn, key, desc, flagOversize)
	sh.tailPos += n
	sh.pageFill[sh.tailPage] = sh.tailPos
	sh.pageMaxLSN[sh.tailPage] = int64(lsn)
	sh.df.bytesSinceCkpt.Add(int64(n))
	valOff := durableValOff(key, desc)
	sh.indexPut(thash, key, valLoc{addr: recStart + int64(valOff), vlen: valLocOversizeBit | oversizeDescriptorLen})
	sh.liveOversizeExtents += cnt
	sh.store.oversizeValues.Add(1)
	// Under Full the SET does not return until its home record is in a synced extent. The
	// whole-file barrier also flushes the cont writes above, so the value is durable as a
	// unit before the SET acknowledges (I6). Under None and Normal the home record reaches
	// the device at the next seal or checkpoint, which likewise flushes the cont writes.
	if sh.durability == DurabilityFull {
		sh.flushDurable(true)
	}
	return nil
}

// writeOversizeChain writes value into a freshly allocated contiguous run of oversize-cont
// extents and returns the run's first extent id and its length. The value bytes plus a
// trailing CRC32C fill the cont bodies back to back; each cont extent carries a
// self-describing header (its kind, owning shard, and chain links) so the file stays
// walkable, and the run is contiguous so the descriptor's headExtent and extentCnt address
// the whole value with no chain walk on the read. It runs under the shard write lock.
func (sh *shard) writeOversizeChain(value []byte) (head, cnt int64, err error) {
	contBody := sh.df.extentSize
	cnt = oversizeExtentCount(int64(len(value)), contBody)
	head, _ = sh.df.alloc.allocRun(cnt)
	if err := sh.df.growExtent(head + cnt - 1); err != nil {
		sh.df.alloc.freeRun(head, cnt)
		return 0, 0, err
	}
	// The payload is the value bytes followed by their CRC32C, laid across the cont bodies.
	// Building it once keeps the chunk-to-extent split trivial even when the CRC straddles
	// the last extent boundary; an oversize value is large and rare, so the one copy is off
	// every hot path.
	payloadLen := int64(len(value)) + recordCRCSize
	payload := make([]byte, payloadLen)
	copy(payload, value)
	binary.LittleEndian.PutUint32(payload[len(value):], crc32c(value))
	for i := int64(0); i < cnt; i++ {
		id := head + i
		prev := int64(-1)
		next := int64(-1)
		if i > 0 {
			prev = id - 1
		}
		if i < cnt-1 {
			next = id + 1
		}
		h := extentHeader{
			kind:       extentKindOversizeCont,
			shardID:    int32(sh.shardID),
			prevExtent: prev,
			nextExtent: next,
			baseAddr:   -1, // cont extents hold raw value bytes, not log records at a logical base
			genStamp:   sh.df.gen.Load(),
		}
		if _, err := sh.df.f.WriteAt(encodeExtentHeader(h), sh.df.extentOffset(id)); err != nil {
			sh.df.alloc.freeRun(head, cnt)
			return 0, 0, err
		}
		lo := i * contBody
		hi := lo + contBody
		if hi > payloadLen {
			hi = payloadLen
		}
		if _, err := sh.df.f.WriteAt(payload[lo:hi], sh.df.extentBodyOffset(id)); err != nil {
			sh.df.alloc.freeRun(head, cnt)
			return 0, 0, err
		}
	}
	return head, cnt, nil
}

// supersedeOldLocked retires the record an overwrite or delete is about to replace: it
// credits the killed record's bytes to its page's dead tally (so compaction reclaims the
// space) and, if the record was an oversize value, queues its cont chain for the next
// checkpoint to free. The cont chain is read off the old home record's descriptor, which
// is still in the log here (the index has not yet repointed). The cont extents follow the
// same checkpoint-gated free as compaction (doc 06 section 7.3): they become holes now and
// are returned to the allocator only after the checkpoint that records the index moving off
// them, so a crash before that checkpoint recovers the prior value with its chain intact.
// It runs under the shard write lock.
func (sh *shard) supersedeOldLocked(old *entry) {
	sh.creditDeadLocked(old)
	if old.loc.isOversize() {
		sh.freeOversizeContLocked(old.loc)
	}
}

// freeOversizeContLocked queues the cont chain of a superseded oversize value for the next
// checkpoint to free. It reads the descriptor from the home record at loc.addr (resident or
// on disk), then appends headExtent..headExtent+extentCnt-1 to the shard's pending-free
// list and drops the live-cont accounting. A descriptor it cannot read or decode leaves the
// chain unfreed rather than guessing, which leaks the extents but never frees a live one;
// the read is from a CRC-validated home record, so a failure here means real corruption.
// It runs under the shard write lock.
func (sh *shard) freeOversizeContLocked(loc valLoc) {
	descBytes := make([]byte, oversizeDescriptorLen)
	if err := sh.readAtLogicalLocked(sh.pages.Load(), loc.addr, descBytes); err != nil {
		return
	}
	desc, err := decodeOversizeDescriptor(descBytes)
	if err != nil || desc.extentCnt < 1 || desc.headExtent < 0 {
		return
	}
	for i := int64(0); i < desc.extentCnt; i++ {
		sh.pendingFree = append(sh.pendingFree, desc.headExtent+i)
	}
	sh.liveOversizeExtents -= desc.extentCnt
}

// readOversizeLocked assembles a spanning value from its cont chain. It reads the
// descriptor from the home record at loc.addr (resident or on disk), walks the cont extents
// the descriptor names, and verifies the trailing CRC32C over the assembled value before
// returning it. The returned slice is a fresh copy (a spanning value is not contiguous in
// any one page), so the caller owns it. It runs under the shard read lock, the same lock
// the inline getLocked read holds, so the home page and the file are stable for the read.
func (sh *shard) readOversizeLocked(ps *pageSet, loc valLoc) ([]byte, bool, error) {
	descBytes := make([]byte, oversizeDescriptorLen)
	if err := sh.readAtLogicalLocked(ps, loc.addr, descBytes); err != nil {
		return nil, false, err
	}
	desc, err := decodeOversizeDescriptor(descBytes)
	if err != nil {
		return nil, false, err
	}
	value, err := sh.readOversizeChain(desc)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

// readOversizeChain reads the value bytes from the cont chain a descriptor names and checks
// their trailing CRC32C. It is fail-closed: it rejects a descriptor whose extentCnt does
// not match the chain length its totalLen implies, bounds the chain against the file before
// allocating, and returns errBadOversize on a CRC mismatch, so a corrupt chain never yields
// bad bytes.
func (sh *shard) readOversizeChain(desc oversizeDescriptor) ([]byte, error) {
	contBody := sh.df.extentSize
	if desc.extentCnt < 1 || desc.headExtent < 0 {
		return nil, errBadOversize
	}
	if oversizeExtentCount(int64(desc.totalLen), contBody) != desc.extentCnt {
		return nil, errBadOversize
	}
	// Bound the chain against the file before trusting extentCnt to size an allocation, so a
	// descriptor read off a damaged record cannot drive a huge make. The atomic file end is read
	// without the grow lock that a concurrent shard may hold to extend the file; it is monotonic,
	// so a lock-free read only ever under-counts a concurrent growth, never the reverse, which
	// keeps this a safe upper bound.
	maxExtents := sh.df.fileEndAtomic.Load()/sh.df.stride + 1
	if desc.headExtent+desc.extentCnt > maxExtents {
		return nil, errBadOversize
	}
	payloadLen := int64(desc.totalLen) + recordCRCSize
	payload := make([]byte, payloadLen)
	for i := int64(0); i < desc.extentCnt; i++ {
		lo := i * contBody
		hi := lo + contBody
		if hi > payloadLen {
			hi = payloadLen
		}
		if _, err := sh.df.f.ReadAt(payload[lo:hi], sh.df.extentBodyOffset(desc.headExtent+i)); err != nil {
			return nil, err
		}
	}
	value := payload[:desc.totalLen]
	if crc32c(value) != binary.LittleEndian.Uint32(payload[desc.totalLen:]) {
		return nil, errBadOversize
	}
	return value, nil
}

// readAtLogicalLocked reads len(dst) bytes at a logical address, from the resident page
// buffer if the page is in memory or from the page's extent on disk otherwise. It is the
// small read the oversize path uses to fetch a home record's descriptor; it bounds the
// address against the directory and the page before indexing, so a stale or corrupt address
// returns errBadOversize rather than panicking. It runs under the shard read or write lock,
// so the page directory it loads is stable.
func (sh *shard) readAtLogicalLocked(ps *pageSet, addr int64, dst []byte) error {
	pageBytes := int64(sh.pageSize)
	pid := addr / pageBytes
	off := int(addr % pageBytes)
	if pid < 0 || pid >= int64(len(ps.pages)) {
		return errBadOversize
	}
	if page := ps.pages[pid]; page != nil {
		if off < 0 || off+len(dst) > len(page) {
			return errBadOversize
		}
		copy(dst, page[off:off+len(dst)])
		return nil
	}
	dOff := ps.diskOff[pid]
	if dOff < 0 {
		return errBadOversize
	}
	_, err := sh.df.f.ReadAt(dst, dOff+int64(off))
	return err
}
