package kv

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
	"sync"
)

// CompressedLog is the space-optimized cold backend: an append-only log that batches records
// into blocks, compresses each block with DEFLATE, and writes the compressed blocks to one
// file, with a small in-memory index from each block's logical start to its place in the file.
// It mirrors the HybridLog interface, Append returns a logical address and At reads it back, so
// it is a drop-in cold tier for a deployment that values disk and read bandwidth over raw
// write throughput.
//
// The tradeoff is measured, not assumed (impl note 182). On real key-value data flate at level
// one gets about a 7x ratio at roughly 400 MB/s, against a 70 GB/s memcpy, so compression is a
// space-and-bandwidth lever, not a throughput one. That is why it lives on the cold tier, where
// data is cold and a block is read rarely and through a cache, and why the hot tier and the
// uncompressed HybridLog stay the default for the throughput goal. Level one is chosen over the
// higher levels because the board shows level nine costs five times the CPU for under one
// percent more ratio, and over gzip because gzip adds framing for the same ratio.
//
// A block that does not compress is stored raw rather than inflated, so an incompressible
// workload pays only the failed attempt, never negative savings. The flate writer is reused
// across blocks so steady-state compression does not allocate a fresh compressor per block.
//
// Concurrency is single-writer, many-reader: Append is serialized and runs from the one
// migrator goroutine, while At takes a read lock. Cold reads tolerate the lock because they are
// infrequent by construction, most reads are served by the hot tier and the read cache above
// this one, and a one-block decode cache serves a run of reads from the same block without
// re-inflating it.
type CompressedLog struct {
	f          *os.File
	cf         *os.File // commit side file: durable block count and tails
	blockBytes int

	mu        sync.RWMutex
	pending   []byte // raw framed records not yet sealed into a block
	pendStart int64  // logical offset where the pending block begins
	logTail   int64  // next logical address, pendStart + len(pending)
	fileTail  int64  // next byte offset to write a block at in the file
	index     []blockEntry

	zw    *flate.Writer
	zbuf  bytes.Buffer
	cache decodeCache
}

// blockEntry locates one sealed block. logStart is the logical address of the block's first
// record; a record at address a lives in the block whose [logStart, logStart+rawLen) contains
// a. fileOff points at the block header on disk. raw marks a block stored uncompressed because
// it did not shrink.
type blockEntry struct {
	logStart  int64
	fileOff   int64
	rawLen    int32
	storedLen int32
	raw       bool
}

// decodeCache holds the most recently decoded block so a run of reads into the same block does
// not re-inflate it. It is guarded by the store's lock.
type decodeCache struct {
	id  int // index into index, -1 for empty
	buf []byte
}

// cblockHdr is the on-disk per-block header: raw length, stored length, and a flag byte that is
// 1 for a raw (uncompressed) block.
const cblockHdr = 9

// OpenCompressedLog opens or creates a compressed cold log at path whose blocks target
// blockBytes of raw records each. A larger block compresses better and amortizes the per-block
// header but costs more to decode on a read; 64 KiB is the measured sweet spot for key-value
// data. An existing file is recovered from its commit side file.
func OpenCompressedLog(path string, blockBytes int) (*CompressedLog, error) {
	if blockBytes < maxRecord {
		blockBytes = maxRecord
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	cf, err := os.OpenFile(path+".commit", os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		f.Close()
		return nil, err
	}
	zw, _ := flate.NewWriter(nil, flate.BestSpeed)
	l := &CompressedLog{
		f:          f,
		cf:         cf,
		blockBytes: blockBytes,
		pending:    make([]byte, 0, blockBytes+maxRecord),
		zw:         zw,
		cache:      decodeCache{id: -1},
	}
	if err := l.recover(); err != nil {
		f.Close()
		cf.Close()
		return nil, err
	}
	return l, nil
}

// Append frames rec into the pending block and returns its logical address. When the pending
// block fills it is sealed to disk. The record is readable immediately, from the pending buffer
// before the seal and from its block after, so a reader never sees a gap.
func (l *CompressedLog) Append(rec []byte) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	addr := l.logTail
	var hdr [hdrLen]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(rec)))
	l.pending = append(l.pending, hdr[:]...)
	l.pending = append(l.pending, rec...)
	l.logTail += int64(hdrLen + len(rec))
	if len(l.pending) >= l.blockBytes {
		l.sealLocked()
	}
	return addr
}

// sealLocked compresses the pending block, stores it raw if compression did not shrink it,
// writes it to the file, and records its index entry. The caller holds the write lock.
func (l *CompressedLog) sealLocked() {
	if len(l.pending) == 0 {
		return
	}
	l.zbuf.Reset()
	l.zw.Reset(&l.zbuf)
	l.zw.Write(l.pending)
	l.zw.Close()
	stored := l.zbuf.Bytes()
	raw := false
	if len(stored) >= len(l.pending) {
		stored = l.pending // incompressible: store raw, never inflate
		raw = true
	}
	var hdr [cblockHdr]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(len(l.pending)))
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(stored)))
	if raw {
		hdr[8] = 1
	}
	l.f.WriteAt(hdr[:], l.fileTail)
	l.f.WriteAt(stored, l.fileTail+cblockHdr)
	l.index = append(l.index, blockEntry{
		logStart:  l.pendStart,
		fileOff:   l.fileTail,
		rawLen:    int32(len(l.pending)),
		storedLen: int32(len(stored)),
		raw:       raw,
	})
	l.fileTail += cblockHdr + int64(len(stored))
	l.pendStart = l.logTail
	l.pending = l.pending[:0]
}

var errBadAddr = errors.New("hlog: address not found in compressed log")

// At returns the record at logical address addr, copied into dst. A record still in the pending
// block is sliced from the in-memory buffer; otherwise the containing block is located by the
// index, decoded (served from the one-block cache when it is the same block as the last read),
// and the record sliced out.
func (l *CompressedLog) At(addr int64, dst []byte) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if addr >= l.pendStart {
		return l.readPendingLocked(addr, dst)
	}
	bi := l.findBlock(addr)
	if bi < 0 {
		return nil, errBadAddr
	}
	block, err := l.decodeLocked(bi)
	if err != nil {
		return nil, err
	}
	off := addr - l.index[bi].logStart
	return sliceRecord(block, off, dst)
}

// readPendingLocked slices a record out of the not-yet-sealed pending buffer.
func (l *CompressedLog) readPendingLocked(addr int64, dst []byte) ([]byte, error) {
	return sliceRecord(l.pending, addr-l.pendStart, dst)
}

// sliceRecord reads the [hdrLen len][payload] record at off in block and copies the payload
// into dst.
func sliceRecord(block []byte, off int64, dst []byte) ([]byte, error) {
	if off < 0 || off+hdrLen > int64(len(block)) {
		return nil, errBadAddr
	}
	n := int64(binary.LittleEndian.Uint32(block[off:]))
	if n < 0 || off+hdrLen+n > int64(len(block)) {
		return nil, errBadAddr
	}
	dst = grow(dst, int(n))
	copy(dst, block[off+hdrLen:off+hdrLen+n])
	return dst, nil
}

// findBlock returns the index of the block containing addr, or -1. The index is sorted by
// logStart, so a binary search finds the last block starting at or before addr.
func (l *CompressedLog) findBlock(addr int64) int {
	i := sort.Search(len(l.index), func(i int) bool { return l.index[i].logStart > addr })
	if i == 0 {
		return -1
	}
	e := l.index[i-1]
	if addr >= e.logStart+int64(e.rawLen) {
		return -1
	}
	return i - 1
}

// decodeLocked returns the raw bytes of block bi, from the one-block cache if it is the same
// block as the last read, otherwise by reading and inflating it and caching the result.
func (l *CompressedLog) decodeLocked(bi int) ([]byte, error) {
	if l.cache.id == bi {
		return l.cache.buf, nil
	}
	e := l.index[bi]
	stored := make([]byte, e.storedLen)
	if _, err := l.f.ReadAt(stored, e.fileOff+cblockHdr); err != nil {
		return nil, err
	}
	var raw []byte
	if e.raw {
		raw = stored
	} else {
		r := flate.NewReader(bytes.NewReader(stored))
		out := make([]byte, e.rawLen)
		if _, err := io.ReadFull(r, out); err != nil {
			r.Close()
			return nil, err
		}
		r.Close()
		raw = out
	}
	l.cache.id = bi
	l.cache.buf = raw
	return raw, nil
}

// Tail returns the next logical address, the total logical bytes appended.
func (l *CompressedLog) Tail() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.logTail
}

// Sync seals the pending block and fsyncs the file and the commit side file, so every appended
// record is durable and the block index can be recovered.
func (l *CompressedLog) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sealLocked()
	return l.persistLocked()
}

// persistLocked fsyncs the data, then writes and fsyncs the durable watermark: the number of
// blocks and the logical and file tails, enough to rebuild the index by re-reading the block
// headers on open. The caller holds the write lock.
func (l *CompressedLog) persistLocked() error {
	if err := l.f.Sync(); err != nil {
		return err
	}
	var tb [24]byte
	binary.LittleEndian.PutUint64(tb[0:], uint64(len(l.index)))
	binary.LittleEndian.PutUint64(tb[8:], uint64(l.logTail))
	binary.LittleEndian.PutUint64(tb[16:], uint64(l.fileTail))
	if _, err := l.cf.WriteAt(tb[:], 0); err != nil {
		return err
	}
	return l.cf.Sync()
}

// recover rebuilds the block index by walking the block headers up to the durable file tail the
// side file records. Each header gives the stored and raw lengths, so the walk reconstructs each
// entry's logical start from the running raw-length total. The pending block is empty after
// recovery: a Sync or Close always seals before recording the tail, so no record is left
// unsealed in a durable image.
func (l *CompressedLog) recover() error {
	var tb [24]byte
	if n, _ := l.cf.ReadAt(tb[:], 0); n != 24 {
		return nil
	}
	blocks := int(binary.LittleEndian.Uint64(tb[0:]))
	logTail := int64(binary.LittleEndian.Uint64(tb[8:]))
	fileTail := int64(binary.LittleEndian.Uint64(tb[16:]))
	fi, err := l.f.Stat()
	if err != nil {
		return err
	}
	if fileTail > fi.Size() {
		return nil // short data: trust nothing rather than a torn tail
	}
	var logStart, fileOff int64
	for range blocks {
		var hdr [cblockHdr]byte
		if _, err := l.f.ReadAt(hdr[:], fileOff); err != nil {
			return err
		}
		rawLen := int32(binary.LittleEndian.Uint32(hdr[0:]))
		storedLen := int32(binary.LittleEndian.Uint32(hdr[4:]))
		l.index = append(l.index, blockEntry{
			logStart:  logStart,
			fileOff:   fileOff,
			rawLen:    rawLen,
			storedLen: storedLen,
			raw:       hdr[8] == 1,
		})
		logStart += int64(rawLen)
		fileOff += cblockHdr + int64(storedLen)
	}
	l.logTail = logTail
	l.pendStart = logTail
	l.fileTail = fileTail
	return nil
}

// Close seals the pending block, fsyncs, and closes the files. After Close every record is on
// disk and recoverable.
func (l *CompressedLog) Close() error {
	l.mu.Lock()
	l.sealLocked()
	err := l.persistLocked()
	l.mu.Unlock()
	cfErr := l.cf.Close()
	closeErr := l.f.Close()
	if err != nil {
		return err
	}
	if cfErr != nil {
		return cfErr
	}
	return closeErr
}
