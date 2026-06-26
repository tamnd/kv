package wal

// This file is M5's leaderless double-buffered WAL framing (spec 05 §4, decision D7),
// built alongside the shipped chained log above and off the live commit path until the
// M8 flip. The shipped WAL (wal.go) is one shared buffer with a group-commit leader: a
// frame chains its checksum onto the previous frame's, so the log is physically ordered
// and recovery stops at the first frame that fails the chain. That chain is exactly what
// a leaderless log cannot keep. D7's whole point is that committers fill disjoint regions
// of a shared buffer concurrently, in any order, each claiming its own byte offset with one
// atomic add and no leader, so when a frame is written its writer does not know the previous
// frame's bytes or checksum and cannot chain onto them.
//
// So the leaderless frame is self-describing instead of chained: its checksum covers only
// its own header and payload, and it carries its own LSN, so a frame is verifiable and
// placeable on its own without the frame before it. A frame is its own commit unit; a valid
// checksum on a kv-batch frame means that batch is durable, with no separate commit frame to
// promote it, because there is no leader to write one and the interleaving would not let a
// commit frame refer to a contiguous run of batch frames anyway (doc 02: every message
// carries its own seq, so the log does not need to be physically ordered to be replayable in
// commit order).
//
// Recovery reconstructs the commit order from the per-frame LSN, not the physical order. The
// durable frontier is the completion watermark from spec 05 §4: the highest LSN M such that
// every LSN from the generation's base through M is present and intact. A leaderless writer
// is acked only when the watermark covers its LSN (M5.2), so the contiguous-LSN prefix the
// recovery computes here is exactly the set of commits that were acked as durable, and any
// frame past the first missing LSN is a commit that was still in flight at the crash and is
// discarded. This is the file's correctness core, and it is built and fuzzed first, before
// the concurrent two-buffer writer (M5.2) that produces these frames, because WAL recovery
// correctness is the milestone's named risk.

import (
	"encoding/binary"
	"io"
	"sort"

	"github.com/tamnd/kv/vfs"
)

// Leaderless header constants. The header is its own 40-byte layout, distinct from the
// chained log's 32-byte header: it carries a base LSN (the first LSN of this generation) that
// the chained log does not need, because the leaderless recovery anchors the contiguous-LSN
// prefix at the base rather than walking a physical chain from the header. A distinct magic
// keeps the two formats from ever being mistaken for one another by either recovery path.
const (
	llHeaderSize = 40
	llMagic      = 0x6b766c6c // "kvll", the leaderless log; the chained log is "kvlg"
	llVersion    = 1
)

// llFrameHeaderSize is the per-frame header: type(1) + length(4) + LSN(8) + version(8) +
// salt(8) + checksum(8). It is byte-for-byte the same shape as the chained log's frame header
// so the archive and offset arithmetic stay familiar; only the checksum's meaning differs.
// In the chained log the checksum covers the previous frame's checksum too; here it covers
// only this frame, which is what lets a frame be written and verified out of order.
const llFrameHeaderSize = 1 + 4 + 8 + 8 + 8 + 8

// llFrameChecksum is the self-contained frame checksum: xxh64 over the frame header sans its
// own checksum slot and the payload, with no previous-frame seed. Two frames with identical
// bytes therefore have identical checksums regardless of where they sit in the log, which is
// the property the out-of-order fill needs and the property the chained checksum deliberately
// lacks.
func llFrameChecksum(headerSansSum, payload []byte) uint64 {
	buf := make([]byte, len(headerSansSum)+len(payload))
	copy(buf, headerSansSum)
	copy(buf[len(headerSansSum):], payload)
	return walChecksum.Sum(buf)
}

// encodeLLFrame writes one self-checksummed frame into dst, which must have room for
// llFrameHeaderSize+len(payload) bytes, and returns the written slice. It is the shared
// encode primitive: the serial reference writer below calls it, and M5.2's concurrent buffer
// will call it to fill its claimed region. The salt binds the frame to its generation so a
// frame left over from a folded generation cannot be mistaken for a current one.
func encodeLLFrame(dst []byte, ft FrameType, lsn, version, salt uint64, payload []byte) []byte {
	frame := dst[:llFrameHeaderSize+len(payload)]
	frame[0] = byte(ft)
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(payload)))
	binary.BigEndian.PutUint64(frame[5:13], lsn)
	binary.BigEndian.PutUint64(frame[13:21], version)
	binary.BigEndian.PutUint64(frame[21:29], salt)
	copy(frame[llFrameHeaderSize:], payload)
	binary.BigEndian.PutUint64(frame[29:37], llFrameChecksum(frame[0:29], payload))
	return frame
}

// LeaderlessResult summarizes a leaderless recovery scan. It mirrors the fields of
// RecoverResult that a recovery driver consumes, so the same replay path can drive either log.
type LeaderlessResult struct {
	// Batches are the committed kv-batches in LSN order, the contiguous-LSN prefix from the
	// generation base through the completion watermark. A frame past the first missing LSN is
	// a commit that was in flight at the crash and is not here.
	Batches []CommittedBatch
	// DurableLSN is the completion watermark: the highest LSN whose whole prefix is durable, or
	// baseLSN-1 (often 0) when nothing contiguous from the base is present.
	DurableLSN uint64
	// DurableEndOff is the file offset just past the last intact frame the scan read, the
	// conservative point a resumed writer appends from without overwriting an intact frame.
	DurableEndOff int64
	// Salt is the generation salt read from the header.
	Salt uint64
	// BaseLSN is the first LSN of this generation, read from the header.
	BaseLSN uint64
	// TornTail is true if the scan stopped at a frame that failed its checksum, was truncated,
	// or carried a stale salt, meaning the file held bytes past the intact region.
	TornTail bool
}

// llFrame is one intact frame collected during the scan, before the contiguous-LSN prefix is
// cut.
type llFrame struct {
	lsn     uint64
	version uint64
	payload []byte
}

// RecoverLeaderless walks a leaderless -wal file from its header, verifies each frame's
// self-contained checksum and generation salt independently, and returns the committed
// batches in the contiguous-LSN durable prefix. Unlike the chained Recover, the first frame
// that fails its checksum ends only the physical scan (a torn or in-flight frame breaks the
// ability to find the next frame boundary); the durable frontier is then computed from the
// LSNs of the intact frames, not their physical order, so a frame that was written out of LSN
// order but is fully intact still counts, and a frame whose commit never completed (its LSN
// is missing from the prefix) is dropped even if a later-LSN frame is intact.
//
// readAt reads exactly len(p) bytes at off, or fewer at EOF, mirroring vfs.File.ReadAt.
func RecoverLeaderless(readAt func(p []byte, off int64) (int, error), size int64) (LeaderlessResult, error) {
	var res LeaderlessResult
	if size < llHeaderSize {
		return res, nil
	}
	hdr := make([]byte, llHeaderSize)
	if _, err := readAt(hdr, 0); err != nil && err != io.EOF {
		return res, err
	}
	if binary.BigEndian.Uint32(hdr[0:4]) != llMagic {
		return res, nil
	}
	if got, want := binary.BigEndian.Uint64(hdr[32:40]), walChecksum.Sum(hdr[:32]); got != want {
		return res, nil
	}
	salt := binary.BigEndian.Uint64(hdr[12:20])
	baseLSN := binary.BigEndian.Uint64(hdr[20:28])
	res.Salt = salt
	res.BaseLSN = baseLSN
	res.DurableEndOff = int64(llHeaderSize)

	off := int64(llHeaderSize)
	hp := make([]byte, llFrameHeaderSize)
	var frames []llFrame
	for off+llFrameHeaderSize <= size {
		if _, err := readAt(hp, off); err != nil && err != io.EOF {
			return res, err
		}
		ft := FrameType(hp[0])
		plen := binary.BigEndian.Uint32(hp[1:5])
		lsn := binary.BigEndian.Uint64(hp[5:13])
		version := binary.BigEndian.Uint64(hp[13:21])
		fsalt := binary.BigEndian.Uint64(hp[21:29])
		sum := binary.BigEndian.Uint64(hp[29:37])

		end := off + int64(llFrameHeaderSize) + int64(plen)
		if end > size {
			res.TornTail = true
			break
		}
		payload := make([]byte, plen)
		if plen > 0 {
			if _, err := readAt(payload, off+int64(llFrameHeaderSize)); err != nil && err != io.EOF {
				return res, err
			}
		}
		if fsalt != salt || llFrameChecksum(hp[0:29], payload) != sum {
			// A torn or in-flight frame. Its length field is no longer trustworthy, so the scan
			// cannot find the next frame boundary; stop here. Everything before this offset is in a
			// fully synced buffer and intact, so stopping at the first failure never drops a frame
			// the durable prefix needs (the prefix lives entirely below this point).
			res.TornTail = true
			break
		}
		if ft == FrameKVBatch {
			frames = append(frames, llFrame{lsn: lsn, version: version, payload: payload})
		}
		off = end
		res.DurableEndOff = off
	}

	res.cutPrefix(frames, baseLSN)
	return res, nil
}

// cutPrefix turns the intact frames into the committed contiguous-LSN prefix. It orders the
// frames by LSN (they may have been written out of order), then walks from the generation base
// taking each frame whose LSN is the next expected one and stopping at the first gap. The
// completion watermark is the last LSN taken; frames past the gap are commits that were in
// flight at the crash and are dropped. A duplicate LSN (which a leaderless writer never emits
// within a generation, but a corrupt or fuzzed file can) is collapsed to its first occurrence
// so the walk stays monotonic.
func (res *LeaderlessResult) cutPrefix(frames []llFrame, baseLSN uint64) {
	if len(frames) == 0 {
		if baseLSN > 0 {
			res.DurableLSN = baseLSN - 1
		}
		return
	}
	sort.Slice(frames, func(i, j int) bool { return frames[i].lsn < frames[j].lsn })
	want := baseLSN
	if baseLSN > 0 {
		res.DurableLSN = baseLSN - 1
	}
	for _, fr := range frames {
		if fr.lsn < want {
			// A duplicate or below-base LSN: already consumed, skip without breaking the run.
			continue
		}
		if fr.lsn != want {
			// A gap: every LSN from here on is past the completion watermark.
			break
		}
		res.Batches = append(res.Batches, CommittedBatch{Version: fr.version, LSN: fr.lsn, Encoded: fr.payload})
		res.DurableLSN = fr.lsn
		want = fr.lsn + 1
	}
}

// CommittedAfter returns the committed batches with an LSN strictly greater than lsn, the
// frames a recovery driver replays past a checkpoint boundary. It mirrors
// RecoverResult.CommittedAfter so either log's result drives the same replay.
func (r LeaderlessResult) CommittedAfter(lsn uint64) []CommittedBatch {
	var out []CommittedBatch
	for _, b := range r.Batches {
		if b.LSN > lsn {
			out = append(out, b)
		}
	}
	return out
}

// llLog is the serial reference writer for the leaderless format: it appends self-committing
// frames one at a time and syncs at the configured level. It is not the leaderless committer
// (that is M5.2's concurrent two-buffer ping-pong); it is the sequential skeleton that owns
// the header, the LSN counter, and the encode-and-append step the concurrent buffer will
// reuse, and it is the fixture the recovery tests produce real logs with. A single writer
// emits a gapless LSN run, so its own recovery returns every frame; the gap and reorder cases
// the recovery must also handle are built by hand and by the fuzzer.
type llLog struct {
	fs       vfs.FS
	file     vfs.File
	path     string
	pageSize int
	salt     uint64
	baseLSN  uint64
	nextLSN  uint64
	tailOff  int64
	syncMode Sync
	scratch  []byte
}

// createLeaderless initializes a fresh leaderless -wal file and returns a writer positioned to
// append after the header. baseLSN is the first LSN this generation will assign.
func createLeaderless(fs vfs.FS, path string, opts Options, baseLSN uint64) (*llLog, error) {
	f, err := fs.Open(path, vfs.OpenReadWrite|vfs.OpenCreate)
	if err != nil {
		return nil, err
	}
	if baseLSN == 0 {
		baseLSN = 1
	}
	sm := opts.Sync
	if sm == syncDefault {
		sm = SyncFull
	}
	l := &llLog{
		fs:       fs,
		file:     f,
		path:     path,
		pageSize: opts.PageSize,
		salt:     opts.Salt,
		baseLSN:  baseLSN,
		nextLSN:  baseLSN,
		tailOff:  llHeaderSize,
		syncMode: sm,
	}
	if err := l.writeHeader(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(vfs.SyncFull); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// writeHeader encodes and writes the 40-byte leaderless header at offset 0.
func (l *llLog) writeHeader() error {
	return writeLeaderlessHeader(l.file, l.pageSize, l.salt, l.baseLSN)
}

// writeLeaderlessHeader encodes and writes the 40-byte leaderless header at offset 0 of file.
// The serial reference writer and the concurrent committer (M5.2) share it so the on-disk
// header is identical whichever writer produced the log, and RecoverLeaderless reads either.
func writeLeaderlessHeader(file vfs.File, pageSize int, salt, baseLSN uint64) error {
	h := make([]byte, llHeaderSize)
	binary.BigEndian.PutUint32(h[0:4], llMagic)
	binary.BigEndian.PutUint32(h[4:8], llVersion)
	binary.BigEndian.PutUint32(h[8:12], uint32(pageSize))
	binary.BigEndian.PutUint64(h[12:20], salt)
	binary.BigEndian.PutUint64(h[20:28], baseLSN)
	// h[28:32] reserved. The checksum covers the first 32 bytes.
	binary.BigEndian.PutUint64(h[32:40], walChecksum.Sum(h[:32]))
	if _, err := file.WriteAt(h, 0); err != nil {
		return err
	}
	return nil
}

// append writes one self-committing kv-batch frame carrying the serialized batch and returns
// the LSN it took. The frame is the commit: once its bytes are synced it is durable, with no
// separate commit frame. It does not sync; the caller batches the sync.
func (l *llLog) append(version uint64, payload []byte) (uint64, error) {
	lsn := l.nextLSN
	need := llFrameHeaderSize + len(payload)
	if cap(l.scratch) < need {
		l.scratch = make([]byte, need)
	}
	frame := encodeLLFrame(l.scratch[:need], FrameKVBatch, lsn, version, l.salt, payload)
	if _, err := l.file.WriteAt(frame, l.tailOff); err != nil {
		return 0, err
	}
	l.tailOff += int64(len(frame))
	l.nextLSN++
	return lsn, nil
}

// sync flushes the log per the configured level. A leaderless frame is its own commit, so a
// sync makes every frame appended since the last sync durable at once, which is the serial
// stand-in for the concurrent buffer's one-fsync-per-buffer amortization (M5.2). The platform
// sync primitive selection (D11) is wired in a later slice; this maps the sync levels onto the
// vfs modes the chained log already uses.
func (l *llLog) sync() error {
	switch l.syncMode {
	case SyncOff, SyncNormal:
		return nil
	case SyncBarrier:
		return l.file.Sync(vfs.SyncBarrier)
	case SyncFull, SyncExtra:
		return l.file.Sync(vfs.SyncData)
	}
	return nil
}

// commit appends a frame and syncs it, the serial single-writer durable commit.
func (l *llLog) commit(version uint64, payload []byte) (uint64, error) {
	lsn, err := l.append(version, payload)
	if err != nil {
		return 0, err
	}
	if err := l.sync(); err != nil {
		return 0, err
	}
	return lsn, nil
}

// Close releases the file. The caller syncs first for a clean shutdown.
func (l *llLog) close() error { return l.file.Close() }
