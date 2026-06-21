// Package wal is the write-ahead log: the shared durability spine both engines
// commit through (spec 07). It logs LOGICAL kv-batch frames -- the exact serialized
// WriteBatch the engine later applies -- so "what is durable" and "what is applied"
// are byte-identical, and redo during recovery shares one code path with normal
// operation. A commit frame makes a batch atomic and durable; a chained, salted
// checksum lets recovery find the exact durable tail without trusting any external
// pointer (spec 08 §2).
//
// This milestone implements the log, group commit, the synchronous levels, and the
// durable-tail reader. Physical page-image frames for torn-write protection
// (spec 07 §5) have their frame type reserved here and are wired in when the
// checkpoint folds pages in place; the logical redo path is correct on its own
// because every mutation is keyed by a unique (user-key, version, kind) internal
// key, so re-applying a committed batch is idempotent (spec 08 §3).
package wal

import (
	"encoding/binary"

	"github.com/tamnd/kv/crypto"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
)

// FrameType tags each WAL frame (spec 07 §2.2).
type FrameType byte

const (
	// FrameKVBatch carries a serialized WriteBatch: the logical mutation both
	// engines replay through Apply.
	FrameKVBatch FrameType = 1
	// FrameCommit makes everything since the previous commit durable and atomic.
	// Its payload is the frame count of the batch it closes.
	FrameCommit FrameType = 2
	// FrameCheckpoint records that frames up to an LSN have been folded into the
	// main file; writing it rotates the salt for the next WAL generation.
	FrameCheckpoint FrameType = 3
	// FramePageImage is a full physical page image for torn-write protection. Its
	// type is reserved in this milestone; the checkpoint path wires it in later.
	FramePageImage FrameType = 4
)

// Sync selects how aggressively commits are flushed (spec 07 §6), mirroring
// SQLite's PRAGMA synchronous.
type Sync int

const (
	// SyncOff never fsyncs the WAL; the OS flushes on its own schedule. No
	// corruption (the checksum chain still holds), but recent commits can be lost.
	SyncOff Sync = iota
	// SyncNormal fdatasyncs at checkpoint and periodically, not every commit. The
	// WAL-mode default: crash-consistent, may lose the most recent commits.
	SyncNormal
	// SyncFull fdatasyncs the WAL on every commit (group-batched). Every acked
	// commit survives power loss.
	SyncFull
	// SyncExtra is SyncFull plus a directory/inode sync on file growth.
	SyncExtra
)

// Header constants for the -wal sidecar.
const (
	headerSize = 32
	walMagic   = 0x6b766c67 // "kvlg"
	walVersion = 1
)

// HeaderSize is the fixed -wal header length. A durable image of exactly this many bytes
// is an empty generation with no committed frames, which the archive path uses to skip
// shipping a generation that carried no commits (spec 18 §6).
const HeaderSize = headerSize

// frameHeaderSize is the fixed per-frame header: type(1) + length(4) + LSN(8) +
// version(8) + salt(8) + checksum(8). The checksum is last so it can cover the
// preceding header bytes plus the payload plus the previous frame's checksum.
const frameHeaderSize = 1 + 4 + 8 + 8 + 8 + 8

// checksum algorithm for the chained frame checksum and the header checksum.
var walChecksum = format.ChecksumXXH64

// WAL is an append-only log over one -wal file. It is not safe for concurrent use
// by multiple goroutines without external synchronization; the host serializes
// appends through the commit path (group commit batches concurrent committers
// above this layer in a later slice).
type WAL struct {
	fs   vfs.FS
	file vfs.File
	path string

	pageSize int
	syncMode Sync

	salt    uint64
	lsn     uint64 // next LSN to assign
	lastSum uint64 // running chained checksum
	tailOff int64  // next append offset
	grew    bool   // whether the file has grown since the last sync (for SyncExtra)
	batchN  uint32 // frames appended in the open (uncommitted) batch
	syncs   uint64 // count of fsyncs performed, for observability

	crypto *crypto.Scheme // encrypts kv-batch payloads when set; nil for a cleartext log
}

// Options configure a WAL at create/open.
type Options struct {
	PageSize int
	Sync     Sync
	// Salt seeds the initial WAL generation. Recovery rotates it at each
	// checkpoint; a caller may pass a fixed value for deterministic tests.
	Salt uint64
	// Encryption, when non-nil, encrypts each kv-batch frame's payload before it is
	// written and decrypts it during recovery (spec 14). Frame headers, commit frames,
	// and checkpoint frames stay in the clear so the durable-tail scan can chain and
	// parse the log without the key; only the serialized batch, which carries user keys
	// and values, is sealed. The chained checksum covers the ciphertext, so torn-tail
	// detection is unchanged.
	Encryption *crypto.Scheme
}

// Create initializes a fresh -wal file and returns an open WAL positioned to
// append after the header.
func Create(fs vfs.FS, path string, opts Options) (*WAL, error) {
	f, err := fs.Open(path, vfs.OpenReadWrite|vfs.OpenCreate)
	if err != nil {
		return nil, err
	}
	w := &WAL{
		fs:       fs,
		file:     f,
		path:     path,
		pageSize: opts.PageSize,
		syncMode: opts.Sync,
		salt:     opts.Salt,
		lsn:      1,
		tailOff:  headerSize,
		crypto:   opts.Encryption,
	}
	if err := w.writeHeader(); err != nil {
		f.Close()
		return nil, err
	}
	// The chain seeds from the header checksum so a frame cannot be lifted from a
	// different WAL generation and still chain.
	w.lastSum = w.headerChecksum()
	if err := f.Sync(vfs.SyncFull); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// Open reopens an existing -wal file and positions the writer to append after the
// durable tail. It runs the durable-tail scan (Recover) to recover the generation
// salt, the next LSN, the append offset, and the running checksum, so a frame
// appended next chains correctly onto the last durable frame and any torn or stale
// tail is overwritten. The returned RecoverResult carries the committed batches the
// caller must redo before serving (spec 08 §2-3). If the file does not exist or its
// header is unreadable, Open returns an error and the caller falls back to Create.
func Open(fs vfs.FS, path string, opts Options) (*WAL, RecoverResult, error) {
	f, err := fs.Open(path, vfs.OpenReadWrite)
	if err != nil {
		return nil, RecoverResult{}, err
	}
	size, err := f.Size()
	if err != nil {
		f.Close()
		return nil, RecoverResult{}, err
	}
	res, err := Recover(f.ReadAt, size)
	if err != nil {
		f.Close()
		return nil, RecoverResult{}, err
	}
	// Decrypt the recovered kv-batch payloads now that the durable-tail scan has
	// verified each frame's chained checksum over its ciphertext. Each batch was sealed
	// under its own LSN, so it opens under the same LSN; a wrong key or a tampered frame
	// surfaces here as crypto.ErrWrongKey before any batch reaches redo.
	if opts.Encryption != nil {
		for i := range res.Batches {
			pt, derr := opts.Encryption.OpenWAL(nil, res.Batches[i].Encoded, res.Batches[i].LSN)
			if derr != nil {
				f.Close()
				return nil, RecoverResult{}, derr
			}
			res.Batches[i].Encoded = pt
		}
	}
	w := &WAL{
		fs:       fs,
		file:     f,
		path:     path,
		pageSize: opts.PageSize,
		syncMode: opts.Sync,
		salt:     res.Salt,
		lsn:      res.DurableLSN + 1,
		lastSum:  res.DurableSum,
		tailOff:  res.DurableEndOff,
		crypto:   opts.Encryption,
	}
	return w, res, nil
}

// writeHeader encodes and writes the 32-byte WAL header at offset 0.
func (w *WAL) writeHeader() error {
	h := make([]byte, headerSize)
	binary.BigEndian.PutUint32(h[0:4], walMagic)
	binary.BigEndian.PutUint32(h[4:8], walVersion)
	binary.BigEndian.PutUint32(h[8:12], uint32(w.pageSize))
	binary.BigEndian.PutUint64(h[12:20], w.salt)
	// h[20:24] reserved. The header checksum covers the first 24 bytes.
	binary.BigEndian.PutUint64(h[24:32], walChecksum.Sum(h[:24]))
	if _, err := w.file.WriteAt(h, 0); err != nil {
		return err
	}
	return nil
}

// headerChecksum recomputes the header's own checksum, used to seed the frame
// chain. It mirrors writeHeader's covered range.
func (w *WAL) headerChecksum() uint64 {
	h := make([]byte, 24)
	binary.BigEndian.PutUint32(h[0:4], walMagic)
	binary.BigEndian.PutUint32(h[4:8], walVersion)
	binary.BigEndian.PutUint32(h[8:12], uint32(w.pageSize))
	binary.BigEndian.PutUint64(h[12:20], w.salt)
	return walChecksum.Sum(h[:24])
}

// SetScheme swaps the encryption scheme new frames are sealed under, the WAL half of a key
// rotation (spec 14 §5). Frames already in the log keep the epoch they recorded, so recovery
// still decrypts them; only frames written after the swap use the new epoch. The caller holds
// the database write lock, so no frame is being appended concurrently.
func (w *WAL) SetScheme(s *crypto.Scheme) { w.crypto = s }

// LSN reports the next LSN that will be assigned.
func (w *WAL) LSN() uint64 { return w.lsn }

// ResumeFrom raises the next LSN to at least minNext when reopening a generation the
// last checkpoint left empty. After a checkpoint folds and empties the log, the
// durable-tail scan finds no frames in the new generation and positions the writer at
// LSN 1, while the pager's persisted checkpoint marker still sits at the folded LSN.
// Writing the next frame at LSN 1 would place it at or below that marker, and redo on the
// following open would skip it as already folded, silently dropping a committed batch.
// The host calls this with pager.CheckpointLSN()+1 right after Open so the next frame
// always lands past the marker. It only ever raises the counter, so a generation that
// already carries post-checkpoint frames keeps the position recovery gave it.
func (w *WAL) ResumeFrom(minNext uint64) {
	if minNext > w.lsn {
		w.lsn = minNext
	}
}

// Salt reports the current WAL generation's salt.
func (w *WAL) Salt() uint64 { return w.salt }

// Syncs reports how many fsyncs the WAL has performed (observability).
func (w *WAL) Syncs() uint64 { return w.syncs }

// appendFrame encodes one frame, appends it to the file, and advances the chain.
// It does not sync; callers batch the sync at the commit boundary.
func (w *WAL) appendFrame(ft FrameType, version uint64, payload []byte) error {
	frame := make([]byte, frameHeaderSize+len(payload))
	frame[0] = byte(ft)
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(payload)))
	binary.BigEndian.PutUint64(frame[5:13], w.lsn)
	binary.BigEndian.PutUint64(frame[13:21], version)
	binary.BigEndian.PutUint64(frame[21:29], w.salt)
	copy(frame[frameHeaderSize:], payload)

	// The chained checksum covers the previous frame's checksum, this frame's
	// header (sans its own checksum slot), and the payload. The first frame chains
	// from the header checksum seeded at Create/Open.
	sum := chain(w.lastSum, frame[0:29], payload)
	binary.BigEndian.PutUint64(frame[29:37], sum)

	if _, err := w.file.WriteAt(frame, w.tailOff); err != nil {
		return err
	}
	w.tailOff += int64(len(frame))
	w.lastSum = sum
	w.lsn++
	w.grew = true
	return nil
}

// chain computes the cumulative frame checksum: xxh64 over the previous checksum
// (8 bytes, big-endian), the frame header sans checksum, and the payload.
func chain(prev uint64, headerSansSum, payload []byte) uint64 {
	buf := make([]byte, 8+len(headerSansSum)+len(payload))
	binary.BigEndian.PutUint64(buf[0:8], prev)
	copy(buf[8:], headerSansSum)
	copy(buf[8+len(headerSansSum):], payload)
	return walChecksum.Sum(buf)
}

// LogBatch appends a kv-batch frame carrying the serialized batch. It does not
// commit; call Commit to make the batch durable and atomic.
func (w *WAL) LogBatch(version uint64, encoded []byte) error {
	if w.crypto != nil {
		// Seal under the LSN this frame will take. appendFrame writes w.lsn into the
		// frame header and increments it, so sealing with the current w.lsn binds the
		// ciphertext to the same LSN recovery will open it under.
		sealed, err := w.crypto.SealWAL(nil, encoded, w.lsn)
		if err != nil {
			return err
		}
		encoded = sealed
	}
	if err := w.appendFrame(FrameKVBatch, version, encoded); err != nil {
		return err
	}
	w.batchN++
	return nil
}

// Commit appends a commit frame for version and flushes per the sync level. After
// it returns at SyncFull/SyncExtra the batch is durable: a crash will redo it. The
// returned LSN is the commit frame's LSN, which the caller records as the
// checkpoint boundary once the batch is folded into the main file.
func (w *WAL) Commit(version uint64) (uint64, error) {
	commitLSN := w.lsn
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], w.batchN)
	if err := w.appendFrame(FrameCommit, version, p[:]); err != nil {
		return 0, err
	}
	w.batchN = 0
	if err := w.sync(); err != nil {
		return 0, err
	}
	return commitLSN, nil
}

// sync flushes the WAL according to the configured level. A sync error is fatal
// and non-retryable (fsyncgate, spec 07 §6): the caller must treat it as a failed
// commit and stop writing until the database is reopened and recovered.
func (w *WAL) sync() error {
	switch w.syncMode {
	case SyncOff, SyncNormal:
		// NORMAL defers the per-commit sync; durability is finalized at checkpoint.
		return nil
	case SyncFull:
		w.syncs++
		return w.file.Sync(vfs.SyncData)
	case SyncExtra:
		w.syncs++
		mode := vfs.SyncData
		if w.grew {
			mode = vfs.SyncFull
			w.grew = false
		}
		return w.file.Sync(mode)
	}
	return nil
}

// Flush forces a sync regardless of level, used by NORMAL at checkpoint to finalize
// the deferred durability backlog (spec 07 §8).
func (w *WAL) Flush() error {
	w.syncs++
	w.grew = false
	return w.file.Sync(vfs.SyncFull)
}

// Checkpointed appends a checkpoint frame recording that the main file now contains
// every committed frame through foldedLSN, then rotates the salt so the folded
// frames cannot be mistaken for current ones on a later recovery. The caller must
// have already folded and fsynced the main file (spec 08 §5: fold, fsync main, then
// advance the marker).
func (w *WAL) Checkpointed(foldedLSN uint64) error {
	var p [8]byte
	binary.BigEndian.PutUint64(p[:], foldedLSN)
	if err := w.appendFrame(FrameCheckpoint, 0, p[:]); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	// Rotate the salt for the next generation and rewrite the header. Subsequent
	// frames chain from the new header checksum.
	w.salt = nextSalt(w.salt, foldedLSN)
	w.tailOff = headerSize
	w.lsn = foldedLSN + 1
	if err := w.writeHeader(); err != nil {
		return err
	}
	w.lastSum = w.headerChecksum()
	return w.Flush()
}

// TruncateFile shrinks the on-disk WAL to just its header, returning the frame space to
// the operating system. It is the extra step the TRUNCATE checkpoint mode performs over
// RESTART (spec 09 §1.2): the caller has already folded and reset the log with
// Checkpointed, so the live tail is the header alone, and truncating to it leaves a valid
// empty-generation log the next open still reads cleanly. It is a no-op if the file is
// already at or below the header.
func (w *WAL) TruncateFile() error {
	sz, err := w.file.Size()
	if err != nil {
		return err
	}
	if sz <= int64(w.tailOff) {
		return nil
	}
	if err := w.file.Truncate(int64(w.tailOff)); err != nil {
		return err
	}
	return w.Flush()
}

// nextSalt deterministically derives the next generation's salt. It avoids any
// runtime randomness (the build forbids Math.random-style entropy in some paths);
// mixing the old salt with the folded LSN is enough to make a stale frame's salt
// mismatch the new generation.
func nextSalt(prev, foldedLSN uint64) uint64 {
	x := prev ^ (foldedLSN * 0x9E3779B97F4A7C15)
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	return x | 1
}

// Close releases the file. It does not sync; the caller checkpoints first for a
// clean shutdown.
func (w *WAL) Close() error { return w.file.Close() }

// Path reports the WAL file path.
func (w *WAL) Path() string { return w.path }

// DurableSize reports the byte length of the log's durable prefix: the header plus
// every frame appended and synced so far, which is the next append offset. A physical
// backup copies exactly this many bytes from the front of the -wal file to capture the
// frames a restore must replay, ignoring any stale bytes a previous larger generation
// left past the tail (spec 18 §2).
func (w *WAL) DurableSize() int64 { return w.tailOff }

// DurableImage returns a copy of the log's durable prefix, the bytes from the front of
// the -wal file up to DurableSize. It is the WAL half of a physical backup: combined
// with the checkpointed main file it reconstructs the exact state a reader at the backup
// version would see, since the main file holds everything folded at the last checkpoint
// and these frames hold everything committed after it (spec 18 §2). For the B-tree core a
// checkpoint folds the whole log, so the image is a header-only empty log; for the LSM
// core it carries the frames kept past the engine's durable point.
func (w *WAL) DurableImage() ([]byte, error) {
	buf := make([]byte, w.tailOff)
	if _, err := w.file.ReadAt(buf, 0); err != nil {
		return nil, err
	}
	return buf, nil
}
