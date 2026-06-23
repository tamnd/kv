package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/wal"
)

// The WAL-shipping container (spec 18 §4). Physical replication and incremental backup
// are the same stream: a primary ships the committed frames of its current WAL
// generation, a follower replays them through the same redo path recovery uses, and the
// result is a consistent point-in-time copy of the primary at the shipped version. The
// container is the live WAL image wrapped in a small self-describing header so a follower
// can validate it and learn how far ahead the primary was before applying a byte.
//
// "Current generation" means the frames since the last checkpoint: a checkpoint folds
// older frames into the main file and resets the log, so once a primary checkpoints past
// a version that version is no longer shippable. A follower that falls behind a primary's
// checkpoint cadence cannot catch up by shipping and must take a fresh full Backup; that
// gap is reported as ErrReplicaGap rather than silently skipping versions.
var shipMagic = [8]byte{'K', 'V', 'W', 'A', 'L', 'S', 'H', 'P'}

// shipFormatVersion is the container layout version, bumped if the header or section
// order ever changes so an old reader rejects a new container instead of misparsing it.
const shipFormatVersion uint32 = 1

// shipHeaderSize is the fixed prefix: magic(8) + formatVersion(4) + pageSize(4) +
// highVersion(8) + walLen(8).
const shipHeaderSize = 8 + 4 + 4 + 8 + 8

// ErrReplicaGap means a shipped WAL generation begins past the version the follower has
// already applied plus one, so applying it would skip commits the follower never saw
// (spec 18 §4). The primary checkpointed away the missing frames; the follower must
// re-seed from a fresh full Backup before WAL shipping can resume.
var ErrReplicaGap = errors.New("kv: replica gap, shipped frames start past the applied version")

// shipHeader is the decoded container prefix.
type shipHeader struct {
	pageSize    uint32
	highVersion uint64
	walLen      uint64
}

func (h shipHeader) encode() []byte {
	b := make([]byte, shipHeaderSize)
	copy(b[0:8], shipMagic[:])
	binary.BigEndian.PutUint32(b[8:12], shipFormatVersion)
	binary.BigEndian.PutUint32(b[12:16], h.pageSize)
	binary.BigEndian.PutUint64(b[16:24], h.highVersion)
	binary.BigEndian.PutUint64(b[24:32], h.walLen)
	return b
}

func decodeShipHeader(b []byte) (shipHeader, error) {
	if len(b) < shipHeaderSize {
		return shipHeader{}, ErrBackupFormat
	}
	if [8]byte(b[0:8]) != shipMagic {
		return shipHeader{}, ErrBackupFormat
	}
	if v := binary.BigEndian.Uint32(b[8:12]); v != shipFormatVersion {
		return shipHeader{}, fmt.Errorf("%w: unsupported ship format version %d", ErrBackupFormat, v)
	}
	return shipHeader{
		pageSize:    binary.BigEndian.Uint32(b[12:16]),
		highVersion: binary.BigEndian.Uint64(b[16:24]),
		walLen:      binary.BigEndian.Uint64(b[24:32]),
	}, nil
}

// ShipWAL streams the current WAL generation to w as a replication delta and returns the
// commit version it captured (spec 18 §4). It is the producer half of WAL shipping: the
// follower's ApplyWAL replays the frames this writes. Unlike Backup it does not
// checkpoint -- a checkpoint would fold the very frames being shipped into the main file
// and reset the log -- so it captures exactly the committed tail the follower still needs.
//
// The captured image ends at a commit boundary: ShipWAL holds the write lock, under which
// no batch is mid-commit, so the durable image never carries a half-written batch. If the
// database was opened with an encryption key the frames are ciphertext, byte for byte, so
// the shipping stream is encrypted at rest exactly like a physical backup (spec 18 §7);
// the follower needs the same key.
func (d *DB) ShipWAL(w io.Writer) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fatal != nil {
		return 0, d.fatal
	}
	// Force any buffered frames to disk so the durable image is the whole committed tail,
	// even at a sync level that defers per-commit fsyncs.
	if err := d.wal.Flush(); err != nil {
		return 0, err
	}
	img, err := d.wal.DurableImage()
	if err != nil {
		return 0, err
	}
	high := d.orc.lastCommitted()

	hdr := shipHeader{pageSize: uint32(d.pgr.PageSize()), highVersion: high, walLen: uint64(len(img))}
	if _, err := w.Write(hdr.encode()); err != nil {
		return 0, err
	}
	if _, err := w.Write(img); err != nil {
		return 0, err
	}
	return high, nil
}

// ApplyWAL replays a shipped WAL generation onto a follower and returns the follower's
// new applied version (spec 18 §4). It is the consumer half of WAL shipping: it recovers
// the committed batches out of the shipped image with the same durable-tail scan recovery
// uses, then applies the ones past the follower's applied version through the same redo
// path -- logging each to the follower's own WAL first, so the follower stays durable and
// crash-recoverable on its own. Reads on the follower advance to the new version once
// ApplyWAL returns.
//
// It is idempotent: a ship that overlaps already-applied versions re-applies nothing,
// because every batch at or below the applied version is skipped. If the shipped
// generation begins past the applied version plus one -- the primary checkpointed away
// the frames in between -- it refuses with ErrReplicaGap rather than skip commits; the
// follower must re-seed from a fresh full Backup.
//
// ApplyWAL is the replication path, not a user write, so it runs even when the database
// was opened ReadReplica (which only fences user writes). On a primary it works too, but
// shipping onto a writable database with its own commit stream is not a supported
// topology; a follower should be opened ReadReplica.
func (d *DB) ApplyWAL(r io.Reader) (uint64, error) {
	return d.applyWAL(r, ^uint64(0))
}

// ApplyWALUntil replays a shipped or archived delta but stops after the target version,
// leaving any later commits in the delta unapplied (spec 18 §6). It is the point-in-time
// recovery primitive: restore a base backup, then feed the archived generations in order
// through ApplyWALUntil with the same target, and the database rolls forward to exactly
// the committed state at that version. Frames at or below the follower's applied version
// are skipped as already present, and frames past target end the replay, so the roll
// forward stops precisely at the target commit. A target at or above the delta's last
// version applies the whole delta, identical to ApplyWAL.
//
// Replaying a chain of archived deltas with the same target is monotone: each earlier
// generation applies fully (its versions are all below the target), and the generation
// that straddles the target applies its frames up to and including it. Versions are
// assigned in commit order and logged in LSN order, so stopping at the first frame past
// the target never skips an earlier commit.
func (d *DB) ApplyWALUntil(r io.Reader, target uint64) (uint64, error) {
	return d.applyWAL(r, target)
}

// applyWAL is the shared body of ApplyWAL and ApplyWALUntil: it recovers the shipped image
// and replays every committed batch whose version is past the follower's applied version
// and at or below target, in order. target is ^uint64(0) for the unbounded ApplyWAL.
func (d *DB) applyWAL(r io.Reader, target uint64) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fatal != nil {
		return 0, d.fatal
	}

	hb := make([]byte, shipHeaderSize)
	if _, err := io.ReadFull(r, hb); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, ErrBackupFormat
		}
		return 0, err
	}
	hdr, err := decodeShipHeader(hb)
	if err != nil {
		return 0, err
	}
	img := make([]byte, hdr.walLen)
	if _, err := io.ReadFull(r, img); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, ErrBackupFormat
		}
		return 0, err
	}

	batches, err := d.recoverShipped(img)
	if err != nil {
		return 0, err
	}

	applied := d.orc.lastCommitted()
	// Gap check: the lowest committed version in the ship must reach the follower's next
	// expected version (applied+1) or overlap below it. If it starts higher, the frames in
	// between were checkpointed away on the primary and are unrecoverable from this stream.
	// A delta entirely past the recovery target contributes nothing and is not a gap, so the
	// check only fires on frames the bounded replay would actually apply (version <= target).
	if low, ok := lowestVersion(batches); ok && low <= target && low > applied+1 {
		return applied, ErrReplicaGap
	}

	for _, cb := range batches {
		if cb.Version <= applied {
			continue // already applied; idempotent skip
		}
		if cb.Version > target {
			break // bounded replay: stop at the target version (batches are in version order)
		}
		if err := d.applyShipped(cb.Version, cb.Encoded); err != nil {
			return d.orc.lastCommitted(), err
		}
	}
	// Record how far the primary had committed so ReplicaLag is observable even when the
	// ship carried frames the follower had already seen.
	if hdr.highVersion > d.replicaHigh {
		d.replicaHigh = hdr.highVersion
	}
	return d.orc.lastCommitted(), nil
}

// recoverShipped runs the WAL durable-tail scan over a shipped image and returns its
// committed batches in version order, decrypting each payload when the follower holds an
// encryption key. It mirrors wal.Open's recover-then-decrypt sequence but over an
// in-memory image rather than the live file, so the follower never has to write the
// shipped bytes to disk to read them.
func (d *DB) recoverShipped(img []byte) ([]wal.CommittedBatch, error) {
	readAt := func(p []byte, off int64) (int, error) {
		if off < 0 || off >= int64(len(img)) {
			return 0, io.EOF
		}
		n := copy(p, img[off:])
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}
	res, err := wal.Recover(readAt, int64(len(img)))
	if err != nil {
		return nil, err
	}
	if d.crypto != nil {
		for i := range res.Batches {
			pt, derr := d.crypto.OpenWAL(nil, res.Batches[i].Encoded, res.Batches[i].LSN)
			if derr != nil {
				return nil, derr
			}
			res.Batches[i].Encoded = pt
		}
	}
	return res.Batches, nil
}

// lowestVersion returns the smallest commit version across the batches, or ok=false when
// there are none.
func lowestVersion(batches []wal.CommittedBatch) (uint64, bool) {
	if len(batches) == 0 {
		return 0, false
	}
	low := batches[0].Version
	for _, b := range batches[1:] {
		if b.Version < low {
			low = b.Version
		}
	}
	return low, true
}

// applyShipped commits one shipped batch at its primary-assigned version: it logs the
// batch to the follower's WAL, applies it to the engine, advances the version counter to
// the shipped version, and publishes it on the change feed. It is applyCommitted for an
// externally versioned batch -- the write-ahead rule still holds (log durably, then
// apply) so a follower that crashes mid-ship redoes the same frames on reopen. The caller
// holds d.mu.
func (d *DB) applyShipped(version uint64, encoded []byte) error {
	// Log durably to the follower's own WAL first. LogBatch re-seals the payload under the
	// follower's WAL scheme and its own LSN, so there is no nonce reuse against the
	// primary's log: the two logs are independent.
	if err := d.wal.LogBatch(version, encoded); err != nil {
		d.fatal = fmt.Errorf("%w: %v", ErrFatalSync, err)
		d.logFatal(d.fatal)
		return d.fatal
	}
	commitLSN, err := d.wal.Commit(version)
	if err != nil {
		d.fatal = fmt.Errorf("%w: %v", ErrFatalSync, err)
		d.logFatal(d.fatal)
		return d.fatal
	}
	d.noteLSN(commitLSN)
	b, err := engine.DecodeBatch(encoded)
	if err != nil {
		return fmt.Errorf("kv: corrupt shipped batch at version %d: %w", version, err)
	}
	if err := d.eng.Apply(b, version); err != nil {
		return fmt.Errorf("kv: apply shipped batch at version %d: %w", version, err)
	}
	// header.LastCommitVersion is stamped at checkpoint time by pgr.Checkpoint; do not
	// update it here to avoid a data race with the lock-free checkpoint I/O path.
	d.orc.advanceTo(version)
	d.counters.commits.Add(1)
	d.maybeCheckpoint()
	d.publish(b, version)
	return nil
}
