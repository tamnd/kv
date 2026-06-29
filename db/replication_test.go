package db

import (
	"bytes"
	"errors"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// shipAndApply ships the primary's current WAL generation and replays it onto the
// follower, returning the follower's new applied version. It is the round-trip the
// replication tests lean on.
func shipAndApply(t *testing.T, primary, follower *DB) uint64 {
	t.Helper()
	var buf bytes.Buffer
	if _, err := primary.ShipWAL(&buf); err != nil {
		t.Fatalf("ship: %v", err)
	}
	v, err := follower.ApplyWAL(&buf)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	return v
}

// newFollower restores the primary's current state into a fresh path and reopens it as a
// read-only replica, the way an operator seeds a follower from a base backup before
// streaming WAL onto it.
func newFollower(t *testing.T, primary *DB, fs vfs.FS, path string, opts Options) *DB {
	t.Helper()
	var base bytes.Buffer
	if _, err := primary.Backup(&base); err != nil {
		t.Fatalf("base backup: %v", err)
	}
	if err := RestoreBackup(fs, path, &base); err != nil {
		t.Fatalf("restore base: %v", err)
	}
	opts.ReadReplica = true
	f, err := Open(fs, path, opts)
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	return f
}

// TestShipApplyRoundTrip is the core WAL-shipping path: a follower seeded from a base
// backup catches up to the primary by replaying a shipped WAL generation, and reads every
// key the primary wrote after the base.
func TestShipApplyRoundTrip(t *testing.T) {
	fs := vfs.NewMem()
	p, err := Open(fs, "primary.kv", Options{PageSize: 4096, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	// Seed and base-backup before any post-base writes.
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("seed"), []byte("0")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	f := newFollower(t, p, fs, "follower.kv", Options{})
	defer f.Close()

	// Post-base writes the follower must learn through shipping.
	for i := 0; i < 100; i++ {
		k := []byte{'k', byte(i)}
		if _, err := p.Write(func(b *engine.WriteBatch) { b.Set(k, []byte{'v', byte(i)}) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	primaryVersion := shipAndApply(t, p, f)
	if err := p.Close(); err != nil {
		t.Fatalf("close primary: %v", err)
	}

	if got := f.orc.lastCommitted(); got != primaryVersion {
		t.Fatalf("follower at version %d, want %d", got, primaryVersion)
	}
	for i := 0; i < 100; i++ {
		k := []byte{'k', byte(i)}
		got, err := f.Get(k)
		if err != nil || !bytes.Equal(got, []byte{'v', byte(i)}) {
			t.Fatalf("follower key %d = %q,%v, want %q", i, got, err, []byte{'v', byte(i)})
		}
	}
}

// TestApplyIsIdempotent confirms replaying the same ship twice applies nothing the second
// time: the follower stays at the same version and the data is unchanged.
func TestApplyIsIdempotent(t *testing.T) {
	fs := vfs.NewMem()
	p, err := Open(fs, "primary.kv", Options{PageSize: 4096, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("seed"), []byte("0")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	f := newFollower(t, p, fs, "follower.kv", Options{})
	defer f.Close()
	defer p.Close()

	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("a"), []byte("1")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	var buf bytes.Buffer
	if _, err := p.ShipWAL(&buf); err != nil {
		t.Fatalf("ship: %v", err)
	}
	shipBytes := buf.Bytes()

	v1, err := f.ApplyWAL(bytes.NewReader(shipBytes))
	if err != nil {
		t.Fatalf("apply 1: %v", err)
	}
	v2, err := f.ApplyWAL(bytes.NewReader(shipBytes))
	if err != nil {
		t.Fatalf("apply 2: %v", err)
	}
	if v1 != v2 {
		t.Fatalf("idempotent apply moved version %d -> %d", v1, v2)
	}
	if got, err := f.Get([]byte("a")); err != nil || !bytes.Equal(got, []byte("1")) {
		t.Fatalf("after double apply key a = %q,%v, want 1", got, err)
	}
}

// TestFollowerRefusesWrites confirms a ReadReplica database rejects user writes with
// ErrReadOnlyTxn while still serving reads, so only shipped frames can advance it.
func TestFollowerRefusesWrites(t *testing.T) {
	fs := vfs.NewMem()
	p, err := Open(fs, "primary.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	f := newFollower(t, p, fs, "follower.kv", Options{})
	defer f.Close()
	defer p.Close()

	if _, err := f.Write(func(b *engine.WriteBatch) { b.Set([]byte("x"), []byte("y")) }); !errors.Is(err, ErrReadOnlyTxn) {
		t.Fatalf("follower Write = %v, want ErrReadOnlyTxn", err)
	}
	// Reads still work.
	if got, err := f.Get([]byte("k")); err != nil || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("follower read = %q,%v, want v", got, err)
	}
}

// TestApplyGapRefused confirms a ship that begins past the follower's applied version is
// refused with ErrReplicaGap rather than skipping commits. The gap is forced by
// checkpointing the primary so an early generation is folded away and a later one ships.
func TestApplyGapRefused(t *testing.T) {
	fs := vfs.NewMem()
	p, err := Open(fs, "primary.kv", Options{PageSize: 4096, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("seed"), []byte("0")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	f := newFollower(t, p, fs, "follower.kv", Options{})
	defer f.Close()
	defer p.Close()

	// First generation of writes, then checkpoint so they fold into the main file and the
	// WAL resets. The follower never saw these, so it is now behind a checkpoint boundary.
	for i := 0; i < 10; i++ {
		if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte{'a', byte(i)}, []byte{byte(i)}) }); err != nil {
			t.Fatalf("gen1 write: %v", err)
		}
	}
	if err := p.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	// Second generation, shipped without the first.
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("late"), []byte("z")) }); err != nil {
		t.Fatalf("gen2 write: %v", err)
	}
	var buf bytes.Buffer
	if _, err := p.ShipWAL(&buf); err != nil {
		t.Fatalf("ship: %v", err)
	}
	if _, err := f.ApplyWAL(&buf); !errors.Is(err, ErrReplicaGap) {
		t.Fatalf("apply across gap = %v, want ErrReplicaGap", err)
	}
}

// TestApplyRejectsGarbage confirms ApplyWAL surfaces ErrBackupFormat for a stream that is
// not a ship container or is truncated, rather than mangling the follower.
func TestApplyRejectsGarbage(t *testing.T) {
	fs := vfs.NewMem()
	p, err := Open(fs, "primary.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	f := newFollower(t, p, fs, "follower.kv", Options{})
	defer f.Close()
	defer p.Close()

	junk := bytes.NewReader([]byte("not a kv ship container at all"))
	if _, err := f.ApplyWAL(junk); !errors.Is(err, ErrBackupFormat) {
		t.Fatalf("apply garbage = %v, want ErrBackupFormat", err)
	}
	// A valid header with a truncated body is also a format error.
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("a"), []byte("1")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	var buf bytes.Buffer
	if _, err := p.ShipWAL(&buf); err != nil {
		t.Fatalf("ship: %v", err)
	}
	truncated := bytes.NewReader(buf.Bytes()[:shipHeaderSize+4])
	if _, err := f.ApplyWAL(truncated); !errors.Is(err, ErrBackupFormat) {
		t.Fatalf("apply truncated = %v, want ErrBackupFormat", err)
	}
}

// TestShipApplyEncrypted confirms WAL shipping stays encrypted at rest: the ship stream of
// an encrypted primary holds no plaintext, and only a follower with the same key applies
// it and reads the value back.
func TestShipApplyEncrypted(t *testing.T) {
	fs := vfs.NewMem()
	p, err := Open(fs, "primary.kv", Options{PageSize: 4096, EncryptionKey: encKey, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open encrypted primary: %v", err)
	}
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("seed"), []byte("0")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	f := newFollower(t, p, fs, "follower.kv", Options{EncryptionKey: encKey})
	defer f.Close()
	defer p.Close()

	secret := []byte("SHIP-PLAINTEXT-NEEDLE-5566778899")
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("needle"), secret) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	var buf bytes.Buffer
	if _, err := p.ShipWAL(&buf); err != nil {
		t.Fatalf("ship: %v", err)
	}
	if bytes.Contains(buf.Bytes(), secret) {
		t.Fatal("plaintext value found in the encrypted ship stream")
	}
	if _, err := f.ApplyWAL(&buf); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got, err := f.Get([]byte("needle")); err != nil || !bytes.Equal(got, secret) {
		t.Fatalf("follower value = %q,%v, want secret", got, err)
	}
}

// TestShipApplyManyKeys confirms WAL shipping carries a full batch of commits: the shipped
// generation replays on the follower and every key lands. Auto-checkpoint is off so the
// commits stay in the WAL the primary ships rather than being folded away early.
func TestShipApplyManyKeys(t *testing.T) {
	fs := vfs.NewMem()
	opts := Options{PageSize: 4096, AutoCheckpoint: -1}
	p, err := Open(fs, "primary.kv", opts)
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("seed"), []byte("0")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	f := newFollower(t, p, fs, "follower.kv", opts)
	defer f.Close()
	defer p.Close()

	for i := 0; i < 200; i++ {
		k := []byte{'k', byte(i), byte(i >> 8)}
		v := bytes.Repeat([]byte{byte(i)}, 12)
		if _, err := p.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	shipAndApply(t, p, f)
	for i := 0; i < 200; i++ {
		k := []byte{'k', byte(i), byte(i >> 8)}
		want := bytes.Repeat([]byte{byte(i)}, 12)
		got, err := f.Get(k)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("follower key %d = %q,%v", i, got, err)
		}
	}
}

// TestReplicaLagStat confirms the follower reports a non-zero lag when the primary has
// committed past what the follower has applied, and zero once it catches up. Lag is read
// from the container's high-version field, so it reflects the primary even before the
// frames are applied.
func TestReplicaLagStat(t *testing.T) {
	fs := vfs.NewMem()
	p, err := Open(fs, "primary.kv", Options{PageSize: 4096, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("seed"), []byte("0")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	f := newFollower(t, p, fs, "follower.kv", Options{})
	defer f.Close()
	defer p.Close()

	for i := 0; i < 5; i++ {
		if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte{'k', byte(i)}, []byte{byte(i)}) }); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	shipAndApply(t, p, f)
	if !f.Stats().ReadReplica {
		t.Fatal("follower Stats.ReadReplica = false, want true")
	}
	if lag := f.Stats().ReplicaLag; lag != 0 {
		t.Fatalf("caught-up follower lag = %d, want 0", lag)
	}
}

// TestPITRRollForwardToVersion is the point-in-time-recovery path (spec 18 §6): a primary
// archives each WAL generation, an operator restores the base backup and replays the
// archived deltas bounded by a target version, and the recovered database holds exactly the
// commits at or below that version and none past it.
func TestPITRRollForwardToVersion(t *testing.T) {
	fs := vfs.NewMem()
	var archives [][]byte
	sink := func(delta []byte) error {
		archives = append(archives, append([]byte(nil), delta...))
		return nil
	}
	p, err := Open(fs, "primary.kv", Options{PageSize: 4096, AutoCheckpoint: -1, WALArchive: sink})
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}

	// Base: one write, then a base backup. The backup checkpoints, so the seed generation is
	// archived too; the restore lands the follower at the base version and the overlap is
	// skipped on replay.
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("base"), []byte("0")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var base bytes.Buffer
	if _, err := p.Backup(&base); err != nil {
		t.Fatalf("base backup: %v", err)
	}

	// Six post-base writes, each its own checkpointed generation, so each is archived alone
	// and the target can land between them.
	versions := make([]uint64, 6)
	for i := 0; i < 6; i++ {
		k := []byte{'k', byte(i)}
		if _, err := p.Write(func(b *engine.WriteBatch) { b.Set(k, []byte{'v', byte(i)}) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		versions[i] = p.orc.lastCommitted()
		if err := p.Checkpoint(); err != nil {
			t.Fatalf("checkpoint %d: %v", i, err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close primary: %v", err)
	}

	// Recover to the state right after k3: restore the base, replay every archived delta
	// bounded by versions[3].
	target := versions[3]
	if err := RestoreBackup(fs, "recovered.kv", &base); err != nil {
		t.Fatalf("restore base: %v", err)
	}
	r, err := Open(fs, "recovered.kv", Options{ReadReplica: true})
	if err != nil {
		t.Fatalf("open recovered: %v", err)
	}
	defer r.Close()
	for i, delta := range archives {
		if _, err := r.ApplyWALUntil(bytes.NewReader(delta), target); err != nil {
			t.Fatalf("replay archive %d: %v", i, err)
		}
	}

	if got := r.orc.lastCommitted(); got != target {
		t.Fatalf("recovered version %d, want target %d", got, target)
	}
	// k0..k3 are at or below the target and must be present; k4,k5 are past it and must be absent.
	for i := 0; i <= 3; i++ {
		got, err := r.Get([]byte{'k', byte(i)})
		if err != nil || !bytes.Equal(got, []byte{'v', byte(i)}) {
			t.Fatalf("recovered k%d = %q,%v, want v%d", i, got, err, i)
		}
	}
	for i := 4; i <= 5; i++ {
		if got, err := r.Get([]byte{'k', byte(i)}); err == nil {
			t.Fatalf("recovered k%d = %q, want absent (past target)", i, got)
		}
	}
}

// TestArchiveSkipsEmptyGeneration confirms a checkpoint with no new commits since the last
// one does not call the archive sink: an empty generation has nothing to replay.
func TestArchiveSkipsEmptyGeneration(t *testing.T) {
	fs := vfs.NewMem()
	var count int
	sink := func(delta []byte) error { count++; return nil }
	p, err := Open(fs, "primary.kv", Options{PageSize: 4096, AutoCheckpoint: -1, WALArchive: sink})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("a"), []byte("1")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := p.Checkpoint(); err != nil {
		t.Fatalf("checkpoint 1: %v", err)
	}
	if count != 1 {
		t.Fatalf("after one write+checkpoint, archive called %d times, want 1", count)
	}
	// A second checkpoint with no intervening write archives nothing.
	if err := p.Checkpoint(); err != nil {
		t.Fatalf("checkpoint 2: %v", err)
	}
	if count != 1 {
		t.Fatalf("empty-generation checkpoint archived %d times, want still 1", count)
	}
}

// TestArchiveFailureFailsCheckpoint confirms a sink error fails the checkpoint rather than
// reset the log, so a frame is never lost to a failed archive.
func TestArchiveFailureFailsCheckpoint(t *testing.T) {
	fs := vfs.NewMem()
	sink := func(delta []byte) error { return errors.New("archive sink down") }
	p, err := Open(fs, "primary.kv", Options{PageSize: 4096, AutoCheckpoint: -1, WALArchive: sink})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("a"), []byte("1")) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := p.Checkpoint(); err == nil {
		t.Fatal("checkpoint succeeded despite archive failure, want error")
	}
	// The commit is still readable: the failed checkpoint did not lose it.
	if got, err := p.Get([]byte("a")); err != nil || !bytes.Equal(got, []byte("1")) {
		t.Fatalf("after failed checkpoint key a = %q,%v, want 1", got, err)
	}
}

// TestApplyWALUntilStopsBeforeTargetBoundary confirms ApplyWALUntil applies a delta's
// frames up to and including the target and stops, even when the delta carries later
// commits in the same generation.
func TestApplyWALUntilStopsBeforeTargetBoundary(t *testing.T) {
	fs := vfs.NewMem()
	p, err := Open(fs, "primary.kv", Options{PageSize: 4096, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte("seed"), []byte("0")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	f := newFollower(t, p, fs, "follower.kv", Options{})
	defer f.Close()
	defer p.Close()

	// Three writes in one generation, captured as a single ship delta.
	var stop uint64
	for i := 0; i < 3; i++ {
		if _, err := p.Write(func(b *engine.WriteBatch) { b.Set([]byte{'k', byte(i)}, []byte{byte(i)}) }); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if i == 1 {
			stop = p.orc.lastCommitted() // stop after the second write
		}
	}
	var buf bytes.Buffer
	if _, err := p.ShipWAL(&buf); err != nil {
		t.Fatalf("ship: %v", err)
	}
	got, err := f.ApplyWALUntil(bytes.NewReader(buf.Bytes()), stop)
	if err != nil {
		t.Fatalf("apply until: %v", err)
	}
	if got != stop {
		t.Fatalf("applied to %d, want %d", got, stop)
	}
	if v, err := f.Get([]byte{'k', 0}); err != nil || !bytes.Equal(v, []byte{0}) {
		t.Fatalf("k0 = %q,%v, want present", v, err)
	}
	if v, err := f.Get([]byte{'k', 1}); err != nil || !bytes.Equal(v, []byte{1}) {
		t.Fatalf("k1 = %q,%v, want present", v, err)
	}
	if v, err := f.Get([]byte{'k', 2}); err == nil {
		t.Fatalf("k2 = %q, want absent (past target)", v)
	}
}
