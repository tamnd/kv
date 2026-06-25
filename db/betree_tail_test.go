package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// This file is M1.2's durability check at the database boundary: the Bε-tree core's
// mutable hot tail holds committed writes in memory until a rollover pushes them onto
// pages, so a crash with a populated tail must lose nothing. The guarantee is the same
// one the LSM memtable rests on: every logical write is in the WAL before Apply runs,
// the engine reports how far its persisted state reaches through DurableLSN so the
// checkpoint never reclaims WAL behind the tail, and recovery replays the kept WAL
// through Apply, rebuilding the tail. These tests drive a betree-backed database, leave
// writes resting in the tail without a checkpoint, simulate a crash by reopening the
// files, and assert the full key space comes back.

// TestBetreeTailRecoversViaWAL writes a key space small enough to rest entirely in the
// hot tail (no checkpoint, so no rollover is forced), then reopens the database without
// a clean shutdown. Recovery has only the WAL to work from, so a key surviving proves
// the tail's contents were durable in the log and the replay rebuilt them.
func TestBetreeTailRecoversViaWAL(t *testing.T) {
	fs := vfs.NewMem()

	const n = 200
	d, err := Open(fs, "test.kv", Options{
		PageSize:       4096,
		Engine:         format.EngineBeta,
		Sync:           wal.SyncFull,
		AutoCheckpoint: -1, // never checkpoint, so the writes stay in the tail and the WAL
	})
	if err != nil {
		t.Fatalf("open betree: %v", err)
	}
	for i := 0; i < n; i++ {
		k, v := []byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	// Intentionally no Close and no Checkpoint: closing or checkpointing would drain the
	// tail onto pages, which is the opposite of the crash-with-populated-tail this models.

	d2, err := Open(fs, "test.kv", Options{Engine: format.EngineBeta, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer d2.Close()

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%05d", i)
		v, ok := get(t, d2, k)
		if !ok {
			t.Fatalf("key %q lost across crash (tail write not recovered from the WAL)", k)
		}
		if want := fmt.Sprintf("val%05d", i); v != want {
			t.Fatalf("key %q = %q after recovery, want %q", k, v, want)
		}
	}
}

// TestBetreeTailRecoversAcrossCheckpoint splits the key space around a checkpoint: the
// first half is written and checkpointed (so it rolls onto pages and the WAL resets),
// the second half is written and left in the tail, then the database crashes. Both
// halves must come back: the first from the folded main file, the second from the WAL
// replay. It proves the durable mark advanced correctly at the checkpoint so the
// post-checkpoint tail writes were kept in the WAL rather than reclaimed.
func TestBetreeTailRecoversAcrossCheckpoint(t *testing.T) {
	fs := vfs.NewMem()

	const half = 150
	d, err := Open(fs, "test.kv", Options{
		PageSize:       4096,
		Engine:         format.EngineBeta,
		Sync:           wal.SyncFull,
		AutoCheckpoint: -1,
	})
	if err != nil {
		t.Fatalf("open betree: %v", err)
	}
	for i := 0; i < half; i++ {
		k, v := []byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	// Checkpoint drains the tail onto pages and folds the WAL: the first half is now in
	// the main file.
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	for i := half; i < 2*half; i++ {
		k, v := []byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i))
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set(k, v) }); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	// Crash: the second half is in the tail and the WAL, never checkpointed.

	d2, err := Open(fs, "test.kv", Options{Engine: format.EngineBeta, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer d2.Close()

	for i := 0; i < 2*half; i++ {
		k := fmt.Sprintf("key%05d", i)
		v, ok := get(t, d2, k)
		if !ok {
			t.Fatalf("key %q lost across crash (half %d)", k, i/half)
		}
		if want := fmt.Sprintf("val%05d", i); v != want {
			t.Fatalf("key %q = %q after recovery, want %q", k, v, want)
		}
	}
}
