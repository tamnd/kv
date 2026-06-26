package vfs

import (
	"bytes"
	"path/filepath"
	"testing"
)

// This file gates the per-platform durability primitives (D11). The vfs Sync seam exposes three
// levels (SyncData, SyncFull, SyncBarrier) and each maps to the cheapest correct syscall on the
// platform: SyncData is fdatasync on Linux and F_FULLFSYNC on macOS, SyncFull is fsync on Linux and
// F_FULLFSYNC on macOS, SyncBarrier is F_BARRIERFSYNC on macOS and fdatasync on Linux. The win is
// SyncData on Linux, which skips the inode-metadata flush a full fsync forces, the level the WAL's
// hot append path asks for. A unit test cannot observe which syscall fired, so these tests pin the
// property that survives any platform: every level returns without error on the OS backend and the
// bytes it acknowledged are read back, including across the file growth the WAL append pattern drives.

// syncRoundTrip writes want at offset 0, flushes at the given level, and asserts the bytes read back.
// It is the durability contract every Sync level must hold: an acknowledged sync means the data is
// there to read.
func syncRoundTrip(t *testing.T, mode SyncMode, name string) {
	t.Helper()
	dir := t.TempDir()
	f, err := NewOS().Open(filepath.Join(dir, name), OpenReadWrite|OpenCreate)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	want := []byte("durable at " + name)
	if _, err := f.WriteAt(want, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := f.Sync(mode); err != nil {
		t.Fatalf("sync mode %d: %v", mode, err)
	}
	got := make([]byte, len(want))
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("after sync mode %d got %q, want %q", mode, got, want)
	}
}

func TestOSDataSync(t *testing.T) {
	// SyncData is fdatasync on Linux, F_FULLFSYNC on macOS. The bytes it acknowledges are durable.
	syncRoundTrip(t, SyncData, "data.kv")
}

func TestOSFullSync(t *testing.T) {
	// SyncFull is fsync on Linux, F_FULLFSYNC on macOS: data and metadata both durable.
	syncRoundTrip(t, SyncFull, "full.kv")
}

// TestOSDataSyncAcrossGrowth drives the WAL's actual pattern: append, flush at the data level, append
// past the old end, flush again. On Linux fdatasync still flushes the size when the file grows (a
// reader needs it), so each flushed extent must read back whole even though the inode metadata fsync
// would touch is skipped.
func TestOSDataSyncAcrossGrowth(t *testing.T) {
	dir := t.TempDir()
	f, err := NewOS().Open(filepath.Join(dir, "grow.kv"), OpenReadWrite|OpenCreate)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	var off int64
	for i := 0; i < 8; i++ {
		chunk := bytes.Repeat([]byte{byte('a' + i)}, 4096)
		if _, err := f.WriteAt(chunk, off); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		off += int64(len(chunk))
		if err := f.Sync(SyncData); err != nil {
			t.Fatalf("data sync %d: %v", i, err)
		}
		// The whole file, including the freshly grown tail, reads back.
		got := make([]byte, off)
		if _, err := f.ReadAt(got, 0); err != nil {
			t.Fatalf("read after append %d: %v", i, err)
		}
		if !bytes.Equal(got[off-int64(len(chunk)):], chunk) {
			t.Fatalf("grown tail after append %d did not read back", i)
		}
	}
	if sz, err := f.Size(); err != nil || sz != off {
		t.Fatalf("final size %d (err %v), want %d", sz, err, off)
	}
}
