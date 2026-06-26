package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tamnd/kv/vfs"
)

// This file gates M5.1, the leaderless WAL framing and its recovery (leaderless.go). The
// leaderless log trades the chained log's physical ordering for concurrent out-of-order fill,
// so its recovery cannot stop at the first chain break and trust everything before it; it must
// verify each frame on its own and reconstruct the durable frontier from the per-frame LSNs.
// These tests pin that frontier: a clean round trip recovers every committed frame, a torn or
// reordered tail recovers exactly the contiguous-LSN prefix and no more, and a stale-generation
// frame is rejected. WAL recovery correctness is the milestone's named risk, so the format is
// proved here before the concurrent writer that produces it lands.

// llBytes builds a real leaderless log with n committed frames at versions 1..n and returns its
// raw bytes plus the writer's generation salt and base LSN, the fixture the recovery tests scan.
func llBytes(tb testing.TB, baseLSN uint64, n int) (raw []byte, salt, base uint64) {
	tb.Helper()
	fs := vfs.NewMem()
	l, err := createLeaderless(fs, "test.kv-wal", Options{PageSize: 4096, Sync: SyncFull, Salt: 0x9e3779b97f4a7c15}, baseLSN)
	if err != nil {
		tb.Fatalf("create: %v", err)
	}
	for i := 0; i < n; i++ {
		v := uint64(i + 1)
		if _, err := l.commit(v, []byte(fmt.Sprintf("batch-%d", v))); err != nil {
			tb.Fatalf("commit %d: %v", v, err)
		}
	}
	raw = readFile(tb, fs, "test.kv-wal")
	salt, base = l.salt, l.baseLSN
	if err := l.close(); err != nil {
		tb.Fatalf("close: %v", err)
	}
	return raw, salt, base
}

func readFile(tb testing.TB, fs *vfs.Mem, path string) []byte {
	tb.Helper()
	f, err := fs.Open(path, vfs.OpenRead)
	if err != nil {
		tb.Fatalf("reopen: %v", err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		tb.Fatalf("size: %v", err)
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		tb.Fatalf("read: %v", err)
	}
	return buf
}

// TestLeaderlessRoundTrip is the clean case: a serial writer emits a gapless LSN run, so
// recovery returns every committed batch in LSN order with the watermark at the last LSN.
func TestLeaderlessRoundTrip(t *testing.T) {
	raw, salt, base := llBytes(t, 1, 5)
	res, err := RecoverLeaderless(readerOver(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if res.TornTail {
		t.Fatalf("clean log reported a torn tail")
	}
	if res.Salt != salt || res.BaseLSN != base {
		t.Fatalf("header mismatch: salt %x/%x base %d/%d", res.Salt, salt, res.BaseLSN, base)
	}
	if len(res.Batches) != 5 {
		t.Fatalf("recovered %d batches, want 5", len(res.Batches))
	}
	if res.DurableLSN != 5 {
		t.Fatalf("watermark at %d, want 5", res.DurableLSN)
	}
	for i, b := range res.Batches {
		wantV := uint64(i + 1)
		if b.LSN != wantV || b.Version != wantV {
			t.Fatalf("batch %d: lsn %d ver %d, want %d/%d", i, b.LSN, b.Version, wantV, wantV)
		}
		if !bytes.Equal(b.Encoded, []byte(fmt.Sprintf("batch-%d", wantV))) {
			t.Fatalf("batch %d payload %q", i, b.Encoded)
		}
	}
}

// TestLeaderlessBaseLSN checks a generation that does not start at LSN 1 (the post-checkpoint
// case): the contiguous prefix anchors at the header's base LSN, and the watermark below the
// base is base-1.
func TestLeaderlessBaseLSN(t *testing.T) {
	raw, _, base := llBytes(t, 100, 3)
	if base != 100 {
		t.Fatalf("base LSN %d, want 100", base)
	}
	res, _ := RecoverLeaderless(readerOver(raw), int64(len(raw)))
	if res.BaseLSN != 100 {
		t.Fatalf("recovered base %d, want 100", res.BaseLSN)
	}
	if len(res.Batches) != 3 || res.Batches[0].LSN != 100 || res.DurableLSN != 102 {
		t.Fatalf("batches %d first %d watermark %d, want 3/100/102", len(res.Batches), res.Batches[0].LSN, res.DurableLSN)
	}
}

// TestLeaderlessTornTail truncates the log mid-frame, the shape a crash leaves when the last
// buffer's write was interrupted. Recovery recovers the intact prefix and flags the tear.
func TestLeaderlessTornTail(t *testing.T) {
	raw, _, _ := llBytes(t, 1, 5)
	// Drop the last few bytes so the final frame's payload is truncated.
	torn := raw[:len(raw)-3]
	res, _ := RecoverLeaderless(readerOver(torn), int64(len(torn)))
	if !res.TornTail {
		t.Fatalf("truncated log not flagged torn")
	}
	if len(res.Batches) != 4 || res.DurableLSN != 4 {
		t.Fatalf("recovered %d batches watermark %d, want 4/4", len(res.Batches), res.DurableLSN)
	}
}

// TestLeaderlessCorruptMiddleFrame flips a byte inside a non-final frame. Because frames are
// self-checksummed and the scan cannot trust a failed frame's length to find the next, the scan
// stops at the corrupt frame, so the watermark is the contiguous prefix below it. This is the
// torn-buffer case: a corrupt frame is always in the final, un-synced buffer.
func TestLeaderlessCorruptMiddleFrame(t *testing.T) {
	raw, _, _ := llBytes(t, 1, 6)
	// The frames start after the header; corrupt a byte in the third frame's payload region.
	off := llHeaderSize + 2*(llFrameHeaderSize+len("batch-1")) + llFrameHeaderSize + 1
	corrupt := append([]byte(nil), raw...)
	corrupt[off] ^= 0xff
	res, _ := RecoverLeaderless(readerOver(corrupt), int64(len(corrupt)))
	if !res.TornTail {
		t.Fatalf("corrupt frame not flagged torn")
	}
	// Frames 1 and 2 are intact and before the corruption; 3 onward are discarded.
	if len(res.Batches) != 2 || res.DurableLSN != 2 {
		t.Fatalf("recovered %d batches watermark %d, want 2/2", len(res.Batches), res.DurableLSN)
	}
}

// TestLeaderlessStaleSalt plants a frame carrying a different generation's salt past the intact
// region. Recovery must reject it: a frame from a folded generation is not part of this one.
func TestLeaderlessStaleSalt(t *testing.T) {
	raw, salt, _ := llBytes(t, 1, 3)
	// Append one well-formed frame but under the wrong salt, as a leftover generation would.
	stale := make([]byte, llFrameHeaderSize+len("stale"))
	encodeLLFrame(stale, FrameKVBatch, 4, 4, salt^0xdeadbeef, []byte("stale"))
	withStale := append(append([]byte(nil), raw...), stale...)
	res, _ := RecoverLeaderless(readerOver(withStale), int64(len(withStale)))
	if !res.TornTail {
		t.Fatalf("stale-salt frame not flagged torn")
	}
	if len(res.Batches) != 3 || res.DurableLSN != 3 {
		t.Fatalf("recovered %d batches watermark %d, want 3/3", len(res.Batches), res.DurableLSN)
	}
}

// TestLeaderlessOutOfOrderIntact is the property the chained log cannot express: frames written
// in a physical order that does not match LSN order, all intact, still recover in full, because
// recovery reconstructs the order from the LSNs rather than the offsets. It hand-builds a log
// whose frames sit at ascending offsets but carry descending LSNs.
func TestLeaderlessOutOfOrderIntact(t *testing.T) {
	const salt = 0x1234
	buf := makeLLHeader(1, salt)
	// Physical order 3, 1, 2 by offset; all under one salt, all intact.
	for _, lsn := range []uint64{3, 1, 2} {
		fr := make([]byte, llFrameHeaderSize+len("p"))
		encodeLLFrame(fr, FrameKVBatch, lsn, lsn, salt, []byte("p"))
		buf = append(buf, fr...)
	}
	res, _ := RecoverLeaderless(readerOver(buf), int64(len(buf)))
	if res.TornTail {
		t.Fatalf("intact out-of-order log flagged torn")
	}
	if len(res.Batches) != 3 || res.DurableLSN != 3 {
		t.Fatalf("recovered %d batches watermark %d, want 3/3", len(res.Batches), res.DurableLSN)
	}
	for i, b := range res.Batches {
		if b.LSN != uint64(i+1) {
			t.Fatalf("batch %d LSN %d, want %d (recovery did not sort by LSN)", i, b.LSN, i+1)
		}
	}
}

// TestLeaderlessGapDiscardsSuffix is the completion-watermark rule: an intact frame whose LSN
// sits past a missing LSN is a commit that was in flight at the crash and must be dropped, even
// though its bytes are intact. It builds a log with LSNs 1, 2, 4 (3 missing) and asserts the
// watermark stops at 2 and frame 4 is discarded.
func TestLeaderlessGapDiscardsSuffix(t *testing.T) {
	const salt = 0x55aa
	buf := makeLLHeader(1, salt)
	for _, lsn := range []uint64{1, 2, 4} {
		fr := make([]byte, llFrameHeaderSize+len("g"))
		encodeLLFrame(fr, FrameKVBatch, lsn, lsn, salt, []byte("g"))
		buf = append(buf, fr...)
	}
	res, _ := RecoverLeaderless(readerOver(buf), int64(len(buf)))
	if len(res.Batches) != 2 || res.DurableLSN != 2 {
		t.Fatalf("recovered %d batches watermark %d, want 2/2 (frame 4 past the gap must drop)", len(res.Batches), res.DurableLSN)
	}
}

// makeLLHeader builds a valid 40-byte leaderless header for a hand-assembled log.
func makeLLHeader(baseLSN, salt uint64) []byte {
	h := make([]byte, llHeaderSize)
	binary.BigEndian.PutUint32(h[0:4], llMagic)
	binary.BigEndian.PutUint32(h[4:8], llVersion)
	binary.BigEndian.PutUint32(h[8:12], 4096)
	binary.BigEndian.PutUint64(h[12:20], salt)
	binary.BigEndian.PutUint64(h[20:28], baseLSN)
	binary.BigEndian.PutUint64(h[32:40], walChecksum.Sum(h[:32]))
	return h
}

// TestLeaderlessEmptyAndGarbage checks the degenerate inputs: a too-short file, a wrong magic,
// and a corrupt header all recover nothing without error.
func TestLeaderlessEmptyAndGarbage(t *testing.T) {
	cases := [][]byte{
		nil,
		make([]byte, llHeaderSize-1),
		make([]byte, llHeaderSize), // zeroed: wrong magic
	}
	for i, c := range cases {
		res, err := RecoverLeaderless(readerOver(c), int64(len(c)))
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if len(res.Batches) != 0 {
			t.Fatalf("case %d recovered %d batches from garbage", i, len(res.Batches))
		}
	}
	// A valid header whose checksum byte is then flipped recovers nothing.
	h := makeLLHeader(1, 7)
	h[len(h)-1] ^= 0xff
	res, _ := RecoverLeaderless(readerOver(h), int64(len(h)))
	if len(res.Batches) != 0 {
		t.Fatalf("corrupt-header recovered %d batches", len(res.Batches))
	}
}
