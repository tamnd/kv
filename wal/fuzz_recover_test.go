package wal

import (
	"testing"

	"github.com/tamnd/kv/vfs"
)

// This is the WAL recovery fuzz harness (spec 23 §5). The durable-tail scan in Recover is the code
// that reads the one file a crash is most likely to have left half-written: the -wal, whose tail is by
// definition a frame that may have been torn mid-append. The scan's whole job is to find the last frame
// that chained correctly and discard everything past it, so it must hold to one contract under any
// bytes at all, that it never panics, hangs, or reads past a frame, and that whatever it reports is
// internally consistent: the durable region never runs past the file, the committed batches are in LSN
// order, and nothing it returns points outside the bytes it was given. A returned RecoverResult is
// always acceptable; a crash is not. Inputs that break the contract are kept under testdata/fuzz as
// regression seeds.

// readerOver turns a byte slice into the readAt callback Recover consumes, mirroring vfs.File.ReadAt:
// it copies as many bytes as are available at the offset and reports a short count at the end, never
// reading past the slice.
func readerOver(data []byte) func(p []byte, off int64) (int, error) {
	return func(p []byte, off int64) (int, error) {
		if off < 0 || off >= int64(len(data)) {
			return 0, nil
		}
		n := copy(p, data[off:])
		return n, nil
	}
}

// validWALBytes builds a real single-generation WAL with several committed batches and one
// logged-but-uncommitted trailing batch, then returns its raw bytes. A checkpoint is deliberately not
// taken: Checkpointed rotates the salt and resets the tail to the header, so later frames overwrite the
// old ones and the file would carry only the post-checkpoint generation. Keeping one generation gives
// the mutator a longer chained, salted, checksummed region to work outward from, so it reaches the
// near-valid tails (a torn last frame, a flipped checksum, a truncated payload) that exercise the scan
// hardest, rather than bouncing off the header magic on every random input.
func validWALBytes(tb testing.TB) []byte {
	tb.Helper()
	fs := vfs.NewMem()
	w, err := Create(fs, "test.kv-wal", Options{Salt: 0x9e3779b97f4a7c15})
	if err != nil {
		tb.Fatalf("create: %v", err)
	}
	commit := func(v uint64, payload string) {
		if err := w.LogBatch(v, []byte(payload)); err != nil {
			tb.Fatalf("log: %v", err)
		}
		if _, err := w.Commit(v); err != nil {
			tb.Fatalf("commit: %v", err)
		}
	}
	commit(1, "batch-one")
	commit(2, "batch-two")
	commit(3, "batch-three")
	// A logged-but-uncommitted trailing batch, the shape recovery must drop as not durable.
	if err := w.LogBatch(4, []byte("uncommitted-tail")); err != nil {
		tb.Fatalf("log tail: %v", err)
	}
	if err := w.Flush(); err != nil {
		tb.Fatalf("flush: %v", err)
	}

	f, err := fs.Open("test.kv-wal", vfs.OpenRead)
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
	if err := w.Close(); err != nil {
		tb.Fatalf("close: %v", err)
	}
	return buf
}

func FuzzRecover(f *testing.F) {
	valid := validWALBytes(f)
	f.Add(valid)
	f.Add([]byte(nil))
	f.Add(make([]byte, headerSize))   // a zeroed header: wrong magic, recover nothing
	f.Add(make([]byte, headerSize-1)) // shorter than a header
	if len(valid) > frameHeaderSize+headerSize {
		f.Add(valid[:headerSize])                   // header only, no frames
		f.Add(valid[:headerSize+frameHeaderSize-1]) // a truncated first frame header
		f.Add(valid[:len(valid)-3])                 // truncated mid-tail
		flipped := append([]byte(nil), valid...)
		flipped[len(flipped)/2] ^= 0xff // a flipped byte deep in a frame
		f.Add(flipped)
		// Header preserved, a frame's length field corrupted to a huge value.
		biglen := append([]byte(nil), valid...)
		for i := headerSize + 1; i < headerSize+5; i++ {
			biglen[i] = 0xff
		}
		f.Add(biglen)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// The contract: scanning arbitrary bytes never panics or over-reads, and the summary it returns is
		// self-consistent.
		res, err := Recover(readerOver(data), int64(len(data)))
		if err != nil {
			// Recover surfaces only a read error from the callback, which the in-memory reader never
			// returns; any error here is acceptable as long as it was returned, not panicked.
			return
		}

		size := int64(len(data))
		// The durable region must lie within the file: a resumed writer appends at DurableEndOff, so a
		// value past the end would corrupt the next write.
		if res.DurableEndOff < 0 || res.DurableEndOff > size {
			t.Fatalf("DurableEndOff %d outside file of %d bytes", res.DurableEndOff, size)
		}
		// Committed batches must be in strictly increasing LSN order (each frame consumes a fresh LSN),
		// must not claim an LSN past the durable tail, and must point at payloads within the file.
		var prev uint64
		for i, b := range res.Batches {
			if b.LSN > res.DurableLSN {
				t.Fatalf("batch %d LSN %d past DurableLSN %d", i, b.LSN, res.DurableLSN)
			}
			if i > 0 && b.LSN <= prev {
				t.Fatalf("batch %d LSN %d not greater than previous %d", i, b.LSN, prev)
			}
			if int64(len(b.Encoded)) > size {
				t.Fatalf("batch %d payload of %d bytes exceeds file of %d", i, len(b.Encoded), size)
			}
			prev = b.LSN
		}
		// A checkpoint LSN, when present, names a frame in the durable region.
		if res.LastCheckpointLSN > res.DurableLSN {
			t.Fatalf("LastCheckpointLSN %d past DurableLSN %d", res.LastCheckpointLSN, res.DurableLSN)
		}

		// CommittedAfter must be a suffix-by-LSN of Batches and never invent a batch.
		after := res.CommittedAfter(res.LastCheckpointLSN)
		if len(after) > len(res.Batches) {
			t.Fatalf("CommittedAfter returned %d batches, more than the %d recovered", len(after), len(res.Batches))
		}
		for _, b := range after {
			if b.LSN <= res.LastCheckpointLSN {
				t.Fatalf("CommittedAfter returned batch at LSN %d, not past checkpoint %d", b.LSN, res.LastCheckpointLSN)
			}
		}
	})
}
