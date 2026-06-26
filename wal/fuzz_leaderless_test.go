package wal

import (
	"testing"
)

// This is the leaderless WAL recovery fuzz harness (spec 05 §4), the counterpart to FuzzRecover
// for the self-checksummed, out-of-order format. RecoverLeaderless reads the file a crash is
// most likely to have left half-written, and its contract under any bytes at all is the same
// shape as the chained scan's: never panic, hang, or read past a frame, and return a summary
// that is internally consistent. On top of that the leaderless scan has its own invariant the
// chained one does not, the one this harness exists to defend: the recovered batches are a
// contiguous-LSN run from the base, so their LSNs are exactly base, base+1, ..., DurableLSN with
// no gap, because a gap is the completion-watermark boundary past which nothing is durable.

func FuzzRecoverLeaderless(f *testing.F) {
	valid, _, _ := llBytes(f, 1, 4)
	f.Add(valid)
	f.Add([]byte(nil))
	f.Add(make([]byte, llHeaderSize))
	f.Add(make([]byte, llHeaderSize-1))
	if len(valid) > llFrameHeaderSize+llHeaderSize {
		f.Add(valid[:llHeaderSize])                     // header only, no frames
		f.Add(valid[:llHeaderSize+llFrameHeaderSize-1]) // a truncated first frame header
		f.Add(valid[:len(valid)-3])                     // truncated mid-tail
		flipped := append([]byte(nil), valid...)
		flipped[len(flipped)/2] ^= 0xff // a flipped byte deep in a frame
		f.Add(flipped)
		biglen := append([]byte(nil), valid...)
		for i := llHeaderSize + 1; i < llHeaderSize+5; i++ {
			biglen[i] = 0xff // a frame length field corrupted to a huge value
		}
		f.Add(biglen)
	}
	// A hand-built gap and an out-of-order log, so the corpus seeds the LSN-prefix paths directly.
	gap := makeLLHeader(1, 0x55aa)
	for _, lsn := range []uint64{1, 2, 4, 5} {
		fr := make([]byte, llFrameHeaderSize+1)
		encodeLLFrame(fr, FrameKVBatch, lsn, lsn, 0x55aa, []byte("g"))
		gap = append(gap, fr...)
	}
	f.Add(gap)

	f.Fuzz(func(t *testing.T, data []byte) {
		res, err := RecoverLeaderless(readerOver(data), int64(len(data)))
		if err != nil {
			// The in-memory reader never returns a read error; any error is acceptable as long as it
			// was returned, not panicked.
			return
		}

		size := int64(len(data))
		if res.DurableEndOff < 0 || res.DurableEndOff > size {
			t.Fatalf("DurableEndOff %d outside file of %d bytes", res.DurableEndOff, size)
		}
		// The recovered batches must be a contiguous-LSN run anchored at the base and ending at the
		// watermark: this is the leaderless invariant that distinguishes a valid recovery from one
		// that leaked a past-the-gap commit.
		for i, b := range res.Batches {
			want := res.BaseLSN + uint64(i)
			if b.LSN != want {
				t.Fatalf("batch %d LSN %d breaks the contiguous run from base %d (want %d)", i, b.LSN, res.BaseLSN, want)
			}
			if b.LSN > res.DurableLSN {
				t.Fatalf("batch %d LSN %d past watermark %d", i, b.LSN, res.DurableLSN)
			}
			if int64(len(b.Encoded)) > size {
				t.Fatalf("batch %d payload of %d bytes exceeds file of %d", i, len(b.Encoded), size)
			}
		}
		// When any batch recovered, the watermark is the last one's LSN; when none did, the watermark
		// never claims a committed frame exists.
		if n := len(res.Batches); n > 0 {
			if res.DurableLSN != res.Batches[n-1].LSN {
				t.Fatalf("watermark %d is not the last recovered LSN %d", res.DurableLSN, res.Batches[n-1].LSN)
			}
		}

		after := res.CommittedAfter(res.DurableLSN)
		if len(after) != 0 {
			t.Fatalf("CommittedAfter(watermark) returned %d batches, want none past the watermark", len(after))
		}
	})
}
