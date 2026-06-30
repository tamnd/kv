package hlog

import (
	"encoding/binary"
	"testing"
)

// This file is a profiler-driven probe, not a design candidate. The Set profile showed
// runtime.memmove dominating the write path at ~59 percent, far more than a 108-byte
// value copy should cost. The hypothesis is that the cost is not the copy itself but
// where it lands: the point benchmarks size the log buffer to the whole run, gigabytes,
// so every write stores into a cold first-touch page that faults in and misses cache.
// A bounded ring that reuses a small resident window would not pay that, and that is the
// case step three builds. This probe confirms the hypothesis with numbers before the
// ring is written, so the ring's win is measured against a known cause rather than
// assumed.
//
// Both benchmarks do the identical per-op work, a length prefix and a 108-byte payload
// copy. They differ only in the buffer: hot reuses a 1 MiB span that stays L2-resident,
// cold streams forward through a buffer sized to the whole run. The gap between them is
// the cold-store tax, the thing the ring removes.

const coldPayloadLen = 108

func writeRecordAt(buf []byte, off int, payload int) {
	binary.LittleEndian.PutUint32(buf[off:off+hdrLen], uint32(payload))
	// Zero-fill the payload span the same way a real copy touches every byte, so the
	// store traffic matches a record write without needing a source slice.
	dst := buf[off+hdrLen : off+hdrLen+payload]
	for i := range dst {
		dst[i] = byte(i)
	}
}

// BenchmarkColdStore streams writes forward through a run-sized buffer, every record on a
// fresh cold page, which is what the point benchmarks do today.
func BenchmarkColdStore(b *testing.B) {
	rec := hdrLen + coldPayloadLen
	buf := make([]byte, b.N*rec+1<<20)
	b.ResetTimer()
	off := 0
	for range b.N {
		writeRecordAt(buf, off, coldPayloadLen)
		off += rec
	}
}

// BenchmarkHotRing writes the same records into a 1 MiB span reused with a mask, so the
// pages stay resident and the stores hit hot cache, which is what the step-three ring
// does.
func BenchmarkHotRing(b *testing.B) {
	rec := hdrLen + coldPayloadLen
	const ringBytes = 1 << 20
	buf := make([]byte, ringBytes)
	mask := ringBytes - 1
	b.ResetTimer()
	off := 0
	for range b.N {
		// Keep each record contiguous: if it would straddle the end, restart at zero,
		// the same no-straddle rule the ring will use.
		if off+rec > ringBytes {
			off = 0
		}
		writeRecordAt(buf, off&mask, coldPayloadLen)
		off += rec
	}
}
