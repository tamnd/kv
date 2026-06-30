package coldstore

import "testing"

// Both benchmarks do identical per-op work, a length prefix and a 108-byte payload write. They
// differ only in the buffer: cold streams forward through a run-sized buffer, every record on a
// fresh page; hot reuses a 1 MiB span that stays resident. The gap is the cold-store tax, the
// thing the ring was assumed to remove.

// BenchmarkColdStore streams writes forward through a run-sized buffer.
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

// BenchmarkHotRing writes the same records into a 1 MiB span reused with a mask.
func BenchmarkHotRing(b *testing.B) {
	rec := hdrLen + coldPayloadLen
	const ringBytes = 1 << 20
	buf := make([]byte, ringBytes)
	mask := ringBytes - 1
	b.ResetTimer()
	off := 0
	for range b.N {
		if off+rec > ringBytes {
			off = 0
		}
		writeRecordAt(buf, off&mask, coldPayloadLen)
		off += rec
	}
}
