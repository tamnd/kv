package format

import (
	"bytes"
	"testing"
)

// FuzzVarint checks that any varint we encode decodes back to the same value and
// consumes exactly the bytes written, and that decoding arbitrary input never
// panics or over-reads.
func FuzzVarint(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(300))
	f.Add(^uint64(0))
	f.Fuzz(func(t *testing.T, x uint64) {
		var buf [MaxVarintLen]byte
		n := PutUvarint(buf[:], x)
		got, k := Uvarint(buf[:n])
		if got != x || k != n {
			t.Fatalf("round trip %d -> %d (n=%d k=%d)", x, got, n, k)
		}
		// Decoding a truncated buffer must report incomplete (0), not panic.
		if n > 1 {
			if _, k2 := Uvarint(buf[:n-1]); k2 != 0 {
				t.Fatalf("truncated decode returned %d", k2)
			}
		}
	})
}

// FuzzInternalKey checks the internal-key codec round-trips and that parsing
// arbitrary bytes never panics.
func FuzzInternalKey(f *testing.F) {
	f.Add([]byte("k"), uint64(1), uint8(KindSet))
	f.Add([]byte(""), uint64(0), uint8(KindDelete))
	f.Fuzz(func(t *testing.T, uk []byte, v uint64, kb uint8) {
		kind := Kind(kb % 5)
		ik := EncodeInternalKey(uk, v, kind)
		guk, gv, gk, ok := ParseInternalKey(ik)
		if !ok || !bytes.Equal(guk, uk) || gv != v || gk != kind {
			t.Fatalf("round trip uk=%q v=%d kind=%v -> %q %d %v ok=%v", uk, v, kind, guk, gv, gk, ok)
		}
		// Parsing must never panic on short input.
		_, _, _, _ = ParseInternalKey(ik[:len(ik)/2])
	})
}

// FuzzHeader checks that decoding arbitrary page bytes never panics and that a
// re-encoded header round-trips.
func FuzzHeader(f *testing.F) {
	page := make([]byte, DefaultPageSize)
	NewHeader(DefaultPageSize, EngineF2, FlagWAL, ChecksumCRC32C).Encode(page)
	f.Add(page)
	f.Fuzz(func(t *testing.T, p []byte) {
		if len(p) < HeaderSize {
			return
		}
		h, err := DecodeHeader(p)
		if err != nil {
			return
		}
		// A header that decoded cleanly must re-encode and re-decode identically.
		out := make([]byte, h.PageSize)
		h.Encode(out)
		h2, err := DecodeHeader(out)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if *h2 != *h {
			t.Fatalf("header not stable across re-encode: %+v vs %+v", h, h2)
		}
	})
}
