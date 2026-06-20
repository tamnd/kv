package format

import (
	"bytes"
	"testing"
)

// TestPageChecksumRoundTrip stamps a page and verifies it for each real algorithm,
// confirming a faithfully written page always passes its own check.
func TestPageChecksumRoundTrip(t *testing.T) {
	for _, algo := range []ChecksumAlgo{ChecksumCRC32C, ChecksumXXH64} {
		page := make([]byte, 512)
		for i := range page {
			page[i] = byte(i * 7)
		}
		StampPageChecksum(page, algo)
		if err := VerifyPageChecksum(page, algo); err != nil {
			t.Fatalf("algo %d: stamped page failed verify: %v", algo, err)
		}
	}
}

// TestPageChecksumDetectsFlip flips one byte in the usable area of a stamped page and
// confirms the checksum no longer matches, for each algorithm. A bit flip anywhere
// before the trailer must be caught (spec 02 §3.2).
func TestPageChecksumDetectsFlip(t *testing.T) {
	for _, algo := range []ChecksumAlgo{ChecksumCRC32C, ChecksumXXH64} {
		page := make([]byte, 512)
		for i := range page {
			page[i] = byte(i)
		}
		StampPageChecksum(page, algo)
		page[100] ^= 0x01 // flip a content bit well before the trailer
		if err := VerifyPageChecksum(page, algo); err == nil {
			t.Fatalf("algo %d: verify passed a bit-flipped page", algo)
		}
	}
}

// TestPageChecksumDetectsTrailerFlip flips a byte of the stored checksum itself and
// confirms the page is rejected, so a corrupted trailer is caught too.
func TestPageChecksumDetectsTrailerFlip(t *testing.T) {
	page := make([]byte, 512)
	for i := range page {
		page[i] = byte(i)
	}
	StampPageChecksum(page, ChecksumCRC32C)
	page[len(page)-1] ^= 0xFF // corrupt the last trailer byte
	if err := VerifyPageChecksum(page, ChecksumCRC32C); err == nil {
		t.Fatal("verify passed a page with a corrupted checksum trailer")
	}
}

// TestPageChecksumNoneIsNoop confirms the none algorithm neither writes nor checks a
// trailer: a page is left byte-for-byte unchanged and always verifies, so a file
// created without checksums behaves exactly as before this feature.
func TestPageChecksumNoneIsNoop(t *testing.T) {
	page := make([]byte, 512)
	for i := range page {
		page[i] = byte(i)
	}
	orig := append([]byte(nil), page...)
	StampPageChecksum(page, ChecksumNone)
	if !bytes.Equal(page, orig) {
		t.Fatal("none algorithm modified the page")
	}
	if err := VerifyPageChecksum(page, ChecksumNone); err != nil {
		t.Fatalf("none algorithm reported corruption: %v", err)
	}
}
