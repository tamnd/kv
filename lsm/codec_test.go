package lsm

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// TestCodecRoundTrip checks every codec id reproduces its input exactly, for empty,
// tiny, compressible, and incompressible blocks. The frame is self-describing, so the
// decoder is given only the frame, never the id or length out of band.
func TestCodecRoundTrip(t *testing.T) {
	compressible := bytes.Repeat([]byte("the quick brown fox jumps "), 300)
	incompressible := make([]byte, 4000)
	for i := range incompressible {
		// A simple non-repeating pattern (a counter's low bytes mixed) so DEFLATE finds
		// little to exploit, standing in for already-compressed or random data.
		incompressible[i] = byte(i*2654435761 + i>>3)
	}
	inputs := [][]byte{nil, {}, []byte("x"), []byte("a small key/value cell"), compressible, incompressible}

	for _, id := range []codecID{codecNone, codecFast, codecHigh} {
		for _, raw := range inputs {
			frame := compressBlock(id, raw)
			got, err := decompressBlock(frame)
			if err != nil {
				t.Fatalf("codec %d: decompress: %v", id, err)
			}
			if !bytes.Equal(got, raw) {
				t.Fatalf("codec %d: round trip mismatch on %d bytes", id, len(raw))
			}
		}
	}
}

// TestCodecCompressibleShrinks confirms the two non-identity codecs actually shrink
// compressible data, so the packing they enable is real and not a no-op, and that the
// high codec reaches at least as small as the fast one.
func TestCodecCompressibleShrinks(t *testing.T) {
	raw := bytes.Repeat([]byte("value-payload-prefix-"), 500)
	fast := compressBlock(codecFast, raw)
	high := compressBlock(codecHigh, raw)
	if len(fast) >= len(raw) {
		t.Fatalf("fast codec did not shrink: %d -> %d", len(raw), len(fast))
	}
	if len(high) > len(fast) {
		t.Fatalf("high codec (%d) larger than fast (%d) on compressible data", len(high), len(fast))
	}
}

// TestCodecToleratesTrailingPadding feeds the decoder a frame followed by zero padding,
// exactly what a reader passes when it hands the decoder a page's bytes after the header:
// the unused tail of the page is zero. The DEFLATE stream is self-terminating and the
// identity payload is sliced to its recorded length, so the padding must be ignored.
func TestCodecToleratesTrailingPadding(t *testing.T) {
	raw := bytes.Repeat([]byte("abc123xyz"), 120)
	for _, id := range []codecID{codecNone, codecFast, codecHigh} {
		frame := compressBlock(id, raw)
		padded := append(append([]byte(nil), frame...), make([]byte, 64)...)
		got, err := decompressBlock(padded)
		if err != nil {
			t.Fatalf("codec %d: decompress padded: %v", id, err)
		}
		if !bytes.Equal(got, raw) {
			t.Fatalf("codec %d: padded round trip mismatch", id)
		}
	}
}

// TestCodecForLevel pins the heat-tiering policy: compression off picks the identity codec
// everywhere, and on it puts the hot shallow levels on the fast codec and the cold deep
// levels on the high codec.
func TestCodecForLevel(t *testing.T) {
	off := &LSM{}
	for _, lvl := range []int{0, 1, 2, 9} {
		if off.codecForLevel(lvl) != codecNone {
			t.Fatalf("compression off: level %d should be codecNone", lvl)
		}
	}
	on := &LSM{compress: true}
	for _, lvl := range []int{0, 1} {
		if on.codecForLevel(lvl) != codecFast {
			t.Fatalf("compression on: hot level %d should be codecFast", lvl)
		}
	}
	for _, lvl := range []int{2, 5, 9} {
		if on.codecForLevel(lvl) != codecHigh {
			t.Fatalf("compression on: cold level %d should be codecHigh", lvl)
		}
	}
}

// dataPageStats walks a segment's data-page chain and reports how many data pages it has
// and how many of them are stored compressed, so a test can prove both that compression
// packs denser (fewer pages) and that pages were genuinely compressed (the flag is set).
func dataPageStats(t *testing.T, pgr *pager.Pager, seg *segment) (total, compressed int) {
	t.Helper()
	for pgno := seg.head; pgno != format.NoPage; {
		fr, err := pgr.Get(pgno, pager.Read)
		if err != nil {
			t.Fatalf("get data page %d: %v", pgno, err)
		}
		data := fr.Data()
		h := format.DecodeCommonHeader(data)
		total++
		if h.Flags&compressedFlag != 0 {
			compressed++
		}
		next := h.Overflow
		pgr.Unpin(fr, false)
		pgno = next
	}
	return total, compressed
}

// TestSegmentCompressionTransparentAndDenser writes the same compressible run twice, once
// uncompressed and once with the high codec, and checks two things: the compressed segment
// scans and point-seeks back exactly like the uncompressed one (compression is invisible to
// a reader), and it packs the run into strictly fewer data pages with at least one of them
// actually compressed (the space win is real, not a no-op).
func TestSegmentCompressionTransparentAndDenser(t *testing.T) {
	const n = 3000
	cells := make([]cell, 0, n)
	for i := 0; i < n; i++ {
		cells = append(cells, cell{
			ik(fmt.Sprintf("key%06d", i), 1),
			[]byte(fmt.Sprintf("value-payload-field-%06d", i)),
		})
	}

	plainPgr := newSegPager(t)
	plain, err := writeSegment(plainPgr, bloomBitsPerKey, filterBloom, codecNone, sourceOf(cells))
	if err != nil {
		t.Fatalf("write uncompressed: %v", err)
	}
	compPgr := newSegPager(t)
	comp, err := writeSegment(compPgr, bloomBitsPerKey, filterBloom, codecHigh, sourceOf(cells))
	if err != nil {
		t.Fatalf("write compressed: %v", err)
	}

	// A full scan must yield identical cells in identical order.
	plainScan := drain(t, plainPgr, plain)
	compScan := drain(t, compPgr, comp)
	if len(plainScan) != len(compScan) {
		t.Fatalf("scan length differs: uncompressed %d, compressed %d", len(plainScan), len(compScan))
	}
	for i := range plainScan {
		if plainScan[i] != compScan[i] {
			t.Fatalf("scan cell %d differs: %q vs %q", i, plainScan[i], compScan[i])
		}
	}

	// Point seeks through the block index and a decompressed page must find each key, so the
	// seek path is transparent too, not just the chain walk.
	for _, i := range []int{0, 1, 1500, 2999} {
		key := []byte(fmt.Sprintf("key%06d", i))
		want := fmt.Sprintf("value-payload-field-%06d", i)
		found := false
		if err := comp.getGroup(compPgr, key, func(_, val []byte) bool {
			if string(val) == want {
				found = true
			}
			return true
		}); err != nil {
			t.Fatalf("getGroup %s: %v", key, err)
		}
		if !found {
			t.Fatalf("compressed getGroup missed %s", key)
		}
	}

	// The compressed run must occupy strictly fewer data pages, and at least one must be a
	// genuinely compressed page, or the feature bought nothing.
	plainTotal, _ := dataPageStats(t, plainPgr, plain)
	compTotal, compZipped := dataPageStats(t, compPgr, comp)
	if compTotal >= plainTotal {
		t.Fatalf("compression did not pack denser: uncompressed %d pages, compressed %d", plainTotal, compTotal)
	}
	if compZipped == 0 {
		t.Fatalf("no data page was stored compressed (%d data pages total)", compTotal)
	}
}
