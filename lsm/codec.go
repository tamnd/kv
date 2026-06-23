package lsm

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"sync"

	"github.com/tamnd/kv/format"
)

// Block compression for the LSM data pages (spec 13 §3). After the record encoding the
// segment writer already does (length-prefixed cells, version-group packing), a data
// page's cell bytes are an opaque blob a general byte codec can shrink. Compression is
// heat-tiered: a page written to a hot, often-rewritten level takes the cheap fast codec
// whose decompress cost is negligible against the I/O it saves, while a page written to a
// cold, write-once-read-seldom level takes the higher-ratio codec whose extra CPU is paid
// rarely (spec 13 §3.1).
//
// Pure Go, zero dependencies (ADR-8): the two non-identity codecs are Go's standard
// library DEFLATE at its fastest and best-compression settings. Spec 13 names LZ4 for the
// hot tier and Zstd for the cold tier; those would drop in behind this same per-page codec
// id without touching the format, but they live in a third-party package, so under the
// no-dependency rule the standard library's two DEFLATE levels stand in for the two tiers.
// The seam, the heat tiering, and the self-describing per-page frame are what this slice
// pins down; swapping in a faster hot codec later is a localized change.

// codecID identifies the byte codec a compressed data page was written with. It is the
// first byte of the page's compressed frame, so a reader decodes the page knowing only the
// page itself, never the policy that wrote it (spec 13 §3.1: the per-block header records
// the codec, so a reader always knows how to decode regardless of policy changes). The ids
// are part of the on-disk format and must never be renumbered.
type codecID uint8

const (
	codecNone codecID = 0 // identity: the frame payload is the raw cell bytes verbatim
	codecFast codecID = 1 // DEFLATE at best speed, the hot-level codec
	codecHigh codecID = 2 // DEFLATE at best compression, the cold-level codec
)

// flate writers and readers are expensive to allocate. The compress side keeps a pool
// per level; the decompress side keeps a single pool since both codec levels produce
// standard DEFLATE streams that the same reader handles. A pooled reader is reset via
// the flate.Resetter interface before each use so no internal state carries over.
// A bytes.Buffer pool feeds the compress side so the backing array grows once and is
// reused across page-compression calls on the same goroutine.
var (
	fastWriterPool = sync.Pool{New: func() any { w, _ := flate.NewWriter(io.Discard, flate.BestSpeed); return w }}
	highWriterPool = sync.Pool{New: func() any { w, _ := flate.NewWriter(io.Discard, flate.BestCompression); return w }}
	readerPool     = sync.Pool{New: func() any { return flate.NewReader(bytes.NewReader(nil)) }}
	compressBufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

// deflate compresses raw with a pooled flate writer at the pool's level and returns the
// compressed stream. The output bytes.Buffer is pooled to avoid re-allocating the backing
// array on each call; the result is copied out before the buffer is returned to the pool.
func deflate(pool *sync.Pool, raw []byte) []byte {
	buf := compressBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	w := pool.Get().(*flate.Writer)
	w.Reset(buf)
	_, _ = w.Write(raw)
	_ = w.Close()
	pool.Put(w)
	out := append([]byte(nil), buf.Bytes()...)
	compressBufPool.Put(buf)
	return out
}

// compressBlock encodes raw into a self-describing frame: a one-byte codec id, a uvarint of
// the raw length, then the codec's payload. The raw length lets the reader size its output
// and verify the decode, and the id lets it pick the decoder, so the frame carries
// everything decode needs and nothing the page's position in the tree could supply.
func compressBlock(id codecID, raw []byte) []byte {
	frame := make([]byte, 0, len(raw)/2+8)
	frame = append(frame, byte(id))
	frame = format.AppendUvarint(frame, uint64(len(raw)))
	switch id {
	case codecFast:
		frame = append(frame, deflate(&fastWriterPool, raw)...)
	case codecHigh:
		frame = append(frame, deflate(&highWriterPool, raw)...)
	default: // codecNone and any unknown id store the bytes verbatim
		frame = append(frame, raw...)
	}
	return frame
}

// decompressBlock reverses compressBlock. It reads the id and raw length, then decodes the
// payload, which runs to the end of the slice the caller passes (a data page's bytes after
// its header, trailing zero padding included): the DEFLATE stream is self-terminating so
// the reader stops at its end marker and ignores the padding, and the identity payload is
// sliced to exactly the recorded raw length. The returned slice is freshly allocated and
// owned by the caller, so it outlives the page it was read from.
func decompressBlock(frame []byte) ([]byte, error) {
	if len(frame) == 0 {
		return nil, fmt.Errorf("lsm: empty compressed block frame")
	}
	id := codecID(frame[0])
	rawLen64, n := format.Uvarint(frame[1:])
	if n <= 0 {
		return nil, fmt.Errorf("lsm: compressed block frame has a malformed length")
	}
	rawLen := int(rawLen64)
	payload := frame[1+n:]
	switch id {
	case codecFast, codecHigh:
		r := readerPool.Get().(io.ReadCloser)
		_ = r.(flate.Resetter).Reset(bytes.NewReader(payload), nil)
		out := make([]byte, rawLen)
		_, err := io.ReadFull(r, out)
		readerPool.Put(r)
		if err != nil {
			return nil, fmt.Errorf("lsm: decompress data block: %w", err)
		}
		return out, nil
	default: // codecNone and any unknown id: the payload is the raw bytes
		if len(payload) < rawLen {
			return nil, fmt.Errorf("lsm: identity block frame is shorter than its recorded length")
		}
		return append([]byte(nil), payload[:rawLen]...), nil
	}
}
