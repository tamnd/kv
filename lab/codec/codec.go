// Package codec is a frozen experiment: which compression codec should the cold tier use,
// and what does compression actually buy and cost? The store is zero-dependency, so the
// candidates are the standard library's compressors plus a no-op baseline. The question has
// two axes, not one: the ratio (how much disk and read bandwidth compression saves) and the
// throughput (how much CPU it costs), and a store chasing throughput cares about both.
//
// The honest result the board settles (impl note 182): stdlib flate gives a useful ratio on
// real key-value data but runs at tens to low hundreds of MB/s, a fraction of a memcpy, so
// compression is a space-and-bandwidth lever, not a throughput lever. It belongs on the cold
// tier, where data is cold and compact and a block is read rarely, and it stays off the hot
// path and off by default when raw throughput is the goal. The codec is an interface so a
// deployment that wants a faster codec can plug one in behind the same seam; the shipped
// default is zero-dependency flate at its fastest level.
//
// Every codec here is also self-describing at the block layer (see hlog block format): a block
// records its codec and its raw length, and a block that does not compress is stored raw rather
// than inflated, so an incompressible workload pays only the failed attempt, never negative
// savings.
package codec

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
)

// Codec compresses and decompresses a block of bytes. dst is appended to and returned, so a
// caller can reuse a buffer across blocks for allocation-free steady state.
type Codec interface {
	Name() string
	Compress(dst, src []byte) []byte
	Decompress(dst, src []byte) ([]byte, error)
}

// Noop is the baseline: a straight copy, the throughput ceiling and the ratio floor. It is what
// the cold tier falls back to for an incompressible block, so it is a real code path, not just a
// yardstick.
type Noop struct{}

func (Noop) Name() string { return "noop" }

func (Noop) Compress(dst, src []byte) []byte { return append(dst, src...) }

func (Noop) Decompress(dst, src []byte) ([]byte, error) { return append(dst, src...), nil }

// Flate is DEFLATE at a fixed level. Level 1 is the fastest, level 9 the smallest, level 6 the
// usual default; the board sweeps all three so the ratio-versus-speed knee is visible.
type Flate struct {
	Level int
}

func (f Flate) Name() string {
	switch f.Level {
	case flate.BestSpeed:
		return "flate1"
	case flate.BestCompression:
		return "flate9"
	default:
		return "flate6"
	}
}

func (f Flate) Compress(dst, src []byte) []byte {
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, f.Level)
	w.Write(src)
	w.Close()
	return append(dst, buf.Bytes()...)
}

func (f Flate) Decompress(dst, src []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(src))
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return dst, err
	}
	return append(dst, out...), nil
}

// Gzip is DEFLATE plus the gzip header and trailing CRC. It is kept to show the framing
// overhead against bare flate: same compressor, a few more bytes per block and a checksum, which
// on the small blocks a cold tier writes is pure loss against flate.
type Gzip struct{}

func (Gzip) Name() string { return "gzip" }

func (Gzip) Compress(dst, src []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write(src)
	w.Close()
	return append(dst, buf.Bytes()...)
}

func (Gzip) Decompress(dst, src []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return dst, err
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return dst, err
	}
	return append(dst, out...), nil
}
