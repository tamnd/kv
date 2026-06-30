package codec

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// kvBlock builds a block of framed key-value records shaped like a cold tier's: structured keys
// with a common prefix and small JSON-ish values, the kind of data with real redundancy a
// compressor can find, not random bytes. blockBytes is the target uncompressed block size.
func kvBlock(blockBytes int) []byte {
	out := make([]byte, 0, blockBytes)
	for i := 0; len(out) < blockBytes; i++ {
		key := fmt.Appendf(nil, "user:%08d:profile", i)
		val := fmt.Appendf(nil, `{"id":%d,"name":"name-%d","active":true,"score":%d}`, i, i, i*7%1000)
		var hdr [4]byte
		binary.LittleEndian.PutUint16(hdr[0:], uint16(len(key)))
		binary.LittleEndian.PutUint16(hdr[2:], uint16(len(val)))
		out = append(out, hdr[:]...)
		out = append(out, key...)
		out = append(out, val...)
	}
	return out
}

const codecBlockBytes = 1 << 16 // 64 KiB, a representative cold block

var codecs = []Codec{
	Noop{},
	Flate{Level: 1},
	Flate{Level: 6},
	Flate{Level: 9},
	Gzip{},
}

// TestCodecRoundTrip checks every codec returns the original bytes and reports its ratio, so the
// board is reproducible from the test log alone.
func TestCodecRoundTrip(t *testing.T) {
	src := kvBlock(codecBlockBytes)
	for _, c := range codecs {
		comp := c.Compress(nil, src)
		back, err := c.Decompress(nil, comp)
		if err != nil {
			t.Fatalf("%s: decompress: %v", c.Name(), err)
		}
		if string(back) != string(src) {
			t.Fatalf("%s: round trip mismatch", c.Name())
		}
		ratio := float64(len(src)) / float64(len(comp))
		t.Logf("%-8s %6d -> %6d  ratio %.2fx", c.Name(), len(src), len(comp), ratio)
	}
}

func BenchmarkCompress(b *testing.B) {
	src := kvBlock(codecBlockBytes)
	for _, c := range codecs {
		b.Run(c.Name(), func(b *testing.B) {
			b.SetBytes(int64(len(src)))
			b.ReportAllocs()
			var dst []byte
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = c.Compress(dst[:0], src)
			}
		})
	}
}

func BenchmarkDecompress(b *testing.B) {
	src := kvBlock(codecBlockBytes)
	for _, c := range codecs {
		comp := c.Compress(nil, src)
		b.Run(c.Name(), func(b *testing.B) {
			b.SetBytes(int64(len(src)))
			b.ReportAllocs()
			var dst []byte
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				dst, err = c.Decompress(dst[:0], comp)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
