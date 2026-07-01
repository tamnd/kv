package kv

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// cblockRec frames a key-value-shaped record with real redundancy, the kind of data a cold tier
// holds, so the compression ratio the tests and benches report is representative.
func cblockRec(i int) []byte {
	return fmt.Appendf(nil, `user:%08d:profile={"id":%d,"name":"name-%d","active":true,"score":%d}`, i, i, i, i*7%1000)
}

func TestCompressedLogRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cold.cblk")
	l, err := OpenCompressedLog(path, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	const n = 50000
	addrs := make([]int64, n)
	for i := range n {
		addrs[i] = l.Append(cblockRec(i))
	}
	var scratch []byte
	check := func(tag string) {
		for i := range n {
			rec, err := l.At(addrs[i], scratch)
			if err != nil {
				t.Fatalf("%s: At(%d): %v", tag, addrs[i], err)
			}
			if want := cblockRec(i); string(rec) != string(want) {
				t.Fatalf("%s: record %d = %q, want %q", tag, i, rec, want)
			}
		}
	}
	check("before seal") // reads cross sealed blocks and the pending tail
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and confirm recovery rebuilt the block index and every record reads back.
	l2, err := OpenCompressedLog(path, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	for i := range n {
		rec, err := l2.At(addrs[i], scratch)
		if err != nil {
			t.Fatalf("after reopen: At(%d): %v", addrs[i], err)
		}
		if want := cblockRec(i); string(rec) != string(want) {
			t.Fatalf("after reopen: record %d = %q, want %q", i, rec, want)
		}
	}
}

// TestCompressedLogActuallyCompresses confirms the file on disk is much smaller than the logical
// bytes, the whole point of the cold backend, and reports the ratio.
func TestCompressedLogActuallyCompresses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cold.cblk")
	l, err := OpenCompressedLog(path, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	const n = 100000
	for i := range n {
		l.Append(cblockRec(i))
	}
	logical := l.Tail()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	ratio := float64(logical) / float64(fi.Size())
	t.Logf("logical %d bytes, file %d bytes, ratio %.2fx", logical, fi.Size(), ratio)
	if ratio < 2 {
		t.Fatalf("ratio %.2fx too low, compression not effective", ratio)
	}
}

// TestCompressedLogIncompressible checks the raw-store guard: random data never inflates the
// file past the logical bytes by more than the per-block headers.
func TestCompressedLogIncompressible(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cold.cblk")
	l, err := OpenCompressedLog(path, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	// Pseudo-random, incompressible payloads.
	var addrs []int64
	var logical int64
	r := uint64(0x9e3779b97f4a7c15)
	for i := range 20000 {
		rec := make([]byte, 48)
		for j := range rec {
			r ^= r << 13
			r ^= r >> 7
			r ^= r << 17
			rec[j] = byte(r)
		}
		_ = i
		addrs = append(addrs, l.Append(rec))
		logical = l.Tail()
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// File must not be much larger than the logical bytes: the guard stored blocks raw.
	if fi.Size() > logical+logical/16 {
		t.Fatalf("incompressible data inflated: file %d vs logical %d", fi.Size(), logical)
	}
	// And it still reads back.
	l2, err := OpenCompressedLog(path, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	var scratch []byte
	if _, err := l2.At(addrs[0], scratch); err != nil {
		t.Fatalf("read back incompressible record: %v", err)
	}
}

// BenchmarkCompressedLogAppend measures the write throughput including compression, the cost
// side of the cold backend. It is the background migration rate, not the hot write path.
func BenchmarkCompressedLogAppend(b *testing.B) {
	path := filepath.Join(b.TempDir(), "cold.cblk")
	l, err := OpenCompressedLog(path, 1<<16)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	rec := cblockRec(12345)
	b.SetBytes(int64(len(rec)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Append(rec)
	}
}

// BenchmarkCompressedLogReadCold reads records spread across the whole log so most reads land in
// a block other than the cached one, the honest cold-read cost including decompression.
func BenchmarkCompressedLogReadCold(b *testing.B) {
	path := filepath.Join(b.TempDir(), "cold.cblk")
	l, err := OpenCompressedLog(path, 1<<16)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	const n = 200000
	addrs := make([]int64, n)
	for i := range n {
		addrs[i] = l.Append(cblockRec(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var scratch []byte
		var i int
		for pb.Next() {
			// Stride by a large step so consecutive reads miss the one-block cache.
			scratch, _ = l.At(addrs[(i*7919)%n], scratch)
			i++
		}
	})
}
