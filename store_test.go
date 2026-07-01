package kv

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// TestStoreSetGet is the end-to-end point contract: a value set under a key reads back,
// an overwrite is observed, and an absent key misses.
func TestStoreSetGet(t *testing.T) {
	s := NewStore(1<<24, 10000)
	for i := range 5000 {
		key := fmt.Appendf(nil, "key-%05d", i)
		val := fmt.Appendf(nil, "value-for-%05d", i)
		s.Set(key, val)
	}
	for i := range 5000 {
		key := fmt.Appendf(nil, "key-%05d", i)
		want := fmt.Sprintf("value-for-%05d", i)
		if got, ok := s.Get(key); !ok || string(got) != want {
			t.Fatalf("get %q: got (%q,%v) want (%q,true)", key, got, ok, want)
		}
	}
	// Overwrite reads the new value.
	s.Set([]byte("key-00042"), []byte("rewritten"))
	if got, ok := s.Get([]byte("key-00042")); !ok || string(got) != "rewritten" {
		t.Fatalf("overwrite get: got (%q,%v) want (rewritten,true)", got, ok)
	}
	if _, ok := s.Get([]byte("absent")); ok {
		t.Fatal("absent key reported present")
	}
}

// TestStoreKeyVerification forces two keys whose maphash fingerprints would have to be
// reconciled by the record key check. It cannot synthesize a real fingerprint collision,
// so it checks the weaker property the collision path relies on: a Get for a key never
// stored returns a miss even when other keys are present, which is the same compare that
// makes a true collision safe.
func TestStoreKeyVerification(t *testing.T) {
	s := NewStore(1<<20, 1000)
	s.Set([]byte("alpha"), []byte("one"))
	s.Set([]byte("beta"), []byte("two"))
	if _, ok := s.Get([]byte("gamma")); ok {
		t.Fatal("unstored key gamma reported present")
	}
	if got, _ := s.Get([]byte("alpha")); string(got) != "one" {
		t.Fatalf("alpha: got %q want one", got)
	}
}

// fixedKey writes an 8-byte key for index i into dst, so the point benchmarks reuse one
// backing array and measure the store, not key formatting.
func fixedKey(dst []byte, i uint64) {
	binary.LittleEndian.PutUint64(dst, i)
}

var pointVal = []byte("a-hundred-byte-value-padded-out-to-look-like-a-realistic-record-payload-for-the-point-benchmarksxx")

// BenchmarkStoreGet measures the end-to-end read path: hash, index probe, record read,
// key verify. It should be allocation-free, since the value aliases the log.
func BenchmarkStoreGet(b *testing.B) {
	const n = 1 << 16
	s := NewStore(int64(n)*256+1<<20, n)
	key := make([]byte, 8)
	for i := range uint64(n) {
		fixedKey(key, i)
		s.Set(key, pointVal)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		k := make([]byte, 8)
		var i uint64
		for pb.Next() {
			fixedKey(k, i&(n-1))
			s.Get(k)
			i++
		}
	})
}

// BenchmarkStoreSet measures the end-to-end write path: frame into the reserved span,
// index publish. It should be allocation-free on the record path. Each appended record
// grows the log, so the buffer is sized for the whole run.
func BenchmarkStoreSet(b *testing.B) {
	recSize := int64(hdrLen + keyLenSize + 8 + len(pointVal))
	s := NewStore(int64(b.N)*recSize+1<<20, b.N+16)
	key := make([]byte, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range uint64(b.N) {
		fixedKey(key, i)
		s.Set(key, pointVal)
	}
}
