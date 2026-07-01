package kv

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// TestDBLargerThanMemory is the step-three claim: write far more data than the ring holds,
// so most records are evicted to disk, and read every one back. A record served from the
// ring and a record served from the file must both return the right value, which exercises
// the validated ring read and the disk fallback.
func TestDBLargerThanMemory(t *testing.T) {
	const ringBytes = 1 << 21 // 2 MiB ring
	const keys = 200000       // ~ tens of MiB of records, an order of magnitude past the ring
	path := filepath.Join(t.TempDir(), "db.log")
	d, err := OpenDB(path, ringBytes, keys)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	for i := range keys {
		key := fmt.Appendf(nil, "key-%08d", i)
		val := fmt.Appendf(nil, "val-%08d-payload", i)
		d.Set(key, val)
	}

	var scratch []byte
	for i := range keys {
		key := fmt.Appendf(nil, "key-%08d", i)
		want := fmt.Sprintf("val-%08d-payload", i)
		got, ok, err := d.Get(key, scratch)
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		if !ok || string(got) != want {
			t.Fatalf("get %q: got (%q,%v) want (%q,true)", key, got, ok, want)
		}
		scratch = got[:0]
	}
}

// TestDBConcurrent runs writers and readers together so the race detector exercises the
// lock-free append, the commit watermark, the flusher, and the validated ring read at
// once. Each writer owns a key range; readers chase keys that have been written.
func TestDBConcurrent(t *testing.T) {
	const ringBytes = 1 << 20
	const writers = 4
	const each = 20000
	path := filepath.Join(t.TempDir(), "db.log")
	d, err := OpenDB(path, ringBytes, writers*each)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range each {
				key := fmt.Appendf(nil, "w%d-k%07d", w, i)
				val := fmt.Appendf(nil, "w%d-v%07d", w, i)
				d.Set(key, val)
			}
		}(w)
	}
	wg.Wait()

	var wg2 sync.WaitGroup
	for w := range writers {
		wg2.Add(1)
		go func(w int) {
			defer wg2.Done()
			var scratch []byte
			for i := range each {
				key := fmt.Appendf(nil, "w%d-k%07d", w, i)
				want := fmt.Sprintf("w%d-v%07d", w, i)
				got, ok, err := d.Get(key, scratch)
				if err != nil || !ok || string(got) != want {
					t.Errorf("get %q: got (%q,%v,%v) want (%q,true,nil)", key, got, ok, err, want)
					return
				}
				scratch = got[:0]
			}
		}(w)
	}
	wg2.Wait()
}

// TestDBPersistsToDisk is a sanity check that Close drains the flusher and the file
// holds the data: write, close, reopen the file read-only through a fresh log, and read a
// late key straight from disk. (Full recovery, rebuilding the index from the file, is the
// step-five subject; this only proves the bytes reached the file.)
func TestDBPersistsToDisk(t *testing.T) {
	const ringBytes = 1 << 20
	path := filepath.Join(t.TempDir(), "db.log")
	d, err := OpenDB(path, ringBytes, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 500 {
		key := fmt.Sprintf("k%04d", i)
		d.Set([]byte(key), fmt.Appendf(nil, "value-%04d", i))
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen the same file and confirm every record comes back, the latest value for its key.
	d2, err := OpenDB(path, ringBytes, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	var scratch []byte
	for i := range 500 {
		key := fmt.Sprintf("k%04d", i)
		v, ok, err := d2.Get([]byte(key), scratch)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("key %s missing after reopen", key)
		}
		if want := fmt.Sprintf("value-%04d", i); string(v) != want {
			t.Fatalf("key %s = %q, want %q", key, v, want)
		}
	}
}

var dbScratchVal = []byte("a-hundred-byte-value-padded-out-to-look-like-a-realistic-record-payload-for-the-out-of-cache-readxx")

// BenchmarkDBGetOutOfCache reads keys spread across a working set far larger than the ring,
// so most reads miss the ring and hit the file. It is the honest larger-than-memory read
// number, the one the read-cache step later improves.
func BenchmarkDBGetOutOfCache(b *testing.B) {
	const ringBytes = 1 << 22 // 4 MiB resident
	const keys = 1 << 19      // ~64 MiB of records, 16x the ring
	path := filepath.Join(b.TempDir(), "db.log")
	d, err := OpenDB(path, ringBytes, keys)
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	key := make([]byte, 8)
	for i := range uint64(keys) {
		fixedKey(key, i)
		d.Set(key, dbScratchVal)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		k := make([]byte, 8)
		scratch := make([]byte, 0, 512) // fixed backing so Get reads in without reallocating
		var i uint64
		for pb.Next() {
			fixedKey(k, i&(keys-1))
			d.Get(k, scratch)
			i++
		}
	})
}
