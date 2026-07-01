package kv

import (
	"sync"
	"testing"
)

// TestIndexPutGet covers the single-threaded contract: a put is found, an update
// replaces the address in place, and an absent fingerprint misses.
func TestIndexPutGet(t *testing.T) {
	ix := NewIndex(1000)
	for i := range 1000 {
		if !ix.Put(uint64(i+1), int64(i*100)) {
			t.Fatalf("put %d failed, table reported full", i)
		}
	}
	for i := range 1000 {
		got, ok := ix.Get(uint64(i + 1))
		if !ok || got != int64(i*100) {
			t.Fatalf("get %d: got (%d,%v) want (%d,true)", i+1, got, ok, i*100)
		}
	}
	// Update in place, the hot-key overwrite path.
	if !ix.Put(42, 999999) {
		t.Fatal("update put failed")
	}
	if got, ok := ix.Get(42); !ok || got != 999999 {
		t.Fatalf("after update get 42: got (%d,%v) want (999999,true)", got, ok)
	}
	if _, ok := ix.Get(1 << 40); ok {
		t.Fatal("absent fingerprint reported present")
	}
}

// TestIndexConcurrentPut is the latch-free correctness claim: many goroutines insert
// disjoint fingerprint ranges at once, and afterward every fingerprint resolves to the
// address its writer stored. A lost insert or a slot handed to two fingerprints would
// drop or corrupt one of these.
func TestIndexConcurrentPut(t *testing.T) {
	const writers = 8
	const each = 4000
	ix := NewIndex(writers * each)

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range each {
				fp := uint64(w*each+i) + 1
				if !ix.Put(fp, int64(fp)*7) {
					t.Errorf("writer %d put fp %d failed", w, fp)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	for w := range writers {
		for i := range each {
			fp := uint64(w*each+i) + 1
			got, ok := ix.Get(fp)
			if !ok || got != int64(fp)*7 {
				t.Fatalf("fp %d: got (%d,%v) want (%d,true)", fp, got, ok, int64(fp)*7)
			}
		}
	}
}
