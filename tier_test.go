package kv

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// TestTieredSetGet is the basic round trip: write more than fits in one hot segment, so the
// migrator runs and records move through active, sealed, and cold, and read every key back.
// A key served from any tier must return its latest value.
func TestTieredSetGet(t *testing.T) {
	const segBytes = 1 << 16 // small segments so many seals happen
	const keys = 50000
	path := filepath.Join(t.TempDir(), "tier.log")
	d, err := OpenTiered(path, segBytes, keys, 1<<20, keys, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	for i := range keys {
		d.Set(fmt.Appendf(nil, "key-%08d", i), fmt.Appendf(nil, "val-%08d", i))
	}
	var scratch []byte
	for i := range keys {
		key := fmt.Appendf(nil, "key-%08d", i)
		want := fmt.Sprintf("val-%08d", i)
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

// TestTieredUpdateWins overwrites every key after it has had time to migrate, then checks the
// newer value wins from whichever tier serves it. This exercises the version-order guarantee:
// the older value may already be in cold while the newer sits in the hot tier, and the read
// path must prefer the hot one, and after the newer migrates it must overwrite cold.
func TestTieredUpdateWins(t *testing.T) {
	const segBytes = 1 << 16
	const keys = 20000
	path := filepath.Join(t.TempDir(), "tier.log")
	d, err := OpenTiered(path, segBytes, keys, 1<<20, keys, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	for i := range keys {
		d.Set(fmt.Appendf(nil, "k%07d", i), []byte("old"))
	}
	for i := range keys {
		d.Set(fmt.Appendf(nil, "k%07d", i), fmt.Appendf(nil, "new-%07d", i))
	}
	var scratch []byte
	for i := range keys {
		key := fmt.Appendf(nil, "k%07d", i)
		want := fmt.Sprintf("new-%07d", i)
		got, ok, _ := d.Get(key, scratch)
		if !ok || string(got) != want {
			t.Fatalf("get %q: got (%q,%v) want (%q,true)", key, got, ok, want)
		}
		scratch = got[:0]
	}
}

// TestTieredConcurrent runs writers and readers together under the race detector across all
// the tiers and the migrator, so the lock-free fast paths, the seal-and-swap, the background
// drain, and the seqlock read cache are all exercised at once.
func TestTieredConcurrent(t *testing.T) {
	const segBytes = 1 << 15
	const writers = 4
	const each = 10000
	path := filepath.Join(t.TempDir(), "tier.log")
	d, err := OpenTiered(path, segBytes, writers*each, 1<<20, writers*each, 2048)
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
				d.Set(fmt.Appendf(nil, "w%d-%07d", w, i), fmt.Appendf(nil, "v%d-%07d", w, i))
			}
		}(w)
	}
	wg.Wait()

	var rg sync.WaitGroup
	for w := range writers {
		rg.Add(1)
		go func(w int) {
			defer rg.Done()
			var scratch []byte
			for i := range each {
				key := fmt.Appendf(nil, "w%d-%07d", w, i)
				want := fmt.Sprintf("v%d-%07d", w, i)
				got, ok, err := d.Get(key, scratch)
				if err != nil || !ok || string(got) != want {
					t.Errorf("get %q: got (%q,%v,%v) want (%q,true,nil)", key, got, ok, err, want)
					return
				}
				scratch = got[:0]
			}
		}(w)
	}
	rg.Wait()
}

// TestTieredDelete checks a delete shadows the value across the tiers it can sit above. It
// writes a key, forces it down to cold with a Sync, reads it so the read cache holds it, then
// deletes it. The key must read absent whether the tombstone is still in the hot tier or has
// itself migrated to cold, and the stale cached value must not leak through.
func TestTieredDelete(t *testing.T) {
	const segBytes = 1 << 16
	path := filepath.Join(t.TempDir(), "tier.log")
	d, err := OpenTiered(path, segBytes, 4096, 1<<20, 4096, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	key := []byte("victim")
	d.Set(key, []byte("present"))
	if err := d.Sync(); err != nil { // push the value to cold
		t.Fatal(err)
	}
	if v, ok, _ := d.Get(key, nil); !ok || string(v) != "present" { // fill the read cache from cold
		t.Fatalf("get before delete: got (%q,%v) want (present,true)", v, ok)
	}
	d.Delete(key)
	if _, ok, _ := d.Get(key, nil); ok { // tombstone in the hot tier shadows the cold value and the cache
		t.Fatal("get after delete (hot tombstone): want miss")
	}
	if err := d.Sync(); err != nil { // migrate the tombstone to cold
		t.Fatal(err)
	}
	if _, ok, _ := d.Get(key, nil); ok { // tombstone now in cold still wins
		t.Fatal("get after delete (cold tombstone): want miss")
	}
}

// TestTieredDeleteThenReinsert checks a key can come back after a delete: the reinserted value
// must win over the tombstone, the same latest-wins order an overwrite uses.
func TestTieredDeleteThenReinsert(t *testing.T) {
	const segBytes = 1 << 16
	path := filepath.Join(t.TempDir(), "tier.log")
	d, err := OpenTiered(path, segBytes, 4096, 1<<20, 4096, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	key := []byte("phoenix")
	d.Set(key, []byte("first"))
	d.Delete(key)
	d.Set(key, []byte("second"))
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := d.Get(key, nil); !ok || string(v) != "second" {
		t.Fatalf("get after reinsert: got (%q,%v) want (second,true)", v, ok)
	}
}

// TestTieredOverwriteAfterCacheFill is the regression for the stale-cache bug the delete work
// surfaced: a value read from cold lands in the read cache, then the key is overwritten and the
// new value migrates to cold. The cached old value must not be served after the hot record that
// shadowed it has drained away. Without the write-side cache invalidation this returns the old
// value.
func TestTieredOverwriteAfterCacheFill(t *testing.T) {
	const segBytes = 1 << 16
	path := filepath.Join(t.TempDir(), "tier.log")
	d, err := OpenTiered(path, segBytes, 4096, 1<<20, 4096, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	key := []byte("mutable")
	d.Set(key, []byte("old"))
	if err := d.Sync(); err != nil { // old to cold
		t.Fatal(err)
	}
	if v, ok, _ := d.Get(key, nil); !ok || string(v) != "old" { // cache the old value from cold
		t.Fatalf("get old: got (%q,%v) want (old,true)", v, ok)
	}
	d.Set(key, []byte("new"))
	if err := d.Sync(); err != nil { // new overwrites old in cold; cache must have been invalidated
		t.Fatal(err)
	}
	if v, ok, _ := d.Get(key, nil); !ok || string(v) != "new" {
		t.Fatalf("get new: got (%q,%v) want (new,true)", v, ok)
	}
}

// TestReadCacheRoundTrip checks the seqlock cache in isolation: put then get returns the
// value, a different key on the same slot is a miss, and an overwrite is visible.
func TestReadCacheRoundTrip(t *testing.T) {
	c := newReadCache(16)
	c.put(forceFP(1), []byte("alpha"), []byte("one"))
	if v, ok := c.get(forceFP(1), []byte("alpha"), nil); !ok || string(v) != "one" {
		t.Fatalf("get alpha: got (%q,%v) want (one,true)", v, ok)
	}
	if _, ok := c.get(forceFP(1), []byte("absent"), nil); ok {
		t.Fatal("get absent: want miss")
	}
	c.put(forceFP(1), []byte("alpha"), []byte("two"))
	if v, ok := c.get(forceFP(1), []byte("alpha"), nil); !ok || string(v) != "two" {
		t.Fatalf("get alpha after overwrite: got (%q,%v) want (two,true)", v, ok)
	}
}
