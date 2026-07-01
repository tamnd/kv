package kv

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// TestSyncWritesDurableOnReturn checks the per-commit-durable contract: with SyncWrites set, a Set
// is on the platter by the time it returns, so the cold log's synced watermark covers every write
// before the store is ever closed. This is the difference from the default path, where a write is
// acked from the hot tier and made durable a moment later by the background flush.
func TestSyncWritesDurableOnReturn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.hlog")
	d, err := Open(path, Options{SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 500 {
		d.Set(fmt.Appendf(nil, "k%05d", i), fmt.Appendf(nil, "v%05d", i))
		// After each write returns, every committed byte must already be synced: durable on return,
		// not durable on a later flush.
		if synced, committed := d.cold.log.Synced(), d.cold.log.committed.Load(); synced < committed {
			t.Fatalf("write %d returned before it was durable: synced=%d committed=%d", i, synced, committed)
		}
	}
	d.Delete([]byte("k00007"))
	if synced, committed := d.cold.log.Synced(), d.cold.log.committed.Load(); synced < committed {
		t.Fatalf("delete returned before it was durable: synced=%d committed=%d", synced, committed)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// A reopen with the default options serves every durable record, and the tombstone survives.
	d2, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	var scratch []byte
	if v, ok, _ := d2.Get([]byte("k00042"), scratch); !ok || string(v) != "v00042" {
		t.Fatalf("get k00042 after reopen: got (%q,%v) want (v00042,true)", v, ok)
	}
	if _, ok, _ := d2.Get([]byte("k00007"), scratch); ok {
		t.Fatal("get k00007 after reopen: durable delete must survive")
	}
}

// TestSyncWritesConcurrent drives the durable path from many goroutines at once, the case its
// group commit is built for: concurrent writers coalesce onto shared fsyncs, and every acked write
// must still read back its own value. Run under -race, it also guards the durable path's shared
// state the way TestTieredConcurrent guards the hot path.
func TestSyncWritesConcurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.hlog")
	const writers, each = 8, 500
	d, err := Open(path, Options{SyncWrites: true, KeyCapacity: writers * each})
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
				k := fmt.Appendf(nil, "w%02d-k%05d", w, i)
				d.Set(k, fmt.Appendf(nil, "w%02d-v%05d", w, i))
			}
		}(w)
	}
	wg.Wait()

	var scratch []byte
	for w := range writers {
		for i := range each {
			k := fmt.Appendf(nil, "w%02d-k%05d", w, i)
			want := fmt.Sprintf("w%02d-v%05d", w, i)
			if v, ok, _ := d.Get(k, scratch); !ok || string(v) != want {
				t.Fatalf("get %s: got (%q,%v) want (%s,true)", k, v, ok, want)
			}
		}
	}
}
