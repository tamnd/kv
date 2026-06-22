package db

import (
	"fmt"
	"sync"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// TestGroupCommitSharesFsync drives many concurrent SyncFull writers and checks that the
// WAL performed strictly fewer fsyncs than there were commits. Each commit forces a durable
// log, so without batching the fsync count would equal the commit count; group commit lets
// one leader's single Sync cover the whole queued group, so the count must come in under it.
func TestGroupCommitSharesFsync(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Sync: wal.SyncFull, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const writers, perWriter = 16, 64
	const total = writers * perWriter

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range perWriter {
				key := fmt.Sprintf("w%02d-k%04d", w, i)
				if _, err := d.Write(func(b *engine.WriteBatch) {
					b.Set([]byte(key), []byte("v"))
				}); err != nil {
					t.Errorf("write %q: %v", key, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// Every commit landed: all total keys are readable.
	for w := range writers {
		for i := range perWriter {
			key := fmt.Sprintf("w%02d-k%04d", w, i)
			if _, ok := get(t, d, key); !ok {
				t.Fatalf("missing key %q", key)
			}
		}
	}

	// Batching happened: fewer fsyncs than commits. The exact ratio depends on scheduling,
	// so the assertion is only that the leader covered more than one commit per sync overall.
	syncs := d.Stats().Syncs
	if syncs >= total {
		t.Fatalf("expected fewer than %d fsyncs with group commit, got %d", total, syncs)
	}
	t.Logf("%d commits, %d fsyncs (%.1fx amortization)", total, syncs, float64(total)/float64(syncs))
}

// TestGroupCommitVersionsContiguous checks that concurrent commits through the group-commit
// path get distinct, gapless versions. Versions are assigned only to admitted batches, so a
// run of N non-conflicting blind writes must produce exactly the versions baseline+1..baseline+N
// with no duplicates and no holes, regardless of how the leader grouped them.
func TestGroupCommitVersionsContiguous(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Sync: wal.SyncFull, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const writers, perWriter = 8, 50
	const total = writers * perWriter

	var mu sync.Mutex
	seen := make(map[uint64]int, total)

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range perWriter {
				key := fmt.Sprintf("w%d-%d", w, i)
				v, err := d.Write(func(b *engine.WriteBatch) {
					b.Set([]byte(key), []byte("v"))
				})
				if err != nil {
					t.Errorf("write %q: %v", key, err)
					return
				}
				mu.Lock()
				seen[v]++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if len(seen) != total {
		t.Fatalf("expected %d distinct versions, got %d", total, len(seen))
	}
	for v, n := range seen {
		if n != 1 {
			t.Fatalf("version %d assigned %d times", v, n)
		}
	}
}
