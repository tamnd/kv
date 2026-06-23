package lsm

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// TestLSMConcurrentReadDuringApply drives point reads against the active memtable while a writer
// applies batches and the small memtable cap forces seals and background flushes throughout. It is
// the load-bearing check for the lock-free read path (perf/03 W1, slice 19): the reader captures
// its snapshot under a brief l.mu.RLock and folds with the lock released, so its getGroup walks the
// active memtable while Apply, whose inserts now run outside l.mu, inserts into the same skip list.
// Under -race a torn forward pointer, a lost snapshot, or a range-delete slice read mid-grow would
// surface here.
//
// The reader keys (0..readKeys) are written once at version 1 and never overwritten, so a reader at
// a high snapshot must always resolve each to its version-1 value, never a miss and never a torn
// read, no matter how the writer churns. The writer keys live in a disjoint range, so the writer's
// inserts, seals, and the occasional range delete it issues over its own range exercise concurrent
// mutation of the structures the reader folds without ever changing the reader's expected answer.
func TestLSMConcurrentReadDuringApply(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.kv", pager.Options{
		PageSize:    4096,
		CacheFrames: 64,
		Engine:      format.EngineLSM,
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	l := New(p)
	// A tiny memtable cap forces a seal every few writer batches, so the seal-and-flush path
	// interleaves with the reads instead of the whole run fitting in one memtable.
	if err := l.Open(&engine.Env{Options: engine.EngineOptions{MemtableSize: 64 << 10}}); err != nil {
		t.Fatalf("open lsm: %v", err)
	}
	defer l.Close()

	const readKeys = 1500
	rkey := func(i int) []byte { return []byte(fmt.Sprintf("r%06d", i)) }
	rval := func(i int) []byte { return []byte(fmt.Sprintf("rv%06d", i)) }
	wkey := func(i int) []byte { return []byte(fmt.Sprintf("w%06d", i)) }

	// Seed the reader keys at version 1. Every reader snapshot at or above 1 must see these.
	b0 := engine.NewWriteBatch(1)
	for i := 0; i < readKeys; i++ {
		b0.Set(rkey(i), rval(i))
	}
	l.NoteLSN(1)
	if err := l.Apply(b0, 1); err != nil {
		t.Fatalf("seed apply: %v", err)
	}

	var writers, readGroup sync.WaitGroup
	stop := make(chan struct{})

	// Writer: churn a disjoint key range at ever-increasing versions, with a range delete over its
	// own range every so often so the memtable's range-delete list grows under the readers.
	writers.Add(1)
	go func() {
		defer writers.Done()
		ver := uint64(2)
		for {
			select {
			case <-stop:
				return
			default:
			}
			b := engine.NewWriteBatch(ver)
			for i := 0; i < 400; i++ {
				b.Set(wkey(i), []byte(fmt.Sprintf("wv%d-%d", ver, i)))
			}
			if ver%8 == 0 {
				b.DeleteRange(wkey(0), wkey(50))
			}
			l.NoteLSN(ver)
			if err := l.Apply(b, ver); err != nil {
				select {
				case <-stop:
				default:
					t.Errorf("writer apply v%d: %v", ver, err)
				}
				return
			}
			ver++
		}
	}()

	// Readers at a high snapshot: every reader key resolves to its version-1 value, always.
	const readers = 6
	for r := 0; r < readers; r++ {
		readGroup.Add(1)
		go func() {
			defer readGroup.Done()
			rd, err := l.NewReader(engine.Snapshot{Version: 1 << 30})
			if err != nil {
				t.Errorf("reader: %v", err)
				return
			}
			defer rd.Close()
			for pass := 0; pass < 120; pass++ {
				for i := 0; i < readKeys; i += 11 {
					got, err := rd.Get(rkey(i))
					if err != nil {
						t.Errorf("get r%06d: %v", i, err)
						return
					}
					if !bytes.Equal(got, rval(i)) {
						t.Errorf("get r%06d = %q, want %q", i, got, rval(i))
						return
					}
				}
			}
		}()
	}

	// Wait for the readers to complete their bounded passes, then signal the writer to stop and
	// wait for it to drain.
	readGroup.Wait()
	close(stop)
	writers.Wait()
}
