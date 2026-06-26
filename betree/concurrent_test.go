package betree

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// newTreeBig opens a core over a fresh in-memory database with a buffer pool large
// enough that the concurrent stress below never exhausts frames when many readers pin
// pages at once. It is otherwise newTree.
func newTreeBig(t *testing.T) *Tree {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.kv", pager.Options{
		PageSize:    4096,
		CacheFrames: 256,
		Engine:      format.EngineBeta,
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	tr := New(p)
	if err := tr.Open(&engine.Env{}); err != nil {
		t.Fatalf("open betree: %v", err)
	}
	return tr
}

// TestConcurrentReadersFrozenSnapshot is the M2 gate: it drives the latch-free read
// path against a live writer and asserts both that the race detector stays quiet and
// that a reader pinned at a frozen snapshot always sees exactly that snapshot's state,
// no matter how the writer churns the tail and the tree at higher versions.
//
// The writer commits the whole key universe at version 1, then commits a bounded run of
// higher versions that overwrite and delete random keys and force tail rollovers, so a
// reader runs concurrently with in-place tail edits, sealed-run rollovers that move
// messages from the tail into the tree, and the leaf and interior splits those
// rollovers drive. Every reader reads at version 1, where each key holds its base
// value, so a correct MVCC fold under a consistent gather must return that base value on
// every read; a torn gather (a structural change crossing the read) would surface as a
// wrong value, a not-found, or a decode error, and the gen seqlock makes the reader
// restart instead, so the only allowed outcome is the base value. The readers loop until
// the writer finishes its bounded run, which keeps the tree small enough that the
// per-read whole-keyspace gather stays cheap under the race detector while still
// overlapping every writer phase. The direct-SPI drive is deliberate: it bypasses the
// DB's read/write latch so the betree's own optimistic protocol is what is under test,
// which is the only context that exercises it before M5 relaxes that latch.
func TestConcurrentReadersFrozenSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency stress in -short")
	}
	tr := newTreeBig(t)
	tr.SetMergeFunc(concatMerge)

	const nkeys = 32
	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("k%03d", i)) }
	// A base value padded so a few batches cross the 32KiB tail budget and the writer
	// exercises real rollovers rather than resting in the heap.
	baseVal := func(i int) []byte { return []byte(fmt.Sprintf("base-%03d-%0300d", i, i)) }

	// Version 1: the frozen snapshot every reader reads at. Every key holds its base.
	b0 := engine.NewWriteBatch(1)
	for i := 0; i < nkeys; i++ {
		b0.Set(keyOf(i), baseVal(i))
	}
	if err := tr.Apply(b0, b0.Version()); err != nil {
		t.Fatalf("seed apply: %v", err)
	}

	var writerDone atomic.Bool
	var wg sync.WaitGroup

	// The writer: a bounded run of higher versions that churns the tree, then it stops.
	const nbatches = 150
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer writerDone.Store(true)
		rng := rand.New(rand.NewSource(99))
		ver := uint64(1)
		for b := 0; b < nbatches; b++ {
			ver++
			wb := engine.NewWriteBatch(ver)
			used := map[int]bool{}
			for n := 0; n < 16; n++ {
				i := rng.Intn(nkeys)
				if used[i] {
					continue
				}
				used[i] = true
				if rng.Intn(6) == 0 {
					wb.Delete(keyOf(i))
				} else {
					wb.Set(keyOf(i), []byte(fmt.Sprintf("v%d-%03d-%0300d", ver, i, i)))
				}
			}
			if err := tr.Apply(wb, wb.Version()); err != nil {
				t.Errorf("writer apply v%d: %v", ver, err)
				return
			}
			if ver%16 == 0 {
				if err := tr.Flush(); err != nil {
					t.Errorf("writer flush v%d: %v", ver, err)
					return
				}
			}
		}
	}()

	// The readers: each pins version 1 and reads the whole universe until the writer is
	// done, asserting every key always resolves to its base. A point reader and a
	// scanning reader exercise both the Get and the cursor gather. Each reader does at
	// least one full pass even if the writer finishes first.
	const nreaders = 6
	for r := 0; r < nreaders; r++ {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			rd, err := tr.NewReader(engine.Snapshot{Version: 1})
			if err != nil {
				t.Errorf("reader %d: new reader: %v", r, err)
				return
			}
			defer rd.Close()
			for pass := 0; ; pass++ {
				if r%2 == 0 {
					// Point path: every key.
					for i := 0; i < nkeys; i++ {
						got, err := rd.Get(keyOf(i))
						if err != nil {
							t.Errorf("reader %d: get k%03d pass %d: %v", r, i, pass, err)
							return
						}
						if string(got) != string(baseVal(i)) {
							t.Errorf("reader %d: get k%03d pass %d = %q, want base", r, i, pass, got)
							return
						}
					}
				} else {
					// Scan path: the cursor must surface all nkeys keys in order, each at base.
					it, err := rd.NewIter(engine.IterOptions{})
					if err != nil {
						t.Errorf("reader %d: new iter pass %d: %v", r, pass, err)
						return
					}
					seen := 0
					for ok := it.First(); ok; ok = it.Next() {
						lv, err := it.Value()
						if err != nil {
							it.Close()
							t.Errorf("reader %d: iter value pass %d: %v", r, pass, err)
							return
						}
						val, err := lv.Value()
						if err != nil {
							it.Close()
							t.Errorf("reader %d: iter lazy value pass %d: %v", r, pass, err)
							return
						}
						want := baseVal(seen)
						if string(it.Key()) != string(keyOf(seen)) || string(val) != string(want) {
							it.Close()
							t.Errorf("reader %d: iter pos %d pass %d = (%q,%q), want (%q,%q)",
								r, seen, pass, it.Key(), val, keyOf(seen), want)
							return
						}
						seen++
					}
					it.Close()
					if seen != nkeys {
						t.Errorf("reader %d: iter pass %d saw %d keys, want %d", r, pass, seen, nkeys)
						return
					}
				}
				if pass > 0 && writerDone.Load() {
					return
				}
			}
		}()
	}

	wg.Wait()
}

// TestConcurrentReadersEndState drives the same churn but verifies the final committed
// state: after the writer's bounded run, a fresh reader at the final version must match
// the single-writer model exactly, so the concurrent rollovers and splits did not
// corrupt the run they wrote. It is the write-side companion of the frozen-snapshot gate.
func TestConcurrentReadersEndState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency stress in -short")
	}
	tr := newTreeBig(t)
	tr.SetMergeFunc(concatMerge)

	const nkeys = 32
	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("k%03d", i)) }

	model := make(map[int][]byte, nkeys)
	b0 := engine.NewWriteBatch(1)
	for i := 0; i < nkeys; i++ {
		v := []byte(fmt.Sprintf("base-%03d", i))
		b0.Set(keyOf(i), v)
		model[i] = v
	}
	if err := tr.Apply(b0, b0.Version()); err != nil {
		t.Fatalf("seed apply: %v", err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup

	// A few readers at version 1 keep the latch-free read path busy under -race while the
	// writer commits the stream the model tracks.
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rd, err := tr.NewReader(engine.Snapshot{Version: 1})
			if err != nil {
				t.Errorf("bg reader: %v", err)
				return
			}
			defer rd.Close()
			for !stop.Load() {
				for i := 0; i < nkeys; i++ {
					if _, err := rd.Get(keyOf(i)); err != nil {
						t.Errorf("bg reader get k%03d: %v", i, err)
						return
					}
				}
			}
		}()
	}

	// The single writer owns the model: last write wins per key, a delete clears it.
	rng := rand.New(rand.NewSource(7))
	ver := uint64(1)
	for step := 0; step < 150; step++ {
		ver++
		b := engine.NewWriteBatch(ver)
		used := map[int]bool{}
		for n := 0; n < 12; n++ {
			i := rng.Intn(nkeys)
			if used[i] {
				continue
			}
			used[i] = true
			if rng.Intn(5) == 0 {
				b.Delete(keyOf(i))
				model[i] = nil
			} else {
				v := []byte(fmt.Sprintf("v%d-%03d", ver, i))
				b.Set(keyOf(i), v)
				model[i] = v
			}
		}
		if err := tr.Apply(b, b.Version()); err != nil {
			t.Fatalf("writer apply v%d: %v", ver, err)
		}
	}
	stop.Store(true)
	wg.Wait()

	// The end state at the final version must equal the model exactly.
	rd, err := tr.NewReader(engine.Snapshot{Version: ver})
	if err != nil {
		t.Fatalf("final reader: %v", err)
	}
	defer rd.Close()
	for i := 0; i < nkeys; i++ {
		got, err := rd.Get(keyOf(i))
		want := model[i]
		if want == nil {
			if !errors.Is(err, engine.ErrNotFound) {
				t.Fatalf("final k%03d: got (%q,%v), want not-found", i, got, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("final k%03d: %v", i, err)
		}
		if string(got) != string(want) {
			t.Fatalf("final k%03d = %q, want %q", i, got, want)
		}
	}
}
