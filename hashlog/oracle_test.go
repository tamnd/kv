package hashlog

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
)

// The oracle is a reference model the durable store is differentially checked
// against (spec 2070 doc 08 section 3). The model is a plain map with last-writer
// wins, which fully specifies the single-key contract (D2), so any divergence is a
// bug in the store, never in the oracle.
type model struct {
	live map[string][]byte
}

func newModel() *model { return &model{live: map[string][]byte{}} }

func (m *model) set(key, value []byte) {
	m.live[string(key)] = append([]byte(nil), value...)
}

func (m *model) del(key []byte) {
	delete(m.live, string(key))
}

func (m *model) get(key []byte) ([]byte, bool) {
	v, ok := m.live[string(key)]
	return v, ok
}

// durableTunables returns tunables that force the larger-than-memory path: a small
// page and a small resident budget, so a workload of more than a few pages spills to
// the one file (doc 08 M1 gate: a larger-than-memory workload through the one file).
func durableTunables(path string) Tunables {
	return Tunables{
		Shards:                8,
		PageSize:              4096,
		ExtentSize:            4096,
		ResidentPagesPerShard: 2,
		Path:                  path,
	}
}

// checkNoAliasing proves I2 and the M1 form of I7: every allocated extent is
// referenced by exactly one shard's page, no extent is both in use and free, and the
// in-use set plus the free stack accounts for every extent (doc 03 section 9).
func checkNoAliasing(t *testing.T, s *Store) {
	t.Helper()
	if s.df == nil {
		return
	}
	inUse := map[int64]bool{}
	for _, sh := range s.shards {
		sh.mu.RLock()
		for _, ext := range sh.pageExtent {
			if ext < 0 {
				continue
			}
			if inUse[ext] {
				sh.mu.RUnlock()
				t.Fatalf("extent %d referenced by two pages (aliasing)", ext)
			}
			inUse[ext] = true
			// I1: every in-use extent is aligned and in-bounds.
			off := s.df.extentOffset(ext)
			if off < s.df.sbSize || off+s.df.extentSize > s.df.fileEnd {
				sh.mu.RUnlock()
				t.Fatalf("extent %d offset %d out of bounds (fileEnd %d)", ext, off, s.df.fileEnd)
			}
		}
		sh.mu.RUnlock()
	}
	count, free := s.df.alloc.counts()
	for _, id := range free {
		if inUse[id] {
			t.Fatalf("extent %d is both in use and on the free stack", id)
		}
	}
	if int64(len(inUse))+int64(len(free)) != count {
		t.Fatalf("conservation broken: inUse=%d + free=%d != count=%d", len(inUse), len(free), count)
	}
}

func key(i int) []byte { return []byte(fmt.Sprintf("key:%06d", i)) }
func value(rng *rand.Rand) []byte {
	n := 50 + rng.Intn(450)
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + rng.Intn(26))
	}
	return b
}

// TestM1DurableEqualsModel drives a randomized larger-than-memory workload against a
// durable store and asserts it equals the reference model at every Get, and that the
// run actually exercised the disk path.
func TestM1DurableEqualsModel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.hlog")
	s, err := New(durableTunables(path))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m := newModel()
	rng := rand.New(rand.NewSource(42))

	const keys = 2000
	for step := 0; step < 30000; step++ {
		k := key(rng.Intn(keys))
		switch rng.Intn(10) {
		case 0, 1: // delete
			if err := s.Delete(k); err != nil {
				t.Fatal(err)
			}
			m.del(k)
		case 2, 3, 4, 5: // set
			v := value(rng)
			if err := s.Set(k, v); err != nil {
				t.Fatal(err)
			}
			m.set(k, v)
		default: // get, the assertion point
			gotV, gotOK, err := s.Get(k)
			if err != nil {
				t.Fatal(err)
			}
			wantV, wantOK := m.get(k)
			if gotOK != wantOK {
				t.Fatalf("step %d key %s: found=%v, want %v", step, k, gotOK, wantOK)
			}
			if gotOK && string(gotV) != string(wantV) {
				t.Fatalf("step %d key %s: value mismatch", step, k)
			}
		}
	}

	// Full-store assertion: every live key reads back its value, and the store has no
	// key the model lacks.
	for ks, wantV := range m.live {
		gotV, ok, err := s.Get([]byte(ks))
		if err != nil {
			t.Fatal(err)
		}
		if !ok || string(gotV) != string(wantV) {
			t.Fatalf("final: key %s mismatch", ks)
		}
	}
	if s.Len() != len(m.live) {
		t.Fatalf("final Len %d, want %d", s.Len(), len(m.live))
	}
	if s.Spilled() == 0 {
		t.Fatal("workload never spilled to disk; not a larger-than-memory test")
	}
	checkNoAliasing(t, s)
}

// TestM1DurableEqualsMemory drives the identical op stream against a memory-only
// store and a durable store and asserts every Get agrees, isolating the substrate
// swap (where the bytes live) as behaviour-neutral.
func TestM1DurableEqualsMemory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.hlog")
	dur, err := New(durableTunables(path))
	if err != nil {
		t.Fatal(err)
	}
	defer dur.Close()
	memT := durableTunables("")
	memT.Path = ""
	memT.ExtentSize = 0
	mem, err := New(memT)
	if err != nil {
		t.Fatal(err)
	}
	defer mem.Close()

	rng := rand.New(rand.NewSource(7))
	const keys = 1500
	for step := 0; step < 20000; step++ {
		k := key(rng.Intn(keys))
		switch rng.Intn(8) {
		case 0:
			if err := dur.Delete(k); err != nil {
				t.Fatal(err)
			}
			if err := mem.Delete(k); err != nil {
				t.Fatal(err)
			}
		case 1, 2, 3:
			v := value(rng)
			if err := dur.Set(k, v); err != nil {
				t.Fatal(err)
			}
			if err := mem.Set(k, v); err != nil {
				t.Fatal(err)
			}
		default:
			dv, dok, err := dur.Get(k)
			if err != nil {
				t.Fatal(err)
			}
			mv, mok, err := mem.Get(k)
			if err != nil {
				t.Fatal(err)
			}
			if dok != mok || (dok && string(dv) != string(mv)) {
				t.Fatalf("step %d key %s: durable (%v,%q) != memory (%v,%q)", step, k, dok, dv, mok, mv)
			}
		}
	}
	checkNoAliasing(t, dur)
}

// TestM1ConcurrentRace drives the durable store from many goroutines, each owning a
// disjoint key range so the expected per-key outcome stays computable, under the
// race detector. It catches a lock-free reader racing a writer's index publish or an
// evictor pulling a page out from under a read (doc 08 section 3.3).
func TestM1ConcurrentRace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.hlog")
	s, err := New(durableTunables(path))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const workers = 8
	const perWorker = 400
	var wg sync.WaitGroup
	models := make([]*model, workers)
	for w := 0; w < workers; w++ {
		models[w] = newModel()
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 1))
			base := w * perWorker
			for step := 0; step < 8000; step++ {
				k := key(base + rng.Intn(perWorker))
				switch rng.Intn(6) {
				case 0:
					_ = s.Delete(k)
					models[w].del(k)
				case 1, 2:
					v := value(rng)
					_ = s.Set(k, v)
					models[w].set(k, v)
				default:
					_, _, _ = s.Get(k)
				}
			}
		}(w)
	}
	wg.Wait()

	// After the run, each worker's keys must match its model (disjoint ranges, so no
	// cross-worker interference on any key).
	for w := 0; w < workers; w++ {
		for ks, wantV := range models[w].live {
			gotV, ok, err := s.Get([]byte(ks))
			if err != nil {
				t.Fatal(err)
			}
			if !ok || string(gotV) != string(wantV) {
				t.Fatalf("worker %d key %s mismatch", w, ks)
			}
		}
	}
	checkNoAliasing(t, s)
}
