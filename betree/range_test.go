package betree

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// This file gates M3.1, the bounded read path (paged.go gatherRange). The invariant it
// pins is the one M3.1 must hold: a bounded read returns exactly the full read clipped to
// its bound. The full read is the M0..M2 path the conformance oracle already proves, so an
// equality against it transitively proves the bounded read without re-deriving the MVCC
// fold here. The property covers every moving part of the bounded gather at once: the
// routed descent to the start leaf, the right-sibling walk with its early stop at the upper
// bound, the range-filtered tail and interior-buffer collection, and the range-delete
// fallback that clips the full gather when a marker's coverage is not local.

// newTreeSmall opens a core over a fresh in-memory database with a small page so a few
// hundred keys span many leaves and several interior levels. The bounded leaf walk and the
// routed descent only do real work across a multi-leaf, multi-level tree, so the small page
// is what makes this file exercise them rather than a single root leaf.
func newTreeSmall(t *testing.T) *Tree {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.kv", pager.Options{
		PageSize:    512,
		CacheFrames: 64,
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

type kvPair struct {
	k []byte
	v []byte
}

// readForward reads the iterator at ver under opts in ascending order into a slice.
func readForward(t *testing.T, tr *Tree, ver uint64, opts engine.IterOptions) []kvPair {
	t.Helper()
	rd, err := tr.NewReader(engine.Snapshot{Version: ver})
	if err != nil {
		t.Fatalf("new reader v%d: %v", ver, err)
	}
	defer rd.Close()
	it, err := rd.NewIter(opts)
	if err != nil {
		t.Fatalf("new iter v%d: %v", ver, err)
	}
	defer it.Close()
	var out []kvPair
	for ok := it.First(); ok; ok = it.Next() {
		lv, err := it.Value()
		if err != nil {
			t.Fatalf("iter value: %v", err)
		}
		v, err := lv.Value()
		if err != nil {
			t.Fatalf("iter lazy value: %v", err)
		}
		out = append(out, kvPair{append([]byte(nil), it.Key()...), append([]byte(nil), v...)})
	}
	return out
}

// clip returns the pairs of view whose key lies in [lower, upper), with a nil bound
// unbounded on that side. It is the test-side mirror of the bounded read's contract.
func clip(view []kvPair, lower, upper []byte) []kvPair {
	var out []kvPair
	for _, e := range view {
		if lower != nil && bytes.Compare(e.k, lower) < 0 {
			continue
		}
		if upper != nil && bytes.Compare(e.k, upper) >= 0 {
			continue
		}
		out = append(out, e)
	}
	return out
}

func sameView(a, b []kvPair) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].k, b[i].k) || !bytes.Equal(a[i].v, b[i].v) {
			return false
		}
	}
	return true
}

// driveRandom applies a random mix of sets, deletes, merges, and (when rangeDels is set)
// range deletes across nver versions over a keyspace of nkeys, returning the versions it
// committed at. Each batch holds at most one write per user key, the engine's one-write-per
// -key-per-version precondition, so the generator tracks the keys used in a batch.
func driveRandom(t *testing.T, tr *Tree, rng *rand.Rand, nkeys, nver int, rangeDels bool) []uint64 {
	t.Helper()
	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("key%04d", i)) }
	var versions []uint64
	ver := uint64(0)
	for v := 0; v < nver; v++ {
		ver++
		b := engine.NewWriteBatch(ver)
		used := map[int]bool{}
		nops := 1 + rng.Intn(8)
		for n := 0; n < nops; n++ {
			i := rng.Intn(nkeys)
			if used[i] {
				continue
			}
			used[i] = true
			switch {
			case rangeDels && rng.Intn(12) == 0:
				// A range delete over a small window starting at this key.
				hi := i + 1 + rng.Intn(5)
				if hi > nkeys {
					hi = nkeys
				}
				b.DeleteRange(keyOf(i), keyOf(hi))
			case rng.Intn(7) == 0:
				b.Delete(keyOf(i))
			case rng.Intn(5) == 0:
				b.Merge(keyOf(i), []byte(fmt.Sprintf("+%d", ver)))
			default:
				b.Set(keyOf(i), []byte(fmt.Sprintf("v%d-%04d", ver, i)))
			}
		}
		if b.Len() == 0 {
			b.Set(keyOf(rng.Intn(nkeys)), []byte(fmt.Sprintf("v%d", ver)))
		}
		tr.NoteLSN(ver)
		if err := tr.Apply(b, ver); err != nil {
			t.Fatalf("apply v%d: %v", ver, err)
		}
		versions = append(versions, ver)
	}
	return versions
}

// checkBoundedEqualsClipped asserts, for a spread of versions and bounds, that the bounded
// read equals the full read clipped to the same bound, and that a point Get agrees with the
// full view. It is the shared body of the plain and the range-delete variants below.
func checkBoundedEqualsClipped(t *testing.T, tr *Tree, rng *rand.Rand, versions []uint64, nkeys int) {
	t.Helper()
	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("key%04d", i)) }
	for _, ver := range versions {
		full := readForward(t, tr, ver, engine.IterOptions{})

		// The unbounded read must already be sorted ascending and duplicate-free; the bounded
		// reads are compared against it.
		for i := 1; i < len(full); i++ {
			if bytes.Compare(full[i-1].k, full[i].k) >= 0 {
				t.Fatalf("v%d: full view not strictly ascending at %d: %q then %q",
					ver, i, full[i-1].k, full[i].k)
			}
		}

		for trial := 0; trial < 12; trial++ {
			a := rng.Intn(nkeys + 2)
			b := rng.Intn(nkeys + 2)
			if a > b {
				a, b = b, a
			}
			var lower, upper []byte
			if rng.Intn(4) != 0 {
				lower = keyOf(a)
			}
			if rng.Intn(4) != 0 {
				upper = keyOf(b)
			}
			got := readForward(t, tr, ver, engine.IterOptions{Lower: lower, Upper: upper})
			want := clip(full, lower, upper)
			if !sameView(got, want) {
				t.Fatalf("v%d bounded [%q,%q): got %d pairs, want %d (clipped full)\n got=%v\nwant=%v",
					ver, lower, upper, len(got), len(want), got, want)
			}
		}

		// Prefix bounds route through the same gather with a computed upper.
		for p := 0; p < 4; p++ {
			pre := []byte(fmt.Sprintf("key%02d", rng.Intn(100)))
			got := readForward(t, tr, ver, engine.IterOptions{Prefix: pre})
			want := clip(full, pre, format.PrefixSuccessor(pre))
			if !sameView(got, want) {
				t.Fatalf("v%d prefix %q: got %d pairs, want %d", ver, pre, len(got), len(want))
			}
		}

		// Point reads must agree with the full view at every key, present or absent.
		fullIdx := map[string][]byte{}
		for _, e := range full {
			fullIdx[string(e.k)] = e.v
		}
		rd, err := tr.NewReader(engine.Snapshot{Version: ver})
		if err != nil {
			t.Fatalf("point reader v%d: %v", ver, err)
		}
		for g := 0; g < 20; g++ {
			k := keyOf(rng.Intn(nkeys))
			got, err := rd.Get(k)
			if want, ok := fullIdx[string(k)]; ok {
				if err != nil || !bytes.Equal(got, want) {
					rd.Close()
					t.Fatalf("v%d get %q = (%q,%v), want %q", ver, k, got, err, want)
				}
			} else if err != engine.ErrNotFound {
				rd.Close()
				t.Fatalf("v%d get %q = (%q,%v), want not-found", ver, k, got, err)
			}
		}
		rd.Close()
	}
}

// TestBoundedEqualsClippedFull is the core M3.1 gate over a plain workload with no range
// deletes, the regime the bounded fast path serves.
func TestBoundedEqualsClippedFull(t *testing.T) {
	tr := newTreeSmall(t)
	tr.SetMergeFunc(concatMerge)
	rng := rand.New(rand.NewSource(1))
	const nkeys = 200
	versions := driveRandom(t, tr, rng, nkeys, 120, false)
	if tr.hasRangeDel.Load() {
		t.Fatal("hasRangeDel set without any range delete written")
	}
	checkBoundedEqualsClipped(t, tr, rng, versions, nkeys)
}

// TestBoundedEqualsClippedWithRangeDels drives the same invariant with range deletes in the
// stream, so the range-delete fallback (full gather then clip) is what answers every bounded
// read, and it still must equal the clipped full view.
func TestBoundedEqualsClippedWithRangeDels(t *testing.T) {
	tr := newTreeSmall(t)
	tr.SetMergeFunc(concatMerge)
	rng := rand.New(rand.NewSource(2))
	const nkeys = 200
	versions := driveRandom(t, tr, rng, nkeys, 120, true)
	if !tr.hasRangeDel.Load() {
		t.Fatal("hasRangeDel not set after range deletes were written")
	}
	checkBoundedEqualsClipped(t, tr, rng, versions, nkeys)
}

// TestReverseIsForwardReversed checks the reverse iterator returns the bounded view in
// descending order: the bound is by user key and the reverse flag only flips the walk, so a
// reverse read must equal the forward bounded read reversed.
func TestReverseIsForwardReversed(t *testing.T) {
	tr := newTreeSmall(t)
	tr.SetMergeFunc(concatMerge)
	rng := rand.New(rand.NewSource(3))
	const nkeys = 150
	versions := driveRandom(t, tr, rng, nkeys, 80, false)
	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("key%04d", i)) }
	ver := versions[len(versions)-1]
	for trial := 0; trial < 20; trial++ {
		a, b := rng.Intn(nkeys), rng.Intn(nkeys)
		if a > b {
			a, b = b, a
		}
		opts := engine.IterOptions{Lower: keyOf(a), Upper: keyOf(b)}
		fwd := readForward(t, tr, ver, opts)

		rd, _ := tr.NewReader(engine.Snapshot{Version: ver})
		opts.Reverse = true
		it, _ := rd.NewIter(opts)
		var rev []kvPair
		for ok := it.First(); ok; ok = it.Next() {
			lv, _ := it.Value()
			v, _ := lv.Value()
			rev = append(rev, kvPair{append([]byte(nil), it.Key()...), append([]byte(nil), v...)})
		}
		it.Close()
		rd.Close()

		reversed := make([]kvPair, len(fwd))
		for i, j := 0, len(fwd)-1; j >= 0; i, j = i+1, j-1 {
			reversed[i] = fwd[j]
		}
		if !sameView(rev, reversed) {
			t.Fatalf("reverse [%q,%q): got %d, want forward reversed %d", keyOf(a), keyOf(b), len(rev), len(reversed))
		}
	}
}

// TestReopenRangeDelFlagBounded proves the Open-time scan: a database with a range delete on
// disk reopens with hasRangeDel set, so its first bounded read takes the correct full path.
func TestReopenRangeDelFlagBounded(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.kv", pager.Options{PageSize: 512, CacheFrames: 64, Engine: format.EngineBeta})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	tr := New(p)
	if err := tr.Open(&engine.Env{}); err != nil {
		t.Fatalf("open: %v", err)
	}
	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("key%04d", i)) }
	const nkeys = 120
	b := engine.NewWriteBatch(1)
	for i := 0; i < nkeys; i++ {
		b.Set(keyOf(i), []byte(fmt.Sprintf("v1-%04d", i)))
	}
	if err := tr.Apply(b, 1); err != nil {
		t.Fatalf("seed: %v", err)
	}
	b2 := engine.NewWriteBatch(2)
	b2.DeleteRange(keyOf(40), keyOf(80))
	if err := tr.Apply(b2, 2); err != nil {
		t.Fatalf("range del: %v", err)
	}
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := p.Checkpoint(2, 2); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := pager.Open(fs, "test.kv", pager.Options{CacheFrames: 64})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	tr2 := New(p2)
	if err := tr2.Open(&engine.Env{}); err != nil {
		t.Fatalf("reopen open: %v", err)
	}
	if !tr2.hasRangeDel.Load() {
		t.Fatal("hasRangeDel not set after reopen of a tree holding a range delete")
	}
	// A bounded read over the deleted window must see the keys gone, the same as the full
	// read does, proving the reopened flag routed the bounded read to the correct path.
	rd, _ := tr2.NewReader(engine.Snapshot{Version: 2})
	defer rd.Close()
	for i := 40; i < 80; i++ {
		if _, err := rd.Get(keyOf(i)); err != engine.ErrNotFound {
			t.Fatalf("reopen v2 get %q: want not-found, got %v", keyOf(i), err)
		}
	}
	for _, i := range []int{0, 39, 80, 119} {
		if _, err := rd.Get(keyOf(i)); err != nil {
			t.Fatalf("reopen v2 get %q: %v", keyOf(i), err)
		}
	}
}

// FuzzBoundedScan programs a write stream and a bound from the corpus bytes and asserts the
// bounded read equals the clipped full read, so adversarial key and bound patterns that the
// table tests miss are explored. It honors the one-write-per-key-per-batch precondition and
// caps the program length so a giant mutated input cannot wedge a worker.
func FuzzBoundedScan(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{0, 9, 9, 0, 5, 5, 1, 1, 2, 2, 3, 3})
	f.Fuzz(func(t *testing.T, prog []byte) {
		if len(prog) > 1024 {
			prog = prog[:1024]
		}
		tr := newTreeSmall(t)
		tr.SetMergeFunc(concatMerge)
		const nkeys = 64
		keyOf := func(i int) []byte { return []byte(fmt.Sprintf("key%04d", i)) }

		ver := uint64(0)
		p := 0
		next := func() int {
			if p >= len(prog) {
				return 0
			}
			b := int(prog[p])
			p++
			return b
		}
		for p < len(prog) {
			ver++
			batch := engine.NewWriteBatch(ver)
			used := map[int]bool{}
			nops := 1 + next()%6
			for n := 0; n < nops && p < len(prog); n++ {
				i := next() % nkeys
				if used[i] {
					continue
				}
				used[i] = true
				switch next() % 8 {
				case 0:
					batch.Delete(keyOf(i))
				case 1:
					hi := i + 1 + next()%4
					if hi > nkeys {
						hi = nkeys
					}
					batch.DeleteRange(keyOf(i), keyOf(hi))
				case 2:
					batch.Merge(keyOf(i), []byte(fmt.Sprintf("+%d", ver)))
				default:
					batch.Set(keyOf(i), []byte(fmt.Sprintf("v%d-%04d", ver, i)))
				}
			}
			if batch.Len() == 0 {
				batch.Set(keyOf(0), []byte("x"))
			}
			tr.NoteLSN(ver)
			if err := tr.Apply(batch, ver); err != nil {
				t.Fatalf("apply v%d: %v", ver, err)
			}
		}
		if ver == 0 {
			return
		}

		full := readForward(t, tr, ver, engine.IterOptions{})
		// A handful of bounds derived from the same program bytes.
		for s := 0; s < len(prog) && s < 16; s += 2 {
			a := int(prog[s]) % nkeys
			b := a + int(prog[(s+1)%len(prog)])%nkeys
			if b > nkeys {
				b = nkeys
			}
			lower, upper := keyOf(a), keyOf(b)
			got := readForward(t, tr, ver, engine.IterOptions{Lower: lower, Upper: upper})
			want := clip(full, lower, upper)
			if !sameView(got, want) {
				t.Fatalf("v%d bounded [%q,%q): got %d, want %d", ver, lower, upper, len(got), len(want))
			}
		}
	})
}
