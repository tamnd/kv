package betree

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// This file gates M4.1, the resident learned point index (learned.go). The model is an
// accelerator the read path consults before the interior descent, and its whole correctness
// story is that it is only ever a hint: the read verifies the predicted leaf and falls back to
// the proven descent, so a wrong or stale model can never return a wrong answer. These tests
// pin both halves of that: the safety invariant the read relies on (the located leaf starts at
// or before the key), and the end-to-end equality that proves the model never changes an
// answer (a read with the model present equals the same read with it cleared to force descent).

// buildLocatorTree drives a workload over a small-page tree and flushes so a rollover builds
// the model, then asserts the model is present and non-trivial. It returns the tree and the
// committed versions for the readers below.
func buildLocatorTree(t *testing.T, seed int64, nkeys, nver int) (*Tree, []uint64) {
	t.Helper()
	tr := newTreeSmall(t)
	tr.SetMergeFunc(concatMerge)
	rng := rand.New(rand.NewSource(seed))
	versions := driveRandom(t, tr, rng, nkeys, nver, false)
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	loc := tr.locator.Load()
	if loc == nil {
		t.Fatalf("locator nil after flush over %d keys: model never built", nkeys)
	}
	if len(loc.entries) < minLocatorLeaves {
		t.Fatalf("locator has %d entries, want >= %d (tree too shallow to exercise the model)", len(loc.entries), minLocatorLeaves)
	}
	return tr, versions
}

// TestLocatorAtOrBeforeInvariant pins the property the right-sibling walk's correctness rests
// on: for every probe key, the leaf the model locates has a smallest key at or before the probe
// (or, when the probe is below the whole run, the model returns the leftmost leaf). A located
// leaf that started after the probe key could skip the leaf that holds it, so this is the
// invariant that must never break.
func TestLocatorAtOrBeforeInvariant(t *testing.T) {
	tr, _ := buildLocatorTree(t, 1, 400, 160)
	loc := tr.locator.Load()
	first0 := loc.entries[0].first

	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("key%04d", i)) }
	for i := 0; i < 400; i++ {
		k := keyOf(i)
		lik := format.EncodeInternalKey(k, format.MaxVersion, format.KindSet)
		pg := loc.locate(lik)
		if pg == format.NoPage {
			t.Fatalf("locate(%q) returned NoPage", k)
		}
		lf, err := tr.viewLeaf(pg)
		if err != nil {
			t.Fatalf("viewLeaf(%d) for %q: %v", pg, k, err)
		}
		if len(lf.records) == 0 {
			t.Fatalf("locate(%q) landed on an empty leaf %d", k, pg)
		}
		got := format.UserKey(lf.records[0].key)
		if bytes.Compare(k, first0) < 0 {
			// Below the whole run: the model must return the leftmost leaf, the only safe start.
			if pg != loc.entries[0].page {
				t.Fatalf("locate(%q) below all keys returned page %d, want leftmost %d", k, pg, loc.entries[0].page)
			}
			continue
		}
		if bytes.Compare(got, k) > 0 {
			t.Fatalf("locate(%q) leaf starts at %q, which is AFTER the probe (at-or-before invariant broken)", k, got)
		}
	}
}

// TestLocatorMatchesDescent is the end-to-end equality: a full read with the model present must
// equal the same read with the model cleared, which forces every start-leaf resolution through
// the leafForKey descent. Since the only difference between the two runs is the model, equality
// proves the model never changes an answer.
func TestLocatorMatchesDescent(t *testing.T) {
	tr, versions := buildLocatorTree(t, 2, 400, 160)
	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("key%04d", i)) }

	for _, ver := range versions[len(versions)-8:] {
		withModel := readForward(t, tr, ver, engine.IterOptions{})

		// Point reads with the model, then with it cleared, must agree at every probe.
		rd, err := tr.NewReader(engine.Snapshot{Version: ver})
		if err != nil {
			t.Fatalf("reader v%d: %v", ver, err)
		}
		type res struct {
			val []byte
			err error
		}
		modelGet := make([]res, 400)
		for i := 0; i < 400; i++ {
			v, e := rd.Get(keyOf(i))
			modelGet[i] = res{append([]byte(nil), v...), e}
		}
		rd.Close()

		saved := tr.locator.Load()
		tr.locator.Store(nil) // force the descent path
		descend := readForward(t, tr, ver, engine.IterOptions{})
		rd2, _ := tr.NewReader(engine.Snapshot{Version: ver})
		for i := 0; i < 400; i++ {
			v, e := rd2.Get(keyOf(i))
			if (e == nil) != (modelGet[i].err == nil) || !bytes.Equal(v, modelGet[i].val) {
				rd2.Close()
				t.Fatalf("v%d get %q: model=(%q,%v) descent=(%q,%v)", ver, keyOf(i), modelGet[i].val, modelGet[i].err, v, e)
			}
		}
		rd2.Close()
		tr.locator.Store(saved) // restore the model for the next version

		if !sameView(withModel, descend) {
			t.Fatalf("v%d: scan with model (%d) != scan via descent (%d)", ver, len(withModel), len(descend))
		}
	}
}

// TestSplineErrorBound validates the spline construction directly: for every key the model was
// built on, the spline's predicted index is within the configured error window of the key's
// true leaf index. This does not gate correctness (the local search self-corrects past any
// error), but it gates the performance property the model exists for: a tight prediction means
// a short local search rather than a scan.
func TestSplineErrorBound(t *testing.T) {
	tr, _ := buildLocatorTree(t, 3, 600, 200)
	loc := tr.locator.Load()
	for i := range loc.entries {
		pred := loc.predictIndex(loc.entries[i].first)
		diff := pred - i
		if diff < 0 {
			diff = -diff
		}
		// The window is the build error plus one, because a query key between two leaf keys can
		// fall up to one index past either bracketing leaf's bounded prediction.
		if diff > locatorMaxErr+1 {
			t.Fatalf("entry %d (%q): predicted index %d, off by %d, want <= %d",
				i, loc.entries[i].first, pred, diff, locatorMaxErr+1)
		}
	}
}

// TestLocatorAdversarialClustered drives a key distribution the spline cannot fit: keys that
// share an eight-byte prefix all collapse to one spline input, so the model is near useless and
// the locate must lean on its self-correcting local search and the binary-search fallback. The
// model must still locate at or before every key and the reads must still equal the descent,
// which proves the bounded-window fallback (D6's answer to the data-dependent worst case) keeps
// the model correct on hostile data.
func TestLocatorAdversarialClustered(t *testing.T) {
	tr := newTreeSmall(t)
	tr.SetMergeFunc(concatMerge)

	// All keys share the prefix "cluster_" (8 bytes), so keyToU64 maps every one to the same
	// uint and the spline has no signal to fit. The distinguishing bytes come after.
	const nkeys = 500
	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("cluster_%08d", i)) }
	ver := uint64(0)
	for round := 0; round < 5; round++ {
		ver++
		b := engine.NewWriteBatch(ver)
		for i := round; i < nkeys; i += 5 {
			b.Set(keyOf(i), []byte(fmt.Sprintf("v%d", ver)))
		}
		tr.NoteLSN(ver)
		if err := tr.Apply(b, ver); err != nil {
			t.Fatalf("apply round %d: %v", round, err)
		}
	}
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	loc := tr.locator.Load()
	if loc == nil {
		t.Fatal("locator nil on the clustered workload")
	}

	// Every key shares the spline input, so the model is forced onto its local search and
	// fallback, but the at-or-before invariant and the read equality must still hold.
	for i := 0; i < nkeys; i++ {
		k := keyOf(i)
		lik := format.EncodeInternalKey(k, format.MaxVersion, format.KindSet)
		pg := loc.locate(lik)
		lf, err := tr.viewLeaf(pg)
		if err != nil {
			t.Fatalf("viewLeaf for %q: %v", k, err)
		}
		// A located leaf is safe when its smallest internal key is at or before lik, or when it is
		// the leftmost leaf (the below-all-internal case: lik at MaxVersion sorts before the real
		// newest version of its own user key, so the smallest user key routes to the leftmost leaf,
		// which the descent returns too). Anything else would start the walk past lik.
		if pg != loc.entries[0].page && len(lf.records) > 0 &&
			format.CompareInternal(lf.records[0].key, lik) > 0 {
			t.Fatalf("clustered locate(%q) leaf starts after the probe", k)
		}
	}

	withModel := readForward(t, tr, ver, engine.IterOptions{})
	tr.locator.Store(nil)
	descend := readForward(t, tr, ver, engine.IterOptions{})
	if !sameView(withModel, descend) {
		t.Fatalf("clustered: scan with model (%d) != descent (%d)", len(withModel), len(descend))
	}
}

// FuzzLocatorMatchesDescent programs a write stream from the corpus, flushes to build the model,
// and asserts every key reads the same with the model present as with it cleared, so adversarial
// key and write patterns the table tests miss cannot make the model diverge from the descent.
func FuzzLocatorMatchesDescent(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	f.Add([]byte{0, 0, 9, 9, 1, 1, 2, 2, 3, 3, 4, 4})
	f.Fuzz(func(t *testing.T, prog []byte) {
		if len(prog) > 1024 {
			prog = prog[:1024]
		}
		tr := newTreeSmall(t)
		tr.SetMergeFunc(concatMerge)
		const nkeys = 256
		keyOf := func(i int) []byte { return []byte(fmt.Sprintf("key%05d", i)) }

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
				i := (next()<<8 | next()) % nkeys
				if used[i] {
					continue
				}
				used[i] = true
				switch next() % 6 {
				case 0:
					batch.Delete(keyOf(i))
				case 1:
					batch.Merge(keyOf(i), []byte(fmt.Sprintf("+%d", ver)))
				default:
					batch.Set(keyOf(i), []byte(fmt.Sprintf("v%d-%05d", ver, i)))
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
		if err := tr.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}

		rd, err := tr.NewReader(engine.Snapshot{Version: ver})
		if err != nil {
			t.Fatalf("reader: %v", err)
		}
		type res struct {
			val []byte
			ok  bool
		}
		model := make([]res, nkeys)
		for i := 0; i < nkeys; i++ {
			v, e := rd.Get(keyOf(i))
			model[i] = res{append([]byte(nil), v...), e == nil}
		}
		rd.Close()

		tr.locator.Store(nil)
		rd2, _ := tr.NewReader(engine.Snapshot{Version: ver})
		for i := 0; i < nkeys; i++ {
			v, e := rd2.Get(keyOf(i))
			if (e == nil) != model[i].ok || !bytes.Equal(v, model[i].val) {
				rd2.Close()
				t.Fatalf("get %q: model=(%q,%v) descent=(%q,%v)", keyOf(i), model[i].val, model[i].ok, v, e == nil)
			}
		}
		rd2.Close()
	})
}
