package engine

import (
	"fmt"
	"math/rand"
	"testing"
)

// concatMerge is a simple associative merge used to exercise the merge path:
// the new value is the old value with the operand appended.
func concatMerge(existing, operand []byte) []byte {
	out := make([]byte, 0, len(existing)+len(operand))
	out = append(out, existing...)
	out = append(out, operand...)
	return out
}

func TestModelBasicSetGetDelete(t *testing.T) {
	m := NewModel()
	m.Open(&Env{})

	b1 := NewWriteBatch(10)
	b1.Set([]byte("a"), []byte("1"))
	b1.Set([]byte("b"), []byte("2"))
	if err := m.Apply(b1, 10); err != nil {
		t.Fatal(err)
	}

	rd, _ := m.NewReader(Snapshot{Version: 10})
	if v, err := rd.Get([]byte("a")); err != nil || string(v) != "1" {
		t.Fatalf("get a = %q, %v", v, err)
	}
	if _, err := rd.Get([]byte("zzz")); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	rd.Close()

	// Snapshot isolation: an older snapshot must not see version 10.
	rd0, _ := m.NewReader(Snapshot{Version: 5})
	if _, err := rd0.Get([]byte("a")); err != ErrNotFound {
		t.Fatalf("snapshot 5 should not see version 10")
	}
	rd0.Close()

	// Delete at a newer version hides the key going forward but not in the past.
	b2 := NewWriteBatch(20)
	b2.Delete([]byte("a"))
	m.Apply(b2, 20)

	rdNew, _ := m.NewReader(Snapshot{Version: 20})
	if _, err := rdNew.Get([]byte("a")); err != ErrNotFound {
		t.Fatalf("deleted key still visible at version 20")
	}
	rdNew.Close()

	rdOld, _ := m.NewReader(Snapshot{Version: 10})
	if v, err := rdOld.Get([]byte("a")); err != nil || string(v) != "1" {
		t.Fatalf("version 10 should still see a=1, got %q %v", v, err)
	}
	rdOld.Close()
}

func TestModelMergeFold(t *testing.T) {
	m := NewModel()
	m.SetMergeFunc(concatMerge)
	m.Open(&Env{})

	b1 := NewWriteBatch(1)
	b1.Set([]byte("k"), []byte("base"))
	m.Apply(b1, 1)
	b2 := NewWriteBatch(2)
	b2.Merge([]byte("k"), []byte("-x"))
	m.Apply(b2, 2)
	b3 := NewWriteBatch(3)
	b3.Merge([]byte("k"), []byte("-y"))
	m.Apply(b3, 3)

	rd, _ := m.NewReader(Snapshot{Version: 3})
	v, err := rd.Get([]byte("k"))
	if err != nil || string(v) != "base-x-y" {
		t.Fatalf("merge fold = %q, %v; want base-x-y", v, err)
	}
	rd.Close()
}

func TestModelReverseScan(t *testing.T) {
	m := NewModel()
	m.Open(&Env{})
	b := NewWriteBatch(1)
	for _, k := range []string{"a", "b", "c", "d"} {
		b.Set([]byte(k), []byte(k))
	}
	m.Apply(b, 1)

	rd, _ := m.NewReader(Snapshot{Version: 1})
	cur, _ := rd.NewIter(IterOptions{Reverse: true})
	var got []string
	for ok := cur.First(); ok; ok = cur.Next() {
		got = append(got, string(cur.Key()))
	}
	cur.Close()
	rd.Close()
	want := []string{"d", "c", "b", "a"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("reverse scan = %v, want %v", got, want)
	}
}

func TestModelPrefixScan(t *testing.T) {
	m := NewModel()
	m.Open(&Env{})
	b := NewWriteBatch(1)
	for _, k := range []string{"app", "apple", "apply", "banana", "az"} {
		b.Set([]byte(k), []byte(k))
	}
	m.Apply(b, 1)

	rd, _ := m.NewReader(Snapshot{Version: 1})
	cur, _ := rd.NewIter(IterOptions{Prefix: []byte("app")})
	var got []string
	for ok := cur.First(); ok; ok = cur.Next() {
		got = append(got, string(cur.Key()))
	}
	cur.Close()
	rd.Close()
	want := []string{"app", "apple", "apply"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("prefix scan = %v, want %v", got, want)
	}
}

// TestModelConformanceRandom drives the model engine through many randomized
// commit sequences and checks it against the oracle at every snapshot. This is
// the M0 backbone: the same CheckEngine harness will later run the real cores.
func TestModelConformanceRandom(t *testing.T) {
	for _, withMerge := range []bool{false, true} {
		for seed := int64(0); seed < 40; seed++ {
			rng := rand.New(rand.NewSource(seed))
			batches := randomBatches(rng, 30, 8)
			eng := NewModel()
			eng.Open(&Env{})
			var mergeFn func(existing, operand []byte) []byte
			if withMerge {
				mergeFn = concatMerge
			}
			if err := CheckEngine(eng, batches, mergeFn); err != nil {
				t.Fatalf("merge=%v seed=%d: %v", withMerge, seed, err)
			}
		}
	}
}

// randomBatches builds a sequence of committed batches with strictly increasing
// versions over a small key space, mixing sets, deletes, and merges.
func randomBatches(rng *rand.Rand, nBatches, keySpace int) []*WriteBatch {
	var out []*WriteBatch
	version := uint64(0)
	for i := 0; i < nBatches; i++ {
		version += uint64(1 + rng.Intn(3))
		b := NewWriteBatch(version)
		ops := 1 + rng.Intn(4)
		// A transaction coalesces writes per key, so each commit version yields at
		// most one internal key per user key. Enforce that here so the generated
		// batches are realistic and free of same-version, same-key ambiguity.
		seen := map[string]bool{}
		for j := 0; j < ops; j++ {
			key := fmt.Sprintf("k%02d", rng.Intn(keySpace))
			if seen[key] {
				continue
			}
			seen[key] = true
			kb := []byte(key)
			switch rng.Intn(5) {
			case 0:
				b.Delete(kb)
			case 1, 2:
				b.Merge(kb, []byte(fmt.Sprintf("+%d", rng.Intn(10))))
			default:
				b.Set(kb, []byte(fmt.Sprintf("v%d", rng.Intn(100))))
			}
		}
		out = append(out, b)
	}
	return out
}
