package betree

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// newTree returns an opened Bε-tree core over a fresh in-memory database. The M0
// skeleton holds its cells in memory, so the pager is only the construction shape
// the later PRs build the paged layout over; the conformance the test asserts does
// not depend on it.
func newTree(t *testing.T) *Tree {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.kv", pager.Options{
		PageSize:    4096,
		CacheFrames: 16,
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

// concatMerge is a deterministic merge resolver: it appends the operand to the
// existing value. The oracle uses the same function, so a conforming core must fold
// merges identically.
func concatMerge(existing, operand []byte) []byte {
	out := make([]byte, 0, len(existing)+len(operand))
	out = append(out, existing...)
	out = append(out, operand...)
	return out
}

func TestKindIsBeta(t *testing.T) {
	tr := newTree(t)
	if tr.Kind() != engine.Beta {
		t.Fatalf("Kind() = %v, want %v", tr.Kind(), engine.Beta)
	}
}

// TestConformanceBasic drives a small mix of sets, deletes, and merges across
// several versions through the conformance oracle.
func TestConformanceBasic(t *testing.T) {
	tr := newTree(t)

	var batches []*engine.WriteBatch

	b1 := engine.NewWriteBatch(10)
	b1.Set([]byte("apple"), []byte("red"))
	b1.Set([]byte("banana"), []byte("yellow"))
	b1.Set([]byte("cherry"), []byte("dark"))
	batches = append(batches, b1)

	b2 := engine.NewWriteBatch(20)
	b2.Set([]byte("apple"), []byte("green")) // overwrite
	b2.Delete([]byte("banana"))              // tombstone
	b2.Merge([]byte("cherry"), []byte("!"))  // merge on top of a set
	batches = append(batches, b2)

	b3 := engine.NewWriteBatch(30)
	b3.Merge([]byte("cherry"), []byte("?")) // second operand
	b3.Set([]byte("date"), []byte("brown"))
	batches = append(batches, b3)

	if err := engine.CheckEngine(tr, batches, concatMerge); err != nil {
		t.Fatalf("conformance: %v", err)
	}
}

// TestConformanceRangeDelete checks that a range delete shadows older versions of
// every covered key while leaving newer writes and out-of-range keys intact.
func TestConformanceRangeDelete(t *testing.T) {
	tr := newTree(t)

	var batches []*engine.WriteBatch

	b1 := engine.NewWriteBatch(10)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		b1.Set([]byte(k), []byte("v1-"+k))
	}
	batches = append(batches, b1)

	b2 := engine.NewWriteBatch(20)
	b2.DeleteRange([]byte("b"), []byte("d")) // covers b, c; leaves a, d, e
	batches = append(batches, b2)

	b3 := engine.NewWriteBatch(30)
	b3.Set([]byte("c"), []byte("v3-c")) // a write newer than the range delete
	batches = append(batches, b3)

	if err := engine.CheckEngine(tr, batches, concatMerge); err != nil {
		t.Fatalf("conformance: %v", err)
	}
}

// TestConformanceRandom is the differential check that matters most for the
// alongside-then-flip plan: a randomized operation stream of sets, deletes, merges,
// and range deletes across many versions is driven through the conformance oracle,
// so any divergence in value, scan order, scan contents, or visibility at any
// snapshot is caught against the known-correct resolution. It runs several seeds so
// a single lucky stream cannot pass a broken core.
func TestConformanceRandom(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			tr := newTree(t)
			batches := randomBatches(rand.New(rand.NewSource(seed)))
			if err := engine.CheckEngine(tr, batches, concatMerge); err != nil {
				t.Fatalf("conformance (seed %d): %v", seed, err)
			}
		})
	}
}

// randomBatches builds a stream of committed batches at strictly increasing
// versions over a small key universe, so most batches touch keys earlier batches
// already wrote and the oracle exercises real version groups, overwrites,
// tombstones, merge chains, and range deletes rather than a flat set of distinct
// keys.
//
// Within one batch each user key is touched at most once. A batch carries a single
// commit version, and the transaction layer above the seam collapses a key's writes
// within a transaction before commit, so the engine never receives two entries that
// share a user key and a version. Honoring that precondition in the generator keeps
// the stream the same shape the real commit path produces; relaxing it would create
// two cells with the identical internal key, which is a malformed batch, not a core
// bug.
func randomBatches(rng *rand.Rand) []*engine.WriteBatch {
	const keyspace = 24
	key := func() []byte { return []byte(fmt.Sprintf("k%02d", rng.Intn(keyspace))) }

	var batches []*engine.WriteBatch
	version := uint64(0)
	nbatch := 8 + rng.Intn(16)
	for bi := 0; bi < nbatch; bi++ {
		version += uint64(1 + rng.Intn(5))
		b := engine.NewWriteBatch(version)
		used := map[string]bool{}
		nops := 1 + rng.Intn(6)
		for oi := 0; oi < nops; oi++ {
			if rng.Intn(10) == 0 {
				// range delete over a small window; its marker is keyed in a separate
				// space from the point cells, so it never collides with them.
				lo := rng.Intn(keyspace)
				hi := lo + 1 + rng.Intn(4)
				b.DeleteRange([]byte(fmt.Sprintf("k%02d", lo)), []byte(fmt.Sprintf("k%02d", hi)))
				continue
			}
			k := key()
			if used[string(k)] {
				continue // one op per key per batch
			}
			used[string(k)] = true
			switch rng.Intn(8) {
			case 0, 1:
				b.Delete(k)
			case 2, 3:
				b.Merge(k, []byte(fmt.Sprintf("m%d", rng.Intn(100))))
			default:
				b.Set(k, []byte(fmt.Sprintf("v%d", rng.Intn(1000))))
			}
		}
		batches = append(batches, b)
	}
	return batches
}
