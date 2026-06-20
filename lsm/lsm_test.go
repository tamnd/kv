package lsm

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// newLSM returns an opened LSM engine over an in-memory pager, the same scaffolding
// the B-tree conformance tests use so both cores run the identical oracle suite.
func newLSM(t *testing.T) *LSM {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.kv", pager.Options{
		PageSize:    4096,
		CacheFrames: 16,
		Engine:      format.EngineLSM,
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	l := New(p)
	if err := l.Open(&engine.Env{}); err != nil {
		t.Fatalf("open lsm: %v", err)
	}
	return l
}

// concatMerge is the test merge resolver: append the operand to the existing value.
func concatMerge(existing, operand []byte) []byte {
	out := make([]byte, 0, len(existing)+len(operand))
	out = append(out, existing...)
	out = append(out, operand...)
	return out
}

// TestLSMKind confirms the engine reports the LSM core kind, so the header and Stats
// name the engine the database was created with.
func TestLSMKind(t *testing.T) {
	l := newLSM(t)
	if l.Kind() != engine.LSM {
		t.Fatalf("Kind = %v, want LSM", l.Kind())
	}
}

// TestLSMConformanceBasic drives sets, deletes, and merges across versions through
// the shared oracle, the same mix the B-tree core passes.
func TestLSMConformanceBasic(t *testing.T) {
	l := newLSM(t)

	var batches []*engine.WriteBatch

	b1 := engine.NewWriteBatch(10)
	b1.Set([]byte("apple"), []byte("red"))
	b1.Set([]byte("banana"), []byte("yellow"))
	b1.Set([]byte("cherry"), []byte("dark"))
	batches = append(batches, b1)

	b2 := engine.NewWriteBatch(20)
	b2.Set([]byte("apple"), []byte("green"))
	b2.Delete([]byte("banana"))
	b2.Merge([]byte("cherry"), []byte("!"))
	batches = append(batches, b2)

	b3 := engine.NewWriteBatch(30)
	b3.Merge([]byte("cherry"), []byte("?"))
	b3.Set([]byte("date"), []byte("brown"))
	batches = append(batches, b3)

	if err := engine.CheckEngine(l, batches, concatMerge); err != nil {
		t.Fatalf("conformance: %v", err)
	}
}

// TestLSMConformanceManyKeys inserts a few hundred distinct keys so the memtable's
// skip list grows several levels, then checks the engine matches the oracle for
// point reads and forward and reverse scans.
func TestLSMConformanceManyKeys(t *testing.T) {
	l := newLSM(t)

	const n = 300
	b := engine.NewWriteBatch(5)
	for i := 0; i < n; i++ {
		b.Set([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i)))
	}
	if err := engine.CheckEngine(l, []*engine.WriteBatch{b}, nil); err != nil {
		t.Fatalf("conformance: %v", err)
	}
}

// TestLSMConformanceVersioned combines overwrites, deletes, and merge waves so
// version groups stack on many keys, exactly the B-tree versioned-splits scenario.
func TestLSMConformanceVersioned(t *testing.T) {
	l := newLSM(t)

	const n = 200
	var batches []*engine.WriteBatch

	b1 := engine.NewWriteBatch(100)
	for i := 0; i < n; i++ {
		b1.Set([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%05d", i)))
	}
	batches = append(batches, b1)

	b2 := engine.NewWriteBatch(200)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		switch {
		case i%5 == 0:
			b2.Delete(k)
		case i%2 == 0:
			b2.Set(k, []byte(fmt.Sprintf("w%05d", i)))
		case i%3 == 0:
			b2.Merge(k, []byte("+"))
		}
	}
	batches = append(batches, b2)

	b3 := engine.NewWriteBatch(300)
	for i := 0; i < n; i++ {
		if i%3 == 0 {
			b3.Merge([]byte(fmt.Sprintf("k%05d", i)), []byte("*"))
		}
	}
	batches = append(batches, b3)

	if err := engine.CheckEngine(l, batches, concatMerge); err != nil {
		t.Fatalf("conformance: %v", err)
	}
}

// TestLSMConformanceRangeDelete exercises range-delete folding: a range delete must
// shadow the keys it covers at and below its version while leaving newer writes and
// out-of-range keys visible, resolved identically to the oracle.
func TestLSMConformanceRangeDelete(t *testing.T) {
	l := newLSM(t)

	var batches []*engine.WriteBatch

	b1 := engine.NewWriteBatch(10)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		b1.Set([]byte(k), []byte("v1"))
	}
	batches = append(batches, b1)

	b2 := engine.NewWriteBatch(20)
	b2.DeleteRange([]byte("b"), []byte("d")) // removes b and c, leaves a, d, e
	batches = append(batches, b2)

	b3 := engine.NewWriteBatch(30)
	b3.Set([]byte("c"), []byte("v3")) // resurrect c above the range delete
	batches = append(batches, b3)

	if err := engine.CheckEngine(l, batches, nil); err != nil {
		t.Fatalf("conformance: %v", err)
	}
}

// TestLSMRangeAndPrefixScan checks bounded and prefix iteration directly against the
// reader, confirming bounds clip the view and prefix translates to a bounded range.
func TestLSMRangeAndPrefixScan(t *testing.T) {
	l := newLSM(t)
	b := engine.NewWriteBatch(1)
	for i := 0; i < 100; i++ {
		b.Set([]byte(fmt.Sprintf("a%03d", i)), []byte("x"))
		b.Set([]byte(fmt.Sprintf("b%03d", i)), []byte("y"))
	}
	if err := l.Apply(b, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}
	rd, err := l.NewReader(engine.Snapshot{Version: 1})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()

	cur, err := rd.NewIter(engine.IterOptions{Prefix: []byte("a")})
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	var count int
	var last []byte
	for ok := cur.First(); ok; ok = cur.Next() {
		k := append([]byte(nil), cur.Key()...)
		if k[0] != 'a' {
			t.Fatalf("prefix scan leaked key %q", k)
		}
		if last != nil && string(k) <= string(last) {
			t.Fatalf("prefix scan out of order: %q after %q", k, last)
		}
		last = k
		count++
	}
	cur.Close()
	if count != 100 {
		t.Fatalf("prefix scan returned %d keys, want 100", count)
	}
}

// TestLSMIdempotentApply re-applies the same committed batch, the way recovery
// redoes the WAL tail, and confirms the visible state is unchanged: every internal
// key is unique per version, so a second Apply is a no-op.
func TestLSMIdempotentApply(t *testing.T) {
	l := newLSM(t)
	b := engine.NewWriteBatch(7)
	b.Set([]byte("x"), []byte("1"))
	b.Set([]byte("y"), []byte("2"))
	if err := l.Apply(b, 7); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := l.Apply(b, 7); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if got := l.mem.count(); got != 2 {
		t.Fatalf("memtable holds %d cells after a duplicate apply, want 2", got)
	}
	rd, _ := l.NewReader(engine.Snapshot{Version: 7})
	defer rd.Close()
	if v, err := rd.Get([]byte("x")); err != nil || string(v) != "1" {
		t.Fatalf("Get(x) = (%q, %v), want (1, nil)", v, err)
	}
}
