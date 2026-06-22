package lsm

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// flushActive seals the active memtable and waits for the background flusher to drain it
// into a segment, the deterministic hook the tests use to force the boundary Apply crosses
// on its own at the cap. It blocks until the sealed memtable has become a segment, so a
// test that calls it can read the segment immediately after.
func (l *LSM) flushActive(t *testing.T) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mem.count() == 0 {
		return
	}
	l.sealActiveLocked()
	for len(l.imm) > 0 && l.flushErr == nil {
		l.flushCond.Wait()
	}
	if l.flushErr != nil {
		t.Fatalf("flush: %v", l.flushErr)
	}
}

// drainSegments waits for the already-sealed memtables to finish flushing, then reports how
// many segments the tree holds. Unlike flushActive it does not seal the active memtable, so a
// test that wants to assert the low cap flushed earlier batches on its own sees exactly the
// segments auto-flush produced, with no segment forced by the drain. It reads the count under
// l.mu, the lock the background flusher publishes versions under, so the read never races the
// install.
func (l *LSM) drainSegments(t *testing.T) int {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	for len(l.imm) > 0 && l.flushErr == nil {
		l.flushCond.Wait()
	}
	if l.flushErr != nil {
		t.Fatalf("flush: %v", l.flushErr)
	}
	return len(l.allSegmentsLocked())
}

// TestLSMFlushConformanceBasic runs the shared oracle suite with the memtable cap set
// so low that every batch after the first flushes the previous one to a segment, so
// the reads resolve across a mix of segments and the live memtable.
func TestLSMFlushConformanceBasic(t *testing.T) {
	l := newLSM(t)
	l.memtableCap = 1

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
	if l.drainSegments(t) == 0 {
		t.Fatal("expected the low cap to have produced segments")
	}
}

// TestLSMFlushConformanceVersioned stacks overwrites, deletes, and merges across many
// keys with the cap low, so versions of one key end up split across several segments
// and the live memtable and must still resolve to the oracle's answer.
func TestLSMFlushConformanceVersioned(t *testing.T) {
	l := newLSM(t)
	l.memtableCap = 1

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

// TestLSMFlushRangeDelete puts a range delete in a flushed segment and a resurrecting
// write in the live memtable, so the read must fold a range delete that lives on disk
// against a newer in-memory write.
func TestLSMFlushRangeDelete(t *testing.T) {
	l := newLSM(t)

	var batches []*engine.WriteBatch
	b1 := engine.NewWriteBatch(10)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		b1.Set([]byte(k), []byte("v1"))
	}
	batches = append(batches, b1)

	b2 := engine.NewWriteBatch(20)
	b2.DeleteRange([]byte("b"), []byte("d")) // removes b and c
	batches = append(batches, b2)

	b3 := engine.NewWriteBatch(30)
	b3.Set([]byte("c"), []byte("v3")) // resurrect c above the range delete
	batches = append(batches, b3)

	// Apply the first two batches, flush them to a segment, then apply the third so
	// the range delete is on disk and the resurrect is in the memtable.
	if err := l.Apply(b1, 10); err != nil {
		t.Fatalf("apply b1: %v", err)
	}
	if err := l.Apply(b2, 20); err != nil {
		t.Fatalf("apply b2: %v", err)
	}
	l.flushActive(t)
	if err := l.Apply(b3, 30); err != nil {
		t.Fatalf("apply b3: %v", err)
	}

	rd, err := l.NewReader(engine.Snapshot{Version: 30})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	want := map[string]string{"a": "v1", "c": "v3", "d": "v1", "e": "v1"}
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		v, err := rd.Get([]byte(k))
		exp, live := want[k]
		if !live {
			if err != engine.ErrNotFound {
				t.Fatalf("Get(%s) = (%q,%v), want ErrNotFound", k, v, err)
			}
			continue
		}
		if err != nil || string(v) != exp {
			t.Fatalf("Get(%s) = (%q,%v), want %q", k, v, err, exp)
		}
	}
}

// TestLSMManySegmentsScan flushes many small batches into separate segments and then
// checks a full forward scan returns every live key in order across all of them.
func TestLSMManySegmentsScan(t *testing.T) {
	l := newLSM(t)

	const segs = 20
	const perSeg = 50
	version := uint64(1)
	for s := 0; s < segs; s++ {
		b := engine.NewWriteBatch(version)
		for i := 0; i < perSeg; i++ {
			key := fmt.Sprintf("key%05d", s*perSeg+i)
			b.Set([]byte(key), []byte(fmt.Sprintf("v%d", version)))
		}
		if err := l.Apply(b, version); err != nil {
			t.Fatalf("apply seg %d: %v", s, err)
		}
		l.flushActive(t)
		version++
	}
	if len(l.allSegmentsLocked()) != segs {
		t.Fatalf("produced %d segments, want %d", len(l.allSegmentsLocked()), segs)
	}

	rd, err := l.NewReader(engine.Snapshot{Version: version})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	cur, err := rd.NewIter(engine.IterOptions{})
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer cur.Close()
	var count int
	var last []byte
	for ok := cur.First(); ok; ok = cur.Next() {
		k := append([]byte(nil), cur.Key()...)
		if last != nil && string(k) <= string(last) {
			t.Fatalf("scan out of order: %q after %q", k, last)
		}
		last = k
		count++
	}
	if count != segs*perSeg {
		t.Fatalf("scan returned %d keys, want %d", count, segs*perSeg)
	}
}

// TestLSMStatsCountsSegments confirms a flush moves footprint from the memtable into
// segment pages: the physical size stays positive and the live memtable empties.
func TestLSMStatsCountsSegments(t *testing.T) {
	l := newLSM(t)
	b := engine.NewWriteBatch(1)
	for i := 0; i < 200; i++ {
		b.Set([]byte(fmt.Sprintf("key%05d", i)), []byte("value"))
	}
	if err := l.Apply(b, 1); err != nil {
		t.Fatalf("apply: %v", err)
	}
	l.flushActive(t)
	if l.mem.count() != 0 {
		t.Fatalf("memtable holds %d cells after flush, want 0", l.mem.count())
	}
	if got := l.Stats().PhysicalBytes; got <= 0 {
		t.Fatalf("PhysicalBytes = %d after flush, want > 0", got)
	}
}
