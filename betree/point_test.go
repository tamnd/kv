package betree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// TestPointGetVersionGroupSpansLeaves targets the subtle surface the on-page point path adds:
// a single user key with enough versions that its version group is split across more than one
// leaf, read back at a range of snapshots. The point seek routes to the leaf holding the
// newest version, then walks right siblings while the group continues, so each snapshot must
// resolve to the newest version at or below it exactly as a whole-keyspace fold would. A bug in
// the right-sibling continuation (stopping at the first leaf, or running past the group) would
// surface here as a wrong version or a not-found at some snapshot.
func TestPointGetVersionGroupSpansLeaves(t *testing.T) {
	tr := newTreeBig(t)

	// One hot key plus padding keys on either side, so the hot key's many versions sit between
	// neighbors and a split lands a leaf boundary inside the version group. The value is padded
	// so the versions fill several leaves rather than one.
	hot := []byte("k500")
	val := func(v uint64) []byte { return []byte(fmt.Sprintf("hot-v%03d-%0400d", v, v)) }

	const versions = 200
	// Seed a band of neighbor keys at v1 so the hot key is interior to the run, not the edge.
	b0 := engine.NewWriteBatch(1)
	for i := 0; i < 64; i++ {
		b0.Set([]byte(fmt.Sprintf("k%03d", i*10)), []byte(fmt.Sprintf("nbr-%03d", i*10)))
	}
	if err := tr.Apply(b0, b0.Version()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Now stack many versions onto the one hot key, flushing periodically so the versions land
	// on real leaves and a split puts a boundary inside the group.
	for v := uint64(2); v <= versions; v++ {
		wb := engine.NewWriteBatch(v)
		wb.Set(hot, val(v))
		if err := tr.Apply(wb, wb.Version()); err != nil {
			t.Fatalf("apply v%d: %v", v, err)
		}
		if v%8 == 0 {
			if err := tr.Flush(); err != nil {
				t.Fatalf("flush at v%d: %v", v, err)
			}
		}
	}
	if err := tr.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}

	// At every snapshot the hot key must resolve to the newest version at or below it.
	for snap := uint64(2); snap <= versions; snap++ {
		rd, err := tr.NewReader(engine.Snapshot{Version: snap})
		if err != nil {
			t.Fatalf("reader at v%d: %v", snap, err)
		}
		got, err := rd.Get(hot)
		rd.Close()
		if err != nil {
			t.Fatalf("get hot at snap %d: %v", snap, err)
		}
		want := val(snap)
		if string(got) != string(want) {
			t.Fatalf("snap %d: hot = %q, want %q", snap, got, want)
		}
	}
}

// TestPointGetDeleteAndMissAndTail checks the point path's three non-Set outcomes: a key whose
// newest visible version is a delete reads not-found, a key never written reads not-found, and
// a write resting in the hot tail (not yet flushed to its leaf) is still seen. The tail case is
// the one the per-key leaf seek could miss if it consulted only the leaf run, so it is the
// regression guard for the tail consultation the point path keeps.
func TestPointGetDeleteAndMissAndTail(t *testing.T) {
	tr := newTreeBig(t)

	keyOf := func(i int) []byte { return []byte(fmt.Sprintf("d%04d", i)) }
	b := engine.NewWriteBatch(1)
	for i := 0; i < 500; i++ {
		b.Set(keyOf(i), []byte(fmt.Sprintf("val-%04d", i)))
	}
	if err := tr.Apply(b, b.Version()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Delete a third of the keys and overwrite another third, leaving these in the tail
	// (no flush), so the read must fold the tail message over the flushed leaf record.
	wb := engine.NewWriteBatch(2)
	for i := 0; i < 500; i += 3 {
		wb.Delete(keyOf(i))
	}
	for i := 1; i < 500; i += 3 {
		wb.Set(keyOf(i), []byte(fmt.Sprintf("OW-%04d", i)))
	}
	if err := tr.Apply(wb, wb.Version()); err != nil {
		t.Fatalf("mutate: %v", err)
	}

	rd, err := tr.NewReader(engine.Snapshot{Version: 2})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()

	for i := 0; i < 500; i++ {
		got, err := rd.Get(keyOf(i))
		switch {
		case i%3 == 0: // deleted in the tail
			if err == nil {
				t.Fatalf("key %d deleted but Get returned %q", i, got)
			}
		case i%3 == 1: // overwritten in the tail
			if err != nil {
				t.Fatalf("key %d: %v", i, err)
			}
			if want := fmt.Sprintf("OW-%04d", i); string(got) != want {
				t.Fatalf("key %d = %q, want %q", i, got, want)
			}
		default: // untouched, served from the flushed leaf
			if err != nil {
				t.Fatalf("key %d: %v", i, err)
			}
			if want := fmt.Sprintf("val-%04d", i); string(got) != want {
				t.Fatalf("key %d = %q, want %q", i, got, want)
			}
		}
	}

	// A key that was never written reads not-found through the point path.
	if got, err := rd.Get([]byte("d9999")); err == nil {
		t.Fatalf("absent key returned %q, want not-found", got)
	}
}
