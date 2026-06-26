package betree

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestVersionEncoding pins the bit layout: a fresh word is unlocked, not obsolete, and
// version zero, the lock and obsolete flags read back independently, and an unlock bumps
// the counter without disturbing the flags.
func TestVersionEncoding(t *testing.T) {
	var v version

	w0, ok := v.readBegin()
	if !ok || locked(w0) || obsolete(w0) {
		t.Fatalf("fresh node not readable/clean: w=%#x ok=%v", w0, ok)
	}

	if !v.tryWriteLock(w0) {
		t.Fatal("tryWriteLock on a clean observed word should succeed")
	}
	if w, ok := v.readBegin(); ok || !locked(w) {
		t.Fatalf("locked node should not be readable: w=%#x ok=%v", w, ok)
	}
	if v.tryWriteLock(v.w.Load()) {
		t.Fatal("tryWriteLock on an already-locked node must fail")
	}

	v.writeUnlock()
	w1, ok := v.readBegin()
	if !ok || locked(w1) || obsolete(w1) {
		t.Fatalf("after unlock node should be clean: w=%#x ok=%v", w1, ok)
	}
	if w1 == w0 {
		t.Fatal("writeUnlock must bump the version counter")
	}
	if !v.validate(w1) {
		t.Fatal("validate should hold for an unchanged word")
	}
	if v.validate(w0) {
		t.Fatal("validate should fail against the stale pre-write word")
	}
}

// TestVersionObsolete checks that retiring a node turns away both an in-flight reader and
// a later writer.
func TestVersionObsolete(t *testing.T) {
	var v version
	w0, _ := v.readBegin()
	if !v.tryWriteLock(w0) {
		t.Fatal("lock failed")
	}
	v.writeUnlockObsolete()

	if w, ok := v.readBegin(); ok || !obsolete(w) {
		t.Fatalf("obsolete node should report obsolete and not be readable: w=%#x ok=%v", w, ok)
	}
	if v.writeLock() {
		t.Fatal("writeLock must refuse an obsolete node")
	}
}

// TestVersionTableStableIdentity checks that the table hands the same word out for a page
// number across calls and that forget drops it.
func TestVersionTableStableIdentity(t *testing.T) {
	var tab versionTable
	a := tab.of(7)
	b := tab.of(7)
	if a != b {
		t.Fatal("versionTable.of must return the same *version for one page number")
	}
	if c := tab.of(8); c == a {
		t.Fatal("distinct page numbers must get distinct version words")
	}
	a.writeLock()
	a.writeUnlock()
	tab.forget(7)
	if d := tab.of(7); d == a {
		t.Fatal("after forget a page number must get a fresh version word")
	}
}

// TestOLCReadersNeverTear is the cross-node consistency test, modeling the content
// discipline doc 05 section 1 mandates and M2.2 wires: a node's content is an immutable
// value published through an atomic pointer and never mutated in place, while the version
// word guards consistency ACROSS nodes. Two nodes hold contents a writer always leaves
// equal; it publishes new contents to both under one lock. A reader loads pointer A then
// pointer B, both race-clean atomic loads, and the only way it can pull A-old with B-new
// is a writer that slipped between the two loads, which the version check before and after
// is exactly there to reject. A reader that validates and still sees the pair unequal has
// witnessed a cross-node tear the protocol was supposed to catch.
func TestOLCReadersNeverTear(t *testing.T) {
	var v version
	type content struct{ val int64 }
	var nodeA, nodeB atomic.Pointer[content]
	nodeA.Store(&content{0})
	nodeB.Store(&content{0})

	const writes = 5000
	const readers = 8

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Single writer: under the lock, publish a fresh equal pair into both nodes. The two
	// stores are not simultaneous, so a lock-free reader can straddle them; the version
	// bump on unlock is what turns that straddle into a failed validate.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= writes; i++ {
			v.writeLock()
			nodeA.Store(&content{i})
			nodeB.Store(&content{i})
			v.writeUnlock()
		}
		stop.Store(true)
	}()

	var tornSeen atomic.Int64
	var consistentReads atomic.Int64
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				w0, ok := v.readBegin()
				if !ok {
					runtime.Gosched()
					continue
				}
				a := nodeA.Load().val
				b := nodeB.Load().val
				if !v.validate(w0) {
					continue
				}
				if a != b {
					tornSeen.Add(1)
				}
				consistentReads.Add(1)
			}
		}()
	}

	wg.Wait()
	if tornSeen.Load() != 0 {
		t.Fatalf("optimistic readers observed %d cross-node torn states", tornSeen.Load())
	}
	if consistentReads.Load() == 0 {
		t.Fatal("no read ever validated; the test exercised nothing")
	}
}
