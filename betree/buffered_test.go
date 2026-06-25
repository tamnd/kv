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

// This file is M1's check that the buffered write path resolves identically to the
// model no matter when the buffer flushes. A buffered message rests in an interior
// node until that node fills and a flush carries it down, so the value a key resolves
// to must not depend on where in the stream that flush happened. The fuzz target
// programs the stream and the page size from its corpus bytes, which moves the flush
// boundaries around, and the differential oracle (engine.CheckEngine) re-derives the
// expected value, scan order, scan contents, and visibility at every snapshot. A
// flush that dropped, reordered, or duplicated a message diverges from the model and
// fails the run. The companion table tests pin the specific orderings a flush most
// easily gets wrong: an overwrite split across a flush, a tombstone that flushes
// while its set stays buffered, and a range-delete marker buffered above the keys it
// covers.

// programToBatches turns a fuzz program into a committed batch stream and the page
// size to run it at. The first byte selects a small page (512, 1024, or 2048) so the
// buffer fills and flushes often; the rest is read as a sequence of batches, each a
// control byte (version bump and op count) followed by two bytes per op (operation
// and key). It honors the engine precondition the real commit path guarantees: one
// op per user key per batch, and range-delete markers keyed in a separate space from
// the point cells, so the stream is never a malformed batch the core is not built to
// take.
func programToBatches(prog []byte) (batches []*engine.WriteBatch, pageSize int) {
	sizes := []int{512, 1024, 2048}
	if len(prog) == 0 {
		return nil, sizes[0]
	}
	pageSize = sizes[int(prog[0])%len(sizes)]
	p := prog[1:]
	// Bound the work per exec so a giant mutated input cannot wedge a fuzz worker for
	// seconds: the oracle rescans after every batch, so an unbounded program turns one
	// exec into a long serial run and starves the rest. A kilobyte is hundreds of
	// batches, plenty to move the flush boundaries around.
	if len(p) > 1024 {
		p = p[:1024]
	}

	const keyspace = 24
	version := uint64(0)
	i := 0
	for i < len(p) {
		ctrl := p[i]
		i++
		version += uint64(1 + int(ctrl%5))
		nops := 1 + int((ctrl>>3)%6)
		b := engine.NewWriteBatch(version)
		used := map[string]bool{}
		any := false
		for o := 0; o < nops && i+1 < len(p); o++ {
			opb := p[i]
			keyb := p[i+1]
			i += 2
			if opb%10 == 0 {
				// Range delete over a small window. Its marker lives in the point-key
				// space here, but DeleteRange records it as a range-begin cell, which the
				// read rebuilds the interval from; it never collides with a point op.
				lo := int(keyb) % keyspace
				hi := lo + 1 + int(opb)%4
				b.DeleteRange([]byte(fmt.Sprintf("k%02d", lo)), []byte(fmt.Sprintf("k%02d", hi)))
				any = true
				continue
			}
			k := []byte(fmt.Sprintf("k%02d", int(keyb)%keyspace))
			if used[string(k)] {
				continue // one op per key per batch
			}
			used[string(k)] = true
			any = true
			// Vary the value length so leaves fill at different rates and the flush and
			// split boundaries land in different places across the stream.
			vlen := int(opb) % 48
			val := make([]byte, vlen)
			for j := range val {
				val[j] = byte('a' + (int(opb)+j)%26)
			}
			switch opb % 8 {
			case 0, 1:
				b.Delete(k)
			case 2, 3:
				b.Merge(k, val)
			default:
				b.Set(k, val)
			}
		}
		if any {
			batches = append(batches, b)
		}
	}
	return batches, pageSize
}

// openFuzzTree opens a core at a chosen page size over a fresh in-memory database.
func openFuzzTree(t *testing.T, pageSize int) *Tree {
	t.Helper()
	p, err := pager.Create(vfs.NewMem(), "fuzz.kv", pager.Options{
		PageSize:    pageSize,
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

// FuzzBufferedStream drives a byte-programmed mix of sets, deletes, merges, and range
// deletes through the buffered write path and the conformance oracle, at a page size
// the corpus chooses, so the buffer fills and flushes at timings the fuzz explores
// rather than ones the test fixes. The seed corpus alone runs under a plain go test,
// so a regression a past run found stays caught without -fuzz.
func FuzzBufferedStream(f *testing.F) {
	// A spread of seeds: empty, single ops, and longer pseudo-random programs at each
	// page-size selector so the corpus exercises all three from the first run.
	f.Add([]byte{})
	f.Add([]byte{0, 0x09, 0x04, 0x05})
	f.Add([]byte{1, 0xff, 0x02, 0x07, 0x13, 0x00, 0x09})
	for seed := int64(1); seed <= 6; seed++ {
		rng := rand.New(rand.NewSource(seed))
		prog := make([]byte, 1+rng.Intn(400))
		for i := range prog {
			prog[i] = byte(rng.Intn(256))
		}
		f.Add(prog)
	}

	f.Fuzz(func(t *testing.T, prog []byte) {
		batches, pageSize := programToBatches(prog)
		if len(batches) == 0 {
			return
		}
		tr := openFuzzTree(t, pageSize)
		if err := engine.CheckEngine(tr, batches, concatMerge); err != nil {
			t.Fatalf("buffered stream diverged (page %d): %v", pageSize, err)
		}
	})
}

// TestBufferedOverwriteAcrossFlush writes a key, then enough other keys at a tiny page
// to force the first key's interior buffer to flush down to its leaf, then overwrites
// it. The overwrite is a newer version that must win whether it is still buffered or
// already flushed, so this pins that a flush in the middle of a key's version history
// does not strand an older value as the answer.
func TestBufferedOverwriteAcrossFlush(t *testing.T) {
	tr := newTreeSized(t, vfs.NewMem(), 512)

	b1 := engine.NewWriteBatch(10)
	b1.Set([]byte("target"), []byte("old"))
	if err := tr.Apply(b1, 10); err != nil {
		t.Fatalf("apply v10: %v", err)
	}

	// Churn enough distinct keys to fill and flush the root buffer several times over.
	ver := uint64(10)
	for base := 0; base < 2000; base += 100 {
		ver++
		b := engine.NewWriteBatch(ver)
		for i := base; i < base+100; i++ {
			b.Set([]byte(fmt.Sprintf("filler%06d", i)), []byte(fmt.Sprintf("v%06d", i)))
		}
		if err := tr.Apply(b, ver); err != nil {
			t.Fatalf("apply filler at %d: %v", base, err)
		}
	}

	ver++
	b2 := engine.NewWriteBatch(ver)
	b2.Set([]byte("target"), []byte("new"))
	if err := tr.Apply(b2, ver); err != nil {
		t.Fatalf("apply overwrite: %v", err)
	}

	rd, err := tr.NewReader(engine.Snapshot{Version: ver})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	v, err := rd.Get([]byte("target"))
	if err != nil {
		t.Fatalf("get target: %v", err)
	}
	if string(v) != "new" {
		t.Fatalf("target = %q, want %q (overwrite lost across a flush)", v, "new")
	}

	// And the old value is still the answer at the older snapshot, so the flush did not
	// collapse the version history.
	rdOld, err := tr.NewReader(engine.Snapshot{Version: 10})
	if err != nil {
		t.Fatalf("old reader: %v", err)
	}
	defer rdOld.Close()
	if v, err := rdOld.Get([]byte("target")); err != nil || string(v) != "old" {
		t.Fatalf("target@10 = %q (err %v), want %q", v, err, "old")
	}
}

// TestBufferedTombstoneAndRangeDelete drives a set, a point delete, and a range delete
// of the same key region through a tiny-page core and the oracle, with enough other
// traffic that the marks and the sets they shadow land on different sides of flushes.
// It pins that a tombstone and a range-delete marker buffered above the keys they
// cover still shadow those keys after the buffer drains.
func TestBufferedTombstoneAndRangeDelete(t *testing.T) {
	tr := newTreeSized(t, vfs.NewMem(), 512)

	var batches []*engine.WriteBatch

	b1 := engine.NewWriteBatch(5)
	for i := 0; i < 40; i++ {
		b1.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("live"))
	}
	batches = append(batches, b1)

	b2 := engine.NewWriteBatch(10)
	b2.Delete([]byte("k001"))
	b2.DeleteRange([]byte("k010"), []byte("k020")) // covers k010..k019
	batches = append(batches, b2)

	b3 := engine.NewWriteBatch(15)
	b3.Set([]byte("k015"), []byte("reborn")) // a write newer than the range delete
	for i := 100; i < 160; i++ {
		b3.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("more")) // churn to force flushes
	}
	batches = append(batches, b3)

	if err := engine.CheckEngine(tr, batches, concatMerge); err != nil {
		t.Fatalf("conformance: %v", err)
	}
}
