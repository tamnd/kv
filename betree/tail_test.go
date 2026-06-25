package betree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// This file pins the mutable hot tail's own mechanics, below the conformance oracle
// that already proves the resolved answer does not move: the in-place overwrite that
// collapses an exact-internal-key rewrite to one slot, the durable mark the host reads
// to keep the WAL above the un-rolled tail, and the Flush that drains the tail onto
// pages before a checkpoint that must stand alone.

// TestTailInPlaceOverwrite replays the same internal key with a new value and asserts
// the tail keeps one slot rather than appending a second, and that the later value
// wins. This is the idempotent-replay and same-version-overwrite collapse, the only
// collapse safe without a GC watermark; distinct versions of a user key carry distinct
// internal keys and are checked to take their own slots.
func TestTailInPlaceOverwrite(t *testing.T) {
	tr := newTree(t)

	ik := format.EncodeInternalKey([]byte("hot"), 1, format.KindSet) // reused verbatim
	for i := 0; i < 50; i++ {
		if err := tr.tailPut(ik, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("tailPut %d: %v", i, err)
		}
	}
	if got := len(tr.tail); got != 1 {
		t.Fatalf("tail holds %d slots after 50 rewrites of one internal key, want 1", got)
	}
	if got := string(tr.tail[string(ik)].val); got != "v49" {
		t.Fatalf("overwritten slot = %q, want the last write v49", got)
	}

	// A different version of the same user key is a different internal key and must not
	// collapse into the slot above.
	ik2 := format.EncodeInternalKey([]byte("hot"), 2, format.KindSet)
	if err := tr.tailPut(ik2, []byte("other")); err != nil {
		t.Fatalf("tailPut second version: %v", err)
	}
	if got := len(tr.tail); got != 2 {
		t.Fatalf("tail holds %d slots after a second version, want 2 (no cross-version collapse)", got)
	}
}

// TestTailDurableMark walks the durable mark through a flush and a following lag. With
// no NoteLSN and nothing flushed it reports zero (the direct-pager driver reaches
// durability through Flush, not the mark). After a batch is applied at an LSN and the
// tail is flushed, the mark is that LSN. A later batch left un-rolled in the tail does
// not advance the mark past the last flush, so the host keeps the WAL above the
// un-rolled write; a final flush advances the mark to the latest applied LSN.
func TestTailDurableMark(t *testing.T) {
	tr := newTree(t)

	if got := tr.DurableLSN(); got != 0 {
		t.Fatalf("DurableLSN on a fresh core = %d, want 0", got)
	}

	// Apply a batch at LSN 10 and flush it onto pages: the durable mark is now 10.
	tr.NoteLSN(10)
	b1 := engine.NewWriteBatch(10)
	b1.Set([]byte("a"), []byte("1"))
	if err := tr.Apply(b1, 10); err != nil {
		t.Fatalf("apply b1: %v", err)
	}
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush b1: %v", err)
	}
	if got := tr.DurableLSN(); got != 10 {
		t.Fatalf("DurableLSN after flushing LSN 10 = %d, want 10", got)
	}

	// Apply a batch at LSN 20 and leave it in the tail: the mark must not advance past
	// the last flush at 10, because LSN 20 lives only in the tail and the WAL.
	tr.NoteLSN(20)
	b2 := engine.NewWriteBatch(20)
	b2.Set([]byte("b"), []byte("2"))
	if err := tr.Apply(b2, 20); err != nil {
		t.Fatalf("apply b2: %v", err)
	}
	if got := tr.DurableLSN(); got != 10 {
		t.Fatalf("DurableLSN with LSN 20 un-rolled in the tail = %d, want 10 (WAL kept above the tail)", got)
	}

	// Flushing the tail moves the mark to the latest applied LSN.
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush b2: %v", err)
	}
	if got := tr.DurableLSN(); got != 20 {
		t.Fatalf("DurableLSN after flushing LSN 20 = %d, want 20", got)
	}
}

// TestFlushEmptyTailNoop pins that Flush on an empty tail does nothing and does not
// fault, so the host can call it unconditionally before a checkpoint.
func TestFlushEmptyTailNoop(t *testing.T) {
	tr := newTree(t)
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush empty tail: %v", err)
	}
	if err := tr.Flush(); err != nil {
		t.Fatalf("second flush empty tail: %v", err)
	}
}

// TestFlushIsReadStable checks the read answer does not move across a Flush: a key
// resolves to the same value whether it is read out of the tail or after the tail has
// rolled onto pages, which is the in-tree restatement of the correctness lever.
func TestFlushIsReadStable(t *testing.T) {
	tr := newTree(t)

	b := engine.NewWriteBatch(5)
	for i := 0; i < 30; i++ {
		b.Set([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i)))
	}
	if err := tr.Apply(b, 5); err != nil {
		t.Fatalf("apply: %v", err)
	}

	read := func(stage string) {
		rd, err := tr.NewReader(engine.Snapshot{Version: 5})
		if err != nil {
			t.Fatalf("%s reader: %v", stage, err)
		}
		defer rd.Close()
		for i := 0; i < 30; i++ {
			k := []byte(fmt.Sprintf("k%03d", i))
			v, err := rd.Get(k)
			if err != nil {
				t.Fatalf("%s get %q: %v", stage, k, err)
			}
			if want := fmt.Sprintf("v%03d", i); string(v) != want {
				t.Fatalf("%s key %q = %q, want %q", stage, k, v, want)
			}
		}
	}

	read("from tail")
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(tr.tail) != 0 {
		t.Fatalf("tail not empty after flush: %d slots", len(tr.tail))
	}
	read("after flush")
}
