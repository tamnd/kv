package db

import (
	"errors"
	"fmt"
	"testing"
)

// TestWriteBatchChunksAndCommits checks the builder commits in bounded chunks: with a chunk
// size of 4 and 10 sets, the auto-flushes plus the Close flush land every key, and Count
// reports every operation issued.
func TestWriteBatchChunksAndCommits(t *testing.T) {
	d := openMem(t, Options{})
	wb := d.NewWriteBatch(4)

	const n = 10
	for i := 0; i < n; i++ {
		if err := wb.Set([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	// After 10 sets at a chunk size of 4, two full chunks have auto-flushed and two ops
	// remain buffered.
	if got := wb.Pending(); got != 2 {
		t.Fatalf("pending = %d, want 2 before close", got)
	}
	if err := wb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := wb.Count(); got != n {
		t.Fatalf("count = %d, want %d", got, n)
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%02d", i)
		if v, ok := txnGet(t, d, key); !ok || v != fmt.Sprintf("v%d", i) {
			t.Fatalf("%s = %q,%v, want v%d", key, v, ok, i)
		}
	}
}

// TestWriteBatchLastWriteWinsInChunk checks that within one chunk the final op for a key
// wins, so a Set then Delete of the same key before any flush resolves to absent.
func TestWriteBatchLastWriteWinsInChunk(t *testing.T) {
	d := openMem(t, Options{})
	wb := d.NewWriteBatch(100) // large enough that nothing auto-flushes mid-test

	if err := wb.Set([]byte("k"), []byte("first")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := wb.Set([]byte("k"), []byte("second")); err != nil {
		t.Fatalf("set again: %v", err)
	}
	if err := wb.Delete([]byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := wb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, ok := txnGet(t, d, "k"); ok {
		t.Fatalf("k present after Set,Set,Delete in one chunk, want absent")
	}
}

// TestWriteBatchClonesInput checks the builder copies the caller's slices, so reusing one
// scratch buffer across many Set calls before a flush does not corrupt buffered keys.
func TestWriteBatchClonesInput(t *testing.T) {
	d := openMem(t, Options{})
	wb := d.NewWriteBatch(100)

	scratch := make([]byte, 2)
	for i := 0; i < 5; i++ {
		scratch[0] = 'k'
		scratch[1] = byte('0' + i)
		if err := wb.Set(scratch, []byte{byte('0' + i)}); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := wb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("k%d", i)
		if v, ok := txnGet(t, d, key); !ok || v != fmt.Sprintf("%d", i) {
			t.Fatalf("%s = %q,%v, want %d", key, v, ok, i)
		}
	}
}

// TestWriteBatchClosedRejects checks Close is idempotent and that any op after Close returns
// ErrBatchClosed rather than silently succeeding.
func TestWriteBatchClosedRejects(t *testing.T) {
	d := openMem(t, Options{})
	wb := d.NewWriteBatch(10)
	if err := wb.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := wb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := wb.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := wb.Set([]byte("x"), []byte("y")); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("set after close = %v, want ErrBatchClosed", err)
	}
	if err := wb.Flush(); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("flush after close = %v, want ErrBatchClosed", err)
	}
}

// TestWriteBatchEmptyClose checks closing an untouched batch is a clean no-op.
func TestWriteBatchEmptyClose(t *testing.T) {
	d := openMem(t, Options{})
	wb := d.NewWriteBatch(0) // zero selects the default chunk size
	if err := wb.Close(); err != nil {
		t.Fatalf("close empty: %v", err)
	}
	if got := wb.Count(); got != 0 {
		t.Fatalf("count = %d on empty batch, want 0", got)
	}
}
