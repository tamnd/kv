package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/format"
)

// TestLSMFlushesSegmentLargerThanPool pins the perf/05 Finding 2 fix: an LSM segment must
// flush under a buffer pool smaller than the segment. Before the fix writeSegment allocated
// and pinned every data page of a sealed memtable at once, so a segment of N pages demanded
// N simultaneous frames; once N passed the pool size the pager found every frame pinned and
// raised "buffer pool exhausted (all frames pinned)", and the database could not load a
// dataset larger than its memtable under a bounded pool. The writers now reserve page
// numbers up front and materialize one page at a time, so the in-flight pin count is one and
// the pool floor decouples from the memtable size.
//
// The test sets a tiny pool and a small memtable, writes enough keys to seal and flush
// several segments each many times the pool size, and checks every key reads back. A
// regression to the pin-everything path would fail the write with an exhaustion error long
// before the reads.
func TestLSMFlushesSegmentLargerThanPool(t *testing.T) {
	const (
		keys     = 6000
		perBatch = 100
		valSize  = 120
	)
	// 16 frames is 64 KiB of pool; the ~256 KiB memtable seals into a segment of dozens of
	// data pages plus index and filter pages, all far past 16, so the old path would exhaust.
	d := openMem(t, Options{
		Engine:         format.EngineLSM,
		CacheFrames:    16,
		MemtableSize:   256 << 10,
		AutoCheckpoint: -1,
	})

	val := make([]byte, valSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	key := func(i int) string { return fmt.Sprintf("key-%06d", i) }

	for base := 0; base < keys; base += perBatch {
		err := d.Update(func(txn *Txn) error {
			for i := base; i < base+perBatch && i < keys; i++ {
				if err := txn.Set([]byte(key(i)), val); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("update at %d (pool exhaustion regression?): %v", base, err)
		}
	}

	// A checkpoint folds the WAL through the same segment writers; it too must not exhaust.
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	for i := 0; i < keys; i++ {
		got, ok := txnGet(t, d, key(i))
		if !ok {
			t.Fatalf("key %s missing after flush under a tiny pool", key(i))
		}
		if got != string(val) {
			t.Fatalf("key %s = %q, want the %d-byte value", key(i), got, valSize)
		}
	}
}
