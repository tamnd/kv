package lsm

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// BenchmarkMemtableApply isolates the apply phase that the parallel group apply changes: it
// pre-builds a group of entries with distinct internal keys (versions differ across batches,
// the same precondition the parallel path relies on) and times only the insert into a fresh
// memtable, excluding the memtable allocation. The serial sub-benchmark inserts on one
// goroutine the way Apply per batch did; the parallel sub-benchmark spreads the inserts the
// way ApplyGroup does. The gap between them is the slice's win, with the WAL encode and key
// generation that dominate the end-to-end commit path stripped away so the apply CPU shows
// through. b.N counts entries, so ns/op is the per-entry apply cost (perf/03 W1, perf/07).
func BenchmarkMemtableApply(b *testing.B) {
	val := []byte("value-payload-1234567890")
	// One large group per apply: enough entries to be well past the parallel threshold and
	// deep enough that a skip-list descent does real CompareInternal work.
	const group = 8192
	batches := buildGroup(group, val)

	b.Run("serial", func(b *testing.B) {
		benchApply(b, group, func(mem *memtable) {
			for _, bt := range batches {
				for _, e := range bt.Entries() {
					mem.set(e.InternalKey, e.Value)
				}
			}
		})
	})
	b.Run("parallel", func(b *testing.B) {
		benchApply(b, group, func(mem *memtable) {
			applyEntriesParallel(mem, batches, group)
		})
	})
}

// benchApply runs apply over fresh memtables until b.N entries have been inserted, charging
// only the apply to the timer. Each apply fills one memtable with `group` entries; the
// memtable is rebuilt off the clock so its allocation never enters the measurement.
func benchApply(b *testing.B, group int, apply func(*memtable)) {
	b.ResetTimer()
	done := 0
	for done < b.N {
		b.StopTimer()
		mem := newMemtable(group * 64)
		b.StartTimer()
		apply(mem)
		done += group
	}
}

// buildGroup returns a group of single-batch write batches whose entries carry distinct
// internal keys across the whole group. Each batch gets its own version so no two entries
// share an internal key, matching the parallel-apply precondition; keys are shuffled so the
// inserts hit splices all over the keyspace rather than appending in order.
func buildGroup(total int, val []byte) []*engine.WriteBatch {
	const perBatch = 256
	var batches []*engine.WriteBatch
	var cur *engine.WriteBatch
	for i := 0; i < total; i++ {
		if i%perBatch == 0 {
			cur = engine.NewWriteBatch(uint64(i/perBatch) + 1)
			batches = append(batches, cur)
		}
		k := (i * 2654435761) % total
		cur.Add(format.EncodeInternalKey([]byte(fmt.Sprintf("key%010d", k)), cur.Version(), format.KindSet), val)
	}
	return batches
}
