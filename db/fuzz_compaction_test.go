package db

import (
	"bytes"
	"testing"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
)

// This is the compaction fuzz harness (spec 23 §5). It is the operation fuzzer of the previous slice
// pointed at the LSM core and turned loose on the one subsystem the B-tree engine does not have: a
// background compaction that rewrites segments, merges overlapping versions of a key, and drops the
// versions a checkpoint's watermark has made unreachable. Compaction is where an LSM is most likely to
// lose data quietly, because it is the only path that deletes a key's older bytes on purpose, and a
// merge that resolves a key the wrong way or a garbage collector that drops a version still visible
// would surface as a read that disagrees with what was written. So this drives the same kind of
// operation program against an LSM database and a plain-map oracle, but forces flushes and compactions
// between transactions, and asserts after every one that a full scan still equals the model.
//
// Two knobs make compaction actually happen rather than sit idle. The memtable cap is set tiny, so a
// handful of writes spills the memtable to an on-disk segment and a short program builds the many
// overlapping segments compaction exists to merge. And a maintenance action between transactions drains
// the compaction backlog to completion, so the merge and the watermark version-GC run on real
// overlapping data and their output is checked against the model while the fuzzer still remembers the
// input that produced it.

func FuzzCompaction(f *testing.F) {
	// Seeds that already build overlapping versions of a few keys and then compact, so the corpus starts
	// from inputs that reach the merge path rather than waiting for the mutator to find it. The decoding
	// matches the operation fuzzer; the trailing structural byte selects maintain, checkpoint, or reopen.
	f.Add(bytes.Repeat([]byte{0, 1, 2}, 30))                   // many overwrites of one key, then it ages into segments
	f.Add([]byte{0, 1, 2, 0, 2, 3, 6, 1, 0, 4, 5, 6, 2})       // set, set, commit+maintain, set, commit+checkpoint
	f.Add([]byte{0, 5, 9, 2, 5, 6, 1, 0, 7, 8, 6, 3})          // set, delete, commit+maintain, set, commit+reopen
	f.Add([]byte{3, 0, 12, 6, 1, 0, 1, 2, 6, 1})               // delete-range, commit+maintain, set, commit+maintain
	f.Add([]byte{0, 1, 2, 6, 1, 0, 1, 9, 6, 1, 0, 1, 5, 6, 3}) // rewrite same key across three compactions then reopen

	f.Fuzz(func(t *testing.T, data []byte) {
		// MemtableSize tiny so writes spill to segments fast and compaction has overlapping segments to
		// merge; a sparse, never-flushing memtable would leave the whole compaction path untested. The
		// filesystem is built once and reused across reopen so the segments survive the round trip.
		fs := vfs.NewMem()
		d, err := Open(fs, "test.kv", Options{Engine: format.EngineLSM, MemtableSize: 256})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		closed := false
		defer func() {
			if !closed {
				d.Close()
			}
		}()

		committed := map[string]string{}
		cur := &opCursor{p: data}

		for {
			if cur.pos >= len(cur.p) {
				break
			}
			work := cloneModel(committed)
			post := byte(0)

			err := d.Update(func(txn *Txn) error {
				for {
					opc, ok := cur.next()
					if !ok {
						return nil
					}
					switch opc % 7 {
					case 0, 1: // set: key, value
						kb, ok1 := cur.next()
						vb, ok2 := cur.next()
						if !ok1 || !ok2 {
							return nil
						}
						k, v := fuzzKey(kb), fuzzVal(vb)
						if err := txn.Set(k, v); err != nil {
							t.Fatalf("set: %v", err)
						}
						work[string(k)] = string(v)
					case 2: // delete: key
						kb, ok := cur.next()
						if !ok {
							return nil
						}
						k := fuzzKey(kb)
						if err := txn.Delete(k); err != nil {
							t.Fatalf("delete: %v", err)
						}
						delete(work, string(k))
					case 3: // delete-range: lo, hi
						a, ok1 := cur.next()
						b, ok2 := cur.next()
						if !ok1 || !ok2 {
							return nil
						}
						lo, hi := orderedRange(a, b)
						if err := txn.DeleteRange(lo, hi); err != nil {
							t.Fatalf("delete-range: %v", err)
						}
						for k := range work {
							kb := []byte(k)
							if bytes.Compare(kb, lo) >= 0 && bytes.Compare(kb, hi) < 0 {
								delete(work, k)
							}
						}
					case 4: // get
						kb, ok := cur.next()
						if !ok {
							return nil
						}
						checkGet(t, txn, work, fuzzKey(kb))
					case 5: // formerly scan: consume the three operand bytes so the program decoding
						// stays aligned with the operation fuzzer. The range surface is gone.
						_, ok1 := cur.next()
						_, ok2 := cur.next()
						_, ok3 := cur.next()
						if !ok1 || !ok2 || !ok3 {
							return nil
						}
					case 6: // commit boundary: read the structural action
						p, ok := cur.next()
						if ok {
							post = p % 3
						}
						return nil
					}
				}
			})
			if err != nil {
				t.Fatalf("transaction commit: %v", err)
			}
			committed = work

			switch post {
			case 0: // maintain: drain the compaction backlog, then assert the merged result still matches
				drainCompaction(t, d)
				assertModel(t, d, committed)
			case 1: // checkpoint: fold the WAL and the flushed segments into the main file
				if err := d.Checkpoint(); err != nil {
					t.Fatalf("checkpoint: %v", err)
				}
				assertModel(t, d, committed)
			case 2: // reopen: close and recover, asserting the segments and the WAL tail rebuild the state
				if err := d.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
				d, err = Open(fs, "test.kv", Options{Engine: format.EngineLSM, MemtableSize: 256})
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				assertModel(t, d, committed)
			}
		}

		// Run compaction once more at the end so the final assertion is made against a fully merged tree,
		// then check the whole keyspace equals the model.
		drainCompaction(t, d)
		assertModel(t, d, committed)
	})
}

// drainCompaction runs maintenance until the engine reports no more work, so the merge and the
// watermark version-GC run to completion on whatever overlapping segments the program built. With no
// long-lived reader open, the oracle's read-mark sits at the latest commit, so GC is free to collapse
// every shadowed version of a key down to the one a reader would see, which is exactly the latest value
// the model holds. A correct collapse leaves the full scan equal to the model; a GC that dropped a
// visible version or a merge that picked the wrong one would not. The loop is bounded so a bug that
// keeps reporting work forever fails loudly rather than hanging.
func drainCompaction(t *testing.T, d *DB) {
	t.Helper()
	for i := 0; i < 64; i++ {
		rep, err := d.Maintain(64)
		if err != nil {
			t.Fatalf("maintain: %v", err)
		}
		if !rep.More {
			return
		}
	}
	t.Fatalf("compaction did not drain in 64 maintenance rounds")
}
