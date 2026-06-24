package db

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// This is the operation fuzz harness (spec 23 §5): it decodes the fuzzer's bytes into a program of
// database operations, runs that program against a real database and against a dead-simple in-memory
// model at the same time, and asserts the two agree at every observable step. Where FuzzOpenFile feeds
// garbage to the opener and asks only that nothing crashes, this feeds well-formed operations to a
// working database and asks the harder question: that the answers are correct. The model is a plain Go
// map with no MVCC, no pages, no WAL, just the obvious meaning of set, delete, delete-range, get, and
// scan. Any input where the database and the map disagree is a correctness bug, and the input is
// retained under testdata/fuzz as a permanent regression test.
//
// The execution is single-threaded and every transaction commits, so there are no write-write
// conflicts and the model needs no conflict logic: a transaction sees the committed state as of its
// begin plus its own buffered writes, which is exactly read-your-writes under snapshot isolation with
// one writer. That keeps the oracle trivial, which is the point, since an oracle with bugs of its own
// proves nothing.

// fuzzKeyspace bounds the key set so the fuzzer collides keys constantly: overwrites, deletes of live
// keys, and ranges that actually cover something are what exercise the overlay, the version chains,
// and the iterator merge, far more than a sparse keyspace of mostly-distinct keys would.
const fuzzKeyspace = 16

func fuzzKey(b byte) []byte { return []byte(fmt.Sprintf("k%02d", int(b)%fuzzKeyspace)) }
func fuzzVal(b byte) []byte { return []byte(fmt.Sprintf("v%02d", int(b)%100)) }

// orderedRange turns two key-selector bytes into a half-open [lo, hi) user-key range, ordering the
// pair so lo <= hi. lo == hi is an empty range, which is itself worth exercising.
func orderedRange(a, b byte) (lo, hi []byte) {
	lo, hi = fuzzKey(a), fuzzKey(b)
	if bytes.Compare(lo, hi) > 0 {
		lo, hi = hi, lo
	}
	return lo, hi
}

// opCursor reads an operation program out of the fuzzer's byte slice. A read past the end reports
// not-ok, which ends the program wherever it runs dry, so a truncated operand simply stops the run
// rather than failing it.
type opCursor struct {
	p   []byte
	pos int
}

func (c *opCursor) next() (byte, bool) {
	if c.pos >= len(c.p) {
		return 0, false
	}
	b := c.p[c.pos]
	c.pos++
	return b, true
}

// modelScan returns the model's keys in [lo, hi) as ordered key/value pairs, ascending or descending,
// the answer a correct iterator over the same bounds must reproduce exactly.
func modelScan(m map[string]string, lo, hi []byte, reverse bool) [][2]string {
	var out [][2]string
	for k, v := range m {
		kb := []byte(k)
		if lo != nil && bytes.Compare(kb, lo) < 0 {
			continue
		}
		if hi != nil && bytes.Compare(kb, hi) >= 0 {
			continue
		}
		out = append(out, [2]string{k, v})
	}
	sort.Slice(out, func(i, j int) bool {
		if reverse {
			return out[i][0] > out[j][0]
		}
		return out[i][0] < out[j][0]
	})
	return out
}

// cloneModel copies the committed map so a transaction can mutate its own view without touching the
// committed state until it commits.
func cloneModel(m map[string]string) map[string]string {
	c := make(map[string]string, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

// checkScan walks a real iterator over [lo, hi) in the given direction and asserts it yields exactly
// the model's keys and values in exactly the model's order. A bound that the engine reflects wrong, a
// buffered write the overlay drops, or a delete-range the iterator forgets all show up here as a
// mismatch.
func checkScan(t *testing.T, txn *Txn, work map[string]string, lo, hi []byte, reverse bool) {
	it, err := txn.NewIterator(engine.IterOptions{Lower: lo, Upper: hi, Reverse: reverse})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()

	want := modelScan(work, lo, hi, reverse)
	var got [][2]string
	// The iterator internalizes direction: with Reverse set, First positions on the highest key and
	// Next steps toward lower keys, so the forward idiom drives both directions.
	for it.First(); it.Valid(); it.Next() {
		v, err := it.Value()
		if err != nil {
			t.Fatalf("iterator value: %v", err)
		}
		got = append(got, [2]string{string(it.Key()), string(v)})
		if len(got) > 1<<12 {
			t.Fatalf("scan over a %d-key model yielded more than %d rows", len(want), 1<<12)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("scan [%s,%s) reverse=%v: got %d rows, model has %d\n got=%v\nwant=%v",
			lo, hi, reverse, len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scan [%s,%s) reverse=%v row %d: got %v, model has %v",
				lo, hi, reverse, i, got[i], want[i])
		}
	}

	// The zero-copy ScanCursor is a forward-only fast path; cross-check it against the same model on
	// the forward scans, so the fuzzer exercises its leaf-crossing, refill, and version fold against
	// every program the Iterator check sees. Its views are transient, so copy each before advancing.
	if reverse {
		return
	}
	sc, err := txn.NewScanCursor(engine.IterOptions{Lower: lo, Upper: hi})
	if err != nil {
		t.Fatalf("NewScanCursor: %v", err)
	}
	defer sc.Close()
	var scgot [][2]string
	for sc.Next() {
		scgot = append(scgot, [2]string{string(sc.Key()), string(sc.Value())})
	}
	if err := sc.Error(); err != nil {
		t.Fatalf("scan cursor [%s,%s): %v", lo, hi, err)
	}
	if len(scgot) != len(want) {
		t.Fatalf("scan cursor [%s,%s): got %d rows, model has %d\n got=%v\nwant=%v",
			lo, hi, len(scgot), len(want), scgot, want)
	}
	for i := range want {
		if scgot[i] != want[i] {
			t.Fatalf("scan cursor [%s,%s) row %d: got %v, model has %v", lo, hi, i, scgot[i], want[i])
		}
	}
}

// checkGet asserts a point read returns exactly what the model holds for the key, value or clean
// not-found.
func checkGet(t *testing.T, txn *Txn, work map[string]string, k []byte) {
	got, err := txn.Get(k)
	want, present := work[string(k)]
	switch {
	case err == nil && !present:
		t.Fatalf("get %s returned %q, model has it absent", k, got)
	case err != nil && present:
		t.Fatalf("get %s errored %v, model has %q", k, err, want)
	case err == nil && present && string(got) != want:
		t.Fatalf("get %s returned %q, model has %q", k, got, want)
	case err != nil && err.Error() != engine.ErrNotFound.Error():
		t.Fatalf("get %s: unexpected error %v", k, err)
	}
}

func FuzzOps(f *testing.F) {
	// A few hand-written programs so the corpus starts from inputs that already touch every op, rather
	// than waiting for the mutator to discover the opcodes. Each byte is opcode-then-operands; the exact
	// decoding is in the fuzz body, so these are just plausible starting points the mutator works from.
	f.Add([]byte{0, 1, 2, 0, 3, 3, 5, 0, 15, 0, 6, 0}) // set, set, get, scan, commit
	f.Add([]byte{0, 5, 9, 1, 5, 3, 5, 0, 9, 1, 6, 1})  // set, delete, scan reverse, commit+checkpoint
	f.Add([]byte{0, 2, 7, 3, 0, 12, 4, 2, 4, 7, 6, 2}) // set, delete-range, get, get, commit+reopen
	f.Add(bytes.Repeat([]byte{0, 1, 2}, 40))           // many overwrites of the same few keys
	f.Add([]byte{6, 6, 6, 6})                          // empty transactions
	f.Add([]byte{3, 0, 4, 1, 5, 0, 15, 0})             // reads against an empty database

	f.Fuzz(func(t *testing.T, data []byte) {
		fs := vfs.NewMem()
		d, err := Open(fs, "test.kv", Options{})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		closed := false
		defer func() {
			if !closed {
				d.Close()
			}
		}()

		// committed is the model's view of the durable state; every committed transaction folds into it.
		committed := map[string]string{}
		cur := &opCursor{p: data}

		// Each outer iteration is one transaction: decode and run its operations inside an Update closure,
		// then, on commit, replace the committed model with the transaction's working view. Update commits
		// by returning nil, which it always does here, so every transaction lands.
		for {
			if cur.pos >= len(cur.p) {
				break
			}
			work := cloneModel(committed)
			post := byte(0) // structural action to run after this transaction commits

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
					case 4: // get: key (read-your-writes check)
						kb, ok := cur.next()
						if !ok {
							return nil
						}
						checkGet(t, txn, work, fuzzKey(kb))
					case 5: // scan: lo, hi, direction flag
						a, ok1 := cur.next()
						b, ok2 := cur.next()
						fl, ok3 := cur.next()
						if !ok1 || !ok2 || !ok3 {
							return nil
						}
						lo, hi := orderedRange(a, b)
						// A flag bit drops the bounds so the unbounded full-scan path is exercised too.
						if fl&0x4 != 0 {
							lo = nil
						}
						if fl&0x8 != 0 {
							hi = nil
						}
						checkScan(t, txn, work, lo, hi, fl&0x1 != 0)
					case 6: // commit boundary: end the transaction, read the structural action
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
			case 1: // checkpoint: fold the WAL into the main file, then keep going
				if err := d.Checkpoint(); err != nil {
					t.Fatalf("checkpoint: %v", err)
				}
			case 2: // reopen: close and recover, asserting the committed state survived the round trip
				if err := d.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
				d, err = Open(fs, "test.kv", Options{})
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				assertFullScan(t, d, committed)
			}
		}

		// The whole program has run; the durable state must equal the model exactly.
		assertFullScan(t, d, committed)
	})
}

// assertFullScan opens a read transaction, walks the entire keyspace, and asserts it equals the model
// key for key. It is the end-to-end check that the database and the map describe the same set of
// key/value pairs, run after every reopen and once at the end.
func assertFullScan(t *testing.T, d *DB, committed map[string]string) {
	t.Helper()
	err := d.View(func(txn *Txn) error {
		checkScan(t, txn, committed, nil, nil, false)
		return nil
	})
	if err != nil {
		t.Fatalf("full-scan view: %v", err)
	}
}
