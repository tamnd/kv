package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// collectIter walks a general Iterator and returns the (key, value) pairs, copying every byte out
// since the iterator's bytes are owned but we compare against the zero-copy cursor's transient ones.
func collectIter(t *testing.T, txn *Txn, opts engine.IterOptions) [][2]string {
	t.Helper()
	it, err := txn.NewIterator(opts)
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()
	var out [][2]string
	for ok := it.First(); ok; ok = it.Next() {
		v, err := it.Value()
		if err != nil {
			t.Fatalf("iter value: %v", err)
		}
		out = append(out, [2]string{string(it.Key()), string(v)})
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	return out
}

// collectScan walks a ScanCursor and returns the pairs, copying each view out immediately (before
// the next advance) exactly as a well-behaved transient consumer must.
func collectScan(t *testing.T, txn *Txn, opts engine.IterOptions) [][2]string {
	t.Helper()
	sc, err := txn.NewScanCursor(opts)
	if err != nil {
		t.Fatalf("NewScanCursor: %v", err)
	}
	defer sc.Close()
	var out [][2]string
	for sc.Next() {
		out = append(out, [2]string{string(sc.Key()), string(sc.Value())})
	}
	if err := sc.Error(); err != nil {
		t.Fatalf("scan error: %v", err)
	}
	return out
}

// TestScanCursorMatchesIterator checks the zero-copy ScanCursor returns exactly the same resolved
// sequence as the general Iterator across both engines and the access shapes that exercise the
// version fold: overwrites, point deletes, a range delete, bounded ranges, and a prefix. It also
// runs enough keys to force the cursor across leaf and refill boundaries, which is where a stale
// zero-copy view would surface as a mismatch.
func TestScanCursorMatchesIterator(t *testing.T) {
	for _, eng := range []format.EngineKind{format.EngineBTree, format.EngineLSM} {
		t.Run(fmt.Sprintf("engine=%v", eng), func(t *testing.T) {
			fs := vfs.NewMem()
			d, err := Open(fs, "t.kv", Options{PageSize: 4096, Engine: eng, MemtableSize: 64 << 10, Sync: wal.SyncOff})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer d.Close()

			const keys = 2000
			for i := range keys {
				if _, err := d.Write(func(wb *engine.WriteBatch) {
					wb.Set([]byte(fmt.Sprintf("k%06d", i)), []byte(fmt.Sprintf("v%06d", i)))
				}); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			// Overwrite every fifth key to give it a second version, delete every seventh, and
			// range-delete a middle band, so the fold has real work and some keys resolve to absent.
			for i := 0; i < keys; i += 5 {
				if _, err := d.Write(func(wb *engine.WriteBatch) {
					wb.Set([]byte(fmt.Sprintf("k%06d", i)), []byte(fmt.Sprintf("V%06d", i)))
				}); err != nil {
					t.Fatalf("overwrite: %v", err)
				}
			}
			for i := 0; i < keys; i += 7 {
				if _, err := d.Write(func(wb *engine.WriteBatch) {
					wb.Delete([]byte(fmt.Sprintf("k%06d", i)))
				}); err != nil {
					t.Fatalf("delete: %v", err)
				}
			}
			if _, err := d.Write(func(wb *engine.WriteBatch) {
				wb.DeleteRange([]byte("k000500"), []byte("k000600"))
			}); err != nil {
				t.Fatalf("range delete: %v", err)
			}

			cases := []struct {
				name string
				opts engine.IterOptions
			}{
				{"full", engine.IterOptions{}},
				{"lower", engine.IterOptions{Lower: []byte("k001000")}},
				{"bounded", engine.IterOptions{Lower: []byte("k000300"), Upper: []byte("k000900")}},
				{"prefix", engine.IterOptions{Prefix: []byte("k0010")}},
				{"keysonly", engine.IterOptions{KeysOnly: true}},
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					if err := d.View(func(txn *Txn) error {
						want := collectIter(t, txn, c.opts)
						got := collectScan(t, txn, c.opts)
						if len(got) != len(want) {
							t.Fatalf("len mismatch: scan %d, iter %d", len(got), len(want))
						}
						for i := range want {
							if got[i] != want[i] {
								t.Fatalf("entry %d mismatch: scan %v, iter %v", i, got[i], want[i])
							}
						}
						return nil
					}); err != nil {
						t.Fatalf("view: %v", err)
					}
				})
			}
		})
	}
}

// TestScanCursorWriteTxnFallback checks the cursor falls back to the Iterator inside a write
// transaction, so it sees the transaction's own buffered writes (read-your-writes) rather than
// taking the zero-copy path, which cannot overlay the buffer.
func TestScanCursorWriteTxnFallback(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "t.kv", Options{PageSize: 4096, Sync: wal.SyncOff})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()
	for i := range 50 {
		if _, err := d.Write(func(wb *engine.WriteBatch) {
			wb.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("base"))
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if err := d.Update(func(txn *Txn) error {
		if err := txn.Set([]byte("k010"), []byte("overlay")); err != nil {
			return err
		}
		got := collectScan(t, txn, engine.IterOptions{Lower: []byte("k010"), Upper: []byte("k011")})
		if len(got) != 1 || got[0][1] != "overlay" {
			t.Fatalf("write-txn scan did not see buffered write: %v", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
}
