package db

import (
	"errors"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// This is the file-format fuzz harness (spec 23 §5): it feeds mutated and random bytes to Open as if
// they were a .kv file and holds the database to one contract under all of them, that it never
// panics, hangs, or over-reads, and that it either opens a coherent database or returns an error. A
// database's integrity checks (the header magic, the page checksums, the structural bounds) are the
// guardrails being exercised here, and the only acceptable response to a byte that violates one is a
// clean error, never a crash. The corpus seeds are retained under testdata/fuzz so every input that
// once crashed the opener stays a permanent regression test.

// materialize writes data into a fresh in-memory filesystem at the standard test path and returns the
// filesystem, so the fuzz body can hand Open a file whose every byte the fuzzer chose. The in-memory
// VFS keeps a fuzz iteration to a few allocations with no disk churn, which is what lets the harness
// run millions of inputs.
func materialize(tb testing.TB, data []byte) vfs.FS {
	tb.Helper()
	fs := vfs.NewMem()
	f, err := fs.Open("test.kv", vfs.OpenCreate|vfs.OpenReadWrite)
	if err != nil {
		tb.Fatalf("create: %v", err)
	}
	if len(data) > 0 {
		if _, err := f.WriteAt(data, 0); err != nil {
			f.Close()
			tb.Fatalf("write: %v", err)
		}
	}
	f.Close()
	return fs
}

// validDBBytes builds a small valid database in memory, closes it cleanly, and reads its main file
// back, so the corpus has at least one well-formed seed for the mutator to work outward from. A
// mutator that starts from a real file finds the interesting near-valid inputs (a flipped checksum
// bit, a truncated page, a corrupt length) far faster than one starting from noise.
func validDBBytes(tb testing.TB) []byte {
	tb.Helper()
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{})
	if err != nil {
		tb.Fatalf("seed open: %v", err)
	}
	for _, k := range []string{"alpha", "beta", "gamma", "delta"} {
		if err := d.Update(func(tx *Txn) error { return tx.Set([]byte(k), []byte("v-"+k)) }); err != nil {
			tb.Fatalf("seed write: %v", err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		tb.Fatalf("seed checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		tb.Fatalf("seed close: %v", err)
	}
	f, err := fs.Open("test.kv", vfs.OpenRead)
	if err != nil {
		tb.Fatalf("seed reopen: %v", err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		tb.Fatalf("seed size: %v", err)
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		tb.Fatalf("seed read: %v", err)
	}
	return buf
}

// exercise opens a database from the materialized bytes and, when the open succeeds, drives a few
// read and write operations across it before closing. The point is not that the operations succeed,
// since a structurally-valid-but-odd file may make any one of them fail; the point is that none of
// them panics, hangs, or reads out of bounds. An error is always an acceptable answer; a crash never
// is.
func exercise(tb testing.TB, fs vfs.FS) {
	tb.Helper()
	d, err := Open(fs, "test.kv", Options{})
	if err != nil {
		// A rejected file is the expected outcome for most corrupt inputs. The contract is that the
		// rejection is an error, which we are holding by reaching here rather than through a panic.
		return
	}
	defer d.Close()

	// A point read of a present-looking key and an absent one: both must resolve to a value or a clean
	// not-found, never a fault.
	for _, k := range []string{"alpha", "missing", ""} {
		_ = d.View(func(tx *Txn) error {
			if _, err := tx.Get([]byte(k)); err != nil && !errors.Is(err, engine.ErrNotFound) {
				return err
			}
			return nil
		})
	}

	// A full scan walks whatever structure the file describes; a corrupt interior that slipped past
	// open must surface as an iteration error, not an over-read.
	_ = d.View(func(tx *Txn) error {
		it, err := tx.NewIterator(engine.IterOptions{})
		if err != nil {
			return err
		}
		defer it.Close()
		n := 0
		for it.First(); it.Valid(); it.Next() {
			_ = it.Key()
			if _, err := it.Value(); err != nil {
				return err
			}
			// Bound the walk so a structure that somehow describes a cycle cannot hang the fuzzer.
			if n++; n > 1<<16 {
				break
			}
		}
		return it.Error()
	})

	// A write exercises the pager and WAL paths over the opened file.
	_ = d.Update(func(tx *Txn) error { return tx.Set([]byte("fuzz-write"), []byte("v")) })
}

func FuzzOpenFile(f *testing.F) {
	valid := validDBBytes(f)

	// A real file, so the mutator works outward from valid bytes toward the near-valid inputs that
	// exercise the integrity checks hardest.
	f.Add(valid)
	// Degenerate shapes the seed mutator would take a long time to reach on its own.
	f.Add([]byte(nil))
	f.Add(make([]byte, 4096)) // a page of zeros: the magic check should reject it
	if len(valid) > 64 {
		f.Add(valid[:64])           // truncated to a partial header
		f.Add(valid[:len(valid)/2]) // truncated mid-file
		// A valid file with one byte flipped deep inside, the canonical "checksum should catch this".
		flipped := append([]byte(nil), valid...)
		flipped[len(flipped)/2] ^= 0xff
		f.Add(flipped)
		// The magic preserved but a later byte zeroed, so the file passes the magic gate and fails a
		// deeper check.
		zeroed := append([]byte(nil), valid...)
		for i := 32; i < 96 && i < len(zeroed); i++ {
			zeroed[i] = 0
		}
		f.Add(zeroed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// The whole contract: opening and exercising arbitrary bytes never panics, hangs, or
		// over-reads. A returned error is always fine. A crash records the input as a permanent seed.
		exercise(t, materialize(t, data))
	})
}
