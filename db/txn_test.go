package db

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// openMem opens a fresh in-memory database for a test, failing on error.
func openMem(t *testing.T, opts Options) *DB {
	t.Helper()
	d, err := Open(vfs.NewMem(), "test.kv", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

// txnGet reads a key inside a View, returning "" and false when absent.
func txnGet(t *testing.T, d *DB, key string) (string, bool) {
	t.Helper()
	var val string
	var ok bool
	err := d.View(func(txn *Txn) error {
		v, err := txn.Get([]byte(key))
		if errors.Is(err, engine.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		val, ok = string(v), true
		return nil
	})
	if err != nil {
		t.Fatalf("view get %q: %v", key, err)
	}
	return val, ok
}

// TestUpdateViewRoundTrip writes through Update and reads it back through View.
func TestUpdateViewRoundTrip(t *testing.T) {
	d := openMem(t, Options{})
	if err := d.Update(func(txn *Txn) error {
		return txn.Set([]byte("a"), []byte("1"))
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if v, ok := txnGet(t, d, "a"); !ok || v != "1" {
		t.Fatalf("a = %q,%v, want 1", v, ok)
	}
	if _, ok := txnGet(t, d, "missing"); ok {
		t.Fatalf("missing key reported present")
	}
}

// TestReadYourWrites checks a write transaction sees its own buffered mutations:
// sets, an overlay over an existing value, a delete, and a re-set.
func TestReadYourWrites(t *testing.T) {
	d := openMem(t, Options{})
	d.Update(func(txn *Txn) error { return txn.Set([]byte("base"), []byte("v0")) })

	err := d.Update(func(txn *Txn) error {
		// A fresh buffered set is visible.
		txn.Set([]byte("k"), []byte("v1"))
		if v, _ := txn.Get([]byte("k")); string(v) != "v1" {
			t.Fatalf("own set k = %q, want v1", v)
		}
		// An overlay over a committed value is visible.
		txn.Set([]byte("base"), []byte("v2"))
		if v, _ := txn.Get([]byte("base")); string(v) != "v2" {
			t.Fatalf("own overwrite base = %q, want v2", v)
		}
		// A buffered delete hides it.
		txn.Delete([]byte("k"))
		if _, err := txn.Get([]byte("k")); !errors.Is(err, engine.ErrNotFound) {
			t.Fatalf("deleted k should be absent, got err %v", err)
		}
		// A re-set after a delete is visible again.
		txn.Set([]byte("k"), []byte("v3"))
		if v, _ := txn.Get([]byte("k")); string(v) != "v3" {
			t.Fatalf("re-set k = %q, want v3", v)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if v, ok := txnGet(t, d, "k"); !ok || v != "v3" {
		t.Fatalf("committed k = %q,%v, want v3", v, ok)
	}
	if v, ok := txnGet(t, d, "base"); !ok || v != "v2" {
		t.Fatalf("committed base = %q,%v, want v2", v, ok)
	}
}

// TestSnapshotIsolation checks a read transaction sees a stable snapshot even as a
// concurrent writer commits newer versions.
func TestSnapshotIsolation(t *testing.T) {
	d := openMem(t, Options{})
	d.Update(func(txn *Txn) error { return txn.Set([]byte("k"), []byte("v1")) })

	// Begin a reader at the v1 snapshot, then commit v2 underneath it.
	reader := d.Begin(false)
	defer reader.Discard()
	if v, _ := reader.Get([]byte("k")); string(v) != "v1" {
		t.Fatalf("reader sees %q at begin, want v1", v)
	}
	d.Update(func(txn *Txn) error { return txn.Set([]byte("k"), []byte("v2")) })

	// The reader's snapshot is unchanged; a fresh reader sees v2.
	if v, _ := reader.Get([]byte("k")); string(v) != "v1" {
		t.Fatalf("reader sees %q after concurrent commit, want stable v1", v)
	}
	if v, ok := txnGet(t, d, "k"); !ok || v != "v2" {
		t.Fatalf("fresh read = %q,%v, want v2", v, ok)
	}
}

// TestWriteWriteConflict checks that two explicit transactions reading the same
// snapshot and writing the same key serialize first-committer-wins: the second
// commit returns ErrConflict.
func TestWriteWriteConflict(t *testing.T) {
	d := openMem(t, Options{})
	d.Update(func(txn *Txn) error { return txn.Set([]byte("k"), []byte("v0")) })

	t1 := d.Begin(true)
	t2 := d.Begin(true)
	t1.Set([]byte("k"), []byte("from-t1"))
	t2.Set([]byte("k"), []byte("from-t2"))

	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}
	if err := t2.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("t2 commit = %v, want ErrConflict", err)
	}
	t2.Discard()

	if v, ok := txnGet(t, d, "k"); !ok || v != "from-t1" {
		t.Fatalf("k = %q,%v, want from-t1 (first committer wins)", v, ok)
	}
}

// TestDisjointWritesNoConflict checks two concurrent transactions writing different
// keys both commit.
func TestDisjointWritesNoConflict(t *testing.T) {
	d := openMem(t, Options{})
	t1 := d.Begin(true)
	t2 := d.Begin(true)
	t1.Set([]byte("a"), []byte("1"))
	t2.Set([]byte("b"), []byte("2"))
	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}
	if err := t2.Commit(); err != nil {
		t.Fatalf("t2 commit (disjoint) = %v, want success", err)
	}
	if v, ok := txnGet(t, d, "a"); !ok || v != "1" {
		t.Fatalf("a = %q,%v", v, ok)
	}
	if v, ok := txnGet(t, d, "b"); !ok || v != "2" {
		t.Fatalf("b = %q,%v", v, ok)
	}
}

// TestUpdateRetriesOnConflict drives many goroutines incrementing the same counter
// through Update; the auto-retry must serialize them so every increment lands.
func TestUpdateRetriesOnConflict(t *testing.T) {
	// Generous retry bound: under 8-way contention a writer may lose many races
	// before it wins, and the test proves retry converges without losing updates.
	d := openMem(t, Options{MaxRetries: 1000})
	d.Update(func(txn *Txn) error { return txn.Set([]byte("n"), []byte("0")) })

	const workers = 8
	const perWorker = 25
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				err := d.Update(func(txn *Txn) error {
					v, err := txn.Get([]byte("n"))
					if err != nil {
						return err
					}
					var n int
					fmt.Sscanf(string(v), "%d", &n)
					return txn.Set([]byte("n"), []byte(fmt.Sprintf("%d", n+1)))
				})
				if err != nil {
					errs[w] = err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	for w, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", w, err)
		}
	}
	want := workers * perWorker
	if v, ok := txnGet(t, d, "n"); !ok || v != fmt.Sprintf("%d", want) {
		t.Fatalf("n = %q,%v, want %d (no lost updates)", v, ok, want)
	}
}

// TestMaxRetriesExhausted checks Update gives up with ErrConflict when a contending
// writer keeps invalidating its snapshot past the retry bound.
func TestMaxRetriesExhausted(t *testing.T) {
	d := openMem(t, Options{MaxRetries: 2})
	d.Update(func(txn *Txn) error { return txn.Set([]byte("k"), []byte("v")) })

	attempts := 0
	err := d.Update(func(txn *Txn) error {
		attempts++
		// Read the key so we conflict, then a sibling writer bumps it before we
		// commit, forcing a conflict on every attempt.
		txn.Get([]byte("k"))
		d.Update(func(inner *Txn) error {
			return inner.Set([]byte("k"), []byte(fmt.Sprintf("bump%d", attempts)))
		})
		return txn.Set([]byte("k"), []byte("mine"))
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("update = %v, want ErrConflict after exhausting retries", err)
	}
	if attempts != 3 { // initial + 2 retries
		t.Fatalf("attempts = %d, want 3 (initial + MaxRetries)", attempts)
	}
}

// TestMergeFoldsLikeOracle checks the transaction read path folds a merge chain over
// a snapshot base the same way the engine does, including a merge over an absent key
// and a delete that resets the fold base.
func TestMergeFoldsLikeOracle(t *testing.T) {
	d := openMem(t, Options{Merge: concatMerge})

	// Merge over an absent key starts from the operand.
	d.Update(func(txn *Txn) error { return txn.Merge([]byte("k"), []byte("a")) })
	d.Update(func(txn *Txn) error { return txn.Merge([]byte("k"), []byte("b")) })
	if v, ok := txnGet(t, d, "k"); !ok || v != "ab" {
		t.Fatalf("k = %q,%v, want ab", v, ok)
	}

	// A set establishes a base the later merge folds over.
	d.Update(func(txn *Txn) error { return txn.Set([]byte("m"), []byte("base")) })
	d.Update(func(txn *Txn) error { return txn.Merge([]byte("m"), []byte("+x")) })
	if v, ok := txnGet(t, d, "m"); !ok || v != "base+x" {
		t.Fatalf("m = %q,%v, want base+x", v, ok)
	}

	// Read-your-writes folds buffered merges over the snapshot too.
	err := d.Update(func(txn *Txn) error {
		txn.Merge([]byte("k"), []byte("c"))
		if v, _ := txn.Get([]byte("k")); string(v) != "abc" {
			t.Fatalf("own merge k = %q, want abc", v)
		}
		txn.Delete([]byte("k"))
		txn.Merge([]byte("k"), []byte("z"))
		if v, _ := txn.Get([]byte("k")); string(v) != "z" {
			t.Fatalf("merge after delete k = %q, want z", v)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if v, ok := txnGet(t, d, "k"); !ok || v != "z" {
		t.Fatalf("committed k = %q,%v, want z", v, ok)
	}
}

// TestReadOnlyTxnRejectsWrites checks a View transaction cannot mutate.
func TestReadOnlyTxnRejectsWrites(t *testing.T) {
	d := openMem(t, Options{})
	err := d.View(func(txn *Txn) error {
		return txn.Set([]byte("k"), []byte("v"))
	})
	if !errors.Is(err, ErrReadOnlyTxn) {
		t.Fatalf("set on View = %v, want ErrReadOnlyTxn", err)
	}
}

// TestUseAfterFinish checks a transaction rejects use after Commit or Discard.
func TestUseAfterFinish(t *testing.T) {
	d := openMem(t, Options{})
	txn := d.Begin(true)
	txn.Set([]byte("k"), []byte("v"))
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := txn.Get([]byte("k")); !errors.Is(err, ErrTxnDone) {
		t.Fatalf("get after commit = %v, want ErrTxnDone", err)
	}
	txn.Discard() // must be a safe no-op
}
