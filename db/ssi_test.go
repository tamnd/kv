package db

import (
	"errors"
	"sync"
	"testing"
)

// These tests cover serializable snapshot isolation (spec 10 §4): the opt-in level
// that adds commit-time read-set validation to close write skew and the other SI
// anomalies. The companion TestWriteSkewAllowedUnderSI in linearizability_test.go
// shows the same scenario committing under the default snapshot isolation, so the two
// together pin the boundary the Isolation option moves.

// openSerializable opens a memory database whose transactions all run at Serializable.
func openSerializable(t *testing.T) *DB {
	t.Helper()
	return openMem(t, Options{Isolation: Serializable})
}

// TestWriteSkewAbortsUnderSSI is the headline: the exact write-skew schedule that both
// commits under SI must lose a committer under SSI. Two transactions read x and y, then
// write disjoint keys; the second to commit read a key the first wrote, a rw-
// antidependency the read-set validation catches.
func TestWriteSkewAbortsUnderSSI(t *testing.T) {
	d := openSerializable(t)
	if err := d.Update(func(txn *Txn) error {
		txn.Set([]byte("x"), []byte("1"))
		txn.Set([]byte("y"), []byte("1"))
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	t1 := d.Begin(true)
	t2 := d.Begin(true)
	for _, k := range []string{"x", "y"} {
		if _, err := t1.Get([]byte(k)); err != nil {
			t.Fatalf("t1 read %s: %v", k, err)
		}
		if _, err := t2.Get([]byte(k)); err != nil {
			t.Fatalf("t2 read %s: %v", k, err)
		}
	}
	t1.Set([]byte("x"), []byte("0"))
	t2.Set([]byte("y"), []byte("0"))

	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}
	// t2 read x, which t1 just wrote at a version above t2's snapshot: the antidependency
	// aborts it, the abort SI would not make.
	if err := t2.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("t2 commit = %v, want ErrConflict (SSI must abort write skew)", err)
	}
	t2.Discard()

	// Only t1's write took effect, so the joint-invariant violation never happened.
	if vx, _ := txnGet(t, d, "x"); vx != "0" {
		t.Fatalf("x = %q, want 0", vx)
	}
	if vy, _ := txnGet(t, d, "y"); vy != "1" {
		t.Fatalf("y = %q, want 1 (t2 aborted)", vy)
	}
}

// TestSSIDisjointReadsNoConflict checks SSI does not over-abort beyond its rw-
// antidependency rule: two serializable transactions that read and write entirely
// separate keys both commit, exactly as under SI.
func TestSSIDisjointReadsNoConflict(t *testing.T) {
	d := openSerializable(t)
	seedRange(t, d, 4) // k00..k03

	t1 := d.Begin(true)
	t2 := d.Begin(true)
	if _, err := t1.Get([]byte("k00")); err != nil {
		t.Fatalf("t1 read: %v", err)
	}
	if _, err := t2.Get([]byte("k02")); err != nil {
		t.Fatalf("t2 read: %v", err)
	}
	t1.Set([]byte("k01"), []byte("a"))
	t2.Set([]byte("k03"), []byte("b"))

	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}
	if err := t2.Commit(); err != nil {
		t.Fatalf("t2 commit = %v, want success (disjoint read/write sets)", err)
	}
}

// TestSSIReadOnlyNeverAborts checks a read-only serializable transaction commits
// trivially: it tracks no reads and never validates, so concurrent writes to keys it
// read do not abort it (it serializes at its snapshot).
func TestSSIReadOnlyNeverAborts(t *testing.T) {
	d := openSerializable(t)
	putN(t, d, "k", "v1")

	reader := d.Begin(false)
	if _, err := reader.Get([]byte("k")); err != nil {
		t.Fatalf("reader get: %v", err)
	}
	// A writer modifies the very key the reader read, and commits.
	putN(t, d, "k", "v2")

	// The read-only transaction still commits (and saw its stable snapshot throughout).
	if v, err := reader.Get([]byte("k")); err != nil || string(v) != "v1" {
		t.Fatalf("reader re-read = %q,%v, want v1 (stable snapshot)", v, err)
	}
	if err := reader.Commit(); err != nil {
		t.Fatalf("read-only commit = %v, want success", err)
	}
}

// TestSSIUpdateRetriesResolveSkew checks the managed Update path: two goroutines run
// the classic write-skew closure under SSI, and the retry loop drives both to a
// serializable outcome. One commits, the loser retries against the winner's snapshot,
// re-reads, and produces the result a serial order would: the invariant holds.
func TestSSIUpdateRetriesResolveSkew(t *testing.T) {
	d := openMem(t, Options{Isolation: Serializable, MaxRetries: 100})
	if err := d.Update(func(txn *Txn) error {
		txn.Set([]byte("on1"), []byte("1"))
		txn.Set([]byte("on2"), []byte("1"))
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Invariant: at least one of on1/on2 stays 1. Each worker turns its own switch off
	// only if the other is on. Under SI this skews to both 0; under SSI the retry forces
	// a serial order, so the second worker re-reads the first's commit and backs off.
	turnOff := func(mine, other string) {
		_ = d.Update(func(txn *Txn) error {
			o, err := txn.Get([]byte(other))
			if err != nil {
				return err
			}
			if string(o) == "1" {
				return txn.Set([]byte(mine), []byte("0"))
			}
			return nil // other already off: leave mine on to keep the invariant
		})
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); turnOff("on1", "on2") }()
	go func() { defer wg.Done(); turnOff("on2", "on1") }()
	wg.Wait()

	v1, _ := txnGet(t, d, "on1")
	v2, _ := txnGet(t, d, "on2")
	if v1 == "0" && v2 == "0" {
		t.Fatalf("both switches off: invariant violated, SSI failed to serialize")
	}
}
