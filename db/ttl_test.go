package db

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// testClock is a controllable wall clock for TTL tests: the test moves time forward
// explicitly so expiry is deterministic rather than racing real time. It reads under
// the database read lock from the test goroutine, but is stored atomically so the race
// detector stays quiet if a background task ever reads it.
type testClock struct{ ns atomic.Uint64 }

func (c *testClock) now() uint64   { return c.ns.Load() }
func (c *testClock) set(ns uint64) { c.ns.Store(ns) }

// openMemClock opens a fresh in-memory database whose TTL clock is the given testClock,
// so the test drives expiry by hand.
func openMemClock(t *testing.T, clk *testClock, opts Options) *DB {
	t.Helper()
	opts.Clock = clk.now
	d, err := Open(vfs.NewMem(), "test.kv", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

// setTTL commits a single TTL set at an absolute expiry through an Update transaction.
func setTTL(t *testing.T, d *DB, key, value string, expiry uint64) {
	t.Helper()
	if err := d.Update(func(txn *Txn) error {
		return txn.SetWithTTL([]byte(key), []byte(value), expiry)
	}); err != nil {
		t.Fatalf("set-ttl %q: %v", key, err)
	}
}

// TestTTLExpiresAtDeadline checks a TTL set reads present strictly before its deadline
// and absent at or after it, the half-open expiry boundary of spec 15 §6.
func TestTTLExpiresAtDeadline(t *testing.T) {
	clk := &testClock{}
	clk.set(50)
	d := openMemClock(t, clk, Options{})

	setTTL(t, d, "k", "v", 100)

	clk.set(99)
	if v, ok := txnGet(t, d, "k"); !ok || v != "v" {
		t.Fatalf("before deadline k = %q,%v, want v", v, ok)
	}
	clk.set(100)
	if _, ok := txnGet(t, d, "k"); ok {
		t.Fatalf("at deadline k still present")
	}
	clk.set(1000)
	if _, ok := txnGet(t, d, "k"); ok {
		t.Fatalf("past deadline k still present")
	}
}

// TestTTLZeroNeverExpires checks a TTL set with a zero expiry behaves like a plain set:
// it never expires no matter how far the clock advances.
func TestTTLZeroNeverExpires(t *testing.T) {
	clk := &testClock{}
	d := openMemClock(t, clk, Options{})

	setTTL(t, d, "k", "v", 0)

	clk.set(^uint64(0) - 1)
	if v, ok := txnGet(t, d, "k"); !ok || v != "v" {
		t.Fatalf("zero-expiry k = %q,%v, want v", v, ok)
	}
}

// TestTTLPlainSetClearsExpiry checks a later plain Set over a TTL key drops the expiry:
// the key then survives past the original deadline.
func TestTTLPlainSetClearsExpiry(t *testing.T) {
	clk := &testClock{}
	clk.set(10)
	d := openMemClock(t, clk, Options{})

	setTTL(t, d, "k", "v", 100)
	if err := d.Update(func(txn *Txn) error {
		return txn.Set([]byte("k"), []byte("plain"))
	}); err != nil {
		t.Fatalf("plain set: %v", err)
	}

	clk.set(500)
	if v, ok := txnGet(t, d, "k"); !ok || v != "plain" {
		t.Fatalf("after plain overwrite k = %q,%v, want plain", v, ok)
	}
}

// TestTTLReadYourWrites checks a transaction sees its own buffered TTL set, and a
// plain set buffered after it in the same transaction clears the expiry for its own
// reads.
func TestTTLReadYourWrites(t *testing.T) {
	clk := &testClock{}
	clk.set(10)
	d := openMemClock(t, clk, Options{})

	err := d.Update(func(txn *Txn) error {
		if err := txn.SetWithTTL([]byte("k"), []byte("v"), 100); err != nil {
			return err
		}
		if v, err := txn.Get([]byte("k")); err != nil || string(v) != "v" {
			t.Fatalf("own ttl set k = %q,%v, want v", v, err)
		}
		// A buffered TTL set whose deadline has already passed reads absent, the same as
		// a committed one.
		if err := txn.SetWithTTL([]byte("past"), []byte("x"), 5); err != nil {
			return err
		}
		if _, err := txn.Get([]byte("past")); !errors.Is(err, engine.ErrNotFound) {
			t.Fatalf("expired buffered ttl set still visible: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
}

// TestTTLScanExcludesExpired checks an iterator skips a key whose TTL has lapsed while
// still returning its live neighbors.
func TestTTLScanExcludesExpired(t *testing.T) {
	clk := &testClock{}
	clk.set(10)
	d := openMemClock(t, clk, Options{})

	setTTL(t, d, "a", "1", 0)   // never expires
	setTTL(t, d, "b", "2", 100) // expires at 100
	setTTL(t, d, "c", "3", 0)   // never expires

	clk.set(200)
	got := map[string]string{}
	err := d.View(func(txn *Txn) error {
		it, err := txn.NewIterator(engine.IterOptions{})
		if err != nil {
			return err
		}
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
			v, err := it.Value()
			if err != nil {
				return err
			}
			got[string(it.Key())] = string(v)
		}
		return it.Error()
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 2 || got["a"] != "1" || got["c"] != "3" {
		t.Fatalf("scan after expiry = %v, want a=1 c=3 only", got)
	}
	if _, ok := got["b"]; ok {
		t.Fatalf("expired key b present in scan")
	}
}

// TestTTLSweepReclaims checks the db-level maintenance sweep reclaims an expired TTL
// key's value through the database clock: after the clock advances past the deadline, a
// Maintain pass reports the key swept and bytes reclaimed, the key stays absent, and a
// still-live TTL key survives.
func TestTTLSweepReclaims(t *testing.T) {
	clk := &testClock{}
	clk.set(10)
	d := openMemClock(t, clk, Options{})

	setTTL(t, d, "dead", "gone", 100)
	setTTL(t, d, "live", "stay", 1000)

	clk.set(200)
	rep, err := d.Maintain(0)
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if rep.ExpiredSwept != 1 {
		t.Fatalf("swept = %d, want 1", rep.ExpiredSwept)
	}
	if rep.BytesReclaimed <= 0 {
		t.Fatalf("bytes reclaimed = %d, want > 0", rep.BytesReclaimed)
	}

	if _, ok := txnGet(t, d, "dead"); ok {
		t.Fatalf("swept key still present")
	}
	if v, ok := txnGet(t, d, "live"); !ok || v != "stay" {
		t.Fatalf("live ttl key = %q,%v, want stay", v, ok)
	}
}

// TestTTLPersistsAcrossReopen checks the absolute deadline is durable: a key written
// with a TTL is still governed by the same deadline after the database is closed and
// reopened, rather than restarting the clock on recovery.
func TestTTLPersistsAcrossReopen(t *testing.T) {
	clk := &testClock{}
	clk.set(10)
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{Clock: clk.now})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.Update(func(txn *Txn) error {
		return txn.SetWithTTL([]byte("k"), []byte("v"), 100)
	}); err != nil {
		t.Fatalf("set-ttl: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen before the deadline: the key is still there.
	clk.set(50)
	d2, err := Open(fs, "test.kv", Options{Clock: clk.now})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if v, ok := txnGet(t, d2, "k"); !ok || v != "v" {
		t.Fatalf("after reopen before deadline k = %q,%v, want v", v, ok)
	}
	d2.Close()

	// Reopen past the deadline: the key has expired against its original absolute time.
	clk.set(500)
	d3, err := Open(fs, "test.kv", Options{Clock: clk.now})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { d3.Close() })
	if _, ok := txnGet(t, d3, "k"); ok {
		t.Fatalf("after reopen past deadline k still present")
	}
}
