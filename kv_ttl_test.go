package kv_test

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/tamnd/kv"
)

// TestSetWithTTLExpires checks the public TTL surface against the real clock: a key
// written with a short TTL reads back immediately and is gone after the deadline passes.
func TestSetWithTTLExpires(t *testing.T) {
	d := open(t)
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.SetWithTTL([]byte("k"), []byte("v"), 40*time.Millisecond)
	}); err != nil {
		t.Fatalf("set-ttl: %v", err)
	}

	// Well before the deadline the value is present.
	if err := d.View(func(txn *kv.Txn) error {
		v, err := txn.Get([]byte("k"))
		if err != nil {
			return err
		}
		if string(v) != "v" {
			t.Fatalf("before expiry k = %q, want v", v)
		}
		return nil
	}); err != nil {
		t.Fatalf("view before expiry: %v", err)
	}

	time.Sleep(120 * time.Millisecond)

	err := d.View(func(txn *kv.Txn) error {
		_, err := txn.Get([]byte("k"))
		return err
	})
	if !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("after expiry err = %v, want ErrNotFound", err)
	}
}

// TestSetWithTTLZeroNeverExpires checks a non-positive TTL behaves like a plain Set: the
// key persists with no deadline.
func TestSetWithTTLZeroNeverExpires(t *testing.T) {
	d := open(t)
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.SetWithTTL([]byte("k"), []byte("v"), 0)
	}); err != nil {
		t.Fatalf("set-ttl: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := d.View(func(txn *kv.Txn) error {
		v, err := txn.Get([]byte("k"))
		if err != nil {
			return err
		}
		if string(v) != "v" {
			t.Fatalf("k = %q, want v", v)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

// TestSetWithTTLSurvivesReopen checks a TTL key written, checkpointed, and reopened is
// still readable when its deadline is comfortably in the future, so the expiry frame
// round-trips through the main file rather than being lost at checkpoint.
func TestSetWithTTLSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.kv")
	d, err := kv.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.SetWithTTL([]byte("k"), []byte("v"), time.Hour)
	}); err != nil {
		t.Fatalf("set-ttl: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := kv.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if err := d2.View(func(txn *kv.Txn) error {
		v, err := txn.Get([]byte("k"))
		if err != nil {
			return err
		}
		if string(v) != "v" {
			t.Fatalf("after reopen k = %q, want v", v)
		}
		return nil
	}); err != nil {
		t.Fatalf("view after reopen: %v", err)
	}
}
