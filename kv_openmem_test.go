package kv_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tamnd/kv"
)

// TestOpenMemRoundTrip checks that an in-memory database serves the normal read
// and write path: a committed value reads back, and a deleted key is gone.
func TestOpenMemRoundTrip(t *testing.T) {
	d, err := kv.OpenMem()
	if err != nil {
		t.Fatalf("OpenMem: %v", err)
	}
	defer d.Close()

	if err := d.Update(func(txn *kv.Txn) error { return txn.Set([]byte("alpha"), []byte("one")) }); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := d.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, []byte("one")) {
		t.Fatalf("get alpha = %q, want one", got)
	}
	if err := d.Update(func(txn *kv.Txn) error { return txn.Delete([]byte("alpha")) }); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := d.Get([]byte("alpha")); !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("get after delete = %v, want ErrNotFound", err)
	}
}

// TestOpenMemTouchesNoDisk confirms an in-memory database is truly off-disk: a
// run in an empty working directory writes no files at all.
func TestOpenMemTouchesNoDisk(t *testing.T) {
	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(wd)

	d, err := kv.OpenMem()
	if err != nil {
		t.Fatalf("OpenMem: %v", err)
	}
	for i := 0; i < 100; i++ {
		k := []byte{byte(i)}
		if err := d.Update(func(txn *kv.Txn) error { return txn.Set(k, []byte("v")) }); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	_ = d.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("in-memory database wrote files to disk: %v", names)
	}
	// Belt and suspenders: the conventional file name is not present anywhere either.
	if _, err := os.Stat(filepath.Join(dir, "kv.db")); !os.IsNotExist(err) {
		t.Fatalf("kv.db exists on disk, want absent: %v", err)
	}
}
