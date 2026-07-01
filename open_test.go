package kv

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestOpenDefaults checks the friendly constructor works with a zero Options: set, get, delete,
// and reopen all behave, so Open(path, Options{}) is a usable embedded store with no tuning.
func TestOpenDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.kv")
	d, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		d.Set(fmt.Appendf(nil, "k%05d", i), fmt.Appendf(nil, "v%05d", i))
	}
	d.Delete([]byte("k00007"))
	var scratch []byte
	if v, ok, _ := d.Get([]byte("k00042"), scratch); !ok || string(v) != "v00042" {
		t.Fatalf("get k00042: got (%q,%v) want (v00042,true)", v, ok)
	}
	if _, ok, _ := d.Get([]byte("k00007"), scratch); ok {
		t.Fatal("get k00007 after delete: want miss")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if v, ok, _ := d2.Get([]byte("k00042"), scratch); !ok || string(v) != "v00042" {
		t.Fatalf("get k00042 after reopen: got (%q,%v) want (v00042,true)", v, ok)
	}
	if _, ok, _ := d2.Get([]byte("k00007"), scratch); ok {
		t.Fatal("get k00007 after reopen: delete must survive")
	}
}
