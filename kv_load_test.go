package kv_test

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv"
)

// TestLoadBulkPopulates drives the public Load surface: it bulk-loads ascending pairs into
// a fresh database and reads them back through an ordinary View, checking the returned
// version and that the data is queryable.
func TestLoadBulkPopulates(t *testing.T) {
	d := open(t)

	const n = 300
	i := 0
	v, err := d.Load(func() (key, value []byte, ok bool) {
		if i >= n {
			return nil, nil, false
		}
		k := []byte(fmt.Sprintf("k%05d", i))
		val := []byte(fmt.Sprintf("v%05d", i))
		i++
		return k, val, true
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if v != 1 {
		t.Fatalf("load version = %d, want 1", v)
	}

	if err := d.View(func(txn *kv.Txn) error {
		for j := 0; j < n; j++ {
			got, err := txn.Get([]byte(fmt.Sprintf("k%05d", j)))
			if err != nil {
				return err
			}
			if want := fmt.Sprintf("v%05d", j); string(got) != want {
				t.Fatalf("get k%05d = %q, want %q", j, got, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}
