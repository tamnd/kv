package db

import (
	"fmt"
	"sort"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// TestLSMStreamScanEquivalence drives the forward streaming scan path (spec 04 slice 7)
// against the LSM engine and pins it to two independent oracles: an in-test model of the
// final state, and the materialized reverse scan reversed. The read-only forward View
// takes the new streaming ScanForward path; the reverse scan takes the old materialized
// foldRange path, so the test asserts the two paths return the same view over data that
// spans the memtable and several on-disk segments, with overwrites, point deletes, and a
// range delete giving the merged view real MVCC depth.
func TestLSMStreamScanEquivalence(t *testing.T) {
	const n = 400
	d := openMem(t, Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 256})

	// Build the expected final state alongside the writes so a bug shared by both scan
	// paths cannot pass.
	model := map[string]string{}

	put := func(k, v string) {
		if err := d.Update(func(txn *Txn) error { return txn.Set([]byte(k), []byte(v)) }); err != nil {
			t.Fatalf("set: %v", err)
		}
		model[k] = v
	}
	del := func(k string) {
		if err := d.Update(func(txn *Txn) error { return txn.Delete([]byte(k)) }); err != nil {
			t.Fatalf("delete: %v", err)
		}
		delete(model, k)
	}

	// Initial fill, then a second pass of overwrites and point deletes at higher versions,
	// each its own transaction so the small memtable flushes repeatedly and the versions
	// land across many segments rather than collapsing in one memtable.
	for i := 0; i < n; i++ {
		put(fmt.Sprintf("key%04d", i), fmt.Sprintf("v%04d", i))
	}
	for i := 0; i < n; i += 3 {
		put(fmt.Sprintf("key%04d", i), fmt.Sprintf("w%04d", i))
	}
	for i := 0; i < n; i += 7 {
		del(fmt.Sprintf("key%04d", i))
	}

	// A range delete over a middle band, then a few resurrections inside it, so the fold
	// has to apply a covering range marker and then let newer point writes win over it.
	lo, hi := "key0150", "key0250"
	if err := d.Update(func(txn *Txn) error { return txn.DeleteRange([]byte(lo), []byte(hi)) }); err != nil {
		t.Fatalf("delete range: %v", err)
	}
	for k := range model {
		if k >= lo && k < hi {
			delete(model, k)
		}
	}
	for _, i := range []int{160, 175, 200, 240} {
		put(fmt.Sprintf("key%04d", i), fmt.Sprintf("r%04d", i))
	}

	settleLSM(t, d)

	// forward streams; reverse materializes.
	forward := func(opts engine.IterOptions) ([]string, []string) {
		var keys, vals []string
		if err := d.View(func(txn *Txn) error {
			it, err := txn.NewIterator(opts)
			if err != nil {
				return err
			}
			defer it.Close()
			keys, vals = collect(t, it)
			return nil
		}); err != nil {
			t.Fatalf("forward scan: %v", err)
		}
		return keys, vals
	}
	reverse := func(opts engine.IterOptions) ([]string, []string) {
		opts.Reverse = true
		var keys, vals []string
		if err := d.View(func(txn *Txn) error {
			it, err := txn.NewIterator(opts)
			if err != nil {
				return err
			}
			defer it.Close()
			for it.First(); it.Valid(); it.Next() {
				keys = append(keys, string(it.Key()))
				v, err := it.Value()
				if err != nil {
					t.Fatalf("value: %v", err)
				}
				vals = append(vals, string(v))
			}
			return it.Error()
		}); err != nil {
			t.Fatalf("reverse scan: %v", err)
		}
		// Reverse to ascending so it lines up with the forward and model views.
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
			vals[i], vals[j] = vals[j], vals[i]
		}
		return keys, vals
	}

	modelView := func(lower, upper string) ([]string, []string) {
		var keys []string
		for k := range model {
			if lower != "" && k < lower {
				continue
			}
			if upper != "" && k >= upper {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		vals := make([]string, len(keys))
		for i, k := range keys {
			vals[i] = model[k]
		}
		return keys, vals
	}

	cases := []struct {
		name         string
		opts         engine.IterOptions
		lower, upper string
	}{
		{"full", engine.IterOptions{}, "", ""},
		{"bounded", engine.IterOptions{Lower: []byte("key0100"), Upper: []byte("key0300")}, "key0100", "key0300"},
		{"lower-only", engine.IterOptions{Lower: []byte("key0350")}, "key0350", ""},
		{"prefix", engine.IterOptions{Prefix: []byte("key01")}, "key01", "key02"},
		{"band-resurrect", engine.IterOptions{Lower: []byte("key0150"), Upper: []byte("key0250")}, "key0150", "key0250"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fk, fv := forward(tc.opts)
			rk, rv := reverse(tc.opts)
			mk, mv := modelView(tc.lower, tc.upper)

			if !eq(fk, mk...) {
				t.Fatalf("forward keys != model:\n got   %v\n model %v", fk, mk)
			}
			if !eq(fv, mv...) {
				t.Fatalf("forward vals != model:\n got   %v\n model %v", fv, mv)
			}
			if !eq(rk, fk...) {
				t.Fatalf("reverse keys != forward (path divergence):\n fwd %v\n rev %v", fk, rk)
			}
			if !eq(rv, fv...) {
				t.Fatalf("reverse vals != forward (path divergence):\n fwd %v\n rev %v", fv, rv)
			}
		})
	}
}
