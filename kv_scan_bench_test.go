package kv_test

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv"
)

// BenchmarkBetaScanOp reproduces the bench harness scanOp on the Beta core through the full public
// DB stack: a fresh View transaction, an unbounded forward iterator, SeekGE to a key, then 50
// bounded Next+Value calls. This is the exact ycsb-e shape the directional bench measures, isolated
// from the directional harness's write mix and machine noise, so it answers cleanly whether the
// streaming scan cursor made the scan op cheaper end to end.
func BenchmarkBetaScanOp(b *testing.B) {
	const n = 20000
	const scanLen = 50
	path := b.TempDir() + "/beta.kv"
	d, err := kv.Open(path, kv.WithEngine(kv.Beta))
	if err != nil {
		b.Fatalf("open beta: %v", err)
	}
	defer d.Close()

	for base := 0; base < n; base += 500 {
		if err := d.Update(func(txn *kv.Txn) error {
			for i := base; i < base+500 && i < n; i++ {
				if err := txn.Set([]byte(fmt.Sprintf("k%08d", i)), []byte(fmt.Sprintf("v%08d-%0100d", i, i))); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatalf("seed at %d: %v", base, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Spread seek keys across the whole keyspace: a worst case for any forward-streaming
		// SeekGE, which pulls from the lower bound to the seek point.
		seek := []byte(fmt.Sprintf("k%08d", (i*977)%(n-scanLen)))
		err := d.View(func(txn *kv.Txn) error {
			it, e := txn.NewIterator(kv.IterOptions{})
			if e != nil {
				return e
			}
			defer it.Close()
			seen := 0
			for ok := it.SeekGE(seek); ok && seen < scanLen; ok = it.Next() {
				if _, e := it.Value(); e != nil {
					return e
				}
				seen++
			}
			if seen != scanLen {
				b.Fatalf("scan saw %d, want %d", seen, scanLen)
			}
			return it.Error()
		})
		if err != nil {
			b.Fatalf("scan: %v", err)
		}
	}
}

// BenchmarkBetaScanOpHot is the same scanOp shape with seek keys concentrated near the start of the
// keyspace, the YCSB-E Zipfian access pattern the directional bench actually uses (the hot keys sit
// low). A forward-streaming SeekGE pulls from the lower bound to the seek point, so a hot, local
// seek pulls only a short prefix and the streaming window pays for a handful of keys, not the whole
// range. This is the realistic end-to-end number for the directional ycsb-e cell.
func BenchmarkBetaScanOpHot(b *testing.B) {
	const n = 20000
	const scanLen = 50
	const hot = 256 // seeks land in the first `hot` keys, the Zipfian-hot region
	path := b.TempDir() + "/beta.kv"
	d, err := kv.Open(path, kv.WithEngine(kv.Beta))
	if err != nil {
		b.Fatalf("open beta: %v", err)
	}
	defer d.Close()

	for base := 0; base < n; base += 500 {
		if err := d.Update(func(txn *kv.Txn) error {
			for i := base; i < base+500 && i < n; i++ {
				if err := txn.Set([]byte(fmt.Sprintf("k%08d", i)), []byte(fmt.Sprintf("v%08d-%0100d", i, i))); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatalf("seed at %d: %v", base, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seek := []byte(fmt.Sprintf("k%08d", (i*7)%hot))
		err := d.View(func(txn *kv.Txn) error {
			it, e := txn.NewIterator(kv.IterOptions{})
			if e != nil {
				return e
			}
			defer it.Close()
			seen := 0
			for ok := it.SeekGE(seek); ok && seen < scanLen; ok = it.Next() {
				if _, e := it.Value(); e != nil {
					return e
				}
				seen++
			}
			if seen != scanLen {
				b.Fatalf("scan saw %d, want %d", seen, scanLen)
			}
			return it.Error()
		})
		if err != nil {
			b.Fatalf("scan: %v", err)
		}
	}
}
