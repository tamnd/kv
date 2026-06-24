package btree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
)

// scanAllBatch drives the zero-copy NextBatch path over [nil, nil) at snap and returns
// every visible key and value, copied out so the views do not outlive the cursor.
func scanAllBatch(t *testing.T, bt *BTree, snap engine.Snapshot, batchCap int) []engine.KV {
	t.Helper()
	rd, err := bt.NewReader(snap)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer rd.Close()
	cur, err := rd.(engine.ForwardCursorer).NewForwardCursor(nil, nil)
	if err != nil {
		t.Fatalf("new forward cursor: %v", err)
	}
	bc := cur.(engine.BatchCursor)
	var out []engine.KV
	dst := make([]engine.KV, batchCap)
	for {
		n, err := bc.NextBatch(dst, false)
		if err != nil {
			t.Fatalf("next batch: %v", err)
		}
		for i := 0; i < n; i++ {
			out = append(out, engine.KV{
				Key:   append([]byte(nil), dst[i].Key...),
				Value: append([]byte(nil), dst[i].Value...),
			})
		}
		if n < len(dst) {
			break // short fill: range exhausted
		}
	}
	return out
}

// TestScanCleanLeafFastPath checks the clean-leaf scan fast path against the point reader,
// the independent oracle. A tree of distinct single-version Sets is the clean shape: the
// fast path emits each cell with no version-group fold, and the scan output must match a
// per-key Get exactly, in order. Several leaves and a small batch cap force the path to
// cross leaf boundaries inside one fill.
func TestScanCleanLeafFastPath(t *testing.T) {
	bt := newBTree(t, 512, 16) // small page so the keys span several leaves

	const n = 200
	b := engine.NewWriteBatch(10)
	for i := 0; i < n; i++ {
		b.Set([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i)))
	}
	if err := bt.Apply(b, 10); err != nil {
		t.Fatalf("apply: %v", err)
	}

	got := scanAllBatch(t, bt, engine.Snapshot{Version: 100}, 7)
	if len(got) != n {
		t.Fatalf("scanned %d keys, want %d", len(got), n)
	}
	rd, _ := bt.NewReader(engine.Snapshot{Version: 100})
	defer rd.Close()
	for i, kv := range got {
		wantKey := fmt.Sprintf("k%04d", i)
		if string(kv.Key) != wantKey {
			t.Fatalf("entry %d key = %q, want %q (out of order or missing)", i, kv.Key, wantKey)
		}
		ref, err := rd.Get(kv.Key)
		if err != nil {
			t.Fatalf("oracle get %q: %v", kv.Key, err)
		}
		if string(kv.Value) != string(ref) {
			t.Fatalf("key %q: scan value %q != get value %q", kv.Key, kv.Value, ref)
		}
	}
}

// TestScanCleanLeafSnapshotVisibility checks the version gate inside the fast path. A leaf can
// be clean (distinct single-version Sets) and still hold cells newer than the reader's
// snapshot; the fast path must skip those exactly as a fold would, never leak a future version.
func TestScanCleanLeafSnapshotVisibility(t *testing.T) {
	bt := newBTree(t, 4096, 16) // one leaf: every key is a distinct single-version Set

	old := engine.NewWriteBatch(10)
	for _, k := range []string{"a", "c", "e", "g"} {
		old.Set([]byte(k), []byte(k+"-old"))
	}
	if err := bt.Apply(old, 10); err != nil {
		t.Fatalf("apply old: %v", err)
	}
	newer := engine.NewWriteBatch(20)
	for _, k := range []string{"b", "d", "f"} {
		newer.Set([]byte(k), []byte(k+"-new"))
	}
	if err := bt.Apply(newer, 20); err != nil {
		t.Fatalf("apply newer: %v", err)
	}

	// Snapshot between the two commit versions: only the version-10 keys are visible.
	got := scanAllBatch(t, bt, engine.Snapshot{Version: 15}, 4)
	want := []string{"a", "c", "e", "g"}
	if len(got) != len(want) {
		t.Fatalf("scanned %d keys at snapshot 15, want %d (%v)", len(got), len(want), want)
	}
	for i, w := range want {
		if string(got[i].Key) != w {
			t.Fatalf("entry %d = %q, want %q (a newer-than-snapshot cell leaked or order broke)", i, got[i].Key, w)
		}
	}

	// At the newer snapshot every key is visible.
	if all := scanAllBatch(t, bt, engine.Snapshot{Version: 20}, 4); len(all) != 7 {
		t.Fatalf("scanned %d keys at snapshot 20, want 7", len(all))
	}
}

// TestScanMultiVersionDisengagesFastPath checks the fast path correctly disengages when a leaf
// is not clean. Overwriting a key makes a two-version group, so leafClean is false and the scan
// must fall through to the fold, returning the newest version of the overwritten key.
func TestScanMultiVersionDisengagesFastPath(t *testing.T) {
	bt := newBTree(t, 4096, 16)

	b1 := engine.NewWriteBatch(10)
	for _, k := range []string{"a", "b", "c"} {
		b1.Set([]byte(k), []byte(k+"-v10"))
	}
	if err := bt.Apply(b1, 10); err != nil {
		t.Fatalf("apply v10: %v", err)
	}
	b2 := engine.NewWriteBatch(20)
	b2.Set([]byte("b"), []byte("b-v20")) // overwrite: b now has two versions in the leaf
	if err := bt.Apply(b2, 20); err != nil {
		t.Fatalf("apply v20: %v", err)
	}

	got := scanAllBatch(t, bt, engine.Snapshot{Version: 100}, 8)
	want := map[string]string{"a": "a-v10", "b": "b-v20", "c": "c-v10"}
	if len(got) != len(want) {
		t.Fatalf("scanned %d keys, want %d", len(got), len(want))
	}
	for _, kv := range got {
		if w := want[string(kv.Key)]; string(kv.Value) != w {
			t.Fatalf("key %q = %q, want %q", kv.Key, kv.Value, w)
		}
	}
}

// iterAll walks the general NewIter cursor over [nil, nil) at snap and returns every visible
// key and value. NewIter materializes and resolves the range up front, the path the clean-range
// fast path optimizes; this drives it the way db.NewIterator and a reverse scan do.
func iterAll(t *testing.T, bt *BTree, snap engine.Snapshot) []engine.KV {
	t.Helper()
	rd, err := bt.NewReader(snap)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer rd.Close()
	cur, err := rd.NewIter(engine.IterOptions{})
	if err != nil {
		t.Fatalf("new iter: %v", err)
	}
	var out []engine.KV
	for ok := cur.First(); ok; ok = cur.Next() {
		lv, err := cur.Value()
		if err != nil {
			t.Fatalf("value: %v", err)
		}
		v, err := lv.Value()
		if err != nil {
			t.Fatalf("lazy value: %v", err)
		}
		out = append(out, engine.KV{
			Key:   append([]byte(nil), cur.Key()...),
			Value: append([]byte(nil), v...),
		})
	}
	return out
}

// TestIterCleanRangeFastPath checks the NewIter clean-range fast path against the point reader.
// A clean single-version tree must iterate to the same keys and values a per-key Get returns, in
// order; the snapshot-visibility skip and the multi-version fall-through mirror the cursor tests.
func TestIterCleanRangeFastPath(t *testing.T) {
	bt := newBTree(t, 512, 16)
	const n = 150
	b := engine.NewWriteBatch(10)
	for i := 0; i < n; i++ {
		b.Set([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i)))
	}
	if err := bt.Apply(b, 10); err != nil {
		t.Fatalf("apply: %v", err)
	}

	got := iterAll(t, bt, engine.Snapshot{Version: 100})
	if len(got) != n {
		t.Fatalf("iterated %d keys, want %d", len(got), n)
	}
	rd, _ := bt.NewReader(engine.Snapshot{Version: 100})
	defer rd.Close()
	for i, kv := range got {
		wantKey := fmt.Sprintf("k%04d", i)
		if string(kv.Key) != wantKey {
			t.Fatalf("entry %d key = %q, want %q", i, kv.Key, wantKey)
		}
		ref, err := rd.Get(kv.Key)
		if err != nil {
			t.Fatalf("oracle get %q: %v", kv.Key, err)
		}
		if string(kv.Value) != string(ref) {
			t.Fatalf("key %q: iter value %q != get value %q", kv.Key, kv.Value, ref)
		}
	}

	// Snapshot visibility: keys written at version 20 are invisible below it even on a clean range.
	newer := engine.NewWriteBatch(20)
	newer.Set([]byte("k0000zz"), []byte("future")) // sorts after k0000, distinct user key, single version
	if err := bt.Apply(newer, 20); err != nil {
		t.Fatalf("apply newer: %v", err)
	}
	for _, kv := range iterAll(t, bt, engine.Snapshot{Version: 15}) {
		if string(kv.Key) == "k0000zz" {
			t.Fatalf("future key leaked into a snapshot-15 iteration")
		}
	}
}

// TestIterMultiVersionDisengages checks the NewIter fast path falls through to the fold when the
// range is not clean: an overwritten key has two versions, so the iterator must return the newest.
func TestIterMultiVersionDisengages(t *testing.T) {
	bt := newBTree(t, 4096, 16)
	b1 := engine.NewWriteBatch(10)
	for _, k := range []string{"a", "b", "c"} {
		b1.Set([]byte(k), []byte(k+"-v10"))
	}
	if err := bt.Apply(b1, 10); err != nil {
		t.Fatalf("apply v10: %v", err)
	}
	b2 := engine.NewWriteBatch(20)
	b2.Set([]byte("b"), []byte("b-v20"))
	if err := bt.Apply(b2, 20); err != nil {
		t.Fatalf("apply v20: %v", err)
	}
	want := map[string]string{"a": "a-v10", "b": "b-v20", "c": "c-v10"}
	got := iterAll(t, bt, engine.Snapshot{Version: 100})
	if len(got) != len(want) {
		t.Fatalf("iterated %d keys, want %d", len(got), len(want))
	}
	for _, kv := range got {
		if w := want[string(kv.Key)]; string(kv.Value) != w {
			t.Fatalf("key %q = %q, want %q", kv.Key, kv.Value, w)
		}
	}
}

// BenchmarkIterCleanRange materializes and walks the whole clean tree through NewIter, the range
// path the fast path targets. benchstat against main shows what skipping the per-key fold buys.
func BenchmarkIterCleanRange(b *testing.B) {
	bt := newBTreeB(b, 4096, 4096)
	const n = 5000
	wb := engine.NewWriteBatch(10)
	for i := 0; i < n; i++ {
		wb.Set([]byte(fmt.Sprintf("k%06d", i)), []byte(fmt.Sprintf("val-%06d-payload", i)))
	}
	if err := bt.Apply(wb, 10); err != nil {
		b.Fatalf("apply: %v", err)
	}
	snap := engine.Snapshot{Version: 100}

	b.ReportAllocs()
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		rd, err := bt.NewReader(snap)
		if err != nil {
			b.Fatalf("reader: %v", err)
		}
		cur, err := rd.NewIter(engine.IterOptions{})
		if err != nil {
			b.Fatalf("iter: %v", err)
		}
		total := 0
		for ok := cur.First(); ok; ok = cur.Next() {
			_ = cur.Key()
			total++
		}
		if total != n {
			b.Fatalf("iterated %d, want %d", total, n)
		}
		rd.Close()
	}
}

// BenchmarkScanCleanLeaf scans a whole clean single-version tree through the zero-copy batch
// path, the readseq shape the fast path targets. The reported numbers are per full-tree scan;
// benchstat against main shows what skipping the per-key fold buys.
func BenchmarkScanCleanLeaf(b *testing.B) {
	bt := newBTreeB(b, 4096, 4096)
	const n = 5000
	wb := engine.NewWriteBatch(10)
	for i := 0; i < n; i++ {
		wb.Set([]byte(fmt.Sprintf("k%06d", i)), []byte(fmt.Sprintf("val-%06d-payload", i)))
	}
	if err := bt.Apply(wb, 10); err != nil {
		b.Fatalf("apply: %v", err)
	}
	dst := make([]engine.KV, 256)
	snap := engine.Snapshot{Version: 100}

	b.ReportAllocs()
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		rd, err := bt.NewReader(snap)
		if err != nil {
			b.Fatalf("reader: %v", err)
		}
		cur, err := rd.(engine.ForwardCursorer).NewForwardCursor(nil, nil)
		if err != nil {
			b.Fatalf("cursor: %v", err)
		}
		bc := cur.(engine.BatchCursor)
		total := 0
		for {
			m, err := bc.NextBatch(dst, false)
			if err != nil {
				b.Fatalf("batch: %v", err)
			}
			total += m
			if m < len(dst) {
				break
			}
		}
		if total != n {
			b.Fatalf("scanned %d, want %d", total, n)
		}
		rd.Close()
	}
}
