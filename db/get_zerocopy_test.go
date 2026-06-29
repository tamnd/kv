package db

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// TestGetZeroCopyMatchesGet checks GetZeroCopy resolves identically to Get across every MVCC
// shape: a plain value, an overwrite (multi-version group), a delete (tombstone folds to
// absent), a merge (folds operands over the base), and a missing key. The f2 core has no
// zero-copy reader, so these calls exercise the copying fallback and must still agree with Get.
func TestGetZeroCopyMatchesGet(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, Merge: concatMerge})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	if _, err := d.Write(func(b *engine.WriteBatch) {
		b.Set([]byte("plain"), []byte("v1"))
		b.Set([]byte("over"), []byte("old"))
		b.Set([]byte("gone"), []byte("doomed"))
		b.Set([]byte("m"), []byte("base"))
	}); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if _, err := d.Write(func(b *engine.WriteBatch) {
		b.Set([]byte("over"), []byte("new"))
		b.Delete([]byte("gone"))
		b.Merge([]byte("m"), []byte("+x"))
	}); err != nil {
		t.Fatalf("write 2: %v", err)
	}

	for _, key := range []string{"plain", "over", "gone", "m", "absent"} {
		want, wantErr := d.Get([]byte(key))
		got, gotErr := d.GetZeroCopy([]byte(key))
		if (wantErr == nil) != (gotErr == nil) || (wantErr != nil && wantErr != gotErr) {
			t.Fatalf("key %q: Get err %v, GetZeroCopy err %v", key, wantErr, gotErr)
		}
		if wantErr == nil && !bytes.Equal(want, got) {
			t.Fatalf("key %q: Get %q != GetZeroCopy %q", key, want, got)
		}
	}
}

// TestGetZeroCopyValidAfterWrites pins the zero-copy contract that matters most: the value
// stays valid for reading after the call returns and after later commits touch other keys.
// On the B-tree path the returned slice aliases the decoded leaf, so a writer that rewrites
// the page in place would be the thing that could corrupt it; the decoded-node immutability
// (a writer replaces the node, never edits the read one) is what makes this hold. The test
// reads zero-copy, keeps the slice, drives enough unrelated writes to churn the cache, and
// checks the bytes are unchanged.
func TestGetZeroCopyValidAfterWrites(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "test.kv", Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	if _, err := d.Write(func(b *engine.WriteBatch) {
		b.Set([]byte("keep"), []byte("the-original-value"))
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	v, err := d.GetZeroCopy([]byte("keep"))
	if err != nil {
		t.Fatalf("zero-copy get: %v", err)
	}
	held := v // keep the aliased slice across the churn below
	want := "the-original-value"

	for i := 0; i < 2000; i++ {
		if _, err := d.Write(func(b *engine.WriteBatch) {
			b.Set([]byte(fmt.Sprintf("churn%06d", i)), bytes.Repeat([]byte("x"), 200))
		}); err != nil {
			t.Fatalf("churn write %d: %v", i, err)
		}
	}

	if string(held) != want {
		t.Fatalf("held zero-copy value changed under writes: got %q want %q", held, want)
	}
}
