package hlog

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestDBRecoverOverwrites checks recovery honors latest-wins: a key written twice comes back
// as the second value, because replay points the index at the later record last.
func TestDBRecoverOverwrites(t *testing.T) {
	const ringBytes = 1 << 20
	path := filepath.Join(t.TempDir(), "db.log")
	d, err := OpenDB(path, ringBytes, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		d.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("first"))
	}
	for i := range 200 {
		d.Set([]byte(fmt.Sprintf("k%03d", i)), fmt.Appendf(nil, "second-%03d", i))
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d2, err := OpenDB(path, ringBytes, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	var scratch []byte
	for i := range 200 {
		v, ok, err := d2.Get([]byte(fmt.Sprintf("k%03d", i)), scratch)
		if err != nil || !ok {
			t.Fatalf("k%03d missing after reopen (ok=%v err=%v)", i, ok, err)
		}
		if want := fmt.Sprintf("second-%03d", i); string(v) != want {
			t.Fatalf("k%03d = %q, want %q", i, v, want)
		}
	}
}

// TestDBRecoverLargerThanMemory recovers a store whose data far exceeds the ring, so the
// replay must read most records from the file, not the resident window.
func TestDBRecoverLargerThanMemory(t *testing.T) {
	const ringBytes = 1 << 20 // 1 MiB resident
	const keys = 20000        // far more than fits the ring
	path := filepath.Join(t.TempDir(), "db.log")
	d, err := OpenDB(path, ringBytes, keys)
	if err != nil {
		t.Fatal(err)
	}
	for i := range keys {
		d.Set([]byte(fmt.Sprintf("key-%06d", i)), fmt.Appendf(nil, "val-%06d", i))
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d2, err := OpenDB(path, ringBytes, keys)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	var scratch []byte
	for i := range keys {
		v, ok, err := d2.Get([]byte(fmt.Sprintf("key-%06d", i)), scratch)
		if err != nil || !ok {
			t.Fatalf("key-%06d missing after reopen (ok=%v err=%v)", i, ok, err)
		}
		if want := fmt.Sprintf("val-%06d", i); string(v) != want {
			t.Fatalf("key-%06d = %q, want %q", i, v, want)
		}
	}
}

// TestDBSyncThenRecover confirms an explicit Sync makes data recoverable without a Close: the
// records written before Sync come back after a reopen of the same file, modeling a crash
// right after the barrier.
func TestDBSyncThenRecover(t *testing.T) {
	const ringBytes = 1 << 20
	path := filepath.Join(t.TempDir(), "db.log")
	d, err := OpenDB(path, ringBytes, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 300 {
		d.Set([]byte(fmt.Sprintf("s%03d", i)), fmt.Appendf(nil, "v%03d", i))
	}
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	// Do not Close: reopen the file as if the process had crashed after the Sync barrier.
	d2, err := OpenDB(path, ringBytes, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	var scratch []byte
	for i := range 300 {
		v, ok, err := d2.Get([]byte(fmt.Sprintf("s%03d", i)), scratch)
		if err != nil || !ok {
			t.Fatalf("s%03d missing after synced reopen (ok=%v err=%v)", i, ok, err)
		}
		if want := fmt.Sprintf("v%03d", i); string(v) != want {
			t.Fatalf("s%03d = %q, want %q", i, v, want)
		}
	}
}

// TestTieredRecover writes a skewed mix through the tiered store, closes it so the hot tier
// drains to cold, then reopens and confirms every key comes back at its latest value.
func TestTieredRecover(t *testing.T) {
	const segBytes = 1 << 16
	const keys = 8000
	path := filepath.Join(t.TempDir(), "tier.log")
	d, err := OpenTiered(path, segBytes, 4096, 1<<20, keys, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for i := range keys {
		d.Set([]byte(fmt.Sprintf("t%05d", i)), []byte("old"))
	}
	for i := range keys {
		d.Set([]byte(fmt.Sprintf("t%05d", i)), fmt.Appendf(nil, "new-%05d", i))
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d2, err := OpenTiered(path, segBytes, 4096, 1<<20, keys, 2048)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	var scratch []byte
	for i := range keys {
		v, ok, err := d2.Get([]byte(fmt.Sprintf("t%05d", i)), scratch)
		if err != nil || !ok {
			t.Fatalf("t%05d missing after reopen (ok=%v err=%v)", i, ok, err)
		}
		if want := fmt.Sprintf("new-%05d", i); string(v) != want {
			t.Fatalf("t%05d = %q, want %q", i, v, want)
		}
	}
}

// TestTieredSyncThenRecover confirms TieredDB.Sync drains the hot tier to cold and fsyncs, so
// the data is recoverable from the file without a Close.
func TestTieredSyncThenRecover(t *testing.T) {
	const segBytes = 1 << 16
	const keys = 5000
	path := filepath.Join(t.TempDir(), "tier.log")
	d, err := OpenTiered(path, segBytes, 4096, 1<<20, keys, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for i := range keys {
		d.Set([]byte(fmt.Sprintf("y%05d", i)), fmt.Appendf(nil, "v%05d", i))
	}
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	d2, err := OpenTiered(path, segBytes, 4096, 1<<20, keys, 2048)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	var scratch []byte
	for i := range keys {
		v, ok, err := d2.Get([]byte(fmt.Sprintf("y%05d", i)), scratch)
		if err != nil || !ok {
			t.Fatalf("y%05d missing after synced reopen (ok=%v err=%v)", i, ok, err)
		}
		if want := fmt.Sprintf("v%05d", i); string(v) != want {
			t.Fatalf("y%05d = %q, want %q", i, v, want)
		}
	}
}
