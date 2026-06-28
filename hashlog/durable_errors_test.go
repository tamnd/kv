package hashlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// TestFlushDurableErrorPropagatesFull pins D4: under Full a SET does not return until
// its record is in a synced extent, so a failed device barrier must surface as the SET's
// error, not be swallowed while the write is acknowledged. flushDurable used to return
// nothing and drop the sync error on the floor, so the caller saw a clean SET over an
// unsynced write. With the error threaded through, the SET whose sync fails returns that
// error, and once the fault clears the store keeps working.
func TestFlushDurableErrorPropagatesFull(t *testing.T) {
	dir := t.TempDir()
	tun := Tunables{Shards: 1, PageSize: 4096, ExtentSize: 4096, Path: filepath.Join(dir, "d4.hlog"), Durability: DurabilityFull}
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// A handful of writes commit cleanly before the fault is armed.
	for i := 0; i < 5; i++ {
		if err := s.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("clean Set %d: %v", i, err)
		}
	}

	failErr := errors.New("injected sync failure")
	var armed atomic.Bool
	s.df.syncHook = func(f *os.File) error {
		if armed.Load() {
			return failErr
		}
		return platformSyncData(f)
	}

	armed.Store(true)
	if err := s.Set([]byte("boom"), []byte("v")); !errors.Is(err, failErr) {
		t.Fatalf("Set over a failed barrier returned %v, want the injected failure", err)
	}

	// Clear the fault: the store must still accept writes and read them back.
	armed.Store(false)
	if err := s.Set([]byte("after"), []byte("ok")); err != nil {
		t.Fatalf("Set after the fault cleared: %v", err)
	}
	v, ok, err := s.Get([]byte("after"))
	if err != nil || !ok || string(v) != "ok" {
		t.Fatalf("Get after recovery: v=%q ok=%v err=%v", v, ok, err)
	}
}

// TestRollForNoRollOnFailedSeal pins D5: under Normal the page seal is the group-commit
// flush point, so rolling to a fresh page after a failed seal-flush would leave a
// non-final sealed page that never reached the device, the one thing the recovery scan
// must be able to trust. rollFor used to seal and roll regardless of the flush result;
// now a failed seal-flush leaves the tail page in place and returns the error. The test
// arms the barrier to fail, drives writes until one triggers a roll, and checks the SET
// errors, the tail did not advance, and after the fault clears the store reopens with
// every acknowledged write intact.
func TestRollForNoRollOnFailedSeal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "d5.hlog")
	tun := Tunables{Shards: 1, PageSize: 256, ExtentSize: 256, Path: path, Durability: DurabilityNormal}
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}
	sh := s.shards[0]

	failErr := errors.New("injected seal-flush failure")
	var armed atomic.Bool
	s.df.syncHook = func(f *os.File) error {
		if armed.Load() {
			return failErr
		}
		return platformSyncData(f)
	}

	oracle := map[string]string{}
	armed.Store(true)
	sawErr := false
	for i := 0; i < 4000 && !sawErr; i++ {
		k := fmt.Sprintf("k%04d", i)
		v := fmt.Sprintf("val-%04d", i)
		err := s.Set([]byte(k), []byte(v))
		if err == nil {
			oracle[k] = v
			continue
		}
		if !errors.Is(err, failErr) {
			t.Fatalf("Set %d returned unexpected error %v", i, err)
		}
		sawErr = true
		// The failed seal-flush must not have rolled the page: the tail is still page 0.
		if sh.tailPage != 0 {
			t.Fatalf("rollFor advanced tailPage to %d on a failed seal-flush", sh.tailPage)
		}
		// Clearing the fault, the same SET now seals page 0 and rolls cleanly.
		armed.Store(false)
		if err := s.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("retry after the fault cleared: %v", err)
		}
		oracle[k] = v
		if sh.tailPage == 0 {
			t.Fatal("the clean retry did not roll the sealed page")
		}
	}
	if !sawErr {
		t.Fatal("workload never triggered a page roll; lower PageSize or raise the count")
	}

	// A few more writes after recovery, then a clean close flushes everything.
	for i := 4000; i < 4100; i++ {
		k := fmt.Sprintf("k%04d", i)
		v := fmt.Sprintf("val-%04d", i)
		if err := s.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("post-recovery Set %d: %v", i, err)
		}
		oracle[k] = v
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: every acknowledged write survives, so the failed seal corrupted nothing.
	s2, err := New(tun)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	for k, want := range oracle {
		v, ok, err := s2.Get([]byte(k))
		if err != nil || !ok || string(v) != want {
			t.Fatalf("after reopen key %q: v=%q ok=%v err=%v want %q", k, v, ok, err, want)
		}
	}
}

// TestCommitUsesTrueBarrier pins D7: the checkpoint commit point must be a real device
// barrier, not a plain Sync that on macOS leaves the slot in the drive cache. commit used
// to pass d.f.Sync to writeCheckpointSlot, which bypasses both the counted barrier and the
// injectable hook. Routed through syncData, a commit now drives platformSyncData, which the
// hook observes and the counter records.
func TestCommitUsesTrueBarrier(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d7.hlog")
	d, err := openDurableFile(path, 16, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	var hooked atomic.Bool
	d.syncHook = func(f *os.File) error {
		hooked.Store(true)
		return platformSyncData(f)
	}

	before := d.syncCount.Load()
	d.alloc.alloc()
	if err := d.commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if !hooked.Load() {
		t.Fatal("commit did not route through syncData; the commit point is not a true barrier (D7)")
	}
	if d.syncCount.Load() == before {
		t.Fatal("commit issued no counted device barrier")
	}
}
