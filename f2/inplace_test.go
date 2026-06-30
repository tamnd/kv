package f2

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// durableEvicting opens a budgeted single-file store under the given dial, the profile
// the in-place overwrite path targets. The path is returned so a test can crash and
// reopen the same file.
func durableEvicting(t *testing.T, dial Durability) (*Store, Tunables) {
	t.Helper()
	tn := Tunables{
		Shards:                4,
		PageSize:              4096,
		ResidentPagesPerShard: 4,
		Path:                  filepath.Join(t.TempDir(), "inplace.db"),
		Durability:            dial,
	}
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, tn
}

// TestInPlaceSameSizeDoesNotGrowLog is the core in-place property: a hot key overwritten
// many times with a same-size value is rewritten where it sits, so InPlaceUpdates climbs
// to the overwrite count, the log does not grow past the single seed record, the space
// amplification stays at 1.0, and the last value reads back.
func TestInPlaceSameSizeDoesNotGrowLog(t *testing.T) {
	s, _ := durableEvicting(t, DurabilityNone)
	defer s.Close()

	key := []byte("hot-key")
	val := []byte("0000000000000000") // 16 bytes, fixed width across overwrites
	if err := s.Set(key, val); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	seedLog := s.Stats().LogBytes

	const n = 500
	for i := 0; i < n; i++ {
		v := []byte(fmt.Sprintf("%016d", i))
		if err := s.Set(key, v); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	if got := s.InPlaceUpdates(); got != n {
		t.Fatalf("InPlaceUpdates = %d, want %d (every same-size overwrite should be in place)", got, n)
	}
	if got := s.Stats().LogBytes; got != seedLog {
		t.Fatalf("LogBytes = %d, want %d (in-place must not append)", got, seedLog)
	}
	if amp := s.Stats().SpaceAmplification; amp != 1.0 {
		t.Fatalf("SpaceAmplification = %v, want 1.0", amp)
	}
	v, ok, err := s.GetCopy(key)
	if err != nil || !ok {
		t.Fatalf("GetCopy = (%q, %v, %v), want the last value", v, ok, err)
	}
	if want := []byte(fmt.Sprintf("%016d", n-1)); !bytes.Equal(v, want) {
		t.Fatalf("value = %q, want %q", v, want)
	}
}

// TestInPlaceDifferentSizeAppends pins that a value whose size differs from the current
// record falls through to the append path (in-place declines), and that a same-size
// overwrite after it goes in place.
func TestInPlaceDifferentSizeAppends(t *testing.T) {
	s, _ := durableEvicting(t, DurabilityNone)
	defer s.Close()

	key := []byte("k")
	if err := s.Set(key, []byte("short")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.Set(key, []byte("a-much-longer-value")); err != nil {
		t.Fatalf("grow: %v", err)
	}
	if got := s.InPlaceUpdates(); got != 0 {
		t.Fatalf("InPlaceUpdates = %d after a size change, want 0", got)
	}
	if err := s.Set(key, []byte("another-longer-valu")); err != nil { // same size as the last
		t.Fatalf("same-size: %v", err)
	}
	if got := s.InPlaceUpdates(); got != 1 {
		t.Fatalf("InPlaceUpdates = %d after a same-size overwrite, want 1", got)
	}
	v, ok, _ := s.GetCopy(key)
	if !ok || !bytes.Equal(v, []byte("another-longer-valu")) {
		t.Fatalf("value = %q, want %q", v, "another-longer-valu")
	}
}

// TestInPlaceOffOtherProfiles pins the gate: the memory-only, durable full-resident, and
// durable Full-dial profiles all keep appending (InPlaceUpdates stays 0) and read back
// correctly, so the lock-free aliasing read of those profiles never sees a byte change.
func TestInPlaceOffOtherProfiles(t *testing.T) {
	mem, err := New(DefaultTunables())
	if err != nil {
		t.Fatalf("New mem: %v", err)
	}
	defer mem.Close()

	fullResident, err := New(Tunables{
		Shards: 4, PageSize: 4096, ResidentPagesPerShard: 0,
		Path: filepath.Join(t.TempDir(), "fr.db"), Durability: DurabilityNone,
	})
	if err != nil {
		t.Fatalf("New full-resident: %v", err)
	}
	defer fullResident.Close()

	fullDial, _ := durableEvicting(t, DurabilityFull)
	defer fullDial.Close()

	for name, s := range map[string]*Store{"memory-only": mem, "full-resident": fullResident, "full-dial": fullDial} {
		key, val := []byte("k"), []byte("vvvvvvvv")
		if err := s.Set(key, val); err != nil {
			t.Fatalf("%s seed: %v", name, err)
		}
		for i := 0; i < 20; i++ {
			if err := s.Set(key, []byte(fmt.Sprintf("%08d", i))); err != nil {
				t.Fatalf("%s Set: %v", name, err)
			}
		}
		if got := s.InPlaceUpdates(); got != 0 {
			t.Fatalf("%s: InPlaceUpdates = %d, want 0 (in-place must be off here)", name, got)
		}
		v, ok, _ := s.GetCopy(key)
		if !ok || !bytes.Equal(v, []byte("00000019")) {
			t.Fatalf("%s: value = %q, want %q", name, v, "00000019")
		}
	}
}

// TestInPlaceConcurrentReaders runs an in-place writer against many readers: the writer
// overwrites hot keys in place with same-size values drawn from a fixed set, and every
// value a reader observes must be a complete member of that set, never a torn mix. The
// shard read lock the in-place profile takes is what makes this race-free; run under
// -race to exercise it.
func TestInPlaceConcurrentReaders(t *testing.T) {
	s, _ := durableEvicting(t, DurabilityNone)
	defer s.Close()

	const hot = 8
	vals := [][]byte{
		[]byte("AAAAAAAAAAAAAAAA"), []byte("BBBBBBBBBBBBBBBB"),
		[]byte("CCCCCCCCCCCCCCCC"), []byte("DDDDDDDDDDDDDDDD"),
	}
	valid := map[string]bool{}
	for _, v := range vals {
		valid[string(v)] = true
	}
	keys := make([][]byte, hot)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("hot-%02d", i))
		if err := s.Set(keys[i], vals[0]); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	var stop atomic.Bool
	var writerWG, readerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		i := 0
		for !stop.Load() {
			if err := s.Set(keys[i%hot], vals[i%len(vals)]); err != nil {
				t.Errorf("writer Set: %v", err)
				return
			}
			i++
		}
	}()

	var torn atomic.Int64
	for r := 0; r < 8; r++ {
		readerWG.Add(1)
		go func(r int) {
			defer readerWG.Done()
			for j := 0; j < 50000; j++ {
				v, ok, err := s.GetCopy(keys[j%hot])
				if err != nil {
					t.Errorf("reader Get: %v", err)
					return
				}
				if ok && !valid[string(v)] {
					torn.Add(1)
				}
			}
		}(r)
	}

	readerWG.Wait()    // readers finish their fixed loops
	stop.Store(true)   // then stop the writer
	writerWG.Wait()    // and wait for it to exit
	if got := torn.Load(); got != 0 {
		t.Fatalf("observed %d torn values, want 0", got)
	}
}

// TestInPlaceCrashRecoversLatestAfterCheckpoint overwrites a key in place, checkpoints so
// the tail page reaches disk, then crashes and reopens: recovery must read the in-place
// value, proving the rewritten bytes and their recomputed CRC survive a reopen.
func TestInPlaceCrashRecoversLatestAfterCheckpoint(t *testing.T) {
	s, tn := durableEvicting(t, DurabilityNone)
	key := []byte("k")
	for i := 0; i < 100; i++ {
		if err := s.Set(key, []byte(fmt.Sprintf("%016d", i))); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if s.InPlaceUpdates() == 0 {
		t.Fatalf("expected in-place overwrites before the crash")
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	_ = s.df.f.Close() // crash with the checkpoint already on disk

	r, err := New(tn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r.Close()
	v, ok, err := r.GetCopy(key)
	if err != nil || !ok {
		t.Fatalf("GetCopy after reopen = (%q, %v, %v)", v, ok, err)
	}
	if want := []byte(fmt.Sprintf("%016d", 99)); !bytes.Equal(v, want) {
		t.Fatalf("recovered value = %q, want %q", v, want)
	}
}

// TestInPlaceCrashUnsyncedNoTorn overwrites in place in a tail page that is never flushed,
// then crashes with no checkpoint and reopens. The in-place bytes were never on disk, so
// recovery must land on a consistent earlier value with no torn record: the reopen
// succeeds and the key, if present, reads a value the test actually wrote.
func TestInPlaceCrashUnsyncedNoTorn(t *testing.T) {
	s, tn := durableEvicting(t, DurabilityNone)
	key := []byte("k")
	written := map[string]bool{}
	// First a durable value via a checkpoint, so there is a committed version to fall back to.
	first := []byte(fmt.Sprintf("%016d", 0))
	if err := s.Set(key, first); err != nil {
		t.Fatalf("seed: %v", err)
	}
	written[string(first)] = true
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	// Now overwrite in place in the unsynced tail, no checkpoint after.
	for i := 1; i <= 50; i++ {
		v := []byte(fmt.Sprintf("%016d", i))
		if err := s.Set(key, v); err != nil {
			t.Fatalf("Set: %v", err)
		}
		written[string(v)] = true
	}
	_ = s.df.f.Close() // crash with no checkpoint after the in-place overwrites

	r, err := New(tn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r.Close()
	v, ok, err := r.GetCopy(key)
	if err != nil {
		t.Fatalf("GetCopy after reopen: %v", err)
	}
	if ok && !written[string(v)] {
		t.Fatalf("recovered value = %q, which was never written (torn record)", v)
	}
}
