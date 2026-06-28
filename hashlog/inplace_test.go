package hashlog

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// inPlaceTunables is a durable eviction-possible store (a Path plus a resident budget),
// the only profile where the in-place same-size overwrite is enabled (doc 04 section
// 7.3). Small pages so a modest workload rolls and spills, matching the larger-than-
// memory regime in-place targets.
func inPlaceTunables(path string, d Durability) Tunables {
	return Tunables{
		Shards:                4,
		PageSize:              512,
		ExtentSize:            512,
		ResidentPagesPerShard: 2,
		Path:                  path,
		Durability:            d,
	}
}

// TestM7InPlaceSameSizeDoesNotGrowLog is the headline M7 unit gate (doc 04 section 7.1,
// doc 08 M7 row): a hot key overwritten with a same-size value over and over takes the
// in-place path every time, so the log does not grow by a record per write. It asserts
// the new value reads back (last-writer-wins) and that the in-place counter climbed while
// the spill count and the bytes-since-checkpoint stayed at the single initial append.
func TestM7InPlaceSameSizeDoesNotGrowLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inplace.hlog")
	s := mustStore(t, inPlaceTunables(path, DurabilityNormal))
	defer s.Close()

	key := []byte("hot-counter")
	// The first SET is an append (the key is absent). Record the log growth it costs, so
	// the in-place overwrites that follow can be shown to add nothing.
	if err := s.Set(key, []byte("val-00000000")); err != nil {
		t.Fatal(err)
	}
	afterInsert := s.CheckpointStats().BytesSinceCheckpoint
	if afterInsert <= 0 {
		t.Fatalf("first append recorded no log growth: %d", afterInsert)
	}

	const overwrites = 200
	for i := 1; i <= overwrites; i++ {
		v := []byte(fmt.Sprintf("val-%08d", i)) // same length every time
		if err := s.Set(key, v); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}

	if got := s.InPlaceUpdates(); got != overwrites {
		t.Fatalf("InPlaceUpdates = %d, want %d (every same-size overwrite of the hot key)", got, overwrites)
	}
	if sp := s.Spilled(); sp != 0 {
		t.Fatalf("Spilled = %d, want 0: in-place overwrites should not grow the log", sp)
	}
	if grew := s.CheckpointStats().BytesSinceCheckpoint; grew != afterInsert {
		t.Fatalf("log grew under in-place overwrites: bytesSinceCheckpoint %d, want the initial %d", grew, afterInsert)
	}
	v, ok, err := s.Get(key)
	if err != nil || !ok {
		t.Fatalf("Get(hot-counter) ok=%v err=%v", ok, err)
	}
	if want := fmt.Sprintf("val-%08d", overwrites); string(v) != want {
		t.Fatalf("Get = %q, want the last write %q", v, want)
	}
}

// TestM7DifferentSizeAppends checks the decision procedure's other arm (doc 04 section
// 7.1 step 4): a SET whose value differs in size from the current record appends and
// repoints rather than overwriting in place, and the value still reads back correct.
func TestM7DifferentSizeAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "diffsize.hlog")
	s := mustStore(t, inPlaceTunables(path, DurabilityNormal))
	defer s.Close()

	key := []byte("k")
	if err := s.Set(key, []byte("aaaa")); err != nil { // insert (append)
		t.Fatal(err)
	}
	if err := s.Set(key, []byte("bbbbbb")); err != nil { // different size: append, not in-place
		t.Fatal(err)
	}
	if got := s.InPlaceUpdates(); got != 0 {
		t.Fatalf("InPlaceUpdates = %d after two different-size writes, want 0", got)
	}
	if err := s.Set(key, []byte("cccccc")); err != nil { // now same size as current: in-place
		t.Fatal(err)
	}
	if got := s.InPlaceUpdates(); got != 1 {
		t.Fatalf("InPlaceUpdates = %d after a same-size overwrite, want 1", got)
	}
	v, ok, err := s.Get(key)
	if err != nil || !ok || string(v) != "cccccc" {
		t.Fatalf("Get = %q ok=%v err=%v, want cccccc", v, ok, err)
	}
}

// TestM7InPlaceDisabledOffDurableEvicting confirms the gate: the in-place overwrite is
// enabled only on the durable eviction-possible profile (doc 04 section 7.3). A memory-
// only store, a Dir scratch store, and a durable full-resident store all keep appending,
// so the counter stays zero and the value is still correct.
func TestM7InPlaceDisabledOffDurableEvicting(t *testing.T) {
	cases := []struct {
		name string
		tun  Tunables
	}{
		{"memory-only", DefaultTunables()},
		{"dir-evicting", Tunables{Shards: 2, PageSize: 1 << 12, ResidentPagesPerShard: 1, Dir: "DIR"}},
		{"durable-full-resident", Tunables{Shards: 2, PageSize: 1 << 12, ResidentPagesPerShard: 0, Path: "PATH"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tun := tc.tun
			if tun.Dir == "DIR" {
				tun.Dir = t.TempDir()
			}
			if tun.Path == "PATH" {
				tun.Path = filepath.Join(t.TempDir(), "x.hlog")
			}
			s := mustStore(t, tun)
			defer s.Close()
			key := []byte("k")
			for i := 0; i < 50; i++ {
				if err := s.Set(key, []byte("same-size-val")); err != nil {
					t.Fatal(err)
				}
			}
			if got := s.InPlaceUpdates(); got != 0 {
				t.Fatalf("InPlaceUpdates = %d, want 0 (in-place is off in this profile)", got)
			}
			v, ok, err := s.Get(key)
			if err != nil || !ok || string(v) != "same-size-val" {
				t.Fatalf("Get = %q ok=%v err=%v", v, ok, err)
			}
		})
	}
}

// TestM7CrashAfterInPlaceSynced is the crash gate's first arm (doc 04 section 7.2, doc
// 08 M7 row): an in-place update whose page is then synced recovers the new value. The
// key is written, overwritten in place, and a checkpoint syncs the tail page; a reopen
// (a clean crash for synced data) must read the new value back.
func TestM7CrashAfterInPlaceSynced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "synced.hlog")
	tun := inPlaceTunables(path, DurabilityNormal)
	s := mustStore(t, tun)

	key := []byte("k")
	if err := s.Set(key, []byte("oldv")); err != nil { // append
		t.Fatal(err)
	}
	if err := s.Set(key, []byte("newv")); err != nil { // in-place (same size, tail, unflushed)
		t.Fatal(err)
	}
	if s.InPlaceUpdates() != 1 {
		t.Fatalf("expected the second SET to be in-place, got %d", s.InPlaceUpdates())
	}
	if err := s.Checkpoint(); err != nil { // syncs the tail page, making newv durable
		t.Fatal(err)
	}

	s2 := reopen(t, s, tun)
	defer s2.Close()
	v, ok, err := s2.Get(key)
	if err != nil || !ok || string(v) != "newv" {
		t.Fatalf("after a synced in-place update, recovered Get = %q ok=%v err=%v, want newv", v, ok, err)
	}
}

// TestM7CrashAfterInPlaceUnsynced is the crash gate's second arm (doc 04 section 7.2):
// an in-place update whose page is never synced recovers the previous durable value, with
// no torn half-update. The key's first value is forced durable in a sealed-and-synced
// page, then a fresh record for the key is appended and overwritten in place in the
// unsynced tail; a crash that loses that tail must recover the previous durable value,
// because the in-place bytes never reached the device.
//
// The crash is modelled with a frozen file image, not a Close. A clean Close is a graceful
// shutdown that flushes the resident tail (so M10 can assert every acknowledged write
// survives a reopen), which would make the unsynced in-place bytes durable. To lose them the
// way a real crash does, the test snapshots the file at its last sync barrier, which is the
// seal that made durv durable, and recovers from that image: the unsynced tail records are
// written into the resident page after the snapshot and so are absent from it.
func TestM7CrashAfterInPlaceUnsynced(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "unsynced.hlog")
	tun := inPlaceTunables(path, DurabilityNormal)
	s := mustStore(t, tun)

	// Freeze a byte-for-byte image of the file at each sync barrier. The last seal during the
	// filler loop overwrites it last, so after the loop the image holds every synced page
	// (durv among them) and nothing from the still-resident tail.
	var frozen []byte
	s.df.syncHook = func(f *os.File) error {
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		buf := make([]byte, fi.Size())
		if _, err := f.ReadAt(buf, 0); err != nil {
			return err
		}
		frozen = buf
		return platformSyncData(f)
	}

	key := []byte("k")
	if err := s.Set(key, []byte("durv")); err != nil {
		t.Fatal(err)
	}
	// Seal and sync the page holding durv by writing filler keys until the shard spills,
	// which under Normal means earlier pages have sealed and synced. durv is now durable.
	for i := 0; s.Spilled() == 0 && i < 100000; i++ {
		if err := s.Set([]byte(fmt.Sprintf("filler-%06d", i)), []byte("payload-payload-payload")); err != nil {
			t.Fatal(err)
		}
	}
	if s.Spilled() == 0 {
		t.Fatal("no page spilled; durv was not forced durable")
	}
	if frozen == nil {
		t.Fatal("no sync barrier captured; durv was not forced durable")
	}
	// Stop updating the image: the unsynced in-place writes that follow must not be captured,
	// just as a crash would not have flushed them.
	s.df.syncHook = nil

	before := s.InPlaceUpdates()
	// A fresh record for key lands in the unsynced tail (its old record is below the
	// read-only boundary now, so this appends), then a same-size overwrite of it goes
	// in place, still in the unsynced tail.
	if err := s.Set(key, []byte("tmp1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(key, []byte("tmp2")); err != nil { // same size as tmp1: in-place
		t.Fatal(err)
	}
	if s.InPlaceUpdates() == before {
		t.Fatal("expected the tmp2 SET to be an in-place update of the unsynced tail record")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Recover from the frozen image: a crash that lost the unsynced tail. Recovery must land
	// on the previous durable value, never a torn record.
	fp := filepath.Join(dir, "unsynced-frozen.hlog")
	if err := os.WriteFile(fp, frozen, 0o644); err != nil {
		t.Fatal(err)
	}
	rt := tun
	rt.Path = fp
	s2, err := New(rt)
	if err != nil {
		t.Fatalf("recover frozen image: %v", err)
	}
	defer s2.Close()
	v, ok, err := s2.Get(key)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if !ok || string(v) != "durv" {
		t.Fatalf("after an unsynced in-place update, recovered Get = %q ok=%v, want the previous durable durv", v, ok)
	}
	// A torn tail on the unsynced page is acceptable; recovery stopping cleanly at it is
	// what the successful Get above proves (no torn half-update reached the durable value).
}

// TestM7ConcurrentInPlaceReaders is the -race gate for the in-place read path (doc 04
// section 7.3): a writer overwrites a small set of hot keys in place with same-size
// values drawn from a fixed set, while many readers GET them concurrently. The in-place
// overwrite rewrites live value bytes, so a lock-free read would race them; the read path
// takes the shard read lock instead, which must keep every observed value a complete
// member of the value set (never a torn mix) and keep the race detector silent.
func TestM7ConcurrentInPlaceReaders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.hlog")
	s := mustStore(t, inPlaceTunables(path, DurabilityNormal))
	defer s.Close()

	const nkeys = 16
	key := func(i int) []byte { return []byte(fmt.Sprintf("hot-%03d", i)) }
	// A fixed set of equal-length values, so every overwrite is a same-size in-place
	// update and any one of them is a legitimate point-in-time read.
	vals := [][]byte{
		[]byte("VALUE-AAAAAA"),
		[]byte("VALUE-BBBBBB"),
		[]byte("VALUE-CCCCCC"),
		[]byte("VALUE-DDDDDD"),
	}
	valid := func(b []byte) bool {
		for _, v := range vals {
			if bytes.Equal(b, v) {
				return true
			}
		}
		return false
	}
	for i := 0; i < nkeys; i++ {
		if err := s.Set(key(i), vals[0]); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: cycle each hot key through the same-size value set, all in place.
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			for i := 0; i < nkeys; i++ {
				if err := s.Set(key(i), vals[(n+i)%len(vals)]); err != nil {
					return
				}
			}
			n++
		}
	}()

	const readers = 8
	const perReader = 20000
	errCh := make(chan error, readers)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rd := s.NewReader()
			for n := 0; n < perReader; n++ {
				i := (seed*31 + n) % nkeys
				got, found, err := rd.Get(key(i))
				if err != nil {
					errCh <- fmt.Errorf("reader %d: Get %d: %v", seed, i, err)
					return
				}
				if !found {
					errCh <- fmt.Errorf("reader %d: key %d missing", seed, i)
					return
				}
				if !valid(got) {
					errCh <- fmt.Errorf("reader %d: key %d = %q is not a complete value (torn in-place read)", seed, i, got)
					return
				}
			}
			errCh <- nil
		}(r)
	}

	var firstErr error
	for r := 0; r < readers; r++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	close(stop)
	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	if s.InPlaceUpdates() == 0 {
		t.Fatal("expected the writer to take the in-place path during the run")
	}
}

// TestM7InPlaceRoundTripUnderRecovery checks the LSN re-stamp keeps last-writer-wins
// across recovery (doc 04 section 7.1, 2.3): many same-size overwrites, a checkpoint to
// make the final value durable, then a reopen recovers exactly the last write.
func TestM7InPlaceRoundTripUnderRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rt.hlog")
	tun := inPlaceTunables(path, DurabilityNormal)
	s := mustStore(t, tun)

	keys := []string{"alpha", "beta", "gamma"}
	for _, k := range keys {
		if err := s.Set([]byte(k), []byte("v-000")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 60; i++ {
		for _, k := range keys {
			if err := s.Set([]byte(k), []byte(fmt.Sprintf("v-%03d", i))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if s.InPlaceUpdates() == 0 {
		t.Fatal("expected in-place updates across the overwrite loop")
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	s2 := reopen(t, s, tun)
	defer s2.Close()
	for _, k := range keys {
		v, ok, err := s2.Get([]byte(k))
		if err != nil || !ok || !bytes.Equal(v, []byte("v-060")) {
			t.Fatalf("recovered %s = %q ok=%v err=%v, want the last write v-060", k, v, ok, err)
		}
	}
}
