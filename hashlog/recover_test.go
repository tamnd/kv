package hashlog

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// cloneModel deep-copies a model so a snapshot of it taken mid-workload is not mutated by
// later writes. The CrashSweep needs the model frozen at each sync boundary it captures.
func cloneModel(m *model) *model {
	c := newModel()
	for k, v := range m.live {
		c.live[k] = append([]byte(nil), v...)
	}
	return c
}

// recTunables builds a durable store small enough that a modest workload rolls pages,
// spills, and checkpoints, so recovery exercises the snapshot-plus-delta join rather
// than a single resident page.
func recTunables(path string, d Durability) Tunables {
	return Tunables{
		Shards:                4,
		PageSize:              512,
		ExtentSize:            512,
		ResidentPagesPerShard: 2,
		Path:                  path,
		Durability:            d,
	}
}

// reopen closes a store and opens a fresh one at the same path with the same tunables,
// which runs recovery. It is the basic durability assertion's vehicle: what survives a
// clean close-and-reopen is what recovery reconstructs from the file.
func reopen(t *testing.T, s *Store, tun Tunables) *Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2, err := New(tun)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return s2
}

// assertStoreMatches checks the recovered store against the model: the same live key
// set and the same value for every key. It is the recovered-set-equals-durable-prefix
// assertion (doc 05 section 8) for a clean close, where the durable prefix is the whole
// acknowledged workload.
func assertStoreMatches(t *testing.T, s *Store, m *model) {
	t.Helper()
	if got := s.Len(); got != len(m.live) {
		t.Fatalf("Len() = %d, want %d", got, len(m.live))
	}
	for k, want := range m.live {
		v, ok, err := s.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if !ok {
			t.Fatalf("Get(%q) missing, want %q", k, want)
		}
		if string(v) != string(want) {
			t.Fatalf("Get(%q) = %q, want %q", k, v, want)
		}
	}
}

// TestM5ReopenFullRoundTrip is the central durability assertion under Full: every
// acknowledged write survives a close and reopen, with overwrites and deletes resolved
// last-writer-wins. No checkpoint is taken, so recovery rebuilds every shard's index by
// replaying the whole log from the start.
func TestM5ReopenFullRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "full.hlog")
	tun := recTunables(path, DurabilityFull)
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}
	m := newModel()
	for i := 0; i < 3000; i++ {
		k := []byte(fmt.Sprintf("key-%d", i%900))
		v := []byte(fmt.Sprintf("val-%d-%d", i, i%900))
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		m.set(k, v)
		if i%7 == 0 {
			dk := []byte(fmt.Sprintf("key-%d", (i+13)%900))
			if err := s.Delete(dk); err != nil {
				t.Fatal(err)
			}
			m.del(dk)
		}
	}
	if s.Spilled() == 0 {
		t.Fatal("workload did not spill; recovery would not exercise the disk path")
	}

	s2 := reopen(t, s, tun)
	defer s2.Close()
	assertStoreMatches(t, s2, m)

	// The LSN resumes past everything recovered: a fresh write gets a strictly greater
	// LSN than any on disk, so the post-recovery order extends the pre-crash order.
	before := s2.df.lsn.Load()
	if err := s2.Set([]byte("after-recovery"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if after := s2.df.lsn.Load(); after != before+1 {
		t.Fatalf("LSN did not resume by one: before %d, after %d", before, after)
	}
}

// TestM5ReopenWithCheckpoint exercises the two-artifact join: a checkpoint snapshots the
// index midway, then more writes extend the log past the frontier. Recovery loads the
// snapshot and replays only the delta, so the recovered store is correct and the
// replayed-record count is the post-checkpoint work, not the whole log.
func TestM5ReopenWithCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt.hlog")
	tun := recTunables(path, DurabilityFull)
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}
	m := newModel()
	put := func(i int) {
		k := []byte(fmt.Sprintf("k%d", i%600))
		v := []byte(fmt.Sprintf("v%d-%d", i, i%600))
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		m.set(k, v)
	}
	for i := 0; i < 2000; i++ {
		put(i)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	const delta = 400
	for i := 2000; i < 2000+delta; i++ {
		put(i)
	}

	s2 := reopen(t, s, tun)
	defer s2.Close()
	assertStoreMatches(t, s2, m)

	rs := s2.RecoveryStats()
	if rs.ReplayedRecords == 0 {
		t.Fatal("replayed zero records but there was a post-checkpoint delta")
	}
	if rs.ReplayedRecords > int64(delta) {
		t.Fatalf("replayed %d records, more than the %d-record delta; the frontier skip did not work",
			rs.ReplayedRecords, delta)
	}
}

// TestM5ReopenNormalSpilled checks recovery under the Normal dial, where the log is
// synced at seal boundaries. A clean close after a Normal workload leaves every sealed
// page durable, and the spilled values must read back from disk after recovery.
func TestM5ReopenNormalSpilled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "normal.hlog")
	tun := recTunables(path, DurabilityNormal)
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}
	m := newModel()
	for i := 0; i < 2500; i++ {
		k := []byte(fmt.Sprintf("n%d", i%700))
		v := []byte(fmt.Sprintf("payload-%d-%d", i, i%700))
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		m.set(k, v)
	}
	// Flush every shard's tail so a clean reopen sees the last partial page too. Under
	// Normal a clean Checkpoint advances each frontier to the tail.
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if s.Spilled() == 0 {
		t.Fatal("Normal workload did not spill")
	}

	s2 := reopen(t, s, tun)
	defer s2.Close()
	assertStoreMatches(t, s2, m)
}

// TestM5RecoverIsIdempotent confirms a second recovery over the same file yields the
// same store: open, reopen, reopen again, and the model still matches. Recovery's apply
// is last-writer-wins on LSN, so re-running it lands the same index (doc 05 section 6).
func TestM5RecoverIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idem.hlog")
	tun := recTunables(path, DurabilityFull)
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}
	m := newModel()
	for i := 0; i < 1200; i++ {
		k := []byte(fmt.Sprintf("i%d", i%300))
		v := []byte(fmt.Sprintf("w%d", i))
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		m.set(k, v)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	s2 := reopen(t, s, tun)
	assertStoreMatches(t, s2, m)
	s3 := reopen(t, s2, tun)
	defer s3.Close()
	assertStoreMatches(t, s3, m)
}

// TestM5CrashSweepFull is the crash-injection sweep doc 05 section 8 (D15) asks for:
// freeze the file image at sampled fsync boundaries, recover each frozen image into a
// fresh store, and assert it equals the durable prefix at that boundary exactly. Under
// Full every acknowledged SET is synced before it returns, so the image frozen at sync c
// holds records 1..c and nothing more, which must reconstruct the model after c sets.
// The sweep also checks the LSN resumes past the prefix and that a second recovery over
// the same image is identical (deterministic and idempotent).
func TestM5CrashSweepFull(t *testing.T) {
	dir := t.TempDir()
	tun := Tunables{Shards: 4, PageSize: 256, ExtentSize: 256, ResidentPagesPerShard: 2, Path: filepath.Join(dir, "live.hlog"), Durability: DurabilityFull}
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}

	// Sets only, so under Full each op is exactly one sync and the sync counter equals the
	// set index. Keys repeat (i%500) so the workload overwrites and the durable prefix is a
	// last-writer-wins set, not a growing append.
	const n = 1500
	freezeAt := map[int64]bool{n / 4: true, n / 2: true, (3 * n) / 4: true, n: true}
	frozen := map[int64][]byte{}
	s.df.syncHook = func(f *os.File) error {
		c := s.df.syncCount.Load()
		if freezeAt[c] {
			fi, err := f.Stat()
			if err != nil {
				return err
			}
			buf := make([]byte, fi.Size())
			if _, err := f.ReadAt(buf, 0); err != nil {
				return err
			}
			frozen[c] = buf
		}
		return platformSyncData(f)
	}

	m := newModel()
	want := map[int64]*model{}
	for i := 1; i <= n; i++ {
		k := []byte(fmt.Sprintf("s%d", i%500))
		v := []byte(fmt.Sprintf("v%d", i))
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		m.set(k, v)
		if freezeAt[int64(i)] {
			want[int64(i)] = cloneModel(m)
		}
	}
	if s.Spilled() == 0 {
		t.Fatal("sweep workload did not spill; recovery would not exercise the disk path")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	cuts := make([]int64, 0, len(frozen))
	for c := range frozen {
		cuts = append(cuts, c)
	}
	sort.Slice(cuts, func(i, j int) bool { return cuts[i] < cuts[j] })
	if len(cuts) != len(freezeAt) {
		t.Fatalf("captured %d frozen images, wanted %d", len(cuts), len(freezeAt))
	}

	for _, c := range cuts {
		// Recover the frozen image: no acknowledged write is lost and nothing past the cut
		// appears, so the recovered store equals the model after exactly c sets.
		p := filepath.Join(dir, fmt.Sprintf("frozen-%d.hlog", c))
		if err := os.WriteFile(p, frozen[c], 0o644); err != nil {
			t.Fatal(err)
		}
		rt := tun
		rt.Path = p
		rt.Durability = DurabilityFull
		rs, err := New(rt)
		if err != nil {
			t.Fatalf("recover frozen image at sync %d: %v", c, err)
		}
		assertStoreMatches(t, rs, want[c])
		before := rs.df.lsn.Load()
		if before != uint64(c) {
			t.Fatalf("recovered LSN high-water %d at sync %d, want %d", before, c, c)
		}
		if err := rs.Close(); err != nil {
			t.Fatal(err)
		}

		// Idempotent: a second recovery over the same image (rs only read it) lands the same
		// store. This runs before any write so the image is still the durable prefix.
		rs2, err := New(rt)
		if err != nil {
			t.Fatalf("second recovery at sync %d: %v", c, err)
		}
		assertStoreMatches(t, rs2, want[c])

		// The LSN resumes past the prefix: c records carry LSNs 1..c, so a fresh write gets
		// c+1 and extends the pre-crash order rather than colliding with it. This mutates
		// the file, so it is the last thing done with this image.
		if err := rs2.Set([]byte("post"), []byte("x")); err != nil {
			t.Fatal(err)
		}
		if after := rs2.df.lsn.Load(); after != before+1 {
			t.Fatalf("LSN did not resume by one at sync %d: before %d, after %d", c, before, after)
		}
		if err := rs2.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// BenchmarkRecover measures open-time recovery: a store is filled, checkpointed, and
// extended past the frontier once, then the loop reopens it (which runs recovery) and
// closes it. It reports the per-open recovery latency, the number that has to stay small
// for a durable store to start fast (doc 08 section 5, the M5 benchmark row).
func BenchmarkRecover(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.hlog")
	tun := Tunables{Shards: 8, PageSize: 4096, ExtentSize: 4096, ResidentPagesPerShard: 64, Path: path, Durability: DurabilityNone}
	s, err := New(tun)
	if err != nil {
		b.Fatal(err)
	}
	const keys = 50000
	for i := 0; i < keys; i++ {
		if err := s.Set([]byte(fmt.Sprintf("key-%08d", i)), []byte(fmt.Sprintf("val-%d", i))); err != nil {
			b.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < keys/10; i++ {
		if err := s.Set([]byte(fmt.Sprintf("key-%08d", i)), []byte(fmt.Sprintf("upd-%d", i))); err != nil {
			b.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		rs, err := New(tun)
		if err != nil {
			b.Fatal(err)
		}
		if err := rs.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// buildRecoverableSeed writes a small durable store (a checkpoint plus a post-checkpoint
// delta, with deletes) and returns its file bytes. It is the structurally valid image the
// fail-closed tests mutate, so a corruption reaches deep into the extent scan and the
// per-shard replay rather than bouncing off the superblock check.
func buildRecoverableSeed(t testing.TB, dir string) []byte {
	t.Helper()
	p := filepath.Join(dir, "seed.hlog")
	tun := Tunables{Shards: 2, PageSize: 256, ExtentSize: 256, ResidentPagesPerShard: 2, Path: p, Durability: DurabilityFull}
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if err := s.Set([]byte(fmt.Sprintf("k%d", i%64)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
		if i%9 == 0 {
			if err := s.Delete([]byte(fmt.Sprintf("k%d", (i+3)%64))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for i := 200; i < 280; i++ {
		if err := s.Set([]byte(fmt.Sprintf("k%d", i%64)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestM5RecoverFailClosed is the deterministic, reproducible half of the fail-closed
// gate (doc 05 section 8, doc 08 section 4.4): recovery over a corrupted or truncated
// durable file returns an error or a usable store, but never panics, never reads out of
// bounds, and always terminates. It sweeps single-byte flips, truncations at every extent
// boundary, and page-window smears over a real recovered-from image, each opened under a
// watchdog that fails the test if recovery does not finish in a bounded time. It is the
// gate FuzzRecover backs up: the fuzzer explores wider but its coordinator counter stalls
// on this platform, so the durable assurance lives here where it runs the same way every
// time.
func TestM5RecoverFailClosed(t *testing.T) {
	dir := t.TempDir()
	seed := buildRecoverableSeed(t, dir)
	path := filepath.Join(dir, "case.hlog")
	tun := Tunables{Shards: 2, PageSize: 256, ExtentSize: 256, ResidentPagesPerShard: 2, Path: path, Durability: DurabilityNone}

	// open writes the case bytes and recovers them under a watchdog. A panic in recovery
	// surfaces as a test failure through the deferred recover; a hang trips the timer. It
	// returns nothing: the assertion is simply that this completes without panic or hang.
	open := func(data []byte, label string) {
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("recovery panicked on %s: %v", label, r)
				}
			}()
			s, err := New(tun)
			if err != nil {
				return // fail-closed: a clean rejection is the expected outcome
			}
			_, _, _ = s.Get([]byte("k0"))
			_ = s.Close()
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("recovery did not terminate on %s within 5s", label)
		}
	}

	// The untouched seed must recover cleanly: the corruption cases are only meaningful
	// against a baseline that works.
	open(seed, "untouched seed")

	// Single-byte flips across the whole image, each to a low and a high value, so both a
	// zeroed field and an all-ones field are exercised at every offset.
	for off := 0; off < len(seed); off++ {
		for _, v := range []byte{0x00, 0xFF} {
			if seed[off] == v {
				continue
			}
			d := append([]byte(nil), seed...)
			d[off] = v
			open(d, fmt.Sprintf("flip off=%d val=0x%02x", off, v))
		}
	}

	// Truncations at every 64-byte boundary: a crash mid-grow or a short write leaves the
	// file ending inside an extent header or a record, which the bounds checks must reject
	// rather than index past.
	for cut := 0; cut < len(seed); cut += 64 {
		open(seed[:cut], fmt.Sprintf("truncate at %d", cut))
	}

	// Page-window smears: zero, then fill with 0xAA, each PageSize-aligned window in turn,
	// the shape of a torn or overwritten page.
	const window = 256
	for start := 0; start < len(seed); start += window {
		end := start + window
		if end > len(seed) {
			end = len(seed)
		}
		zero := append([]byte(nil), seed...)
		for i := start; i < end; i++ {
			zero[i] = 0
		}
		open(zero, fmt.Sprintf("zero window %d:%d", start, end))
		smear := append([]byte(nil), seed...)
		for i := start; i < end; i++ {
			smear[i] = 0xAA
		}
		open(smear, fmt.Sprintf("smear window %d:%d", start, end))
	}
}

// FuzzRecover asserts recovery is fail-closed on arbitrary file bytes: New over a
// corrupted or truncated durable file returns an error or a usable store, but never
// panics and never reads out of bounds (doc 05 section 8, doc 08 section 4.4). The seed
// is a real recovered-from file, so the fuzzer mutates a structurally valid image and
// reaches deep into the extent scan and the per-shard replay, not just the superblock
// reject.
func FuzzRecover(f *testing.F) {
	f.Add(buildRecoverableSeed(f, f.TempDir()))
	f.Add([]byte{})
	f.Add(make([]byte, 4096))

	// One reused path per worker process, not a fresh temp dir per exec: the per-exec
	// mkdir and recursive cleanup dominated wall-clock and starved the fuzzer of execs.
	// os.WriteFile truncates, so each input fully replaces the last.
	p := filepath.Join(f.TempDir(), "fuzz.hlog")
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		// Reopen under None: recovery reads the same bytes regardless of the durability
		// dial, and skipping the per-close fsync keeps the fuzzer fast enough to cover the
		// extent scan and replay rather than stalling on device barriers.
		tun := Tunables{Shards: 2, PageSize: 256, ExtentSize: 256, ResidentPagesPerShard: 2, Path: p, Durability: DurabilityNone}
		// The contract is no panic and no out-of-bounds read. Either New rejects the file
		// (fail-closed) or it returns a store, which must then be safe to query and close.
		s, err := New(tun)
		if err != nil {
			return
		}
		_, _, _ = s.Get([]byte("k0"))
		_ = s.Close()
	})
}

// TestM5TornTailDiscarded corrupts the very tail of a shard's last log page with
// non-zero garbage (a crash mid-append), reopens, and asserts the torn record is not
// applied while every good record before it survives. The CRC-stop is the durable
// frontier's safety net (doc 05 section 7).
func TestM5TornTailDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.hlog")
	// One shard so the tail is unambiguous and the LSNs are contiguous.
	tun := Tunables{Shards: 1, PageSize: 512, ExtentSize: 512, ResidentPagesPerShard: 4, Path: path, Durability: DurabilityFull}
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}
	m := newModel()
	const n = 400
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("t%d", i))
		v := []byte(fmt.Sprintf("good-%d", i))
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		m.set(k, v)
	}
	sh := s.shards[0]
	tailPage := sh.tailPage
	tailExt := sh.pageExtent[tailPage]
	tailFill := sh.pageFill[tailPage]
	bodyOff := s.df.logBodyOffset(tailExt)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Smear non-zero garbage right after the last good record on the tail page, the
	// shape of a half-written record a crash leaves behind.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	garbage := []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04}
	if _, err := f.WriteAt(garbage, bodyOff+int64(tailFill)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	// Every good record survived and the garbage was rejected at the CRC-stop.
	assertStoreMatches(t, s2, m)
	rs := s2.RecoveryStats()
	if rs.TornTailOffset[0] < 0 {
		t.Fatal("expected a torn-tail offset to be recorded, got none")
	}
	if want := tailPage*int64(tun.PageSize) + int64(tailFill); rs.TornTailOffset[0] != want {
		t.Fatalf("torn-tail offset = %d, want %d (just past the last good record)", rs.TornTailOffset[0], want)
	}
}
