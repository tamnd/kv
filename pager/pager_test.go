package pager

import (
	"bytes"
	"testing"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
)

// newTestPager creates a fresh in-memory database and returns its pager.
func newTestPager(t *testing.T, opts Options) (*vfs.Mem, *Pager) {
	t.Helper()
	fs := vfs.NewMem()
	p, err := Create(fs, "test.kv", opts)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return fs, p
}

func TestCreateThenOpenRoundTrip(t *testing.T) {
	fs, p := newTestPager(t, Options{PageSize: 4096, Engine: format.EngineBTree})
	if p.PageSize() != 4096 {
		t.Fatalf("page size = %d, want 4096", p.PageSize())
	}
	if p.DBSize() != 1 {
		t.Fatalf("fresh db size = %d, want 1", p.DBSize())
	}
	if err := p.Checkpoint(0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if p2.PageSize() != 4096 {
		t.Fatalf("reopened page size = %d, want 4096", p2.PageSize())
	}
	if p2.Header().Engine != format.EngineBTree {
		t.Fatalf("reopened engine = %v", p2.Header().Engine)
	}
}

// TestPageRoundTripThroughEviction writes more distinct pages than the pool holds
// so every page is forced out to the main file and read back, exercising CLOCK
// eviction and dirty write-back.
func TestPageRoundTripThroughEviction(t *testing.T) {
	fs, p := newTestPager(t, Options{PageSize: 4096, CacheFrames: 8})
	const npages = 64

	nums := make([]uint32, npages)
	for i := 0; i < npages; i++ {
		pgno, fr, err := p.Allocate()
		if err != nil {
			t.Fatalf("allocate %d: %v", i, err)
		}
		nums[i] = pgno
		// Stamp the page with its own number so we can verify it later.
		writePattern(fr.Data(), pgno)
		p.Unpin(fr, true)
	}

	// Read every page back; most have been evicted and must be reloaded.
	for i, pgno := range nums {
		fr, err := p.Get(pgno, Read)
		if err != nil {
			t.Fatalf("get %d: %v", pgno, err)
		}
		if !checkPattern(fr.Data(), pgno) {
			t.Fatalf("page %d (alloc #%d) corrupt after eviction", pgno, i)
		}
		p.Unpin(fr, false)
	}
	_ = fs
}

// TestCheckpointDurability checkpoints, simulates a crash that drops unsynced
// bytes, reopens, and verifies every checkpointed page survived.
func TestCheckpointDurability(t *testing.T) {
	fs, p := newTestPager(t, Options{PageSize: 4096, CacheFrames: 8})
	const npages = 40

	nums := make([]uint32, npages)
	for i := 0; i < npages; i++ {
		pgno, fr, _ := p.Allocate()
		nums[i] = pgno
		writePattern(fr.Data(), pgno)
		p.Unpin(fr, true)
	}
	if err := p.Checkpoint(42); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	// Dirty one more page but do NOT checkpoint it.
	pgno, fr, _ := p.Allocate()
	writePattern(fr.Data(), 0xDEAD)
	p.Unpin(fr, true)
	uncheckpointed := pgno

	fs.Crash()

	p2, err := Open(fs, "test.kv", Options{CacheFrames: 8})
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	if got := p2.CheckpointLSN(); got != 42 {
		t.Fatalf("checkpoint LSN = %d, want 42", got)
	}
	for _, pgno := range nums {
		fr, err := p2.Get(pgno, Read)
		if err != nil {
			t.Fatalf("get %d after crash: %v", pgno, err)
		}
		if !checkPattern(fr.Data(), pgno) {
			t.Fatalf("checkpointed page %d lost after crash", pgno)
		}
		p2.Unpin(fr, false)
	}
	// The uncheckpointed page's contents are undefined after the crash; we only
	// assert that the durable set is intact, which the loop above proved.
	_ = uncheckpointed
}

// TestAllocateFreeReuse verifies a freed page number is handed back out before
// the file grows again, and that the freelist survives a checkpoint+reopen.
func TestAllocateFreeReuse(t *testing.T) {
	fs, p := newTestPager(t, Options{PageSize: 4096, CacheFrames: 16})

	var freed []uint32
	for i := 0; i < 10; i++ {
		pgno, fr, _ := p.Allocate()
		p.Unpin(fr, true)
		freed = append(freed, pgno)
	}
	sizeBefore := p.DBSize()
	for _, pgno := range freed {
		p.Free(pgno)
	}
	// Next allocations should reuse freed pages, not grow the file.
	for i := 0; i < 10; i++ {
		pgno, fr, _ := p.Allocate()
		p.Unpin(fr, false)
		if pgno > sizeBefore {
			t.Fatalf("allocation grew file to %d past %d despite free pages", pgno, sizeBefore)
		}
	}

	// Free a couple, checkpoint, reopen, and confirm the freelist persisted.
	p.Free(3)
	p.Free(5)
	if err := p.Checkpoint(7); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	freeCount := len(p.free)
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if len(p2.free) != freeCount {
		t.Fatalf("freelist after reopen has %d pages, want %d", len(p2.free), freeCount)
	}
}

// TestBufferPoolExhaustion confirms that pinning every frame and asking for one
// more fails cleanly instead of corrupting state.
func TestBufferPoolExhaustion(t *testing.T) {
	_, p := newTestPager(t, Options{PageSize: 4096, CacheFrames: 4})
	var pinned []*Frame
	for i := 0; i < 4; i++ {
		_, fr, err := p.Allocate()
		if err != nil {
			t.Fatalf("allocate %d: %v", i, err)
		}
		pinned = append(pinned, fr)
	}
	if _, _, err := p.Allocate(); err == nil {
		t.Fatalf("expected exhaustion error with all frames pinned")
	}
	// Unpin one and the next allocation succeeds.
	p.Unpin(pinned[0], false)
	if _, fr, err := p.Allocate(); err != nil {
		t.Fatalf("allocate after unpin: %v", err)
	} else {
		p.Unpin(fr, false)
	}
}

// writePattern fills a page with a repeating big-endian encoding of seed so a
// later read can detect corruption or a stale frame.
func writePattern(p []byte, seed uint32) {
	for i := 0; i+4 <= len(p); i += 4 {
		p[i] = byte(seed >> 24)
		p[i+1] = byte(seed >> 16)
		p[i+2] = byte(seed >> 8)
		p[i+3] = byte(seed)
	}
}

func checkPattern(p []byte, seed uint32) bool {
	want := []byte{byte(seed >> 24), byte(seed >> 16), byte(seed >> 8), byte(seed)}
	for i := 0; i+4 <= len(p); i += 4 {
		if !bytes.Equal(p[i:i+4], want) {
			return false
		}
	}
	return true
}
