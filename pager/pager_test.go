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

// TestIOStatsCountsHitsAndReads checks the buffer pool's traffic counters: a Get served
// from a resident frame is a cache hit and issues no physical read, and a Get for a page
// evicted out of the pool is a miss that issues exactly one physical read. These are the
// numbers read amplification and the cache hit ratio rest on (spec 19, spec 21 §1).
func TestIOStatsCountsHitsAndReads(t *testing.T) {
	_, p := newTestPager(t, Options{PageSize: 4096, CacheFrames: 8})

	// Allocate a first page; it stays resident and dirty in the pool.
	pgno, fr, err := p.Allocate()
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	writePattern(fr.Data(), pgno)
	p.Unpin(fr, true)

	// Two Gets of a resident page are two cache hits and zero physical reads.
	base := p.IOStats()
	for i := 0; i < 2; i++ {
		fr, err := p.Get(pgno, Read)
		if err != nil {
			t.Fatalf("get resident: %v", err)
		}
		p.Unpin(fr, false)
	}
	hit := p.IOStats()
	if hit.CacheHits != base.CacheHits+2 {
		t.Fatalf("cache hits = %d, want %d", hit.CacheHits, base.CacheHits+2)
	}
	if hit.PageReads != base.PageReads {
		t.Fatalf("resident gets issued %d physical reads, want 0", hit.PageReads-base.PageReads)
	}

	// Allocate well past the 8-frame pool so the first page is evicted to the file.
	for i := 0; i < 24; i++ {
		_, fr, err := p.Allocate()
		if err != nil {
			t.Fatalf("allocate filler %d: %v", i, err)
		}
		writePattern(fr.Data(), fr.PageNo())
		p.Unpin(fr, true)
	}

	// Getting the now-evicted first page is a miss: exactly one physical read, and the page
	// comes back intact so the read actually happened.
	before := p.IOStats()
	fr, err = p.Get(pgno, Read)
	if err != nil {
		t.Fatalf("get evicted: %v", err)
	}
	if !checkPattern(fr.Data(), pgno) {
		t.Fatalf("evicted page %d came back corrupt", pgno)
	}
	p.Unpin(fr, false)
	after := p.IOStats()
	if after.PageReads != before.PageReads+1 {
		t.Fatalf("miss issued %d physical reads, want 1", after.PageReads-before.PageReads)
	}
	if after.CacheHits != before.CacheHits {
		t.Fatalf("miss counted %d cache hits, want 0", after.CacheHits-before.CacheHits)
	}
}

// TestTruncateTailShrinksFile allocates a run of pages, frees the ones at the very end,
// and confirms TruncateTail hands them back to the file (the page count and on-disk size
// both fall), while a free page buried in the middle is left on the freelist (spec 09 §3.1).
func TestTruncateTailShrinksFile(t *testing.T) {
	fs, p := newTestPager(t, Options{PageSize: 4096, CacheFrames: 64})

	var pages []uint32
	for i := 0; i < 20; i++ {
		pgno, fr, err := p.Allocate()
		if err != nil {
			t.Fatalf("allocate %d: %v", i, err)
		}
		pages = append(pages, pgno)
		p.Unpin(fr, true)
	}
	if err := p.Checkpoint(1); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	sizeBefore := p.DBSize()

	// Free a buried page (must survive) and the top three pages (must be reclaimed).
	p.Free(pages[5])
	top := pages[len(pages)-3:]
	for _, pg := range top {
		p.Free(pg)
	}

	freed, err := p.TruncateTail(0)
	if err != nil {
		t.Fatalf("truncate tail: %v", err)
	}
	if freed != 3 {
		t.Fatalf("freed = %d, want 3 trailing pages", freed)
	}
	if got, want := p.DBSize(), sizeBefore-3; got != want {
		t.Fatalf("page count = %d after truncate, want %d", got, want)
	}
	// The buried free page is still reclaimable for reallocation.
	if p.FreeCount() != 1 {
		t.Fatalf("freelist depth = %d after truncate, want 1 (the buried page)", p.FreeCount())
	}
	// The file on disk shrank to the new high-water mark.
	f, err := fs.Open("test.kv", vfs.OpenReadWrite)
	if err != nil {
		t.Fatalf("open for size: %v", err)
	}
	defer f.Close()
	sz, err := f.Size()
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	if want := int64(p.DBSize()) * int64(p.PageSize()); sz != want {
		t.Fatalf("file size = %d after truncate, want %d", sz, want)
	}
}

// TestTruncateTailBudgetAndReopen confirms a budget caps how many tail pages a single
// call returns, and that the smaller file and surviving freelist round-trip through a
// reopen.
func TestTruncateTailBudgetAndReopen(t *testing.T) {
	fs, p := newTestPager(t, Options{PageSize: 4096, CacheFrames: 64})

	var pages []uint32
	for i := 0; i < 12; i++ {
		pgno, fr, _ := p.Allocate()
		pages = append(pages, pgno)
		p.Unpin(fr, true)
	}
	if err := p.Checkpoint(1); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	// Free the top four pages; reclaim only two this round.
	for _, pg := range pages[len(pages)-4:] {
		p.Free(pg)
	}
	freed, err := p.TruncateTail(2)
	if err != nil {
		t.Fatalf("truncate tail: %v", err)
	}
	if freed != 2 {
		t.Fatalf("freed = %d under budget 2, want 2", freed)
	}
	sizeAfter := p.DBSize()
	freeAfter := p.FreeCount()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := Open(fs, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := p2.DBSize(); got != sizeAfter {
		t.Fatalf("page count after reopen = %d, want %d", got, sizeAfter)
	}
	if got := p2.FreeCount(); got != freeAfter {
		t.Fatalf("freelist depth after reopen = %d, want %d", got, freeAfter)
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
