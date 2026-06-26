package betree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// This file gates the M8 slice that wires the M6 pooled-transients substrate (pool.go) onto the node
// encode call sites (the encBufGet helper in tree.go). The pool reuses one page-sized scratch buffer
// across encodes instead of allocating a fresh one per encode, so the gate has two parts: the
// correctness invariant the pooling must not break (a persisted page's slack is always zero, so the
// on-disk bytes stay byte-identical to the make([]byte, ...) it replaced and no prior key or value
// lingers in page slack), proven by a deterministic test, and the allocation win itself, measured by a
// write-path benchmark. The existing conformance, paged, and reopen suites already run every write
// through the pooled encoders, so this file adds only what those do not directly assert.

// TestEncBufGetZeroingIsClean asserts that a buffer drawn for a persisted encode (zero=true) is always
// fully zero, no matter what a prior pooled use left in it. The buffer is dirtied and released between
// gets, so whether the pool hands back the dirtied buffer (the zero clears it) or a fresh one (a fresh
// make is already zero), a zero=true get is clean either way. This is the determinism-and-no-leak
// property the persisted write paths depend on: the encoder leaves the page tail untouched, so the
// buffer it writes into must arrive zeroed.
func TestEncBufGetZeroingIsClean(t *testing.T) {
	tr := newTreeSized(t, vfs.NewMem(), 512)
	size := tr.pgr.UsablePageSize()

	for round := 0; round < 64; round++ {
		// Dirty a buffer through the discard path (zero=false) and release it, so the pool may hand
		// this same backing array to the next get.
		dirty, dirtyRef := tr.encBufGet(false)
		for i := range dirty {
			dirty[i] = 0xAB
		}
		tr.encBufPut(dirtyRef)

		// A persisted-path get must come back fully zero regardless.
		buf, ref := tr.encBufGet(true)
		if len(buf) != size {
			t.Fatalf("round %d: buffer length %d, want usable page size %d", round, len(buf), size)
		}
		for i, b := range buf {
			if b != 0 {
				t.Fatalf("round %d: buffer byte %d is %#x, want 0 (persisted slack must be clean)", round, i, b)
			}
		}
		tr.encBufPut(ref)
	}
}

// TestPooledEncodeReopenRoundTrips builds a multi-level tree entirely through the pooled encoders, then
// closes and reopens it and reads every key back, so a bug in the buffer reuse (a stale-slack page, a
// buffer handed to two encodes at once, a wrong reslice length) would corrupt a page on disk and the
// reopened read would diverge. It is the end-to-end on-disk check the unit zeroing test cannot give.
func TestPooledEncodeReopenRoundTrips(t *testing.T) {
	fs := vfs.NewMem()
	tr := newTreeSized(t, fs, 512)

	const n = 3000
	const perBatch = 150
	ver := uint64(0)
	for base := 0; base < n; base += perBatch {
		ver++
		b := engine.NewWriteBatch(ver)
		for i := base; i < base+perBatch && i < n; i++ {
			b.Set([]byte(fmt.Sprintf("key%06d", i)), []byte(fmt.Sprintf("val%06d", i)))
		}
		if err := tr.Apply(b, ver); err != nil {
			t.Fatalf("apply at %d: %v", base, err)
		}
	}
	if err := tr.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := tr.pgr.Checkpoint(ver, ver); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := tr.pgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := pager.Open(fs, "test.kv", pager.Options{})
	if err != nil {
		t.Fatalf("reopen pager: %v", err)
	}
	tr2 := New(p2)
	if err := tr2.Open(&engine.Env{}); err != nil {
		t.Fatalf("reopen betree: %v", err)
	}
	rd, err := tr2.NewReader(engine.Snapshot{Version: ver})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%06d", i))
		v, err := rd.Get(k)
		if err != nil {
			t.Fatalf("key %q after pooled-encode reopen: %v", k, err)
		}
		if want := fmt.Sprintf("val%06d", i); string(v) != want {
			t.Fatalf("key %q = %q, want %q", k, v, want)
		}
	}
}

// newTreeSizedB is the benchmark twin of newTreeSized: it opens a betree core over a fresh in-memory
// database with the given page size, taking a *testing.B.
func newTreeSizedB(b *testing.B, pageSize int) *Tree {
	b.Helper()
	p, err := pager.Create(vfs.NewMem(), "bench.kv", pager.Options{
		PageSize:    pageSize,
		CacheFrames: 64,
		Engine:      format.EngineBeta,
	})
	if err != nil {
		b.Fatalf("create pager: %v", err)
	}
	tr := New(p)
	if err := tr.Open(&engine.Env{}); err != nil {
		b.Fatalf("open betree: %v", err)
	}
	return tr
}

// BenchmarkLeafFitsTrial isolates the allocation the pool removes. leafFits is the greedy pack loop's
// hot per-record check: it draws a page-sized scratch buffer, encodes a trial leaf into it, reads the
// fit result, and discards the buffer. Before the pool that buffer was a fresh make([]byte, pageSize)
// every call; with the pool it is reused, so the per-call buffer allocation goes to zero while the
// encode itself stays allocation-free. Run with -benchmem: the allocs/op delta against the stashed
// pre-pool source (one buffer alloc per call) is the win this slice claims, measured where it is real
// rather than where the harness drowns it.
func BenchmarkLeafFitsTrial(b *testing.B) {
	tr := newTreeSizedB(b, 512)
	// A leaf packed near the page limit, so the trial encode does real work each call. The records are
	// built once, outside the timed loop, so the benchmark times only the draw-encode-discard cycle.
	lf := &leaf{bucketSize: defaultBucketSize}
	for i := 0; len(lf.records) < 16; i++ {
		lf.records = append(lf.records, record{
			key: []byte(fmt.Sprintf("k%06d", i)),
			val: []byte("value"),
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tr.leafFits(lf)
	}
}

// BenchmarkApplyAlloc measures the same buffer reuse at the end-to-end write granularity, where the pool
// is one allocation source among many. A small page forces frequent leaf and interior splits, so every
// Apply drives node encodes and trial-fit checks, but the batch construction and key formatting allocate
// far more than the pooled page buffers do. The number is recorded in the build log as the honest
// counterpoint to the microbenchmark: the pool removes a real allocation, and at this granularity that
// allocation is in the noise of the surrounding write work. Run with -benchmem.
func BenchmarkApplyAlloc(b *testing.B) {
	tr := newTreeSizedB(b, 512)
	const perBatch = 64
	ver := uint64(0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ver++
		batch := engine.NewWriteBatch(ver)
		base := i * perBatch
		for k := 0; k < perBatch; k++ {
			key := []byte(fmt.Sprintf("key%09d", base+k))
			batch.Set(key, []byte("v"))
		}
		if err := tr.Apply(batch, ver); err != nil {
			b.Fatalf("apply: %v", err)
		}
	}
}
