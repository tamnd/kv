package lsm

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// newSegPager returns an opened pager over an in-memory file for the segment tests.
func newSegPager(t *testing.T) *pager.Pager {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "seg.kv", pager.Options{
		PageSize:    4096,
		CacheFrames: 64,
		Engine:      format.EngineLSM,
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	return p
}

// cell is one (internalKey, value) pair for building a test source.
type cell struct {
	ik  []byte
	val []byte
}

// sourceOf returns a src callback that emits the cells in order.
func sourceOf(cells []cell) func(func(ik, val []byte) bool) {
	return func(emit func(ik, val []byte) bool) {
		for _, c := range cells {
			if !emit(c.ik, c.val) {
				return
			}
		}
	}
}

// drain scans a segment and returns its cells as strings for comparison.
func drain(t *testing.T, pgr *pager.Pager, seg *segment) []string {
	t.Helper()
	var got []string
	if err := seg.scan(pgr, func(ik, val []byte) bool {
		got = append(got, fmt.Sprintf("%s@%d=%s",
			format.UserKey(ik), format.Version(ik), val))
		return true
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return got
}

// TestSegmentRoundTrip writes a small ordered run, scans it back, and confirms the
// cells and the recorded metadata survive intact.
func TestSegmentRoundTrip(t *testing.T) {
	pgr := newSegPager(t)
	cells := []cell{
		{ik("apple", 7), []byte("a7")},
		{ik("apple", 3), []byte("a3")},
		{ik("banana", 5), []byte("b5")},
		{ik("cherry", 1), []byte("c1")},
	}
	seg, err := writeSegment(pgr, bloomBitsPerKey, filterBloom, sourceOf(cells))
	if err != nil {
		t.Fatalf("writeSegment: %v", err)
	}
	if seg.numCells != 4 {
		t.Fatalf("numCells = %d, want 4", seg.numCells)
	}
	if string(seg.minKey) != "apple" || string(seg.maxKey) != "cherry" {
		t.Fatalf("range = [%s,%s], want [apple,cherry]", seg.minKey, seg.maxKey)
	}
	if seg.maxVersion != 7 {
		t.Fatalf("maxVersion = %d, want 7", seg.maxVersion)
	}
	want := []string{"apple@7=a7", "apple@3=a3", "banana@5=b5", "cherry@1=c1"}
	got := drain(t, pgr, seg)
	if len(got) != len(want) {
		t.Fatalf("scan produced %d cells, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("cell %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestSegmentReopen reads a segment back through its footer page number alone, the
// path recovery takes from a MANIFEST record, and confirms it scans identically.
func TestSegmentReopen(t *testing.T) {
	pgr := newSegPager(t)
	cells := []cell{
		{ik("k1", 1), []byte("v1")},
		{ik("k2", 1), []byte("v2")},
		{ik("k3", 1), []byte("v3")},
	}
	seg, err := writeSegment(pgr, bloomBitsPerKey, filterBloom, sourceOf(cells))
	if err != nil {
		t.Fatalf("writeSegment: %v", err)
	}
	reopened, err := openSegment(pgr, seg.footer)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	if reopened.head != seg.head || reopened.numCells != seg.numCells || reopened.maxVersion != seg.maxVersion {
		t.Fatalf("reopened metadata = %+v, want %+v", reopened, seg)
	}
	if string(reopened.minKey) != "k1" || string(reopened.maxKey) != "k3" {
		t.Fatalf("reopened range = [%s,%s], want [k1,k3]", reopened.minKey, reopened.maxKey)
	}
	got := drain(t, pgr, reopened)
	want := []string{"k1@1=v1", "k2@1=v2", "k3@1=v3"}
	if len(got) != len(want) {
		t.Fatalf("reopened scan produced %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("reopened cell %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestSegmentMultiPage writes enough cells to span many data pages and confirms the
// chain walk returns every cell in order, exercising the next-pointer linkage and an
// allocation that reaches past the file tail.
func TestSegmentMultiPage(t *testing.T) {
	pgr := newSegPager(t)
	const n = 5000
	cells := make([]cell, n)
	for i := 0; i < n; i++ {
		cells[i] = cell{ik(fmt.Sprintf("key%06d", i), 1), []byte(fmt.Sprintf("value%06d", i))}
	}
	seg, err := writeSegment(pgr, bloomBitsPerKey, filterBloom, sourceOf(cells))
	if err != nil {
		t.Fatalf("writeSegment: %v", err)
	}
	if seg.numCells != n {
		t.Fatalf("numCells = %d, want %d", seg.numCells, n)
	}
	var seen int
	var prev []byte
	if err := seg.scan(pgr, func(ik, val []byte) bool {
		if prev != nil && format.CompareInternal(prev, ik) >= 0 {
			t.Fatalf("scan out of order at %q after %q", ik, prev)
		}
		prev = append([]byte(nil), ik...)
		seen++
		return true
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if seen != n {
		t.Fatalf("scan visited %d cells, want %d", seen, n)
	}
	// The run must have spanned several data pages for this to mean anything.
	if seg.head == seg.footer {
		t.Fatal("expected distinct head and footer pages")
	}
}

// TestSegmentEmpty writes a run with no cells and confirms the handle reports an
// empty segment that scans to nothing.
func TestSegmentEmpty(t *testing.T) {
	pgr := newSegPager(t)
	seg, err := writeSegment(pgr, bloomBitsPerKey, filterBloom, sourceOf(nil))
	if err != nil {
		t.Fatalf("writeSegment: %v", err)
	}
	if seg.numCells != 0 || seg.head != format.NoPage || seg.minKey != nil || seg.maxKey != nil {
		t.Fatalf("empty segment = %+v", seg)
	}
	if got := drain(t, pgr, seg); len(got) != 0 {
		t.Fatalf("empty scan returned %v", got)
	}
}

// TestSegmentScanEarlyStop confirms a scan that returns false stops without reading
// the rest of the chain.
func TestSegmentScanEarlyStop(t *testing.T) {
	pgr := newSegPager(t)
	const n = 1000
	cells := make([]cell, n)
	for i := 0; i < n; i++ {
		cells[i] = cell{ik(fmt.Sprintf("k%05d", i), 1), []byte("v")}
	}
	seg, err := writeSegment(pgr, bloomBitsPerKey, filterBloom, sourceOf(cells))
	if err != nil {
		t.Fatalf("writeSegment: %v", err)
	}
	var seen int
	if err := seg.scan(pgr, func(ik, val []byte) bool {
		seen++
		return seen < 10
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if seen != 10 {
		t.Fatalf("early-stop scan visited %d cells, want 10", seen)
	}
}
