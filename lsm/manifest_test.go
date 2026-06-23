package lsm

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// newDurablePager creates an LSM pager over an in-memory file that survives a pager
// close and reopen, so a test can fold a flush to the file and read it back through a
// fresh engine, the no-WAL proof that the MANIFEST alone restores the segment set.
func newDurablePager(t *testing.T) (vfs.FS, *pager.Pager) {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "m.kv", pager.Options{
		PageSize:    4096,
		CacheFrames: 64,
		Engine:      format.EngineLSM,
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	return fs, p
}

// reopenPager closes the pager and opens a fresh one over the same file.
func reopenPager(t *testing.T, fs vfs.FS, pgr *pager.Pager) *pager.Pager {
	t.Helper()
	if err := pgr.Close(); err != nil {
		t.Fatalf("close pager: %v", err)
	}
	p, err := pager.Open(fs, "m.kv", pager.Options{CacheFrames: 64})
	if err != nil {
		t.Fatalf("reopen pager: %v", err)
	}
	return p
}

// openLSM wires a fresh LSM over a pager and installs the test merge resolver, the
// engine half of what the host does at openEngine.
func openLSM(t *testing.T, pgr *pager.Pager) *LSM {
	t.Helper()
	l := New(pgr)
	// Manual-compaction tests drive Maintain by hand against a known segment shape, so the
	// background compactor is off here (it is covered by the auto-compaction tests).
	l.autoCompact = false
	if err := l.Open(&engine.Env{Pager: pgr, Options: engine.EngineOptions{PageSize: pgr.PageSize()}}); err != nil {
		t.Fatalf("open lsm: %v", err)
	}
	l.SetMergeFunc(concatMerge)
	t.Cleanup(func() { l.Close() })
	return l
}

// applyLSN notes the batch's WAL position and applies it, the pair the host runs under
// its writer lock so the engine's durable mark can track the flush frontier.
func applyLSN(t *testing.T, l *LSM, lsn, version uint64, fill func(b *engine.WriteBatch)) {
	t.Helper()
	b := engine.NewWriteBatch(version)
	fill(b)
	l.NoteLSN(lsn)
	if err := l.Apply(b, version); err != nil {
		t.Fatalf("apply at lsn %d: %v", lsn, err)
	}
}

// TestManifestPersistAcrossReopen writes several segments, folds them and the MANIFEST
// to the file, then reopens through a fresh engine with no WAL in the picture, so the
// only way the data and the segment set can return is the MANIFEST the last checkpoint
// recorded.
func TestManifestPersistAcrossReopen(t *testing.T) {
	fs, pgr := newDurablePager(t)
	l := openLSM(t, pgr)
	l.memtableCap = 1 // every applied batch after the first flushes the previous one

	want := map[string]string{}
	for i := uint64(1); i <= 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		val := fmt.Sprintf("val%03d", i)
		applyLSN(t, l, i, i, func(b *engine.WriteBatch) { b.Set([]byte(key), []byte(val)) })
		want[key] = val
	}
	l.flushActive(t) // seal the tail so all ten keys are on disk
	if len(l.allSegmentsLocked()) == 0 {
		t.Fatal("expected segments before checkpoint")
	}

	// Fold the segment and MANIFEST pages to the file at the engine's durable mark, the
	// checkpoint boundary a reopened pager reports.
	dl := l.DurableLSN()
	if err := pgr.Checkpoint(dl, 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	pgr2 := reopenPager(t, fs, pgr)
	l2 := openLSM(t, pgr2)
	if len(l2.allSegmentsLocked()) == 0 {
		t.Fatal("reopened engine restored no segments from the MANIFEST")
	}
	if got := l2.DurableLSN(); got != dl {
		t.Fatalf("reopened DurableLSN = %d, want the folded mark %d", got, dl)
	}

	rd, err := l2.NewReader(engine.Snapshot{Version: 100})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	for key, val := range want {
		v, err := rd.Get([]byte(key))
		if err != nil || string(v) != val {
			t.Fatalf("after reopen Get(%s) = (%q,%v), want %q", key, v, err, val)
		}
	}
}

// TestManifestVersionedAcrossReopen folds overwrites and a delete spread across
// segments, then reopens and confirms each key resolves to its newest visible version
// through the restored set alone.
func TestManifestVersionedAcrossReopen(t *testing.T) {
	fs, pgr := newDurablePager(t)
	l := openLSM(t, pgr)
	l.memtableCap = 1

	applyLSN(t, l, 1, 10, func(b *engine.WriteBatch) {
		b.Set([]byte("a"), []byte("a1"))
		b.Set([]byte("b"), []byte("b1"))
		b.Set([]byte("c"), []byte("c1"))
	})
	applyLSN(t, l, 2, 20, func(b *engine.WriteBatch) {
		b.Set([]byte("a"), []byte("a2"))
		b.Delete([]byte("b"))
	})
	applyLSN(t, l, 3, 30, func(b *engine.WriteBatch) {
		b.Merge([]byte("c"), []byte("+"))
	})
	l.flushActive(t)

	dl := l.DurableLSN()
	if err := pgr.Checkpoint(dl, 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	pgr2 := reopenPager(t, fs, pgr)
	l2 := openLSM(t, pgr2)
	rd, err := l2.NewReader(engine.Snapshot{Version: 100})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()

	if v, err := rd.Get([]byte("a")); err != nil || string(v) != "a2" {
		t.Fatalf("Get(a) = (%q,%v), want a2", v, err)
	}
	if _, err := rd.Get([]byte("b")); err != engine.ErrNotFound {
		t.Fatalf("Get(b) err = %v, want ErrNotFound", err)
	}
	if v, err := rd.Get([]byte("c")); err != nil || string(v) != "c1+" {
		t.Fatalf("Get(c) = (%q,%v), want c1+", v, err)
	}
}

// TestManifestSpansMultiplePages flushes more segments than fit in one MANIFEST page,
// forcing a fresh head page chained to the older one, then reopens and confirms every
// segment came back, exercising the chain walk and the page-allocation path.
func TestManifestSpansMultiplePages(t *testing.T) {
	fs, pgr := newDurablePager(t)
	l := openLSM(t, pgr)
	l.memtableCap = 1

	perPage := manifestEntriesPerPage(pgr.Header().UsablePageSize())
	n := perPage + 5 // cross into a second MANIFEST page
	for i := 1; i <= n; i++ {
		key := fmt.Sprintf("k%06d", i)
		applyLSN(t, l, uint64(i), uint64(i), func(b *engine.WriteBatch) { b.Set([]byte(key), []byte("v")) })
		l.flushActive(t) // one segment per key
	}
	if len(l.allSegmentsLocked()) != n {
		t.Fatalf("wrote %d segments, want %d", len(l.allSegmentsLocked()), n)
	}

	if err := pgr.Checkpoint(l.DurableLSN(), 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	pgr2 := reopenPager(t, fs, pgr)
	l2 := openLSM(t, pgr2)
	if len(l2.allSegmentsLocked()) != n {
		t.Fatalf("restored %d segments from MANIFEST, want %d", len(l2.allSegmentsLocked()), n)
	}

	rd, err := l2.NewReader(engine.Snapshot{Version: uint64(n) + 1})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	for i := 1; i <= n; i++ {
		key := fmt.Sprintf("k%06d", i)
		if v, err := rd.Get([]byte(key)); err != nil || string(v) != "v" {
			t.Fatalf("after reopen Get(%s) = (%q,%v), want v", key, v, err)
		}
	}
}

// TestDurableLSNAdvance pins the durable mark's movement: zero until a flush, then the
// largest LSN the sealed memtable held, monotonic across flushes.
func TestDurableLSNAdvance(t *testing.T) {
	l := newLSM(t)
	l.memtableCap = 1

	if got := l.DurableLSN(); got != 0 {
		t.Fatalf("initial DurableLSN = %d, want 0", got)
	}
	applyLSN(t, l, 5, 5, func(b *engine.WriteBatch) { b.Set([]byte("a"), []byte("1")) })
	if got := l.DurableLSN(); got != 0 {
		t.Fatalf("DurableLSN before flush = %d, want 0", got)
	}
	l.flushActive(t)
	if got := l.DurableLSN(); got != 5 {
		t.Fatalf("DurableLSN after first flush = %d, want 5", got)
	}
	applyLSN(t, l, 9, 9, func(b *engine.WriteBatch) { b.Set([]byte("b"), []byte("2")) })
	l.flushActive(t)
	if got := l.DurableLSN(); got != 9 {
		t.Fatalf("DurableLSN after second flush = %d, want 9", got)
	}
}
