package lsm

import (
	"bytes"
	"context"
	"testing"

	"github.com/tamnd/kv/engine"
)

// forceVLogGC runs one value-log garbage collection under the lock with the given
// page budget, bypassing the picker so a test drives GC directly without first
// settling every compaction.
func forceVLogGC(t *testing.T, l *LSM, maxPages int) engine.MaintReport {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	rep, err := l.runVLogGCLocked(engine.MaintBudget{MaxPages: maxPages})
	if err != nil {
		t.Fatalf("vlog gc: %v", err)
	}
	return rep
}

// vlogChainLen returns the number of pages currently in the value-log chain, the
// fingerprint a GC test compares before and after a sweep.
func vlogChainLen(t *testing.T, l *LSM) int {
	t.Helper()
	chain, err := l.vlog.walkChain()
	if err != nil {
		t.Fatalf("walk vlog chain: %v", err)
	}
	return len(chain)
}

// TestVLogGCReclaimsDeadPages writes a large value, overwrites it with another large
// value, and compacts the two versions together so the first version's pointer is
// dropped. Its value bytes are then dead in the log, and GC frees the pages they
// occupied while the surviving value still reads back whole.
func TestVLogGCReclaimsDeadPages(t *testing.T) {
	l := newLSM(t)
	l.valueSepThreshold = 16
	l.l0Trigger = 2

	body := l.pgr.Header().UsablePageSize() - segDataHeaderSize
	bigA := bytes.Repeat([]byte("a"), body*2) // two whole pages
	bigB := bytes.Repeat([]byte("b"), body*2)

	applyBatch(t, l, 1, func(b *engine.WriteBatch) { b.Set([]byte("k"), bigA) })
	l.flushActive(t)
	applyBatch(t, l, 2, func(b *engine.WriteBatch) { b.Set([]byte("k"), bigB) })
	l.flushActive(t)

	before := vlogChainLen(t, l)
	// Merge the two L0 segments at a watermark past version 1, so the splitter keeps the
	// version-2 base and drops the version-1 set, retiring its pointer.
	compact(t, l, 5)

	rep := forceVLogGC(t, l, 1<<30)
	if rep.BytesReclaimed <= 0 {
		t.Fatalf("GC reclaimed nothing, report %+v", rep)
	}
	after := vlogChainLen(t, l)
	if after >= before {
		t.Fatalf("GC did not shrink the chain: before %d, after %d", before, after)
	}
	if v, ok := getAt(t, l, "k", 2); !ok || !bytes.Equal(v, bigB) {
		t.Fatalf("surviving value lost after GC: ok=%v len=%d want=%d", ok, len(v), len(bigB))
	}
}

// TestVLogGCKeepsLiveValues runs GC over a log whose every value is still referenced and
// confirms it frees nothing and disturbs nothing: the chain keeps its length and every
// value reads back.
func TestVLogGCKeepsLiveValues(t *testing.T) {
	l := newLSM(t)
	l.valueSepThreshold = 16

	want := map[string][]byte{
		"a": bytes.Repeat([]byte("1"), 300),
		"b": bytes.Repeat([]byte("2"), 300),
		"c": bytes.Repeat([]byte("3"), 300),
	}
	applyBatch(t, l, 1, func(b *engine.WriteBatch) {
		for k, v := range want {
			b.Set([]byte(k), v)
		}
	})
	l.flushActive(t)

	before := vlogChainLen(t, l)
	rep := forceVLogGC(t, l, 1<<30)
	if rep.BytesReclaimed != 0 {
		t.Fatalf("GC reclaimed %d bytes over a fully live log, want 0", rep.BytesReclaimed)
	}
	if after := vlogChainLen(t, l); after != before {
		t.Fatalf("GC changed the chain length over a live log: before %d, after %d", before, after)
	}
	for k, v := range want {
		if got, ok := getAt(t, l, k, 1); !ok || !bytes.Equal(got, v) {
			t.Fatalf("value for %q lost after GC: ok=%v len=%d", k, ok, len(got))
		}
	}
}

// TestVLogGCBudgetCaps confirms a budget bounds the sweep: with two dead pages and a
// one-page budget, the first call frees one and asks to be called again, and the second
// frees the other and reports done.
func TestVLogGCBudgetCaps(t *testing.T) {
	l := newLSM(t)
	l.valueSepThreshold = 16
	l.l0Trigger = 2

	body := l.pgr.Header().UsablePageSize() - segDataHeaderSize
	page := bytes.Repeat([]byte("p"), body) // exactly one page, so each value owns a page

	// Three keys each with a page-sized separated value, so the log is three full pages.
	applyBatch(t, l, 1, func(b *engine.WriteBatch) {
		b.Set([]byte("k1"), page)
		b.Set([]byte("k2"), page)
		b.Set([]byte("k3"), page)
	})
	l.flushActive(t)
	// Overwrite each with a tiny inline value, so the new versions never touch the log.
	applyBatch(t, l, 2, func(b *engine.WriteBatch) {
		b.Set([]byte("k1"), []byte("x"))
		b.Set([]byte("k2"), []byte("y"))
		b.Set([]byte("k3"), []byte("z"))
	})
	l.flushActive(t)
	// Drop the separated version-1 cells, leaving the first two pages dead; the third is
	// the append tail GC always keeps.
	compact(t, l, 5)

	pageSize := int64(l.pgr.PageSize())
	rep := forceVLogGC(t, l, 1)
	if rep.BytesReclaimed != pageSize || !rep.More {
		t.Fatalf("first capped GC = %+v, want one page reclaimed and More", rep)
	}
	rep = forceVLogGC(t, l, 1)
	if rep.BytesReclaimed != pageSize || rep.More {
		t.Fatalf("second capped GC = %+v, want one page reclaimed and not More", rep)
	}
	// Every live key still resolves to its inline overwrite.
	for _, k := range []string{"k1", "k2", "k3"} {
		if v, ok := getAt(t, l, k, 2); !ok || len(v) != 1 {
			t.Fatalf("inline overwrite for %q lost: ok=%v len=%d", k, ok, len(v))
		}
	}
}

// TestVLogGCSurvivesReopen folds a GC's work to the file and reopens through a fresh
// engine with no WAL: the reclaimed pages stay reclaimed, the re-rooted chain is walked
// from the head the MANIFEST recorded, and the surviving value still reads back.
func TestVLogGCSurvivesReopen(t *testing.T) {
	fs, pgr := newDurablePager(t)
	l := openLSM(t, pgr)
	l.valueSepThreshold = 16
	l.l0Trigger = 2

	body := pgr.Header().UsablePageSize() - segDataHeaderSize
	bigA := bytes.Repeat([]byte("a"), body*2)
	bigB := bytes.Repeat([]byte("b"), body*2)

	applyLSN(t, l, 1, 1, func(b *engine.WriteBatch) { b.Set([]byte("k"), bigA) })
	l.flushActive(t)
	applyLSN(t, l, 2, 2, func(b *engine.WriteBatch) { b.Set([]byte("k"), bigB) })
	l.flushActive(t)
	if _, err := l.Maintain(context.Background(), engine.MaintBudget{MaxPages: 1 << 30, Watermark: 5}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	forceVLogGC(t, l, 1<<30)
	afterGC := vlogChainLen(t, l)
	if err := pgr.Checkpoint(l.DurableLSN(), 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	pgr2 := reopenPager(t, fs, pgr)
	l2 := openLSM(t, pgr2)
	if l2.vlog.head == 0 {
		t.Fatalf("reopened engine did not restore the value-log head")
	}
	if got := vlogChainLen(t, l2); got != afterGC {
		t.Fatalf("reopened chain length %d, want the post-GC %d", got, afterGC)
	}
	if v, ok := getAt(t, l2, "k", 2); !ok || !bytes.Equal(v, bigB) {
		t.Fatalf("surviving value lost across reopen: ok=%v len=%d want=%d", ok, len(v), len(bigB))
	}
}
