package btree

import (
	"context"
	"testing"

	"github.com/tamnd/kv/engine"
)

// readAt resolves a key through a fresh reader at the given snapshot, returning the
// value and whether it is present.
func readAt(t *testing.T, r engine.Reader, key string) (string, bool) {
	t.Helper()
	v, err := r.Get([]byte(key))
	if err == engine.ErrNotFound {
		return "", false
	}
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	return string(v), true
}

// TestTTLExpiryInCore checks the B-tree core resolves a TTL set live before its deadline
// and absent at or after it, with the deadline carried by the snapshot's Now. A snapshot
// with Now == 0 disables expiry, so the same cell folds live.
func TestTTLExpiryInCore(t *testing.T) {
	bt := newBTree(t, 4096, 16)

	b := engine.NewWriteBatch(10)
	b.SetWithTTL([]byte("k"), []byte("v"), 100) // expires at wall clock 100
	b.Set([]byte("plain"), []byte("p"))         // no expiry
	if err := bt.Apply(b, 10); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Before the deadline both keys are present.
	rd, _ := bt.NewReader(engine.Snapshot{Version: 10, Now: 50})
	if v, ok := readAt(t, rd, "k"); !ok || v != "v" {
		t.Fatalf("before deadline k = %q,%v, want v", v, ok)
	}
	if v, ok := readAt(t, rd, "plain"); !ok || v != "p" {
		t.Fatalf("plain = %q,%v, want p", v, ok)
	}
	rd.Close()

	// At the deadline the TTL key is gone, the plain key stays.
	rd, _ = bt.NewReader(engine.Snapshot{Version: 10, Now: 100})
	if _, ok := readAt(t, rd, "k"); ok {
		t.Fatalf("at deadline k still present")
	}
	if v, ok := readAt(t, rd, "plain"); !ok || v != "p" {
		t.Fatalf("plain after deadline = %q,%v, want p", v, ok)
	}
	rd.Close()

	// Now == 0 disables expiry: the TTL cell folds live regardless of the deadline.
	rd, _ = bt.NewReader(engine.Snapshot{Version: 10, Now: 0})
	if v, ok := readAt(t, rd, "k"); !ok || v != "v" {
		t.Fatalf("now=0 k = %q,%v, want v", v, ok)
	}
	rd.Close()
}

// TestTTLSweep checks the maintenance sweep turns an expired TTL set into a tombstone,
// reclaiming its framed value, while leaving a live TTL set and a plain set untouched.
// After the sweep the expired key reads absent even with expiry disabled (Now == 0),
// proving the cell is now a real tombstone rather than a value the read path hides.
func TestTTLSweep(t *testing.T) {
	bt := newBTree(t, 4096, 16)

	b := engine.NewWriteBatch(10)
	b.SetWithTTL([]byte("dead"), []byte("gone"), 100) // expires at 100
	b.SetWithTTL([]byte("live"), []byte("stay"), 500) // expires at 500
	b.Set([]byte("plain"), []byte("keep"))            // never expires
	if err := bt.Apply(b, 10); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Sweep at wall clock 200: only "dead" is past its deadline. Watermark 0 leaves the
	// version history alone, so the sweep is the only change.
	rep, err := bt.Maintain(context.Background(), engine.MaintBudget{Now: 200})
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if rep.ExpiredSwept != 1 {
		t.Fatalf("swept = %d, want 1", rep.ExpiredSwept)
	}
	if rep.BytesReclaimed <= 0 {
		t.Fatalf("bytes reclaimed = %d, want > 0", rep.BytesReclaimed)
	}

	// With expiry disabled the swept key is still absent (a real tombstone), the live TTL
	// key still resolves, and the plain key is untouched.
	rd, _ := bt.NewReader(engine.Snapshot{Version: 10, Now: 0})
	defer rd.Close()
	if _, ok := readAt(t, rd, "dead"); ok {
		t.Fatalf("swept key still present at now=0")
	}
	if v, ok := readAt(t, rd, "live"); !ok || v != "stay" {
		t.Fatalf("live ttl key = %q,%v, want stay", v, ok)
	}
	if v, ok := readAt(t, rd, "plain"); !ok || v != "keep" {
		t.Fatalf("plain key = %q,%v, want keep", v, ok)
	}

	// A second sweep at the same clock has nothing left to do.
	rep2, err := bt.Maintain(context.Background(), engine.MaintBudget{Now: 200})
	if err != nil {
		t.Fatalf("second maintain: %v", err)
	}
	if rep2.ExpiredSwept != 0 {
		t.Fatalf("second sweep swept = %d, want 0", rep2.ExpiredSwept)
	}
}

// TestTTLCoreMatchesModel drives the same TTL batch through both the B-tree core and the
// Model engine and asserts they resolve identically at a live and an expired snapshot, so
// the shared Op-builder keeps the two cores in lockstep on expiry (spec 04 §7).
func TestTTLCoreMatchesModel(t *testing.T) {
	bt := newBTree(t, 4096, 16)
	mdl := engine.NewModel()
	if err := mdl.Open(&engine.Env{}); err != nil {
		t.Fatalf("open model: %v", err)
	}

	b := engine.NewWriteBatch(10)
	b.SetWithTTL([]byte("a"), []byte("1"), 100)
	b.SetWithTTL([]byte("b"), []byte("2"), 0) // never expires
	b.Set([]byte("c"), []byte("3"))
	if err := bt.Apply(b, 10); err != nil {
		t.Fatalf("apply btree: %v", err)
	}
	if err := mdl.Apply(b, 10); err != nil {
		t.Fatalf("apply model: %v", err)
	}

	for _, now := range []uint64{50, 100, 1000} {
		snap := engine.Snapshot{Version: 10, Now: now}
		btr, _ := bt.NewReader(snap)
		mr, _ := mdl.NewReader(snap)
		for _, key := range []string{"a", "b", "c"} {
			bv, bok := readAt(t, btr, key)
			mv, mok := readAt(t, mr, key)
			if bok != mok || bv != mv {
				t.Fatalf("now=%d key=%q: btree (%q,%v) != model (%q,%v)", now, key, bv, bok, mv, mok)
			}
		}
		btr.Close()
		mr.Close()
	}
}
