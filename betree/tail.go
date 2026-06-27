package betree

// This file is M1's mutable hot tail: the in-memory ordered region that sits in
// front of the on-disk buffered tree and absorbs writes before any of them becomes a
// Be-tree message (doc 02 section 3, decision D9). A write lands in the tail first.
// When the same internal key is written again the tail overwrites the slot in place
// rather than appending a second message, so a hot key that is rewritten a thousand
// times costs one slot, not a thousand messages flowing down the tree (the in-place
// hot update FASTER names, SIGMOD 2018). The tail rolls over when it crosses its byte
// budget: it seals into one sorted run and pushes that run into the tree in a single
// batched descent, so the tree is touched once per rollover instead of once per
// write. Reads consult the tail first, then the tree, and the fold below resolves the
// two together exactly as it resolves buffered messages and leaf records, because
// resolution is by commit version and not by where a message sits.
//
// What in-place overwrite can and cannot collapse here. Two writes of one user key at
// different commit versions carry different internal keys, and the conformance oracle
// reads at every committed version, so both versions stay visible and the tail keeps
// both. The in-place collapse therefore fires only for an exact-internal-key rewrite,
// which is the idempotent-replay case and the same-version overwrite case; collapsing
// a user key's superseded older versions needs the GC watermark below which no
// snapshot can see them, which arrives with version GC, not here. What this slice
// banks is the in-memory absorption of that rewrite traffic and the batching of the
// tree descent, both real without any version collapse.
//
// Durability. The tail is a write-back cache, not the durability boundary; the WAL
// is. Every logical write is in the WAL before Apply is called, so a crash with a
// populated tail loses nothing: recovery replays the WAL through Apply and rebuilds
// the tail. The two hosts that drive this core need the tail on pages at different
// moments, and the tail serves both. A host with a logical WAL (the database) keeps
// the WAL above the tail's un-rolled point through DurableLSN, exactly as it does for
// the LSM memtable, so a checkpoint that does not fully drain still recovers; and it
// drains the tail through Flush before a checkpoint that must stand alone (migration,
// close). A host that drives the pager directly (the betree reopen tests) has no
// logical WAL to replay, so it calls Flush to put the tail on pages before it
// checkpoints. Both paths end with every visible write either in a page or in the
// WAL, and never only in the heap.
//
// What this leaves. The rollover is synchronous under the single write latch: a
// crossing of the budget seals and pushes inline. The background folder that rolls
// sealed blocks over off the write path (doc 02 section 3) waits for M2's optimistic
// latching and epoch reclamation, which make a concurrent fold safe; until then the
// inline rollover is the correct, simple form, and it keeps the same fold answer the
// oracle already pins.

import (
	"bytes"
	"sort"

	"github.com/tamnd/kv/format"
)

// tailFlushBytes is the mutable tail's size budget in live message bytes. Crossing it
// seals the tail and rolls it over into the tree. It is deliberately small so a
// write-heavy stream rolls over often and the tree stays populated and grows, rather
// than the whole stream resting in the heap; the in-memory absorption it buys is the
// rewrite traffic that lands between rollovers, which a small budget does not give up.
// A later milestone tunes this against a real write benchmark; M1.2's job is the
// mechanism, not the constant.
const tailFlushBytes = 32 * 1024

// tailPut installs one message into the mutable hot tail. When the same internal key
// is already present it overwrites the slot in place (the idempotent-replay and
// same-version-overwrite collapse; distinct versions of a user key carry distinct
// internal keys and each take their own slot, so no visible version is dropped). It
// rolls the tail over into the tree when the live bytes cross the budget. The caller
// holds the write latch.
func (t *Tree) tailPut(key, val []byte) error {
	// A range-delete marker sets the sticky read-path flag so every later read takes the
	// full gather, where the interval set is rebuilt whole and a marker's non-local coverage
	// is honored even for a bounded scan (betree.go hasRangeDel, paged.go gatherRange). The
	// caller holds wmu, so this set is serialized with the gen window the read validates
	// against. The flag is never cleared: a tree that has held a range delete keeps taking
	// the correct full path.
	if format.KindOf(key) == format.KindRangeBegin {
		t.hasRangeDel.Store(true)
	}
	// The map edit runs under tailMu so a latch-free reader gathering the tail never reads
	// the map while it is being written, which in Go is a hard panic, not a race the gen
	// check could absorb after the fact. The caller holds wmu, so this is the only writer.
	// The gate is released before any rollover: a rollover takes tailMu itself, and a Go
	// RWMutex is not reentrant, so holding it across the call would self-deadlock.
	t.tailMu.Lock()
	if t.tail == nil {
		t.tail = make(map[string]message)
	}
	ik := string(key)
	if prev, ok := t.tail[ik]; ok {
		// Same internal key: overwrite the value in place. The key bytes are unchanged,
		// so the slot's key allocation is reused and only the value delta moves the
		// running byte count. The seq and kind are a function of the key and do not move.
		t.tailBytes += len(val) - len(prev.val)
		prev.val = append(prev.val[:0:0], val...)
		t.tail[ik] = prev
	} else {
		// A fresh slot. The first slot of a tail epoch (the tail going from empty to
		// non-empty) fixes the epoch's low-water LSN: every later write of the epoch has
		// an LSN at or above it because batches apply in LSN order, so DurableLSN can hold
		// the WAL from this point and cover the whole tail. An in-place overwrite raises a
		// slot's effective LSN but never lowers the epoch's low-water, so the low-water
		// stays conservative and safe.
		if len(t.tail) == 0 {
			t.tailMinLSN = t.curLSN
		}
		t.tail[ik] = message{
			kind: byte(format.KindOf(key)),
			seq:  format.Version(key),
			key:  append([]byte(nil), key...),
			val:  append([]byte(nil), val...),
		}
		t.tailBytes += len(key) + len(val)
	}
	over := t.tailBytes >= tailFlushBytes
	t.tailMu.Unlock()
	if over {
		return t.rollover()
	}
	return nil
}

// rollover seals the mutable tail into one sorted, internal-key-deduplicated run and
// pushes it into the tree in a single batched descent, then resets the tail empty.
// The map already holds at most one slot per internal key, so the sealed run is free
// of exact-internal-key duplicates and only needs sorting; that is exactly the shape
// pushDown's merge requires. After the push every message that was in the tail lives
// on a page, so the durable mark advances to the highest LSN the tail had seen. The
// caller holds the write latch.
func (t *Tree) rollover() error {
	// Seal the tail under a read lock: snapshot the slots into a sorted run without yet
	// emptying the map. The caller holds wmu, so no other writer competes; the read lock
	// only fences a concurrent reader's tail gather.
	t.tailMu.RLock()
	if len(t.tail) == 0 {
		t.tailMu.RUnlock()
		return nil
	}
	sealed := make([]message, 0, len(t.tail))
	for _, m := range t.tail {
		sealed = append(sealed, m)
	}
	curLSN := t.curLSN
	t.tailMu.RUnlock()
	sort.Slice(sealed, func(i, j int) bool {
		return format.CompareInternal(sealed[i].key, sealed[j].key) < 0
	})
	// Push the sealed run into the tree BEFORE clearing the tail, not after. The two
	// stores are not atomic to a latch-free reader, so the order decides what a reader
	// caught in the window sees. Applying first leaves every message in both the tail and
	// the tree across the window, which a reader folds idempotently because the tail slot
	// and the tree record it became carry the identical internal key and value. Clearing
	// first would open a window where a message lives in neither place and a reader would
	// miss it. The gen seqlock makes the reader restart anyway, but the order keeps the
	// invariant true even for the read that started before this rollover bumped gen.
	if err := t.applyToTree(sealed); err != nil {
		return err
	}
	// The run is on pages now, so empty the tail under the write lock and advance the
	// durable mark: everything the tail held is on pages the checkpoint will fold, so the
	// WAL up to the highest LSN the tail saw is redundant.
	t.tailMu.Lock()
	t.tail = make(map[string]message)
	t.tailBytes = 0
	t.durableLSN = curLSN
	t.tailMu.Unlock()
	// The run just changed shape, so refresh the resident learned point index over it on its
	// amortized schedule (learned.go). This runs under wmu inside the gen-odd write window, so
	// readers either restart or pick up the freshly published model, never a half-built one.
	t.maybeRebuildLocator()
	return nil
}

// collectTailMessages returns the live tail slots as records for the read gather. A
// tail message and the tree record it will become after a rollover carry the same
// internal key and value, so folding the tail run together with the leaf run and the
// interior buffers resolves identically whether a write is still in the tail or has
// rolled into the tree. The caller holds at least the read latch.
func (t *Tree) collectTailMessages() []record {
	// Copy the slots out under the read lock so the map walk never overlaps a writer's map
	// edit. The records are fresh copies, so they outlive the lock and the gen-validation
	// the caller wraps this in decides whether the snapshot keeps them.
	t.tailMu.RLock()
	defer t.tailMu.RUnlock()
	if len(t.tail) == 0 {
		return nil
	}
	out := make([]record, 0, len(t.tail))
	for _, m := range t.tail {
		out = append(out, record{
			key: append([]byte(nil), m.key...),
			val: append([]byte(nil), m.val...),
		})
	}
	return out
}

// collectTailMessagesRange is the range-bounded twin of collectTailMessages: it copies out only the
// tail slots whose user key falls in [lower, upper), so a short scan window never pays to copy the
// whole tail. The full collector hands every slot back and the caller filters after the copy, which
// for a 50-key scan over a tail of a few hundred recent writes copies hundreds of key and value
// slices the window then throws away. The filter is the dominant cost and the dominant allocation of
// a hot-tail scan (a tree whose recent inserts have not flushed), so the scan path folds only the
// window's slice of the tail instead. The same RLock fences a concurrent writer's map edit; the
// returned records are fresh copies, so they outlive the lock and the caller's gen-validation
// decides whether the snapshot keeps them. nil bounds mean unbounded on that side, matching
// inHalfOpen, so a nil/nil call copies the whole tail exactly as collectTailMessages does.
func (t *Tree) collectTailMessagesRange(lower, upper []byte) []record {
	t.tailMu.RLock()
	defer t.tailMu.RUnlock()
	if len(t.tail) == 0 {
		return nil
	}
	var out []record
	for _, m := range t.tail {
		uk := format.UserKey(m.key)
		if lower != nil && bytes.Compare(uk, lower) < 0 {
			continue
		}
		if upper != nil && bytes.Compare(uk, upper) >= 0 {
			continue
		}
		out = append(out, record{
			key: append([]byte(nil), m.key...),
			val: append([]byte(nil), m.val...),
		})
	}
	return out
}
