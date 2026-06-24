package db

import (
	"bytes"
	"sort"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// Iterator is the caller-facing, snapshot-consistent, version-resolved iterator of
// spec 11 §1. It walks the user-visible keys in a range in order: every version is
// already resolved to one value (newest <= snapshot, tombstones removed, merges
// folded), and a write transaction's own buffered mutations are overlaid on the
// snapshot (read-your-writes, spec 11 §6).
//
// The buffer `items` holds the resolved entries in ascending user-key order. There
// are two ways it gets populated. A read-only forward scan over an engine that can
// stream (an unbuffered B-tree) fills it lazily from the engine one entry at a time,
// so a bounded scan reads O(ScanLen) entries instead of the whole range (spec 04).
// A write transaction (its buffered writes must overlay the base), a reverse scan,
// or an engine that cannot stream, falls back to materializing the whole resolved
// range up front; that path sets drained and leaves stream nil, so every navigation
// method below indexes a complete buffer exactly as it did before streaming existed.
type Iterator struct {
	items    []iterItem // ascending by user key, resolved and overlaid; grows lazily when streaming
	pos      int
	reverse  bool
	keysOnly bool

	// Streaming state. stream is nil for the materialized path (drained already true).
	db      *DB
	rd      engine.Reader
	stream  forwardScanner
	scursor engine.StreamCursor // stateful forward cursor, when the reader provides one; preferred over stream
	lower   []byte
	upper   []byte
	after   []byte // exclusive cursor: the last key pulled from stream, nil before the first pull
	drained bool   // no more entries to pull (range exhausted, or fully materialized)
	err     error  // first mid-scan error, surfaced through Error
}

// forwardScanner is the optional capability an engine reader exposes when it can
// serve a forward streaming scan without materializing the range. The db Iterator
// type-asserts it and, when present and streamable, drives it one entry at a time
// under the engine read lock instead of copying the whole range up front. Both the
// unbuffered B-tree and the LSM core satisfy it; a buffered B-tree (whose interior
// messages a single-leaf gather would miss) returns StreamForward false, and the
// Iterator materializes that case and every reverse or write-transaction scan.
type forwardScanner interface {
	StreamForward() bool
	ScanForward(after, lower, upper []byte, keysOnly bool) (key, val []byte, ok bool, err error)
}

// iterItem is one resolved, visible user key and its value at the iterator's
// snapshot (overlaid with the transaction's writes).
type iterItem struct {
	key []byte
	val []byte
}

// NewIterator returns an iterator over the transaction's snapshot, overlaid with its
// own buffered writes. Bounds, prefix, reverse, and key-only come from opts; the
// iterator enforces them so the engine need not (spec 11 §3). The caller must Close
// it. The returned keys and values are owned copies, valid past the iterator's life.
func (t *Txn) NewIterator(opts engine.IterOptions) (*Iterator, error) {
	if t.done {
		return nil, ErrTxnDone
	}
	t.db.counters.scan.Add(1)
	lower, upper := opts.Lower, opts.Upper
	if len(opts.Prefix) > 0 {
		// A prefix scan is a bounded range scan over [prefix, prefix_successor)
		// (spec 11 §3); no special engine support needed.
		lower = opts.Prefix
		upper = format.PrefixSuccessor(opts.Prefix)
	}

	// A serializable transaction depends on every key in the scanned interval, so the
	// predicate is tracked for commit-time validation (spec 10 §4); this is what gives
	// a scan phantom protection a per-key read set could not.
	t.trackRange(lower, upper)

	// Streaming fast path: a read-only, forward scan over an engine that can stream
	// pulls entries lazily so a bounded scan does not walk the whole range (spec 04).
	// A write transaction needs the buffered-write overlay and a reverse scan needs the
	// whole range to walk backward, so both take the materialized path below.
	if len(t.ops) == 0 && !opts.Reverse {
		// Create the reader under the read lock, the same lock the materialized walk
		// takes: an engine reader may snapshot mutable engine state (the LSM segment
		// list) at construction and must not race a flush or compaction. The streaming
		// path keeps the reader open afterward and reacquires the lock per pull.
		sh := t.db.rl.RLock()
		rd, err := t.db.eng.NewReader(engine.Snapshot{Version: t.readVersion, Clock: t.db.now})
		if err != nil {
			t.db.rl.RUnlock(sh)
			return nil, err
		}
		sf, ok := rd.(forwardScanner)
		streamable := ok && sf.StreamForward()
		var scursor engine.StreamCursor
		if streamable {
			// A reader that can hold its scan position (the B-tree's ForwardCursorer)
			// builds the stateful cursor here, under the same read lock, so the cursor
			// snapshots any engine state it depends on (the range-delete set) atomically
			// against a concurrent writer. A reader without it keeps the stateless stream.
			if fc, ok := rd.(engine.ForwardCursorer); ok {
				c, err := fc.NewForwardCursor(lower, upper)
				if err != nil {
					rd.Close()
					t.db.rl.RUnlock(sh)
					return nil, err
				}
				scursor = c
			}
		} else {
			rd.Close()
		}
		t.db.rl.RUnlock(sh)
		if streamable {
			return &Iterator{
				pos:      -1,
				keysOnly: opts.KeysOnly,
				db:       t.db,
				rd:       rd,
				stream:   sf,
				scursor:  scursor,
				lower:    lower,
				upper:    upper,
			}, nil
		}
	}

	base, err := t.db.rangeSnapshot(t.readVersion, lower, upper, opts.KeysOnly)
	if err != nil {
		return nil, err
	}
	items, err := t.overlayBuffer(base, lower, upper, opts.KeysOnly)
	if err != nil {
		return nil, err
	}
	return &Iterator{items: items, pos: -1, reverse: opts.Reverse, keysOnly: opts.KeysOnly, drained: true}, nil
}

// pullOne pulls the next streamed entry into items, or marks the iterator drained
// at end of range. It takes the engine read lock only for the single ScanForward
// call: ScanForward holds no position across calls, so the lock spans one step, not
// the whole scan, and a writer or checkpoint is never blocked behind a slow consumer.
func (it *Iterator) pullOne() {
	if it.drained || it.stream == nil {
		it.drained = true
		return
	}
	sh := it.db.rl.RLock()
	var (
		k, v []byte
		ok   bool
		err  error
	)
	if it.scursor != nil {
		// Stateful cursor: holds its leaf and index across calls, so a step within a
		// leaf is a slice advance and only a leaf boundary resolves a page. after is
		// unused, the cursor tracks its own position.
		k, v, ok, err = it.scursor.NextEntry(it.keysOnly)
	} else {
		k, v, ok, err = it.stream.ScanForward(it.after, it.lower, it.upper, it.keysOnly)
	}
	it.db.rl.RUnlock(sh)
	if err != nil {
		it.err = err
		it.drained = true
		return
	}
	if !ok {
		it.drained = true
		return
	}
	it.items = append(it.items, iterItem{key: k, val: v})
	it.after = k
}

// fillTo pulls until items[n] exists or the range is drained. A no-op once
// materialized (drained set, stream nil), so the navigation methods are unchanged
// on that path.
func (it *Iterator) fillTo(n int) {
	for !it.drained && len(it.items) <= n {
		it.pullOne()
	}
}

// fillKeyGE pulls until the last buffered key is >= key or the range is drained, so
// a forward SeekGE/SeekLT can binary-search the buffer for the boundary.
func (it *Iterator) fillKeyGE(key []byte) {
	for !it.drained {
		if len(it.items) > 0 && bytes.Compare(it.items[len(it.items)-1].key, key) >= 0 {
			return
		}
		it.pullOne()
	}
}

// drainAll materializes the rest of the range. A reverse walk or a Last needs the
// final entry, which on a forward stream is only known once the range is exhausted.
func (it *Iterator) drainAll() {
	for !it.drained {
		it.pullOne()
	}
}

// rangeSnapshot materializes the resolved, version-collapsed view of [lower, upper)
// at version, by walking the engine cursor ascending. It takes the shared read lock
// so it never reads a page mid-commit. KeysOnly skips value materialization.
func (d *DB) rangeSnapshot(version uint64, lower, upper []byte, keysOnly bool) ([]iterItem, error) {
	sh := d.rl.RLock()
	defer d.rl.RUnlock(sh)
	rd, err := d.eng.NewReader(engine.Snapshot{Version: version, Clock: d.now})
	if err != nil {
		return nil, err
	}
	defer rd.Close()
	// The engine cursor already applies bounds and version resolution; reverse and
	// key-only are handled above the seam, so it is walked ascending here.
	cur, err := rd.NewIter(engine.IterOptions{Lower: lower, Upper: upper})
	if err != nil {
		return nil, err
	}
	defer cur.Close()

	var out []iterItem
	for cur.First(); cur.Valid(); cur.Next() {
		it := iterItem{key: append([]byte(nil), cur.Key()...)}
		if !keysOnly {
			lv, err := cur.Value()
			if err != nil {
				return nil, err
			}
			b, err := lv.Value()
			if err != nil {
				return nil, err
			}
			it.val = append([]byte(nil), b...)
		}
		out = append(out, it)
	}
	if err := cur.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

// overlayBuffer merges the transaction's buffered writes in [lower, upper) onto the
// base snapshot view, so the scan sees the transaction's own uncommitted puts,
// deletes, and range deletes (spec 11 §6). Each affected key is resolved to its net
// effect (the same fold Get uses), then it replaces, inserts, or removes the base
// entry. The affected set is every buffered point-op key plus every base key a
// buffered range delete covers. A read-only transaction has no buffer, so base
// passes through.
func (t *Txn) overlayBuffer(base []iterItem, lower, upper []byte, keysOnly bool) ([]iterItem, error) {
	if len(t.ops) == 0 {
		return base, nil
	}
	combined := make(map[string][]byte, len(base)+len(t.ops))
	order := make([]string, 0, len(base)+len(t.ops))
	for _, it := range base {
		k := string(it.key)
		combined[k] = it.val
		order = append(order, k)
	}

	resolveSet := make(map[string]struct{}, len(t.ops))
	for _, op := range t.ops {
		if op.kind == opRangeDelete {
			// A range delete affects the base keys it covers; a buffered insert in
			// the range is already its own point op below.
			for _, it := range base {
				if rangeCovers(op.key, op.value, it.key) {
					resolveSet[string(it.key)] = struct{}{}
				}
			}
			continue
		}
		resolveSet[string(op.key)] = struct{}{}
	}

	for ks := range resolveSet {
		key := []byte(ks)
		if !inBounds(key, lower, upper) {
			continue
		}
		val, exists, err := t.resolve(key)
		if err != nil {
			return nil, err
		}
		_, inBase := combined[ks]
		switch {
		case !exists:
			delete(combined, ks)
		case !inBase:
			combined[ks] = stripValue(val, keysOnly)
			order = append(order, ks)
		default:
			combined[ks] = stripValue(val, keysOnly)
		}
	}

	out := make([]iterItem, 0, len(combined))
	for _, k := range order {
		v, ok := combined[k]
		if !ok {
			continue // removed by an overlay delete
		}
		out = append(out, iterItem{key: []byte(k), val: v})
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].key, out[j].key) < 0 })
	return out, nil
}

// stripValue drops the value for a key-only scan so the overlay matches the base.
func stripValue(v []byte, keysOnly bool) []byte {
	if keysOnly {
		return nil
	}
	return v
}

// inBounds reports whether key is in [lower, upper); a nil bound is open.
func inBounds(key, lower, upper []byte) bool {
	if lower != nil && bytes.Compare(key, lower) < 0 {
		return false
	}
	if upper != nil && bytes.Compare(key, upper) >= 0 {
		return false
	}
	return true
}

// SeekGE positions at the first visible key >= key (in forward order) and reports
// whether the iterator is valid. Under reverse it positions at the last key <= the
// equivalent point, mirroring the engine primitive (spec 11 §3).
func (it *Iterator) SeekGE(key []byte) bool {
	if it.reverse {
		// Reverse needs the whole materialized range to step backward; it always
		// comes through the materialized path, so the buffer is already complete.
		idx := sort.Search(len(it.items), func(i int) bool {
			return bytes.Compare(it.items[i].key, key) >= 0
		})
		// In reverse, "seek to >= key" means the largest key <= the search point;
		// step back from the first key >= key.
		it.pos = idx - 1
		return it.Valid()
	}
	it.fillKeyGE(key)
	it.pos = sort.Search(len(it.items), func(i int) bool {
		return bytes.Compare(it.items[i].key, key) >= 0
	})
	return it.Valid()
}

// SeekLT positions at the last visible key < key in the iteration direction.
func (it *Iterator) SeekLT(key []byte) bool {
	if it.reverse {
		idx := sort.Search(len(it.items), func(i int) bool {
			return bytes.Compare(it.items[i].key, key) >= 0
		})
		it.pos = idx
		return it.Valid()
	}
	it.fillKeyGE(key)
	idx := sort.Search(len(it.items), func(i int) bool {
		return bytes.Compare(it.items[i].key, key) >= 0
	})
	it.pos = idx - 1
	return it.Valid()
}

// First positions at the first key in the iteration direction.
func (it *Iterator) First() bool {
	if it.reverse {
		it.pos = len(it.items) - 1
	} else {
		it.fillTo(0)
		it.pos = 0
	}
	return it.Valid()
}

// Last positions at the last key in the iteration direction.
func (it *Iterator) Last() bool {
	if it.reverse {
		it.pos = 0
	} else {
		it.drainAll()
		it.pos = len(it.items) - 1
	}
	return it.Valid()
}

// Next advances one key in the iteration direction.
func (it *Iterator) Next() bool {
	if it.reverse {
		it.pos--
	} else {
		it.pos++
		it.fillTo(it.pos)
	}
	return it.Valid()
}

// Prev steps back one key in the iteration direction.
func (it *Iterator) Prev() bool {
	if it.reverse {
		it.pos++
	} else {
		it.pos--
	}
	return it.Valid()
}

// Valid reports whether the iterator is positioned on a key.
func (it *Iterator) Valid() bool {
	return it.pos >= 0 && it.pos < len(it.items)
}

// Key returns the user key at the cursor. The bytes are owned by the iterator.
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.items[it.pos].key
}

// Value returns the value at the cursor, or nil for a key-only iterator.
func (it *Iterator) Value() ([]byte, error) {
	if !it.Valid() {
		return nil, nil
	}
	return it.items[it.pos].val, nil
}

// Error reports any error that ended iteration. The materialized path surfaces its
// build-time error at NewIterator and never sets this; the streaming path records a
// mid-scan ScanForward error here so a fill that stops early is distinguishable from
// a genuine end of range.
func (it *Iterator) Error() error { return it.err }

// Close releases the iterator. The streaming path holds an engine reader open for its
// lifetime (ScanForward re-descends per call, so it pins nothing between calls, but the
// reader object itself is closed here); the materialized path holds no engine resources
// past construction.
func (it *Iterator) Close() error {
	if it.rd != nil {
		it.rd.Close()
		it.rd = nil
	}
	it.stream = nil
	it.scursor = nil
	it.drained = true
	it.items = nil
	it.pos = -1
	return nil
}
