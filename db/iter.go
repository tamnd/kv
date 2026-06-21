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
// For the B-tree core there is a single engine source, so this layer materializes
// the resolved range once and overlays the small private write buffer on it, rather
// than running a streaming heap-merge (which the LSM core's many sources will need).
// Streaming and read-ahead are later slices; the protocol the caller sees is final.
type Iterator struct {
	items    []iterItem // ascending by user key, already resolved and overlaid
	pos      int
	reverse  bool
	keysOnly bool
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

	base, err := t.db.rangeSnapshot(t.readVersion, lower, upper, opts.KeysOnly)
	if err != nil {
		return nil, err
	}
	items, err := t.overlayBuffer(base, lower, upper, opts.KeysOnly)
	if err != nil {
		return nil, err
	}
	return &Iterator{items: items, pos: -1, reverse: opts.Reverse, keysOnly: opts.KeysOnly}, nil
}

// rangeSnapshot materializes the resolved, version-collapsed view of [lower, upper)
// at version, by walking the engine cursor ascending. It takes the shared read lock
// so it never reads a page mid-commit. KeysOnly skips value materialization.
func (d *DB) rangeSnapshot(version uint64, lower, upper []byte, keysOnly bool) ([]iterItem, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	rd, err := d.eng.NewReader(engine.Snapshot{Version: version, Now: d.now()})
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
	idx := sort.Search(len(it.items), func(i int) bool {
		return bytes.Compare(it.items[i].key, key) >= 0
	})
	if it.reverse {
		// In reverse, "seek to >= key" means the largest key <= the search point;
		// step back from the first key >= key.
		it.pos = idx - 1
	} else {
		it.pos = idx
	}
	return it.Valid()
}

// SeekLT positions at the last visible key < key in the iteration direction.
func (it *Iterator) SeekLT(key []byte) bool {
	idx := sort.Search(len(it.items), func(i int) bool {
		return bytes.Compare(it.items[i].key, key) >= 0
	})
	if it.reverse {
		it.pos = idx
	} else {
		it.pos = idx - 1
	}
	return it.Valid()
}

// First positions at the first key in the iteration direction.
func (it *Iterator) First() bool {
	if it.reverse {
		it.pos = len(it.items) - 1
	} else {
		it.pos = 0
	}
	return it.Valid()
}

// Last positions at the last key in the iteration direction.
func (it *Iterator) Last() bool {
	if it.reverse {
		it.pos = 0
	} else {
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

// Error reports any error that ended iteration. The materialized iterator surfaces
// build-time errors at NewIterator, so this is always nil for now; it exists so the
// streaming form can report mid-scan I/O errors without an API change.
func (it *Iterator) Error() error { return nil }

// Close releases the iterator. The materialized form holds no engine resources past
// construction, so this resets state; the streaming form will unpin sources here.
func (it *Iterator) Close() error {
	it.items = nil
	it.pos = -1
	return nil
}
