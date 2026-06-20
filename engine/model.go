package engine

import (
	"bytes"
	"context"
	"sort"
	"sync"

	"github.com/tamnd/kv/format"
)

// Model is a trivial in-memory ordered-map engine used as the correctness oracle
// (spec 04 §7, spec 23). The real cores must match it for every operation
// sequence. It stores every version of every key keyed by full internal key and
// resolves MVCC visibility with the same internal-key ordering the cores use, so
// any divergence between a core and the Model is a bug in the core.
//
// The Model is also what the host layers above the seam (transactions, iterators,
// cache) are tested against in isolation, without either real core.
type Model struct {
	mu sync.RWMutex
	// store maps string(internalKey) -> value. Re-applying the same internal key
	// (as recovery does) is idempotent.
	store map[string][]byte
	env   *Env
	// merge folds an existing value and an operand into a new value. If nil, a
	// merge operand behaves as a plain set (operand replaces). The real merge
	// registry arrives with the library API (spec 15).
	merge func(existing, operand []byte) []byte
}

// NewModel returns an empty model engine.
func NewModel() *Model {
	return &Model{store: map[string][]byte{}}
}

// SetMergeFunc installs the merge resolver used during version resolution.
func (m *Model) SetMergeFunc(f func(existing, operand []byte) []byte) {
	m.merge = f
}

// Kind implements Engine.
func (m *Model) Kind() Kind { return BTree }

// Open implements Engine.
func (m *Model) Open(env *Env) error {
	m.env = env
	return nil
}

// Close implements Engine.
func (m *Model) Close() error { return nil }

// Apply implements Engine: it installs every entry's internal key into the store.
func (m *Model) Apply(batch *WriteBatch, commitVersion uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range batch.Entries() {
		ik := string(e.InternalKey)
		// A tombstone still occupies a key slot so that it shadows older versions.
		m.store[ik] = append([]byte(nil), e.Value...)
	}
	return nil
}

// Maintain implements Engine: the model has no background work.
func (m *Model) Maintain(ctx context.Context, budget MaintBudget) (MaintReport, error) {
	return MaintReport{}, nil
}

// Stats implements Engine.
func (m *Model) Stats() EngineStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var bytesN int64
	for k, v := range m.store {
		bytesN += int64(len(k) + len(v))
	}
	return EngineStats{PhysicalBytes: bytesN, Amplification: 1}
}

// Reclaim implements Engine: nothing to reclaim in memory.
func (m *Model) Reclaim(budget int) (int, error) { return 0, nil }

// RecoverFinished implements Engine.
func (m *Model) RecoverFinished(lastVersion uint64) error { return nil }

// NewReader implements Engine.
func (m *Model) NewReader(snap Snapshot) (Reader, error) {
	return &modelReader{m: m, snap: snap}, nil
}

// snapshot returns the sorted, MVCC-resolved (userKey,value) view at snap: for
// each user key, the newest version <= snap.Version, with tombstones removed and
// merges folded. The result is sorted by user key ascending.
func (m *Model) snapshot(snap Snapshot) []resolved {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Collect every internal key, sort by the shared ordering (user asc, version
	// desc, kind asc).
	iks := make([][]byte, 0, len(m.store))
	for k := range m.store {
		iks = append(iks, []byte(k))
	}
	sort.Slice(iks, func(i, j int) bool {
		return format.CompareInternal(iks[i], iks[j]) < 0
	})

	var out []resolved
	var i int
	for i < len(iks) {
		uk := format.UserKey(iks[i])
		// Walk this user key's version group (already newest-first). Collect merge
		// operands above the newest visible base (a set or a delete).
		var operands [][]byte // newest-first
		var baseVal []byte
		baseIsSet := false
		baseFound := false
		j := i
		// Consume the entire version group so older, shadowed versions are not
		// reprocessed as a fresh key on the next outer iteration.
		for j < len(iks) && bytes.Equal(format.UserKey(iks[j]), uk) {
			ik := iks[j]
			j++
			if baseFound {
				continue // older than the resolved base; shadowed
			}
			if format.Version(ik) > snap.Version {
				continue // not visible at this snapshot
			}
			if format.KindOf(ik) == format.KindMerge {
				operands = append(operands, m.store[string(ik)])
				continue
			}
			if format.KindOf(ik) == format.KindSet {
				baseVal = m.store[string(ik)]
				baseIsSet = true
			}
			baseFound = true // a set or delete is the base; stop collecting
		}
		i = j

		// The key is present if there is a set base or any merge operand. A delete
		// base with no operands shadows the key; merges on top of a delete start
		// fresh from the operands.
		if !baseIsSet && len(operands) == 0 {
			continue
		}
		var val []byte
		if baseIsSet {
			val = baseVal
		}
		for k := len(operands) - 1; k >= 0; k-- { // fold oldest operand first
			if m.merge != nil {
				val = m.merge(val, operands[k])
			} else {
				val = operands[k]
			}
		}
		out = append(out, resolved{uk: append([]byte(nil), uk...), val: append([]byte(nil), val...)})
	}
	return out
}

type resolved struct {
	uk  []byte
	val []byte
}

type modelReader struct {
	m    *Model
	snap Snapshot
}

func (r *modelReader) Get(userKey []byte) ([]byte, error) {
	view := r.m.snapshot(r.snap)
	idx := sort.Search(len(view), func(i int) bool {
		return bytes.Compare(view[i].uk, userKey) >= 0
	})
	if idx < len(view) && bytes.Equal(view[idx].uk, userKey) {
		return append([]byte(nil), view[idx].val...), nil
	}
	return nil, ErrNotFound
}

func (r *modelReader) NewIter(opts IterOptions) (Cursor, error) {
	view := r.m.snapshot(r.snap)
	lower, upper := opts.Lower, opts.Upper
	if len(opts.Prefix) > 0 {
		lower = opts.Prefix
		upper = format.PrefixSuccessor(opts.Prefix)
	}
	var filtered []resolved
	for _, e := range view {
		if lower != nil && bytes.Compare(e.uk, lower) < 0 {
			continue
		}
		if upper != nil && bytes.Compare(e.uk, upper) >= 0 {
			continue
		}
		filtered = append(filtered, e)
	}
	return &modelCursor{view: filtered, pos: -1, reverse: opts.Reverse}, nil
}

func (r *modelReader) Close() error { return nil }

// modelCursor walks a pre-resolved snapshot view. Bounds and prefix have already
// been applied; reverse flips the direction of First/Last/Next/Prev.
type modelCursor struct {
	view    []resolved
	pos     int
	reverse bool
}

func (c *modelCursor) First() bool {
	if c.reverse {
		c.pos = len(c.view) - 1
	} else {
		c.pos = 0
	}
	return c.Valid()
}

func (c *modelCursor) Last() bool {
	if c.reverse {
		c.pos = 0
	} else {
		c.pos = len(c.view) - 1
	}
	return c.Valid()
}

func (c *modelCursor) Next() bool {
	if c.reverse {
		c.pos--
	} else {
		c.pos++
	}
	return c.Valid()
}

func (c *modelCursor) Prev() bool {
	if c.reverse {
		c.pos++
	} else {
		c.pos--
	}
	return c.Valid()
}

func (c *modelCursor) SeekGE(userKey []byte) bool {
	idx := sort.Search(len(c.view), func(i int) bool {
		return bytes.Compare(c.view[i].uk, userKey) >= 0
	})
	if c.reverse {
		// In reverse, "seek >= key" lands on the first key >= userKey but iteration
		// proceeds downward; callers rarely mix these, so position at idx.
		c.pos = idx
	} else {
		c.pos = idx
	}
	return c.Valid()
}

func (c *modelCursor) SeekLT(userKey []byte) bool {
	idx := sort.Search(len(c.view), func(i int) bool {
		return bytes.Compare(c.view[i].uk, userKey) >= 0
	})
	c.pos = idx - 1
	return c.Valid()
}

func (c *modelCursor) Valid() bool { return c.pos >= 0 && c.pos < len(c.view) }

func (c *modelCursor) Key() []byte {
	if !c.Valid() {
		return nil
	}
	return c.view[c.pos].uk
}

func (c *modelCursor) InternalKey() []byte {
	if !c.Valid() {
		return nil
	}
	// The resolved view does not carry a version; synthesize a max-version
	// internal key so the merge layer's comparisons remain well-defined.
	return format.EncodeInternalKey(c.view[c.pos].uk, format.MaxVersion, format.KindSet)
}

func (c *modelCursor) Value() (LazyValue, error) {
	if !c.Valid() {
		return LazyValue{}, nil
	}
	return InlineValue(c.view[c.pos].val), nil
}

func (c *modelCursor) Error() error { return nil }
func (c *modelCursor) Close() error { return nil }
