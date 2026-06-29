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
// The Model is also what the host layers above the seam (transactions, cache)
// are tested against in isolation, without the real core.
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
	// rangeDels is the live set of range-delete intervals, rebuilt from the marker
	// cells in store. Reads consult it to fold range deletes (spec 11 §4).
	rangeDels []format.RangeDel
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
func (m *Model) Kind() Kind { return F2 }

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
		// A range-delete marker also records its interval so reads can fold it.
		if format.KindOf(e.InternalKey) == format.KindRangeBegin {
			m.rangeDels = append(m.rangeDels, format.RangeDel{
				Lo:      append([]byte(nil), format.UserKey(e.InternalKey)...),
				Hi:      append([]byte(nil), e.Value...),
				Version: format.Version(e.InternalKey),
			})
		}
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
	tc := snap.TTLClock()
	var i int
	for i < len(iks) {
		uk := format.UserKey(iks[i])
		// Gather this user key's version group (already newest-first), dropping the
		// range-delete markers, which resolve through rangeDels rather than as ops.
		var ops []format.Op
		j := i
		for j < len(iks) && bytes.Equal(format.UserKey(iks[j]), uk) {
			ik := iks[j]
			j++
			op, ok := format.OpFromCell(ik, m.store[string(ik)], tc.For(format.KindOf(ik)))
			if !ok {
				continue // range markers resolve through rangeDels, not as ops
			}
			ops = append(ops, op)
		}
		i = j

		rd := format.NewestCoveringRangeDel(m.rangeDels, uk, snap.Version)
		val, ok := format.Fold(ops, snap.Version, rd, m.merge)
		if !ok {
			continue
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

func (r *modelReader) Close() error { return nil }
