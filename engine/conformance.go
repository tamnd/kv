package engine

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/tamnd/kv/format"
)

// Oracle is a simple deterministic reference model of engine semantics, separate
// from the Model engine implementation. The conformance suite (spec 04 §7,
// spec 23) drives both the Oracle and a real Engine through the same operation
// sequence and asserts identical observable results. Any divergence is a bug in
// the engine, since the contract is identical.
//
// The Oracle tracks, per user key, the full version history so it can answer
// point reads and range scans at any snapshot exactly as a correct engine must.
type Oracle struct {
	// versions[userKey] is the list of (version, kind, value) newest-first.
	versions map[string][]oracleVer
	merge    func(existing, operand []byte) []byte
}

type oracleVer struct {
	version uint64
	kind    format.Kind
	value   []byte
}

// NewOracle returns an empty oracle.
func NewOracle(merge func(existing, operand []byte) []byte) *Oracle {
	return &Oracle{versions: map[string][]oracleVer{}, merge: merge}
}

// Apply records a committed batch at commitVersion.
func (o *Oracle) Apply(batch *WriteBatch, commitVersion uint64) {
	for _, e := range batch.Entries() {
		uk := string(format.UserKey(e.InternalKey))
		v := format.Version(e.InternalKey)
		k := format.KindOf(e.InternalKey)
		o.versions[uk] = append(o.versions[uk], oracleVer{version: v, kind: k, value: append([]byte(nil), e.Value...)})
		// Keep newest-first.
		sort.SliceStable(o.versions[uk], func(i, j int) bool {
			return o.versions[uk][i].version > o.versions[uk][j].version
		})
	}
}

// Get returns the value visible at snap, or false if absent.
func (o *Oracle) Get(userKey []byte, snap Snapshot) ([]byte, bool) {
	vers := o.versions[string(userKey)]
	var operands [][]byte
	baseIsSet := false
	var baseVal []byte
	for _, ver := range vers {
		if ver.version > snap.Version {
			continue
		}
		if ver.kind == format.KindMerge {
			operands = append(operands, ver.value)
			continue
		}
		if ver.kind == format.KindSet {
			baseVal = ver.value
			baseIsSet = true
		}
		break
	}
	if !baseIsSet && len(operands) == 0 {
		return nil, false
	}
	var val []byte
	if baseIsSet {
		val = baseVal
	}
	for k := len(operands) - 1; k >= 0; k-- {
		if o.merge != nil {
			val = o.merge(val, operands[k])
		} else {
			val = operands[k]
		}
	}
	return val, true
}

// Scan returns the visible (key,value) pairs in [lower,upper) at snap, sorted by
// key ascending.
func (o *Oracle) Scan(lower, upper []byte, snap Snapshot) []KV {
	keys := make([]string, 0, len(o.versions))
	for k := range o.versions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []KV
	for _, k := range keys {
		uk := []byte(k)
		if lower != nil && bytes.Compare(uk, lower) < 0 {
			continue
		}
		if upper != nil && bytes.Compare(uk, upper) >= 0 {
			continue
		}
		if v, ok := o.Get(uk, snap); ok {
			out = append(out, KV{Key: append([]byte(nil), uk...), Value: v})
		}
	}
	return out
}

// KV is a key/value pair returned by a scan.
type KV struct {
	Key   []byte
	Value []byte
}

// CheckEngine drives eng through the same sequence of committed batches as the
// oracle and verifies that point reads and full scans agree at a range of
// snapshots. It returns the first divergence as an error, or nil if the engine
// conforms. mergeFn is the merge resolver both sides use (may be nil).
//
// ops is a list of committed batches in version order; each batch's Version is
// its commit version. snaps lists the snapshot versions to verify at; if empty,
// every commit version plus the final version is checked.
func CheckEngine(eng Engine, batches []*WriteBatch, mergeFn func(existing, operand []byte) []byte) error {
	if m, ok := eng.(*Model); ok {
		m.SetMergeFunc(mergeFn)
	}
	oracle := NewOracle(mergeFn)

	var maxVer uint64
	snapSet := map[uint64]bool{0: true}
	for _, b := range batches {
		if err := eng.Apply(b, b.Version()); err != nil {
			return fmt.Errorf("engine Apply at version %d: %w", b.Version(), err)
		}
		oracle.Apply(b, b.Version())
		if b.Version() > maxVer {
			maxVer = b.Version()
		}
		snapSet[b.Version()] = true
	}
	snapSet[maxVer] = true

	snaps := make([]uint64, 0, len(snapSet))
	for v := range snapSet {
		snaps = append(snaps, v)
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i] < snaps[j] })

	// Gather the universe of keys to point-check.
	keySet := map[string]bool{}
	for _, b := range batches {
		for _, e := range b.Entries() {
			keySet[string(format.UserKey(e.InternalKey))] = true
		}
	}

	for _, sv := range snaps {
		snap := Snapshot{Version: sv}
		rd, err := eng.NewReader(snap)
		if err != nil {
			return fmt.Errorf("NewReader at %d: %w", sv, err)
		}

		// Point reads.
		for k := range keySet {
			want, wantOK := oracle.Get([]byte(k), snap)
			got, gotErr := rd.Get([]byte(k))
			if wantOK {
				if gotErr != nil {
					rd.Close()
					return fmt.Errorf("snap %d key %q: oracle has value %q, engine returned error %v", sv, k, want, gotErr)
				}
				if !bytes.Equal(got, want) {
					rd.Close()
					return fmt.Errorf("snap %d key %q: engine value %q != oracle %q", sv, k, got, want)
				}
			} else if gotErr != ErrNotFound {
				rd.Close()
				return fmt.Errorf("snap %d key %q: oracle absent, engine returned (%q, %v)", sv, k, got, gotErr)
			}
		}

		// Full forward scan.
		want := oracle.Scan(nil, nil, snap)
		got, err := scanAll(rd, false)
		if err != nil {
			rd.Close()
			return fmt.Errorf("snap %d scan: %w", sv, err)
		}
		if err := compareKVs(sv, want, got); err != nil {
			rd.Close()
			return err
		}

		// Full reverse scan must yield the same set in reverse.
		gotRev, err := scanAll(rd, true)
		if err != nil {
			rd.Close()
			return fmt.Errorf("snap %d reverse scan: %w", sv, err)
		}
		reverseKVs(gotRev)
		if err := compareKVs(sv, want, gotRev); err != nil {
			rd.Close()
			return fmt.Errorf("reverse: %w", err)
		}

		rd.Close()
	}
	_ = context.Background
	return nil
}

func scanAll(rd Reader, reverse bool) ([]KV, error) {
	cur, err := rd.NewIter(IterOptions{Reverse: reverse})
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	var out []KV
	for ok := cur.First(); ok; ok = cur.Next() {
		lv, err := cur.Value()
		if err != nil {
			return nil, err
		}
		v, err := lv.Value()
		if err != nil {
			return nil, err
		}
		out = append(out, KV{Key: append([]byte(nil), cur.Key()...), Value: append([]byte(nil), v...)})
	}
	return out, cur.Error()
}

func reverseKVs(s []KV) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func compareKVs(snap uint64, want, got []KV) error {
	if len(want) != len(got) {
		return fmt.Errorf("snap %d scan: %d pairs, oracle has %d", snap, len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(want[i].Key, got[i].Key) || !bytes.Equal(want[i].Value, got[i].Value) {
			return fmt.Errorf("snap %d scan[%d]: engine (%q=%q) != oracle (%q=%q)",
				snap, i, got[i].Key, got[i].Value, want[i].Key, want[i].Value)
		}
	}
	return nil
}
