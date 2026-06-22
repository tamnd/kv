package lsm

import (
	"testing"

	"github.com/tamnd/kv/engine"
)

// TestLSMPointReadShortCircuit spreads several keys' versions across the memtable, an L0
// segment, and a compacted L1, then reads them at snapshots that exercise every branch of
// the newest-first short-circuit: a shallow set that resolves at the memtable, an old
// snapshot whose visible version sits in the deepest level (so the read must skip the
// invisible newer versions and descend), a merge chain whose operands span the shallow
// sources over a base in the deepest level (so the read must not stop at a merge), and a
// range delete in a shallow source that shadows a set in the deepest level. The
// short-circuit must read no deeper than correctness requires yet still agree with the fold
// in every case.
func TestLSMPointReadShortCircuit(t *testing.T) {
	l := newLSM(t)
	l.SetMergeFunc(concatMerge)

	// v10: the deepest layer. Push it to L1 so later reads have to descend past two
	// shallower sources to reach it.
	b1 := engine.NewWriteBatch(10)
	b1.Set([]byte("k"), []byte("v10"))
	b1.Set([]byte("m"), []byte("base"))
	b1.Set([]byte("r"), []byte("r10"))
	if err := l.Apply(b1, 10); err != nil {
		t.Fatalf("apply b1: %v", err)
	}
	l.flushActive(t)      // -> L0
	forceCompact(t, l, 0) // L0 -> L1, no version dropped at watermark 0

	// v20: the middle layer, an L0 segment. A merge operand on m, a range delete that
	// covers r, and an overwrite of k.
	b2 := engine.NewWriteBatch(20)
	b2.Set([]byte("k"), []byte("v20"))
	b2.Merge([]byte("m"), []byte("+a"))
	b2.DeleteRange([]byte("r"), []byte("r\x00"))
	if err := l.Apply(b2, 20); err != nil {
		t.Fatalf("apply b2: %v", err)
	}
	l.flushActive(t) // -> L0

	// v30: the live memtable. Newest overwrite of k and the newest merge operand on m.
	b3 := engine.NewWriteBatch(30)
	b3.Set([]byte("k"), []byte("v30"))
	b3.Merge([]byte("m"), []byte("+b"))
	if err := l.Apply(b3, 30); err != nil {
		t.Fatalf("apply b3: %v", err)
	}

	type want struct {
		val  string
		gone bool
	}
	cases := []struct {
		snap uint64
		keys map[string]want
	}{
		{snap: 30, keys: map[string]want{
			"k": {val: "v30"},      // resolves at the memtable, reads no segment
			"m": {val: "base+a+b"}, // merge chain folds memtable over L0 over L1 base
			"r": {gone: true},      // range delete at v20 shadows the v10 set
		}},
		{snap: 25, keys: map[string]want{
			"k": {val: "v20"},    // memtable v30 invisible, resolves in L0
			"m": {val: "base+a"}, // only the v20 operand is visible
			"r": {gone: true},    // range delete at v20 is visible
		}},
		{snap: 15, keys: map[string]want{
			"k": {val: "v10"},  // both v30 and v20 invisible, resolves in L1
			"m": {val: "base"}, // no operand visible yet
			"r": {val: "r10"},  // range delete at v20 invisible, the v10 set survives
		}},
	}

	for _, c := range cases {
		rd, err := l.NewReader(engine.Snapshot{Version: c.snap})
		if err != nil {
			t.Fatalf("reader at %d: %v", c.snap, err)
		}
		for k, w := range c.keys {
			v, err := rd.Get([]byte(k))
			if w.gone {
				if err != engine.ErrNotFound {
					t.Fatalf("snap %d Get(%s) = (%q, %v), want ErrNotFound", c.snap, k, v, err)
				}
				continue
			}
			if err != nil || string(v) != w.val {
				t.Fatalf("snap %d Get(%s) = (%q, %v), want %q", c.snap, k, v, err, w.val)
			}
		}
		rd.Close()
	}
}
