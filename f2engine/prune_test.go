package f2engine

import (
	"testing"

	"github.com/tamnd/kv/format"
)

// TestPruneCells checks the snapshot-isolation GC rule on a key's version group: drop the
// cells no reader at or above the watermark can observe, keep the rest, and never drop below a
// merge that still needs its base (redesign-v2 doc 02).
func TestPruneCells(t *testing.T) {
	set := func(v uint64) cell { return cell{version: v, kind: format.KindSet} }
	del := func(v uint64) cell { return cell{version: v, kind: format.KindDelete} }
	mrg := func(v uint64) cell { return cell{version: v, kind: format.KindMerge} }
	versions := func(cs []cell) []uint64 {
		out := make([]uint64, len(cs))
		for i, c := range cs {
			out[i] = c.version
		}
		return out
	}
	eq := func(a, b []uint64) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	cases := []struct {
		name      string
		cells     []cell // newest-first
		watermark uint64
		want      []uint64
	}{
		{
			// No reader pins anything below the head, so only the newest set survives.
			name:      "churn collapses to one",
			cells:     []cell{set(10), set(9), set(8), set(7)},
			watermark: 10,
			want:      []uint64{10},
		},
		{
			// A snapshot held at 8 keeps the head and everything back to the base at or below 8.
			name:      "reader at 8 keeps back to 8",
			cells:     []cell{set(10), set(9), set(8), set(7), set(6)},
			watermark: 8,
			want:      []uint64{10, 9, 8},
		},
		{
			// Watermark zero (pre-announcement, recovery) prunes nothing.
			name:      "zero watermark keeps all",
			cells:     []cell{set(3), set(2), set(1)},
			watermark: 0,
			want:      []uint64{3, 2, 1},
		},
		{
			// A merge at or below the watermark is not a base, so the prune walks past it to the
			// set beneath, which the merge still folds onto.
			name:      "merge keeps its base",
			cells:     []cell{mrg(10), set(5), set(4)},
			watermark: 10,
			want:      []uint64{10, 5},
		},
		{
			// A delete tombstone is a base: a fold that lands on it resolves to not-found.
			name:      "delete is a base",
			cells:     []cell{del(7), set(6), set(5)},
			watermark: 7,
			want:      []uint64{7},
		},
		{
			// Nothing at or below the watermark, so nothing is droppable yet.
			name:      "all above watermark",
			cells:     []cell{set(9), set(8)},
			watermark: 5,
			want:      []uint64{9, 8},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := versions(pruneCells(tc.cells, tc.watermark))
			if !eq(got, tc.want) {
				t.Fatalf("pruneCells(%v, %d) = %v, want %v", versions(tc.cells), tc.watermark, got, tc.want)
			}
		})
	}
}
