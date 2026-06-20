package format

import (
	"bytes"
	"testing"
)

// concat is a deterministic merge resolver used to check operand folding order.
func concat(existing, operand []byte) []byte {
	return append(append([]byte(nil), existing...), operand...)
}

// op is a small constructor for a newest-first version list.
func op(version uint64, kind Kind, value string) Op {
	return Op{Version: version, Kind: kind, Value: []byte(value)}
}

func TestFoldBasics(t *testing.T) {
	cases := []struct {
		name     string
		ops      []Op // newest-first
		snap     uint64
		rangeDel uint64
		want     string
		present  bool
	}{
		{
			name:    "newest set wins",
			ops:     []Op{op(20, KindSet, "new"), op(10, KindSet, "old")},
			snap:    100,
			want:    "new",
			present: true,
		},
		{
			name:    "snapshot hides newer versions",
			ops:     []Op{op(20, KindSet, "new"), op(10, KindSet, "old")},
			snap:    15,
			want:    "old",
			present: true,
		},
		{
			name:    "delete tombstone hides the key",
			ops:     []Op{op(20, KindDelete, ""), op(10, KindSet, "old")},
			snap:    100,
			present: false,
		},
		{
			name:    "merge folds oldest-first over a set base",
			ops:     []Op{op(30, KindMerge, "c"), op(20, KindMerge, "b"), op(10, KindSet, "a")},
			snap:    100,
			want:    "abc",
			present: true,
		},
		{
			name:    "merge over an absent base starts from the operand",
			ops:     []Op{op(20, KindMerge, "y"), op(10, KindMerge, "x")},
			snap:    100,
			want:    "xy",
			present: true,
		},
		{
			name:     "range delete hides an older set",
			ops:      []Op{op(10, KindSet, "old")},
			snap:     100,
			rangeDel: 20,
			present:  false,
		},
		{
			name:     "set newer than the range delete survives",
			ops:      []Op{op(30, KindSet, "fresh")},
			snap:     100,
			rangeDel: 20,
			want:     "fresh",
			present:  true,
		},
		{
			name:     "merge above a range delete folds over an empty base",
			ops:      []Op{op(30, KindMerge, "+b"), op(10, KindSet, "base")},
			snap:     100,
			rangeDel: 20,
			want:     "+b",
			present:  true,
		},
		{
			name:     "range delete newer than snapshot is ignored",
			ops:      []Op{op(10, KindSet, "old")},
			snap:     15,
			rangeDel: 20,
			want:     "old",
			present:  true,
		},
		{
			name:     "set at the same version as the range delete survives",
			ops:      []Op{op(20, KindSet, "tie")},
			snap:     100,
			rangeDel: 20,
			want:     "tie",
			present:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, present := Fold(tc.ops, tc.snap, tc.rangeDel, concat)
			if present != tc.present {
				t.Fatalf("present = %v, want %v", present, tc.present)
			}
			if present && !bytes.Equal(got, []byte(tc.want)) {
				t.Fatalf("value = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNewestCoveringRangeDel(t *testing.T) {
	dels := []RangeDel{
		{Lo: []byte("b"), Hi: []byte("f"), Version: 10},
		{Lo: []byte("a"), Hi: []byte("z"), Version: 30},
		{Lo: []byte("b"), Hi: []byte("f"), Version: 50}, // newest over [b,f)
	}
	cases := []struct {
		key  string
		snap uint64
		want uint64
	}{
		{"c", 100, 50}, // covered by all three; newest is 50
		{"c", 40, 30},  // 50 is above the snapshot; newest visible is 30
		{"c", 20, 10},  // only the v10 interval is visible
		{"y", 100, 30}, // only [a,z) covers y
		{"z", 100, 0},  // z is excluded by the half-open upper bound
		{"a", 100, 30}, // a is the inclusive lower bound of [a,z)
		{"!", 100, 0},  // before every interval
	}
	for _, tc := range cases {
		got := NewestCoveringRangeDel(dels, []byte(tc.key), tc.snap)
		if got != tc.want {
			t.Fatalf("NewestCoveringRangeDel(%q, snap=%d) = %d, want %d", tc.key, tc.snap, got, tc.want)
		}
	}
}
