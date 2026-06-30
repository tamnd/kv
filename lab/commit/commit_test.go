package commit

import (
	"testing"
)

var commitRec = make([]byte, 64) // a small record, the framing plus a short value

func BenchmarkSyncEach(b *testing.B) {
	s, err := NewSyncEach(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Append(commitRec)
	}
}

func BenchmarkNoSync(b *testing.B) {
	n, err := NewNoSync(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer n.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n.Append(commitRec)
	}
}

// BenchmarkGroupCommit sweeps the batch size so the board shows where amortizing the fsync
// stops paying. Run with -bench=GroupCommit to see the curve between the two ceilings.
func BenchmarkGroupCommit(b *testing.B) {
	for _, batch := range []int64{1, 8, 32, 128, 1024} {
		b.Run(batchName(batch), func(b *testing.B) {
			g, err := NewGroupCommit(b.TempDir(), batch)
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				g.Append(commitRec)
			}
		})
	}
}

func batchName(n int64) string {
	switch n {
	case 1:
		return "batch1"
	case 8:
		return "batch8"
	case 32:
		return "batch32"
	case 128:
		return "batch128"
	case 1024:
		return "batch1024"
	}
	return "batchN"
}

// TestGroupCommitDurabilityWindow checks the loss window is bounded to one batch: after N
// appends the synced watermark is the largest multiple of batchN not exceeding N, so at most
// batchN-1 records are unsynced at any time.
func TestGroupCommitDurabilityWindow(t *testing.T) {
	const batch = 8
	g, err := NewGroupCommit(t.TempDir(), batch)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	for i := int64(1); i <= 100; i++ {
		g.Append(commitRec)
		unsynced := i - g.Synced()
		if unsynced < 0 || unsynced >= batch {
			t.Fatalf("after %d appends, %d unsynced records, want < %d", i, unsynced, batch)
		}
	}
}
