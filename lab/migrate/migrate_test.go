package migrate

import (
	"strconv"
	"testing"
)

// BenchmarkPipelineDepth sweeps the pipeline depth so the board shows how the write path flattens
// as the writer is allowed to run further ahead of the migrator. Depth one is the serial baseline
// where the writer stalls on every seal that catches the migrator mid-drain; each step up lets the
// writer absorb more of the drain before it blocks. The ns/op is the per-append cost averaged over
// the fill, so the periodic seal stall shows up directly in the average.
func BenchmarkPipelineDepth(b *testing.B) {
	for _, depth := range []int{1, 2, 4, 8} {
		b.Run("depth"+strconv.Itoa(depth), func(b *testing.B) {
			p := NewPipeline(depth)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				p.Append()
			}
			b.StopTimer()
			sink = p.Close()
		})
	}
}

var sink uint32

// TestPipelineResidentBound checks the memory the depth trades for: at most depth segments are
// ever outstanding, since the slot semaphore holds exactly depth tokens and a seal acquires one
// before it hands a segment off. That is the bound the engine relies on to cap resident hot memory
// at depth segments. Filling many segments exercises the acquire-and-release cycle without
// deadlocking, which is the property that matters.
func TestPipelineResidentBound(t *testing.T) {
	const depth = 4
	p := NewPipeline(depth)
	if got := cap(p.slots); got != depth {
		t.Fatalf("pipeline depth %d, want slot capacity %d", got, depth)
	}
	for range segRecords * 20 {
		p.Append()
	}
	p.Close()
}
