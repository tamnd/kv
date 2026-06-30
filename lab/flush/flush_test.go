package flush

import "testing"

// BenchmarkWakeEach is the storm baseline: a wake on every commit. The ns/op is the writer's per
// commit cost with the flusher perpetually runnable behind it, and the reported flushes/op shows
// the storm: roughly one flush per commit.
func BenchmarkWakeEach(b *testing.B) {
	w := NewWakeEach()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		w.Commit()
	}
	b.StopTimer()
	b.ReportMetric(float64(w.Flushes())/float64(b.N), "flushes/op")
	w.Close()
}

// BenchmarkWakeThreshold is the engine's policy: wake only when the unflushed prefix reaches the
// trigger. The flushes/op should be a small fraction of WakeEach's, one flush per triggerBytes of
// commits, for the same bounded loss window, and the per commit cost should drop with it because
// the flusher is no longer contending on every record.
func BenchmarkWakeThreshold(b *testing.B) {
	w := NewWakeThreshold()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		w.Commit()
	}
	b.StopTimer()
	b.ReportMetric(float64(w.Flushes())/float64(b.N), "flushes/op")
	w.Close()
}

// TestThresholdBatchesWakes checks the threshold policy triggers about one flush per triggerBytes
// of commits rather than one per commit, the batching the engine relies on. After n commits the
// flush count from the threshold itself (ignoring the slow ticker backstop) is near n*recordBytes
// over triggerBytes, far below n.
func TestThresholdBatchesWakes(t *testing.T) {
	w := NewWakeThreshold()
	const n = triggerBytes / recordBytes * 8 // eight trigger windows
	for range n {
		w.Commit()
	}
	w.Close()
	got := w.Flushes()
	// At most a handful of threshold wakes plus a few ticker backstops; nowhere near one per commit.
	if got > n/100 {
		t.Fatalf("threshold triggered %d flushes over %d commits, want batched (far below n)", got, n)
	}
	if got == 0 {
		t.Fatalf("threshold triggered no flushes over %d commits, want about eight", n)
	}
}
