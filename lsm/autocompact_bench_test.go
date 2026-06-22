package lsm

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// BenchmarkAutoCompactionReadFanout measures what W4 buys the read path: under a sustained
// write stream the background compactor keeps L0 shallow, so a point read folds a handful of
// segments instead of the whole unbounded run a flush-only engine accumulates. It pre-loads a
// fixed stream of small batches (one segment each at the low cap), then times point reads over
// the settled tree, reporting read latency percentiles in microseconds and the final L0 depth.
// The "off" variant disables the compactor, so L0 grows to one segment per batch and every
// read pays that fan-out; the "on" variant is the default. The gap between the two L0-depth
// and p99 numbers is the slice's headline.
func BenchmarkAutoCompactionReadFanout(b *testing.B) {
	for _, auto := range []bool{false, true} {
		name := "compactor=off"
		if auto {
			name = "compactor=on"
		}
		b.Run(name, func(b *testing.B) {
			l := newAutoLSMBench(b, auto)
			l.memtableCap = 1 // one segment per applied batch

			const batches = 200
			const perBatch = 20
			version := uint64(1)
			for s := 0; s < batches; s++ {
				batch := engine.NewWriteBatch(version)
				for i := 0; i < perBatch; i++ {
					key := fmt.Sprintf("key%05d", (s*perBatch+i)%(batches*perBatch))
					batch.Set([]byte(key), []byte(fmt.Sprintf("v%d", version)))
				}
				if err := l.Apply(batch, version); err != nil {
					b.Fatalf("apply batch %d: %v", s, err)
				}
				version++
			}
			l.settleAutoBench(b)

			l.mu.Lock()
			l0 := 0
			if len(l.levelsLocked()) > 0 {
				l0 = len(l.levelsLocked()[0])
			}
			l.mu.Unlock()

			rd, err := l.NewReader(engine.Snapshot{Version: version})
			if err != nil {
				b.Fatalf("reader: %v", err)
			}
			defer rd.Close()

			const total = batches * perBatch
			samples := make([]time.Duration, 0, b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("key%05d", i%total))
				start := time.Now()
				if _, err := rd.Get(key); err != nil {
					b.Fatalf("get: %v", err)
				}
				samples = append(samples, time.Since(start))
			}
			b.StopTimer()

			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			pct := func(p float64) float64 {
				if len(samples) == 0 {
					return 0
				}
				return float64(samples[int(p*float64(len(samples)-1))].Nanoseconds()) / 1000
			}
			b.ReportMetric(pct(0.50), "p50-us/read")
			b.ReportMetric(pct(0.99), "p99-us/read")
			b.ReportMetric(float64(l0), "L0-depth")
		})
	}
}

// BenchmarkReadUnderCompaction measures what R3 (the immutable segment-list snapshot) buys: a
// point read takes the engine lock only briefly to reference the current version, then probes
// that version's segments with no lock held, so a continuously running background compactor no
// longer serializes against readers on l.mu. The benchmark runs parallel readers against a tree
// a writer goroutine keeps churning, so flushes and compactions install new versions throughout
// the read phase, the exact moment the old RLock-across read path would have stalled behind a
// compaction's install. It reports read throughput and the p99 read latency; run with
// -mutexprofile to confirm l.mu contention from the read path is gone.
func BenchmarkReadUnderCompaction(b *testing.B) {
	l := newAutoLSMBench(b, true)
	l.memtableCap = 1

	const total = 4000
	version := uint64(1)
	for s := 0; s < 200; s++ {
		batch := engine.NewWriteBatch(version)
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key%05d", (s*20+i)%total)
			batch.Set([]byte(key), []byte(fmt.Sprintf("v%d", version)))
		}
		if err := l.Apply(batch, version); err != nil {
			b.Fatalf("apply batch %d: %v", s, err)
		}
		version++
	}
	l.settleAutoBench(b)

	// A writer that keeps applying so the compactor stays busy installing new versions for the
	// whole read phase. It stops when the readers finish.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		v := version
		for {
			select {
			case <-stop:
				return
			default:
			}
			batch := engine.NewWriteBatch(v)
			for i := 0; i < 20; i++ {
				key := fmt.Sprintf("key%05d", int(v*20+uint64(i))%total)
				batch.Set([]byte(key), []byte(fmt.Sprintf("v%d", v)))
			}
			if err := l.Apply(batch, v); err != nil {
				return
			}
			v++
		}
	}()

	snap := engine.Snapshot{Version: version}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rd, err := l.NewReader(snap)
		if err != nil {
			b.Errorf("reader: %v", err)
			return
		}
		defer rd.Close()
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("key%05d", i%total))
			if _, err := rd.Get(key); err != nil {
				b.Errorf("get: %v", err)
				return
			}
			i++
		}
	})
	b.StopTimer()
	close(stop)
	<-done
}

// newAutoLSMBench is newAutoLSM for a benchmark, with the compactor toggle exposed so the
// bench can compare a flush-only engine against the self-compacting default.
func newAutoLSMBench(b *testing.B, auto bool) *LSM {
	b.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "autobench.kv", pager.Options{
		PageSize:    4096,
		CacheFrames: 256,
		Engine:      format.EngineLSM,
	})
	if err != nil {
		b.Fatalf("create pager: %v", err)
	}
	l := New(p)
	l.autoCompact = auto
	if err := l.Open(&engine.Env{Pager: p, Options: engine.EngineOptions{PageSize: p.PageSize()}}); err != nil {
		b.Fatalf("open lsm: %v", err)
	}
	b.Cleanup(func() { l.Close() })
	return l
}

// settleAutoBench is settleAuto for a benchmark: drain the sealed queue and any due
// compaction so the read phase measures a stable tree.
func (l *LSM) settleAutoBench(b *testing.B) {
	b.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	for (len(l.imm) > 0 || l.compactionDueLocked()) && l.flushErr == nil {
		l.flushCond.Wait()
	}
	if l.flushErr != nil {
		b.Fatalf("background flush/compaction: %v", l.flushErr)
	}
}
