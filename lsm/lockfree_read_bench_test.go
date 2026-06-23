package lsm

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// BenchmarkReadUnderApply measures point-read throughput against the active memtable while a
// writer applies batches into that same memtable. The reader keys are seeded once and never
// overwritten, so each read resolves out of the active memtable's skip list, the structure
// slice 19 lets the reader fold with l.mu released. The two sub-benchmarks bracket the win:
// "idle" reads with no writer running, "busy" reads while a writer churns a disjoint key range
// at increasing versions. Before slice 19 the reader's fold held l.mu.RLock and the writer's
// inserts held l.mu.Lock, so "busy" serialized read against write; with the inserts moved out
// from under l.mu the two run concurrently and "busy" should track "idle" rather than collapse
// toward the writer's apply latency (perf/03 W1, perf/07).
func BenchmarkReadUnderApply(b *testing.B) {
	newBench := func(b *testing.B) (*LSM, int) {
		fs := vfs.NewMem()
		p, err := pager.Create(fs, "rua.kv", pager.Options{
			PageSize:    4096,
			CacheFrames: 256,
			Engine:      format.EngineLSM,
		})
		if err != nil {
			b.Fatalf("create pager: %v", err)
		}
		l := New(p)
		// A large cap keeps the reader keys resident in one active memtable for the whole run,
		// so the benchmark isolates the active-memtable read-vs-apply race rather than seal churn.
		if err := l.Open(&engine.Env{Options: engine.EngineOptions{MemtableSize: 64 << 20}}); err != nil {
			b.Fatalf("open lsm: %v", err)
		}
		b.Cleanup(func() { l.Close() })

		const readKeys = 4000
		seed := engine.NewWriteBatch(1)
		for i := 0; i < readKeys; i++ {
			seed.Set([]byte(fmt.Sprintf("r%06d", i)), []byte(fmt.Sprintf("rv%06d", i)))
		}
		l.NoteLSN(1)
		if err := l.Apply(seed, 1); err != nil {
			b.Fatalf("seed apply: %v", err)
		}
		return l, readKeys
	}

	readLoop := func(b *testing.B, l *LSM, readKeys int) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rd, err := l.NewReader(engine.Snapshot{Version: 1 << 30})
			if err != nil {
				b.Errorf("reader: %v", err)
				return
			}
			defer rd.Close()
			i := 0
			for pb.Next() {
				if _, err := rd.Get([]byte(fmt.Sprintf("r%06d", i%readKeys))); err != nil {
					b.Errorf("get: %v", err)
					return
				}
				i++
			}
		})
		b.StopTimer()
	}

	b.Run("idle", func(b *testing.B) {
		l, readKeys := newBench(b)
		readLoop(b, l, readKeys)
	})

	b.Run("busy", func(b *testing.B) {
		l, readKeys := newBench(b)
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			ver := uint64(2)
			for {
				select {
				case <-stop:
					return
				default:
				}
				batch := engine.NewWriteBatch(ver)
				for i := 0; i < 200; i++ {
					batch.Set([]byte(fmt.Sprintf("w%06d", i)), []byte(fmt.Sprintf("wv%d-%d", ver, i)))
				}
				l.NoteLSN(ver)
				if err := l.Apply(batch, ver); err != nil {
					return
				}
				ver++
			}
		}()
		readLoop(b, l, readKeys)
		close(stop)
		<-done
	})
}
