package f2

import (
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// TestSlotPoolEnterLeave is the mechanism unit test: a pool with no reader inside
// reports the max-uint64 safe epoch, a reader that enters pins the global epoch
// into its stripe so the safe epoch drops to it, and leaving restores the empty
// state. This is the QSBR contract the compactor's deferred free will rely on.
func TestSlotPoolEnterLeave(t *testing.T) {
	p := newSlotPool(16)
	var ge atomic.Uint64
	ge.Store(5)

	if got := p.safeEpoch(); got != math.MaxUint64 {
		t.Fatalf("empty pool safe epoch = %d, want MaxUint64", got)
	}

	g := p.enter(&ge, 0)
	if got := p.safeEpoch(); got != 5 {
		t.Fatalf("one reader at epoch 5: safe epoch = %d, want 5", got)
	}

	// A second reader at a later epoch must not raise the safe epoch: it is the
	// minimum across active readers, so the older reader still pins it down.
	ge.Store(9)
	g2 := p.enter(&ge, 1)
	if got := p.safeEpoch(); got != 5 {
		t.Fatalf("two readers (5,9): safe epoch = %d, want 5", got)
	}

	g.leave()
	if got := p.safeEpoch(); got != 9 {
		t.Fatalf("after the older reader left: safe epoch = %d, want 9", got)
	}
	g2.leave()
	if got := p.safeEpoch(); got != math.MaxUint64 {
		t.Fatalf("after all left: safe epoch = %d, want MaxUint64", got)
	}
}

// TestSafeEpochNeverPassesActiveReader is the property the deferred free depends
// on: while a reader holds an epoch r, the safe epoch never advances past r no
// matter how far the global epoch and other readers move. If it did, a block
// retired at r could be reused while this reader still holds an offset into it.
func TestSafeEpochNeverPassesActiveReader(t *testing.T) {
	p := newSlotPool(64)
	var ge atomic.Uint64
	ge.Store(1)

	pinned := p.enter(&ge, 7) // holds epoch 1 for the whole test
	pinnedEpoch := uint64(1)

	var wg sync.WaitGroup
	var bad atomic.Bool
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(stripe uint64) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				ge.Add(1) // churn the global epoch
				g := p.enter(&ge, stripe)
				if p.safeEpoch() > pinnedEpoch {
					bad.Store(true)
				}
				g.leave()
			}
		}(uint64(w + 10))
	}
	wg.Wait()
	pinned.leave()

	if bad.Load() {
		t.Fatal("safe epoch advanced past an active reader's pinned epoch")
	}
	if got := p.safeEpoch(); got != math.MaxUint64 {
		t.Fatalf("after every reader left: safe epoch = %d, want MaxUint64", got)
	}
}

// TestEpochStatsModes checks the observability surface: a memory-only store has no
// epoch state and reports the empty stats with a max safe epoch, while a durable
// store reports a live global epoch and a max safe epoch when no read is in flight.
func TestEpochStatsModes(t *testing.T) {
	mem := mustOpen(t)
	st := mem.EpochStats()
	if st.GlobalEpoch != 0 || st.SafeEpoch != math.MaxUint64 || st.DeferredFrees != 0 {
		t.Fatalf("memory-only EpochStats = %+v, want zero/MaxUint64", st)
	}

	dur := mustOpenDurable(t)
	st = dur.EpochStats()
	if st.GlobalEpoch < 1 {
		t.Fatalf("durable global epoch = %d, want >= 1", st.GlobalEpoch)
	}
	if st.SafeEpoch != math.MaxUint64 {
		t.Fatalf("durable idle safe epoch = %d, want MaxUint64", st.SafeEpoch)
	}
	if st.DeferredFrees != 0 {
		t.Fatalf("durable deferred frees = %d, want 0", st.DeferredFrees)
	}
}

// TestGuardedReadConcurrent runs guarded durable reads against concurrent writers
// while the global epoch is advanced underneath them, with -race on. It guards the
// enter/leave wiring on the real read path: a pin must never tear a lookup and the
// epoch churn must never corrupt a read. It asserts liveness, the same way
// TestConcurrent does, and leans on the race detector for the rest.
func TestGuardedReadConcurrent(t *testing.T) {
	s := mustOpenDurable(t)
	const keys = 20000
	for i := 0; i < keys; i++ {
		if err := s.Set(tkey(i), tval(i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	stop := make(chan struct{})
	var spinner sync.WaitGroup

	// A goroutine churning the global epoch, standing in for the background
	// compactor that will advance it for real. It runs on its own WaitGroup so the
	// readers can finish first and signal it to stop; folding it into the readers'
	// group would deadlock, since it only exits once stop closes.
	spinner.Add(1)
	go func() {
		defer spinner.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.ep.advance()
			}
		}
	}()

	var readers sync.WaitGroup
	for r := 0; r < 8; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			rd := s.NewReader()
			for n := 0; n < keys; n++ {
				v, ok, err := rd.Get(tkey(n))
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				if !ok || string(v) != string(tval(n)) {
					t.Errorf("key %d: ok=%v v=%q", n, ok, v)
					return
				}
			}
		}()
	}
	readers.Wait()
	close(stop)
	spinner.Wait()
}

// mustOpenDurable opens a Full durable store in a temp file for epoch tests.
func mustOpenDurable(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "f2.db")
	s, err := New(Tunables{
		Shards:     8,
		PageSize:   1 << 16,
		Path:       path,
		Durability: DurabilityNormal,
	})
	if err != nil {
		t.Fatalf("New durable: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
