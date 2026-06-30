// Package flush is a frozen experiment: how often should a committing writer wake the background
// flusher? Every committed record could be flushed, but the flush is the expensive step, it ends
// in an fsync, so waking the flusher per commit makes it run a flush per record: a wakeup storm
// where the flusher burns a core doing tiny one-record flushes and the writer pays the signal on
// every commit. The alternative is to let committed-but-unflushed bytes pile up and wake the
// flusher only once a threshold's worth has accumulated, so one flush covers many records and the
// signal is paid once per batch instead of once per record. A timer backstop drains a trickle
// that never reaches the threshold, so an idle writer still becomes durable promptly.
//
// The two candidates bracket the choice. WakeEach signals the flusher on every commit, the
// storm. WakeThreshold signals only when the unflushed prefix reaches triggerBytes and leans on
// a ticker for the tail, the engine's policy. Both run one flusher goroutine whose flush is a
// fixed CPU cost standing in for the write-plus-fsync, so the board is reproducible and shows the
// wakeup shape rather than the disk's latency. The board reports two numbers: the writer's per
// commit cost and the count of flushes the policy triggered. WakeThreshold wins on both, fewer
// flushes for the same durability bound, which is the decision impl note 185 settles.
package flush

import (
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"
)

// flushWork is the fixed cost of one flush, modeled as a CRC over a buffer: a deterministic
// stand-in for the write and fsync the real flusher does once per wake. It is the same for every
// policy, so the only variable is how many times each policy triggers it.
const flushWork = 4 << 10

var flushBuf = make([]byte, flushWork)

func flushOnce() uint32 { return crc32.ChecksumIEEE(flushBuf) }

// recordBytes is the size charged per committed record, so the threshold policy can measure the
// unflushed prefix in bytes the way the engine does (its trigger is a byte count, not a record
// count, because records vary in size).
const recordBytes = 64

// triggerBytes is the unflushed prefix WakeThreshold waits for before it wakes the flusher: one
// flush then covers about triggerBytes/recordBytes records. It mirrors the engine's flushTrigger,
// batch the wakeups so a burst collapses to a handful of flushes.
const triggerBytes = 1 << 20 // 1 MiB, ~16k 64-byte records per flush

// tick is the backstop interval: even when the unflushed prefix never reaches triggerBytes, the
// flusher wakes this often to drain the tail, so an idle writer still becomes durable promptly.
const tick = 2 * time.Millisecond

// flusher is the shared background half both policies drive: a goroutine that waits on a wake
// signal, runs one flush, and advances the flushed watermark. wakes counts how many flushes ran,
// the storm metric the board reports.
type flusher struct {
	wake   chan struct{}
	done   chan struct{}
	wg     sync.WaitGroup
	flushes atomic.Int64 // how many flushes ran, the wakeup-storm count
	sink   atomic.Uint32
}

func newFlusher() *flusher {
	f := &flusher{wake: make(chan struct{}, 1), done: make(chan struct{})}
	f.wg.Add(1)
	go f.loop()
	return f
}

func (f *flusher) loop() {
	defer f.wg.Done()
	for {
		select {
		case <-f.wake:
			f.sink.Store(flushOnce())
			f.flushes.Add(1)
		case <-f.done:
			return
		}
	}
}

// signal is the non-blocking wake the engine uses: a send to a one-slot channel, so a wake that
// finds one already pending coalesces instead of queueing. This is why a storm of WakeEach
// signals does not unboundedly back up the channel; it still forces the flusher to run far more
// flushes than the threshold policy, which is the cost the board measures.
func (f *flusher) signal() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *flusher) stop() {
	close(f.done)
	f.wg.Wait()
}

// WakeEach signals the flusher on every commit: the storm. Each commit pays the signal and the
// flusher is left perpetually runnable, flushing a record at a time.
type WakeEach struct{ f *flusher }

func NewWakeEach() *WakeEach { return &WakeEach{f: newFlusher()} }

func (w *WakeEach) Commit() {
	w.f.signal()
}

// Flushes reports how many flushes the policy triggered, the storm count.
func (w *WakeEach) Flushes() int64 { return w.f.flushes.Load() }

func (w *WakeEach) Close() { w.f.stop() }

// WakeThreshold signals the flusher only when the unflushed prefix reaches triggerBytes, with a
// ticker backstop for the tail: the engine's policy. A burst of commits collapses to one wake per
// triggerBytes, so the flusher runs far fewer flushes for the same bounded loss window.
type WakeThreshold struct {
	f        *flusher
	unflushed int64
	ticker   *time.Ticker
	stop     chan struct{}
	wg       sync.WaitGroup
}

func NewWakeThreshold() *WakeThreshold {
	w := &WakeThreshold{f: newFlusher(), ticker: time.NewTicker(tick), stop: make(chan struct{})}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-w.ticker.C:
				w.f.signal() // backstop: drain a tail that never reached the trigger
			case <-w.stop:
				return
			}
		}
	}()
	return w
}

func (w *WakeThreshold) Commit() {
	w.unflushed += recordBytes
	if w.unflushed >= triggerBytes {
		w.f.signal()
		w.unflushed = 0
	}
}

func (w *WakeThreshold) Flushes() int64 { return w.f.flushes.Load() }

func (w *WakeThreshold) Close() {
	close(w.stop)
	w.wg.Wait()
	w.ticker.Stop()
	w.f.stop()
}
