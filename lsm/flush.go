package lsm

import "sync"

// Background flush (spec 06 §2, perf/03 W3). The active memtable fills at memtableCap;
// turning it into an on-disk segment serializes the whole thing (~64 MiB), and doing that
// on the writing commit froze every other writer for the duration. This file moves the
// work off the foreground path: Apply seals the full memtable into an immutable queue and
// opens a fresh active one in a few instructions, then a background goroutine drains the
// queue, building each sealed memtable into a segment and installing it. A writer now waits
// only when flushing genuinely cannot keep up, which the bounded queue turns into
// backpressure rather than an unbounded memory climb.
//
// Two locks cooperate. l.mu (the engine's RWMutex) guards the in-memory metadata a reader
// folds: the active memtable, the sealed queue, the levels, the durable mark. The flusher
// holds it only for the two quick metadata steps (peeking the queue head, installing the
// finished segment), never across the segment write, so foreground writers keep inserting
// into the fresh active memtable while a flush runs. flushMu serializes the structural
// value-log and segment writes a build performs against the value-log garbage collector,
// the only other writer of the vLog chain; it is always taken before l.mu when both are
// held, so the two orderings never invert. A reader needs neither lock against the vLog: a
// sealed memtable is write-frozen, and a value pointer becomes visible only after the build
// has synced the pages it names, so a read never races the append cursor.

// defaultMaxImm bounds the sealed queue. Two sealed memtables in flight lets one drain
// while the next seals without the writer waiting, and caps resident memtable memory at
// roughly three memtables (the active plus the queue). When the queue is full Apply blocks
// until the flusher drains one, the backpressure that keeps a write burst that outruns the
// flusher from growing memory without bound.
const defaultMaxImm = 2

// immMem is one sealed memtable awaiting flush. maxLSN is the largest WAL LSN of any batch
// it holds, the mark the flush advances the durable LSN to once the segment is installed,
// so the host can reclaim the log behind it.
type immMem struct {
	mem    *memtable
	maxLSN uint64
}

// startFlusherLocked launches the background flush goroutine, once, at Open. The caller
// holds l.mu. The cond is built over l.mu so the flusher, a backpressured Apply, and a
// flushActive waiter all coordinate through the one lock that guards the queue.
func (l *LSM) startFlusherLocked() {
	if l.flusherUp {
		return
	}
	l.flushCond = sync.NewCond(&l.mu)
	l.flusherDone = make(chan struct{})
	l.maxImm = defaultMaxImm
	l.flusherUp = true
	go l.flushLoop()
}

// sealActiveLocked moves the active memtable into the sealed queue and opens a fresh empty
// one, then wakes the flusher. The caller holds l.mu. The seal is a pointer swap plus a new
// arena allocation, so the writer that triggers it returns at once instead of waiting for a
// segment write. The sealed memtable is now write-frozen: Apply only ever inserts into the
// active one, so the flusher and any reader can walk a sealed memtable's skip list with no
// lock against a concurrent insert.
func (l *LSM) sealActiveLocked() {
	l.imm = append(l.imm, &immMem{mem: l.mem, maxLSN: l.memMaxLSN})
	l.mem = newMemtable(defaultArenaCap)
	l.memMaxLSN = 0
	l.flushCond.Broadcast()
}

// sealForFlushLocked applies backpressure, then seals. The caller holds l.mu. It waits while
// the sealed queue is at its bound so a sustained write burst that outruns the flusher
// blocks the writer rather than growing memory without limit; a sticky flush failure breaks
// the wait so the error surfaces instead of hanging.
func (l *LSM) sealForFlushLocked() error {
	for len(l.imm) >= l.maxImm && l.flushErr == nil {
		l.flushCond.Wait()
	}
	if l.flushErr != nil {
		return l.flushErr
	}
	l.sealActiveLocked()
	return nil
}

// flushLoop is the background flusher. It waits for a sealed memtable, flushes it, and
// repeats until Close. It owns no state across iterations: each sealed memtable is handled
// start to finish by flushOne, so a failure stops the queue without corrupting a partial
// one.
func (l *LSM) flushLoop() {
	defer close(l.flusherDone)
	for {
		l.mu.Lock()
		for len(l.imm) == 0 && !l.closing {
			l.flushCond.Wait()
		}
		if l.closing {
			// Drop any still-sealed memtables: their batches are durable in the WAL and a
			// segment written now would not be folded by a checkpoint after Close, so the
			// host replays them into a fresh memtable on the next open. This matches the
			// active memtable, which Close has always discarded the same way.
			l.mu.Unlock()
			return
		}
		ent := l.imm[0]
		l.mu.Unlock()
		l.flushOne(ent)
	}
}

// flushOne builds one sealed memtable into a segment and installs it. The build runs under
// flushMu but not l.mu, so foreground writers keep inserting while ~64 MiB serializes; the
// install runs under l.mu so the new segment becomes visible and the sealed memtable leaves
// the queue in one step, which is what keeps a reader from folding both the memtable and the
// segment it became (a merge would otherwise apply its operand twice). flushMu wraps both so
// the value-log head the build moved is read by the install before the GC can touch it.
func (l *LSM) flushOne(ent *immMem) {
	// The build and install write segment, manifest, and value-log pages off the host write
	// path, so bracket them in the pager's external-write gate: a checkpoint waits for this
	// flush rather than folding a frame the build is still filling.
	l.pgr.BeginExternalWrite()
	defer l.pgr.EndExternalWrite()
	l.flushMu.Lock()
	seg, buildErr := l.buildSegmentFromMem(ent.mem)

	l.mu.Lock()
	err := buildErr
	if err == nil {
		err = l.installSegmentLocked(seg, ent.maxLSN)
	}
	// Pop the head whether it flushed or failed: on success it is now in the levels, on
	// failure the sticky error stops the engine and retrying the same memtable would only
	// spin. The pop shares this critical section with the install so the queue and the
	// levels move together.
	if len(l.imm) > 0 {
		l.imm = l.imm[1:]
	}
	if err != nil {
		l.flushErr = err
	}
	l.flushCond.Broadcast()
	l.mu.Unlock()
	l.flushMu.Unlock()
}
