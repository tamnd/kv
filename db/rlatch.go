package db

import (
	"runtime"
	"sync"
	"unsafe"
)

// rlatch is a distributed reader-writer latch: a read-mostly replacement for sync.RWMutex
// with the identical contract (any number of readers, or one exclusive writer) but a read
// side that does not serialize readers against one another.
//
// A plain sync.RWMutex makes every RLock do an atomic read-modify-write on one shared
// readerCount word, so at a few million reads per second across cores that single cache line
// ping-pongs and the reads serialize on it however cheap the work under the lock is (perf/10
// R1, measured: point reads flat at ~5-7M/sec no matter the core count). This latch instead
// stripes the lock into one padded RWMutex per P. A reader locks only the stripe for the P it
// is running on, so readers on different cores touch different cache lines and never invalidate
// each other; a writer locks every stripe, which is the rare path (maintenance, and the commit
// leader once per group) and can afford to be O(stripes). The mutual exclusion is exactly that
// of a single RWMutex: a writer holding all stripes excludes every reader, since each reader
// holds exactly one, and each stripe's mutex supplies the happens-before.
type rlatch struct {
	shards []rlatchShard
}

// rlatchShard is one stripe: a single RWMutex padded out so no two stripes share a cache line.
// Without the pad two adjacent stripes' mutex words could land on one 64-byte line and a reader
// on one core would invalidate the other core's stripe on every lock, reintroducing the exact
// ping-pong the striping exists to remove. The pad over-provisions to two lines to stay correct
// regardless of slice element alignment.
type rlatchShard struct {
	mu sync.RWMutex
	_  [128 - unsafe.Sizeof(sync.RWMutex{})]byte
}

// newRlatch builds a latch with one stripe per P. GOMAXPROCS is read once here; if it later
// grows, rlock folds the larger P id back into range with a modulo, so the latch stays correct
// (two Ps may share a stripe) without resizing.
func newRlatch() *rlatch {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	return &rlatch{shards: make([]rlatchShard, n)}
}

// rlock takes the read side on the stripe for the current P and returns its index, which the
// caller passes back to runlock. It pins to read the P id (the same primitive sync.Pool shards
// on) and unpins immediately: the pin window is just the id read, never the locked region, so it
// adds no preemption latency. The id picks the stripe; a goroutine that migrates to another P
// before runlock still releases the stripe it took, because the index is captured here.
func (l *rlatch) rlock() int {
	pid := runtimeProcPin()
	shard := pid % len(l.shards)
	runtimeProcUnpin()
	l.shards[shard].mu.RLock()
	return shard
}

// runlock releases the read side on the stripe rlock returned.
func (l *rlatch) runlock(shard int) {
	l.shards[shard].mu.RUnlock()
}

// lock takes the write side: every stripe, in index order. Holding all stripes excludes every
// reader (each holds one) and every other writer (consistent acquisition order, so two writers
// serialize on stripe 0 and never deadlock). This is the rare path.
func (l *rlatch) lock() {
	for i := range l.shards {
		l.shards[i].mu.Lock()
	}
}

// unlock releases the write side.
func (l *rlatch) unlock() {
	for i := range l.shards {
		l.shards[i].mu.Unlock()
	}
}

// runtimeProcPin and runtimeProcUnpin link to the runtime's P pin/unpin, the same primitives
// sync.Pool uses to shard its per-P caches. They add no module dependency (the runtime is the
// standard library) and have been stable since Go 1.3. procPin disables preemption and returns
// the current P id; procUnpin re-enables it. The pin must be released promptly, which rlock does
// after the single id read.
//
//go:linkname runtimeProcPin runtime.procPin
func runtimeProcPin() int

//go:linkname runtimeProcUnpin runtime.procUnpin
func runtimeProcUnpin()
