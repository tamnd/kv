// Package latch provides a distributed reader-writer latch: a read-mostly drop-in for
// sync.RWMutex with the identical contract (any number of readers, or one exclusive writer)
// but a read side that does not serialize readers against one another.
//
// A plain sync.RWMutex makes every RLock do an atomic read-modify-write on one shared
// readerCount word, so at a few million reads per second across cores that single cache line
// ping-pongs and the reads serialize on it however cheap the work under the lock is. This latch
// instead stripes the lock into one cache-line-padded RWMutex per P. A reader locks only the
// stripe for the P it is running on, so readers on different cores touch different cache lines
// and never invalidate each other; a writer locks every stripe, which is the rare path and can
// afford to be O(stripes). The mutual exclusion is exactly that of a single RWMutex: a writer
// holding all stripes excludes every reader, since each reader holds exactly one, and each
// stripe's mutex supplies the happens-before.
//
// Both the DB-level read latch (perf/10 R1) and the pager shard latch (perf/10 R2) use it, so it
// lives in its own package rather than being duplicated in each.
package latch

import (
	"runtime"
	"sync"
	"unsafe"
)

// RLatch is the distributed reader-writer latch. The zero value is not usable; build one with New.
type RLatch struct {
	shards []shard
}

// shard is one stripe: a single RWMutex padded out so no two stripes share a cache line. Without
// the pad two adjacent stripes' mutex words could land on one 64-byte line and a reader on one
// core would invalidate the other core's stripe on every lock, reintroducing the exact ping-pong
// the striping exists to remove. The pad over-provisions to two lines to stay correct regardless
// of slice element alignment.
type shard struct {
	mu sync.RWMutex
	_  [128 - unsafe.Sizeof(sync.RWMutex{})]byte
}

// New builds a latch with one stripe per P. GOMAXPROCS is read once here; if it later grows,
// RLock folds the larger P id back into range with a modulo, so the latch stays correct (two Ps
// may share a stripe) without resizing.
func New() *RLatch {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	return &RLatch{shards: make([]shard, n)}
}

// RLock takes the read side on the stripe for the current P and returns its index, which the
// caller passes back to RUnlock. It pins to read the P id (the same primitive sync.Pool shards
// on) and unpins immediately: the pin window is just the id read, never the locked region, so it
// adds no preemption latency. The id picks the stripe; a goroutine that migrates to another P
// before RUnlock still releases the stripe it took, because the index is captured here.
func (l *RLatch) RLock() int {
	pid := runtimeProcPin()
	shard := pid % len(l.shards)
	runtimeProcUnpin()
	l.shards[shard].mu.RLock()
	return shard
}

// RUnlock releases the read side on the stripe RLock returned.
func (l *RLatch) RUnlock(shard int) {
	l.shards[shard].mu.RUnlock()
}

// Lock takes the write side: every stripe, in index order. Holding all stripes excludes every
// reader (each holds one) and every other writer (consistent acquisition order, so two writers
// serialize on stripe 0 and never deadlock). This is the rare path.
func (l *RLatch) Lock() {
	for i := range l.shards {
		l.shards[i].mu.Lock()
	}
}

// Unlock releases the write side.
func (l *RLatch) Unlock() {
	for i := range l.shards {
		l.shards[i].mu.Unlock()
	}
}

// runtimeProcPin and runtimeProcUnpin link to the runtime's P pin/unpin, the same primitives
// sync.Pool uses to shard its per-P caches. They add no module dependency (the runtime is the
// standard library) and have been stable since Go 1.3. procPin disables preemption and returns
// the current P id; procUnpin re-enables it. The pin must be released promptly, which RLock does
// after the single id read.
//
//go:linkname runtimeProcPin runtime.procPin
func runtimeProcPin() int

//go:linkname runtimeProcUnpin runtime.procUnpin
func runtimeProcUnpin()
