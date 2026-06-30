package hlog

import (
	"encoding/binary"
	"sync"
	"testing"
)

// This file is the step-one technique decision for the clean-room engine: how the write
// path reserves space in the log. The append path is the single hottest path in a
// write-heavy store, so the choice between a lock-free fetch-add and a mutex-guarded
// bump is the first thing to settle with numbers, not opinion.
//
// The claim under test: a fetch-add append holds its per-op cost flat (or improving) as
// cores rise, because two appenders never wait on each other, while a mutex append
// serializes on the lock and its per-op cost climbs once the lock cache-line starts
// bouncing between cores. Run with -cpu=1,2,4,8 and read the trend, not a single row.
//
// The verdict and the measured board live in the impl note,
// notes/Spec/2059/implementation/101-lock-free-append.md. The losing candidate stays
// here as lockedLog so the comparison is reproducible and the decision is auditable.

// lockedLog is the mutex-guarded append the lock-free Log replaces. It does identical
// per-record work, a length prefix and a payload copy, behind a mutex instead of a
// fetch-add, so the benchmark isolates the cost of the concurrency control alone.
type lockedLog struct {
	buf  []byte
	tail int64
	mu   sync.Mutex
}

func newLockedLog(capBytes int64) *lockedLog { return &lockedLog{buf: make([]byte, capBytes)} }

func (l *lockedLog) Append(rec []byte) int64 {
	l.mu.Lock()
	off := l.tail
	n := int64(hdrLen + len(rec))
	l.tail += n
	binary.LittleEndian.PutUint32(l.buf[off:off+hdrLen], uint32(len(rec)))
	copy(l.buf[off+hdrLen:off+n], rec)
	l.mu.Unlock()
	return off
}

var benchRec = []byte("a-typical-value-stands-in-here-for-the-append-benchmark-payload-x")

// BenchmarkAppendLockFree measures the lock-free Log under contention.
func BenchmarkAppendLockFree(b *testing.B) {
	l := New(int64(b.N)*int64(hdrLen+len(benchRec)) + 1<<20)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Append(benchRec)
		}
	})
}

// BenchmarkAppendMutex measures the mutex-guarded baseline under the same contention.
func BenchmarkAppendMutex(b *testing.B) {
	l := newLockedLog(int64(b.N)*int64(hdrLen+len(benchRec)) + 1<<20)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Append(benchRec)
		}
	})
}
