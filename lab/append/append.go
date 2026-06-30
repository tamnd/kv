// Package append is a frozen experiment from the clean-room engine: how should the log's
// write path reserve space, with a lock-free fetch-add or a mutex-guarded bump?
//
// Verdict: lock-free fetch-add. Two appenders never wait on each other, so the per-op cost
// holds flat or improves as cores rise, while the mutex serializes and its cache line
// bounces between cores once contended. The full board is in impl note 173.
//
// This package is self-contained on purpose: it is a record of the comparison as it was run,
// not a dependency of the engine. The engine carries the winner; this carries both so the
// number is reproducible.
package append

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// hdrLen is the per-record length prefix, the same framing the engine uses, so the two
// candidates copy the same bytes per op and the benchmark isolates the concurrency control.
const hdrLen = 4

// Log is the winner: a fixed buffer with an atomic tail. An append reserves its span with one
// fetch-add and never takes a lock.
type Log struct {
	buf  []byte
	tail atomic.Int64
}

func NewLog(capBytes int64) *Log { return &Log{buf: make([]byte, capBytes)} }

func (l *Log) Append(rec []byte) int64 {
	n := int64(hdrLen + len(rec))
	off := l.tail.Add(n) - n
	binary.LittleEndian.PutUint32(l.buf[off:off+hdrLen], uint32(len(rec)))
	copy(l.buf[off+hdrLen:off+n], rec)
	return off
}

// LockedLog is the loser kept for the comparison: identical per-record work behind a mutex
// instead of a fetch-add.
type LockedLog struct {
	buf  []byte
	tail int64
	mu   sync.Mutex
}

func NewLockedLog(capBytes int64) *LockedLog { return &LockedLog{buf: make([]byte, capBytes)} }

func (l *LockedLog) Append(rec []byte) int64 {
	l.mu.Lock()
	off := l.tail
	n := int64(hdrLen + len(rec))
	l.tail += n
	binary.LittleEndian.PutUint32(l.buf[off:off+hdrLen], uint32(len(rec)))
	copy(l.buf[off+hdrLen:off+n], rec)
	l.mu.Unlock()
	return off
}
