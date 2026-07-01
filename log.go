// Package hlog is the ground-up F2 hybrid log: a lock-free append-only record log
// over one logical address space. It is the core the clean-room F2 engine is built
// on, and it deliberately drops everything the older f2 package wrapped around the
// idea: no per-shard page pager, no block allocator, no per-page header, no
// directory of page refs. There is one address space. A logical address is a byte
// offset into it. The most recent bytes live in an in-memory ring; older bytes are
// the cold prefix of the same address space on disk, reached by mapping the logical
// address straight to a file offset with no block table in between.
//
// The one invariant that earns the name lock-free: an append reserves its bytes with
// a single atomic fetch-add on the tail and then copies its record into the reserved
// span. Two appenders never block each other and never take a mutex, so the write
// path does not pay a lock tax that serializes cores. This file is step one of the
// build: the in-memory core, with the ring spill and the cold pread added in later
// steps. It is kept small on purpose, because the engine's speed comes from this path
// being short, not from machinery around it.
package kv

import (
	"encoding/binary"
	"sync/atomic"
)

// hdrLen is the per-record length prefix: a little-endian uint32 written ahead of the
// payload. It lets a forward scan find record boundaries during recovery, and it lets
// At read a record back from its address alone. Four bytes caps a record at 4 GiB,
// far above any value the engine stores.
const hdrLen = 4

// Log is the lock-free append-only core. tail is the only write-path state: it is the
// next free logical address, advanced by fetch-add. buf is the in-memory backing for
// the address range the log currently holds. In this step the buffer is sized to hold
// the whole run, so a logical address indexes buf directly; the ring wrap and the disk
// spill that make it larger-than-memory arrive in a later step and change only how an
// address resolves to a byte, never the append path.
type Log struct {
	buf  []byte
	cap  int64
	tail atomic.Int64
}

// New returns a log backed by capBytes of memory. capBytes bounds the total record
// bytes this step can hold, since there is no spill yet; the engine sizes it to the
// working set under test.
func New(capBytes int64) *Log {
	return &Log{buf: make([]byte, capBytes), cap: capBytes}
}

// Reserve claims space for one record of n payload bytes and returns its logical address
// together with the payload slice the caller fills in place. It is the zero-copy write
// primitive: the caller frames its record (a key and a value, say) straight into the
// returned slice with no temporary buffer and no allocation. The reservation is one
// atomic fetch-add on the tail, so two callers never block and never overlap.
//
// A record is published to readers by the index store that follows, which carries the
// happens-before a reader synchronizes against, so the order in which the prefix and the
// payload are written within the reservation does not matter for index-based reads. A
// forward scan reads the prefix to find boundaries, and that runs only during recovery
// when no appender is concurrent.
func (l *Log) Reserve(n int) (int64, []byte) {
	total := int64(hdrLen + n)
	off := l.tail.Add(total) - total
	binary.LittleEndian.PutUint32(l.buf[off:off+hdrLen], uint32(n))
	return off, l.buf[off+hdrLen : off+total]
}

// Append writes one record and returns its logical address. It is Reserve plus a copy,
// kept for callers that already hold the record bytes; the hot write path uses Reserve
// directly to avoid the intermediate buffer.
func (l *Log) Append(rec []byte) int64 {
	off, dst := l.Reserve(len(rec))
	copy(dst, rec)
	return off
}

// At returns the record bytes stored at logical address addr. The returned slice
// aliases the log buffer and must not be mutated by the caller. No lock is taken: the
// address came from a prior Append (directly or through the index), which is the
// happens-before edge that makes the bytes visible.
func (l *Log) At(addr int64) []byte {
	n := binary.LittleEndian.Uint32(l.buf[addr : addr+hdrLen])
	start := addr + hdrLen
	return l.buf[start : start+int64(n)]
}

// Tail returns the current tail, the total logical bytes appended so far. It is the
// upper bound of every valid address and the point a forward scan stops at.
func (l *Log) Tail() int64 { return l.tail.Load() }
