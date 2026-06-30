package hlog

import (
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
)

// HybridLog is the larger-than-memory log: one growing file whose recent tail is held in
// a fixed in-memory ring, so the working set lives in RAM and the cold prefix lives on
// disk, in one file, with the look and feel of a single-file store.
//
// The idea it keeps from FASTER, and the tax it drops. A logical address is a byte
// offset into one ever-growing address space. The file IS that address space: the bytes
// of logical address a live at file offset a, with no block table, no per-page header,
// and no allocator mapping addresses to scattered blocks. That direct mapping is the
// whole point. The recent window (tail-ringBytes, tail] is mirrored in the ring buffer
// buf, indexed by a&mask, so a hot read never touches the disk. Older addresses are read
// straight from the file at offset a. None of the pager machinery the old engine carried
// exists here.
//
// Three watermarks order the work, all monotonic, all atomic:
//   - tail: next free logical address, advanced by a single fetch-add per append. This is
//     the lock-free reservation; two appenders never block here.
//   - committed: the highest address below which every record is fully written. Reserve
//     does not write the length header; Commit does, after the caller has filled the
//     payload, so a nonzero header means a complete record. Committed advances in address
//     order, which is what lets the flusher stream a clean prefix.
//   - flushed: the highest address the file holds. The background flusher writes the ring
//     range (flushed, committed] to the file and advances flushed. A record below flushed
//     is safe to overwrite in the ring because it is durable on disk.
//
// Backpressure, not a lock, bounds memory. An append may not overwrite a ring slot whose
// previous occupant is not yet flushed, so it waits until flushed catches up. In steady
// state the flusher keeps ahead and the wait never triggers, so the common append path is
// the fetch-add and a copy.
type HybridLog struct {
	buf       []byte
	ringBytes int64
	mask      int64
	f         *os.File

	tail      atomic.Int64
	committed atomic.Int64
	flushed   atomic.Int64

	flushWake chan struct{}
	closed    chan struct{}
	wg        sync.WaitGroup
}

// maxRecord bounds a single record so a torn length read during a racing ring read is
// rejected by a sanity check instead of driving an out-of-range copy. It also bounds the
// flush chunk. A record larger than this is a misuse the engine does not store.
const maxRecord = 1 << 20

// OpenHybridLog creates or truncates the backing file at path and returns a log whose ring
// holds ringBytes of the recent tail. ringBytes is rounded up to a power of two so the
// address-to-slot map is a mask. It must exceed maxRecord so a record always fits the ring.
func OpenHybridLog(path string, ringBytes int64) (*HybridLog, error) {
	n := int64(1)
	for n < ringBytes {
		n <<= 1
	}
	if n < maxRecord*2 {
		n = maxRecord * 2
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	l := &HybridLog{
		buf:       make([]byte, n),
		ringBytes: n,
		mask:      n - 1,
		f:         f,
		flushWake: make(chan struct{}, 1),
		closed:    make(chan struct{}),
	}
	l.wg.Add(1)
	go l.flushLoop()
	return l, nil
}

// ringWrite copies src into the ring at logical offset off, splitting at the ring end so a
// record that wraps the buffer is written as two contiguous halves. The file never wraps,
// so this split is only about the in-memory mirror.
func (l *HybridLog) ringWrite(off int64, src []byte) {
	pos := off & l.mask
	end := pos + int64(len(src))
	if end <= l.ringBytes {
		copy(l.buf[pos:end], src)
		return
	}
	first := l.ringBytes - pos
	copy(l.buf[pos:], src[:first])
	copy(l.buf[:end-l.ringBytes], src[first:])
}

// ringRead copies len(dst) bytes starting at logical offset off out of the ring, undoing
// the same wrap split. The caller validates the read against the tail afterward.
func (l *HybridLog) ringRead(off int64, dst []byte) {
	pos := off & l.mask
	end := pos + int64(len(dst))
	if end <= l.ringBytes {
		copy(dst, l.buf[pos:end])
		return
	}
	first := l.ringBytes - pos
	copy(dst[:first], l.buf[pos:])
	copy(dst[first:], l.buf[:end-l.ringBytes])
}

// Append writes one record and returns its logical address. It reserves the span with one
// fetch-add, waits only if it would overwrite a not-yet-flushed ring slot, copies the
// payload, then writes the header last so its nonzero value marks the record complete, and
// advances the commit watermark. A wrapping ring cannot hand back one contiguous in-place
// slice, so the store frames its record into a pooled buffer and calls Append; the single
// ring copy here is the cost of the wrap, measured in note 178.
func (l *HybridLog) Append(rec []byte) int64 {
	total := int64(hdrLen + len(rec))
	off := l.tail.Add(total) - total
	for off+total > l.flushed.Load()+l.ringBytes {
		l.wakeFlusher()
		runtime.Gosched()
	}
	var hdr [hdrLen]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(rec)))
	l.ringWrite(off+hdrLen, rec)
	l.ringWrite(off, hdr[:]) // header last: its nonzero value marks the record complete
	l.commit(off, total)
	return off
}

// commit advances the committed watermark in address order. The appender whose record
// starts at the current watermark moves it past its own record; a later appender that
// finished first waits its turn, so committed only ever exposes a contiguous fully-written
// prefix. The wait is the cost of an ordered durable prefix and is short, bounded by the
// number of concurrent appenders, and absent under low concurrency.
func (l *HybridLog) commit(off, total int64) {
	for l.committed.Load() != off {
		runtime.Gosched()
	}
	l.committed.Store(off + total)
	l.wakeFlusher()
}

func (l *HybridLog) wakeFlusher() {
	select {
	case l.flushWake <- struct{}{}:
	default:
	}
}

// flushLoop streams the committed-but-unflushed prefix to the file and advances flushed,
// which both makes the tail durable and frees ring slots for reuse. It writes at the
// record's logical offset, so the file mirrors the address space exactly.
func (l *HybridLog) flushLoop() {
	defer l.wg.Done()
	for {
		c := l.committed.Load()
		f := l.flushed.Load()
		if c > f {
			l.flushRange(f, c)
			l.flushed.Store(c)
			continue
		}
		select {
		case <-l.closed:
			// Final drain: persist whatever committed after the last wake.
			if last := l.committed.Load(); last > l.flushed.Load() {
				l.flushRange(l.flushed.Load(), last)
				l.flushed.Store(last)
			}
			return
		case <-l.flushWake:
		}
	}
}

// flushRange writes logical bytes [from, to) to the file at offset from, splitting the
// read out of the ring at the wrap and writing the file in one or two contiguous pwrites.
func (l *HybridLog) flushRange(from, to int64) {
	for from < to {
		pos := from & l.mask
		span := to - from
		if pos+span > l.ringBytes {
			span = l.ringBytes - pos
		}
		l.f.WriteAt(l.buf[pos:pos+span], from)
		from += span
	}
}

var errShortRecord = errors.New("hlog: record past tail")

// At returns the record bytes at logical address addr, copied into dst (grown as needed)
// since the bytes may live in the ring or on disk and a ring slot can be reused under a
// reader. It first tries the ring with a validating read: snapshot the tail, copy, then
// re-check that the address is still inside the window; if the window moved during the
// copy, the record is now on disk (the backpressure invariant guarantees it was flushed
// before being overwritten), so it falls back to a file read. A record comfortably inside
// the window takes the lock-free ring path.
func (l *HybridLog) At(addr int64, dst []byte) ([]byte, error) {
	t := l.tail.Load()
	if addr < 0 || addr+hdrLen > t {
		return nil, errShortRecord
	}
	if addr >= t-l.ringBytes {
		// Resident path: read length, sanity-check, read payload, then validate.
		var hb [hdrLen]byte
		l.ringRead(addr, hb[:])
		n := int64(binary.LittleEndian.Uint32(hb[:]))
		if n >= 0 && n <= maxRecord && addr+hdrLen+n <= t {
			dst = grow(dst, int(n))
			l.ringRead(addr+hdrLen, dst)
			if addr >= l.tail.Load()-l.ringBytes {
				return dst, nil
			}
		}
		// Window moved during the read, or a torn length: serve from disk below.
	}
	return l.readDisk(addr, dst)
}

// readDisk reads a record straight from the file at offset addr. The file mirrors the
// address space, so the record is contiguous on disk even if it wrapped in the ring.
func (l *HybridLog) readDisk(addr int64, dst []byte) ([]byte, error) {
	var hb [hdrLen]byte
	if _, err := l.f.ReadAt(hb[:], addr); err != nil {
		return nil, err
	}
	n := int(binary.LittleEndian.Uint32(hb[:]))
	if n < 0 || n > maxRecord {
		return nil, errShortRecord
	}
	dst = grow(dst, n)
	if _, err := l.f.ReadAt(dst, addr+hdrLen); err != nil {
		return nil, err
	}
	return dst, nil
}

// grow returns dst resized to n bytes, reusing its capacity when it already fits so a
// reader that passes a scratch buffer reads without allocating.
func grow(dst []byte, n int) []byte {
	if cap(dst) >= n {
		return dst[:n]
	}
	return make([]byte, n)
}

// Tail returns the current tail, the total logical bytes appended.
func (l *HybridLog) Tail() int64 { return l.tail.Load() }

// Close stops the flusher after a final drain and closes the file. After Close every
// committed record is on disk.
func (l *HybridLog) Close() error {
	close(l.closed)
	l.wakeFlusher()
	l.wg.Wait()
	if err := l.f.Sync(); err != nil {
		l.f.Close()
		return err
	}
	return l.f.Close()
}
