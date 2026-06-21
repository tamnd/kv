package lsm

import (
	"fmt"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// The value log is where WiscKey-separated values live (spec 06 §7). A large value is
// the dominant cost in a leveled LSM: every compaction that touches its key rewrites
// the whole value, over and over, as the key ages down the tree. Separation breaks
// that. The value bytes are written once, sequentially, into the vLog, and the segment
// cell keeps only a small pointer (page, offset, length). Compaction then moves the
// pointer, not the bytes, so a blob is written to disk once no matter how many times
// its key is compacted.
//
// The vLog is a forward-growing chain of PageVLog pages in the one database file, the
// same single-file discipline every other structure follows. Values are appended into
// the current tail page; when a value does not fit the remaining body it spills onto a
// fresh page, chained through the common header's overflow slot, so a value larger than
// a page is no problem. This is also what lifts the old inline-cell ceiling: a value
// too large to fit a segment cell is no longer rejected, it is separated here.
//
// The append cursor is in-memory only. A pointer names an absolute page and offset, so
// it resolves after a reopen without any cursor state: the pages it references are
// allocated (never on the freelist) and folded by the checkpoint, so they survive a
// restart, and a read just follows the chain from the page the pointer names. A fresh
// open starts a new tail page and leaves the old chain in place for its pointers to
// keep resolving. Reclaiming the space dead values leave behind is the value-log GC,
// a later slice; until it lands a superseded value's bytes linger in the vLog the way a
// superseded segment's pages linger until compaction.

// vlog is the append cursor and reader for the value log. It is owned by the LSM and
// guarded by the LSM's lock: appends happen under the write lock during a flush, reads
// under the read lock during a Get or scan.
type vlog struct {
	pgr    *pager.Pager
	usable int           // bytes of a page that carry content, after the reserved tail
	tail   format.PageNo // the page appends land on, NoPage before the first append
	buf    []byte        // the tail page's working image, kept resident between appends
	used   int           // body bytes written on the tail page so far
}

// newVLog returns a value log bound to a pager, with no pages allocated yet. The first
// append allocates the first tail page.
func newVLog(pgr *pager.Pager) *vlog {
	return &vlog{
		pgr:    pgr,
		usable: pgr.Header().UsablePageSize(),
		tail:   format.NoPage,
	}
}

// bodyCap is the number of value bytes a single vLog page body holds, the usable page
// size minus the common header.
func (v *vlog) bodyCap() int { return v.usable - segDataHeaderSize }

// append writes value to the tail of the log and returns a pointer to it. A value that
// does not fit the current page spills onto fresh pages, so any size is accepted. The
// returned pointer names the first page and the byte offset within its body where the
// value starts, plus the length, so a reader follows the chain for exactly that many
// bytes. The caller holds the LSM write lock.
func (v *vlog) append(value []byte) (format.ValuePointer, error) {
	if err := v.ensureTail(); err != nil {
		return format.ValuePointer{}, err
	}
	// Start the record on a fresh page when the tail body is full, so the pointer's
	// offset is always a real position inside a body.
	if v.used >= v.bodyCap() {
		if err := v.advance(); err != nil {
			return format.ValuePointer{}, err
		}
	}
	ptr := format.ValuePointer{Page: uint32(v.tail), Offset: uint32(v.used), Length: uint32(len(value))}
	rest := value
	for {
		room := v.bodyCap() - v.used
		n := len(rest)
		if n > room {
			n = room
		}
		copy(v.buf[segDataHeaderSize+v.used:], rest[:n])
		v.used += n
		rest = rest[n:]
		if len(rest) == 0 {
			break
		}
		// The value runs past this page; chain to a new one and keep writing.
		if err := v.advance(); err != nil {
			return format.ValuePointer{}, err
		}
	}
	return ptr, nil
}

// ensureTail allocates the first tail page if the log is empty. The freshly allocated
// page is left to be written at the next sync or advance; only its number is needed
// now.
func (v *vlog) ensureTail() error {
	if v.tail != format.NoPage {
		return nil
	}
	pgno, fr, err := v.pgr.Allocate()
	if err != nil {
		return err
	}
	v.pgr.Unpin(fr, false)
	v.tail = format.PageNo(pgno)
	v.buf = make([]byte, v.usable)
	v.used = 0
	return nil
}

// advance finalizes the current tail page, linking it to a freshly allocated successor,
// and makes that successor the new tail. The finalized page is written with its overflow
// slot pointing at the successor so a read can follow a value that spans the boundary.
func (v *vlog) advance() error {
	pgno, fr, err := v.pgr.Allocate()
	if err != nil {
		return err
	}
	v.pgr.Unpin(fr, false)
	if err := v.writeTail(format.PageNo(pgno)); err != nil {
		return err
	}
	v.tail = format.PageNo(pgno)
	v.buf = make([]byte, v.usable)
	v.used = 0
	return nil
}

// writeTail persists the current tail page image with the given overflow link. It is
// used both to finalize a full page (overflow points at the next page) and to sync a
// partial tail at the end of a flush (overflow is NoPage, since the partial page is the
// last one until more is appended).
func (v *vlog) writeTail(overflow format.PageNo) error {
	fr, err := v.pgr.Get(uint32(v.tail), pager.Write)
	if err != nil {
		return err
	}
	format.CommonHeader{Type: format.PageVLog, CellCount: uint16(v.used), Overflow: overflow}.Encode(v.buf)
	data := fr.Data()
	copy(data, v.buf[:v.usable])
	for j := v.usable; j < len(data); j++ {
		data[j] = 0
	}
	v.pgr.Unpin(fr, true)
	return nil
}

// sync writes the current partial tail page so the values appended into it are durable
// before the segment that points at them becomes visible. It does not finalize the
// page: a later flush keeps appending into the same tail. A log with no tail yet is a
// no-op. The caller holds the LSM write lock.
func (v *vlog) sync() error {
	if v.tail == format.NoPage {
		return nil
	}
	return v.writeTail(format.NoPage)
}

// read returns the value the pointer names, following the page chain from the pointer's
// page and offset for length bytes. The caller holds the LSM read lock.
func (v *vlog) read(p format.ValuePointer) ([]byte, error) {
	out := make([]byte, 0, p.Length)
	pgno := format.PageNo(p.Page)
	off := int(p.Offset)
	remaining := int(p.Length)
	for remaining > 0 {
		if pgno == format.NoPage {
			return nil, fmt.Errorf("lsm: value pointer runs off the end of the value log")
		}
		fr, err := v.pgr.Get(uint32(pgno), pager.Read)
		if err != nil {
			return nil, err
		}
		data := fr.Data()
		h := format.DecodeCommonHeader(data)
		if h.Type != format.PageVLog {
			v.pgr.Unpin(fr, false)
			return nil, fmt.Errorf("lsm: page %d in the value-log chain is not a vLog page", pgno)
		}
		start := segDataHeaderSize + off
		avail := v.usable - start
		if avail < 0 {
			avail = 0
		}
		n := remaining
		if n > avail {
			n = avail
		}
		out = append(out, data[start:start+n]...)
		remaining -= n
		next := h.Overflow
		v.pgr.Unpin(fr, false)
		pgno = next
		off = 0
	}
	return out, nil
}
