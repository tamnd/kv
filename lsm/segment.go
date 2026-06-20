package lsm

import (
	"encoding/binary"
	"fmt"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// A segment is one immutable on-disk sorted run, the single-file analog of a
// classic LSM's SST (spec 06 §1). A flush turns one sealed memtable into a
// segment; a compaction merges several into one. This slice builds the segment's
// on-disk format and its reader in isolation, ahead of the flush that produces one
// and the MANIFEST that catalogs one, so the format is settled and tested before
// anything depends on it.
//
// The layout is a chain of data pages plus a footer page (spec 06 §4.1). A data
// page is a run of sorted (internalKey, value) cells, identical in shape to a
// B-tree leaf, and carries the page number of the next data page in its common
// header's overflow slot, so a scan walks the chain without a separate index. The
// footer page records the chain head and the segment's metadata: its key range, its
// largest version, and its cell count. The footer is the page the MANIFEST will
// reference, the segment's durable root.
//
// Deferred to later slices: a persistent block index for in-segment seeks, the
// Bloom/Ribbon filter that skips a segment on a point miss, block compression, and
// value separation for cells too large to inline. A cell larger than the usable
// page area is rejected here for that reason; the value log removes the limit.

const (
	// segDataHeaderSize is a data page's header: the common 8-byte preamble, whose
	// overflow slot holds the next data page number (0 ends the chain).
	segDataHeaderSize = format.CommonHeaderSize
	// segFooterHeaderSize is the footer page's header; its payload follows.
	segFooterHeaderSize = format.CommonHeaderSize
)

// segment is a read handle to one on-disk run. The byte slices are owned copies, so
// a handle outlives the pages it was read from.
type segment struct {
	footer     format.PageNo // the page the MANIFEST records
	head       format.PageNo // first data page, or NoPage when the run is empty
	minKey     []byte        // smallest user key, nil when empty
	maxKey     []byte        // largest user key, nil when empty
	maxVersion uint64        // largest commit version any cell carries
	numCells   int           // total cells across the chain
	pages      int           // data pages plus the footer, for space accounting
}

// pendingPage is one data page's cells, accumulated before the pages are allocated
// so their next-pointers can be filled in once the page numbers are known.
type pendingPage struct {
	body  []byte // cells, appended after the header is reserved
	cells int
	first []byte // first internal key on the page, for a future block index
}

// writeSegment serializes the cells yielded by src, in ascending internal-key
// order, into a fresh on-disk segment and returns its handle. src must emit cells
// already ordered by format.CompareInternal, which is exactly what a memtable scan
// produces. The pages are allocated from the pager's freelist or the file tail and
// left dirty for the next checkpoint to fold, the same path every engine write
// takes.
func writeSegment(pgr *pager.Pager, src func(emit func(internalKey, value []byte) bool)) (*segment, error) {
	usable := pgr.Header().UsablePageSize()
	// The most a single cell can occupy: two length varints plus the bytes. A page
	// must hold at least one whole cell, so a cell larger than the usable area minus
	// the header cannot be stored inline and is rejected until value separation lands.
	maxCell := usable - segDataHeaderSize

	var (
		pages      []pendingPage
		cur        pendingPage
		minKey     []byte
		maxKey     []byte
		maxVersion uint64
		numCells   int
		emitErr    error
	)
	startPage := func(ik []byte) {
		cur = pendingPage{
			body:  make([]byte, segDataHeaderSize, usable),
			first: append([]byte(nil), ik...),
		}
	}
	src(func(ik, val []byte) bool {
		cellLen := uvarintLen(uint64(len(ik))) + len(ik) + uvarintLen(uint64(len(val))) + len(val)
		if cellLen > maxCell {
			emitErr = fmt.Errorf("lsm: cell of %d bytes exceeds the usable page area of %d; value separation is required", cellLen, maxCell)
			return false
		}
		if cur.cells == 0 {
			startPage(ik)
		} else if len(cur.body)+cellLen > usable {
			pages = append(pages, cur)
			startPage(ik)
		}
		cur.body = format.AppendUvarint(cur.body, uint64(len(ik)))
		cur.body = append(cur.body, ik...)
		cur.body = format.AppendUvarint(cur.body, uint64(len(val)))
		cur.body = append(cur.body, val...)
		cur.cells++

		uk := format.UserKey(ik)
		if minKey == nil {
			minKey = append([]byte(nil), uk...)
		}
		maxKey = append(maxKey[:0], uk...)
		if v := format.Version(ik); v > maxVersion {
			maxVersion = v
		}
		numCells++
		return true
	})
	if emitErr != nil {
		return nil, emitErr
	}
	if cur.cells > 0 {
		pages = append(pages, cur)
	}

	seg := &segment{
		head:       format.NoPage,
		minKey:     minKey,
		maxKey:     append([]byte(nil), maxKey...),
		maxVersion: maxVersion,
		numCells:   numCells,
	}
	if numCells == 0 {
		seg.maxKey = nil
	}

	// Allocate every data page up front so each page's next-pointer can name its
	// successor before any page is written.
	pgnos := make([]format.PageNo, len(pages))
	frames := make([]*pager.Frame, len(pages))
	for i := range pages {
		pgno, fr, err := pgr.Allocate()
		if err != nil {
			return nil, err
		}
		pgnos[i] = pgno
		frames[i] = fr
	}
	for i := range pages {
		next := format.NoPage
		if i+1 < len(pages) {
			next = pgnos[i+1]
		}
		header := format.CommonHeader{
			Type:      format.PageLSMBlock,
			CellCount: uint16(pages[i].cells),
			Overflow:  next,
		}
		header.Encode(pages[i].body)
		data := frames[i].Data()
		copy(data, pages[i].body)
		// Zero any tail left from a reused page so the unused area is deterministic.
		for j := len(pages[i].body); j < len(data); j++ {
			data[j] = 0
		}
		pgr.Unpin(frames[i], true)
	}
	if len(pgnos) > 0 {
		seg.head = pgnos[0]
	}

	footer, err := writeFooter(pgr, seg)
	if err != nil {
		return nil, err
	}
	seg.footer = footer
	seg.pages = len(pgnos) + 1 // data pages plus the footer
	return seg, nil
}

// writeFooter writes the segment's footer page and returns its number. The footer
// is small and fixed in shape, so it always fits one page.
func writeFooter(pgr *pager.Pager, seg *segment) (format.PageNo, error) {
	pgno, fr, err := pgr.Allocate()
	if err != nil {
		return 0, err
	}
	body := make([]byte, segFooterHeaderSize)
	format.CommonHeader{Type: format.PageLSMBlock, Flags: footerFlag}.Encode(body)
	var u32 [4]byte
	binary.BigEndian.PutUint32(u32[:], seg.head)
	body = append(body, u32[:]...)
	body = format.AppendUvarint(body, uint64(seg.numCells))
	var u64 [8]byte
	binary.BigEndian.PutUint64(u64[:], seg.maxVersion)
	body = append(body, u64[:]...)
	body = format.AppendUvarint(body, uint64(len(seg.minKey)))
	body = append(body, seg.minKey...)
	body = format.AppendUvarint(body, uint64(len(seg.maxKey)))
	body = append(body, seg.maxKey...)

	usable := pgr.Header().UsablePageSize()
	if len(body) > usable {
		pgr.Unpin(fr, false)
		return 0, fmt.Errorf("lsm: segment footer of %d bytes exceeds the usable page area of %d", len(body), usable)
	}
	data := fr.Data()
	copy(data, body)
	for j := len(body); j < len(data); j++ {
		data[j] = 0
	}
	pgr.Unpin(fr, true)
	return pgno, nil
}

// footerFlag marks a footer page in its common header's flags byte, distinguishing
// it from a data page of the same page type.
const footerFlag byte = 0x01

// openSegment reads a segment's footer page and returns the handle, the path
// recovery takes to rebuild a segment from the page number the MANIFEST recorded.
func openSegment(pgr *pager.Pager, footer format.PageNo) (*segment, error) {
	fr, err := pgr.Get(footer, pager.Read)
	if err != nil {
		return nil, err
	}
	defer pgr.Unpin(fr, false)
	data := fr.Data()
	h := format.DecodeCommonHeader(data)
	if h.Type != format.PageLSMBlock || h.Flags != footerFlag {
		return nil, fmt.Errorf("lsm: page %d is not a segment footer", footer)
	}
	off := segFooterHeaderSize
	seg := &segment{footer: footer}
	seg.head = binary.BigEndian.Uint32(data[off:])
	off += 4
	numCells, n := format.Uvarint(data[off:])
	off += n
	seg.numCells = int(numCells)
	seg.maxVersion = binary.BigEndian.Uint64(data[off:])
	off += 8
	minLen, n := format.Uvarint(data[off:])
	off += n
	if minLen > 0 {
		seg.minKey = append([]byte(nil), data[off:off+int(minLen)]...)
		off += int(minLen)
	}
	maxLen, n := format.Uvarint(data[off:])
	off += n
	if maxLen > 0 {
		seg.maxKey = append([]byte(nil), data[off:off+int(maxLen)]...)
	}
	return seg, nil
}

// scan calls fn for every (internalKey, value) cell in the segment in ascending
// internal-key order, walking the data-page chain. The slices alias the pinned
// page and are valid only for the duration of the call, so fn copies what it
// keeps. It stops early if fn returns false.
func (s *segment) scan(pgr *pager.Pager, fn func(internalKey, value []byte) bool) error {
	for pgno := s.head; pgno != format.NoPage; {
		fr, err := pgr.Get(pgno, pager.Read)
		if err != nil {
			return err
		}
		data := fr.Data()
		h := format.DecodeCommonHeader(data)
		if h.Type != format.PageLSMBlock {
			pgr.Unpin(fr, false)
			return fmt.Errorf("lsm: page %d in segment chain is not an LSM block", pgno)
		}
		off := segDataHeaderSize
		stop := false
		for i := 0; i < int(h.CellCount); i++ {
			klen, n := format.Uvarint(data[off:])
			off += n
			key := data[off : off+int(klen)]
			off += int(klen)
			vlen, n := format.Uvarint(data[off:])
			off += n
			val := data[off : off+int(vlen)]
			off += int(vlen)
			if !fn(key, val) {
				stop = true
				break
			}
		}
		next := h.Overflow
		pgr.Unpin(fr, false)
		if stop {
			return nil
		}
		pgno = next
	}
	return nil
}

// uvarintLen reports how many bytes the unsigned varint encoding of v occupies,
// so the packer can size a cell without encoding it twice.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
