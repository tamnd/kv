package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// A segment is one immutable on-disk sorted run, the single-file analog of a
// classic LSM's SST (spec 06 §1). A flush turns one sealed memtable into a
// segment; a compaction merges several into one.
//
// The layout is a chain of data pages, a chain of index pages, an optional chain
// of range-delete pages, an optional chain of Bloom-filter pages, and a footer page
// (spec 06 §4.1). A data page is a run of
// sorted (internalKey, value) cells, identical in shape to a B-tree leaf, and
// carries the page number of the next data page in its common header's overflow
// slot, so a full scan walks the chain without an index. The index pages hold one
// separator per data page, the data page's first user key paired with its page
// number, so a point read binary-searches to the page that may hold a key instead
// of scanning the run. The range-delete pages hold the segment's range-delete
// intervals, so a point read can fold the deletes that cover a key without scanning
// the run for their markers. The footer records the chain heads and the segment's
// metadata: its key range, its largest version, its cell count, and its total page
// count. The footer is the page the MANIFEST references, the segment's durable root.
//
// Packing keeps a user key's whole version group on one data page (a group larger
// than a page is the one exception and spills onto continuation pages that repeat
// its first user key), so the index seek lands on the page that holds the group's
// newest version and a point read never misses a fresher version on an earlier
// page. This is the segment analog of the B-tree's rule that a split never cuts a
// version group.
//
// The per-segment Bloom filter over the run's distinct user keys (its own page
// chain, head and probe count in the footer) lets a point miss skip the segment
// without touching its block index. Deferred to later slices: block compression and
// value separation for cells too large to inline. A cell larger than the usable page
// area is rejected here for that reason; the value log removes the limit.

// segDataHeaderSize is a data page's header: the common 8-byte preamble, whose
// overflow slot holds the next data page number (0 ends the chain). The index and
// range-delete pages reuse the same preamble.
const segDataHeaderSize = format.CommonHeaderSize

// Flags-byte markers distinguishing the LSM block page roles that share the
// PageLSMBlock type. A data page carries no flag.
const (
	footerFlag     byte = 0x01 // the footer page
	indexFlag      byte = 0x02 // a block-index page
	rangeDelFlag   byte = 0x04 // a range-delete page
	filterFlag     byte = 0x08 // a Bloom-filter page
	compressedFlag byte = 0x10 // a data page whose cells are stored as a compressed frame
)

// segFilter is the per-segment membership filter seam (spec 06 §5). A point read
// consults it before touching a segment's block index: mayContain returns false only
// when the key was definitely never written to the segment, so the segment is skipped;
// a true answer may be a false positive, so the read proceeds. encode returns the blob
// the filter's pages hold, decoded back at open by the kind the footer records. Both
// the default Bloom filter and the opt-in Ribbon filter implement it, so a segment
// carries whichever the engine was configured with behind one field. A nil segFilter
// means "no filter", and a read of such a segment always proceeds.
type segFilter interface {
	mayContain(key []byte) bool
	encode() []byte
}

// filterKind discriminates the filter a segment's footer records, so open decodes its
// filter blob as the right structure. It is stored as a uvarint in the footer.
type filterKind uint8

const (
	filterBloom  filterKind = 0 // the default double-hashing Bloom filter
	filterRibbon filterKind = 1 // the opt-in Standard Ribbon filter
)

// indexEntry is one block-index separator: the first user key of a data page and
// that page's number. The entries are globally ascending by user key, so a point
// read binary-searches them.
type indexEntry struct {
	firstUser []byte
	page      format.PageNo
}

// segment is a read handle to one on-disk run. The byte slices are owned copies, so
// a handle outlives the pages it was read from. The index and range-delete sets are
// loaded into memory once, at write or open, so a point read needs no extra page
// reads to locate a key or to learn which deletes cover it.
type segment struct {
	footer     format.PageNo // the page the MANIFEST records
	head       format.PageNo // first data page, or NoPage when the run is empty
	indexHead  format.PageNo // first index page, or NoPage when the run is empty
	rdHead     format.PageNo // first range-delete page, or NoPage when there are none
	filterHead format.PageNo // first Bloom-filter page, or NoPage when there is none
	minKey     []byte        // smallest user key, nil when empty
	maxKey     []byte        // largest user key, nil when empty
	maxVersion uint64        // largest commit version any cell carries
	numCells   int           // total cells across the chain
	pages      int           // every page the segment owns, for space accounting

	index     []indexEntry      // one separator per data page, ascending by user key
	rangeDels []format.RangeDel // the segment's range-delete intervals
	filter    segFilter         // membership filter over the segment's user keys, or nil

	// vrefs counts how many live versions name this segment (version.go, perf/03 R3). A
	// publish that includes the segment increments it, a version drop that named it
	// decrements it, and the segment's pages are freed when it reaches zero, so a segment a
	// compaction removed from the current version survives until the last reader holding an
	// older version that still names it lets go. Guarded by l.mu.
	vrefs int
}

// pendingPage is one data page's cells, accumulated before the pages are allocated
// so their next-pointers can be filled in once the page numbers are known.
type pendingPage struct {
	body  []byte // cells, appended after the header is reserved
	cells int
	first []byte // first user key on the page, the block-index separator
}

// writeSegment serializes the cells yielded by src, in ascending internal-key
// order, into a fresh on-disk segment and returns its handle. src must emit cells
// already ordered by format.CompareInternal, which is exactly what a memtable scan
// produces. The cells are packed a version group at a time so a group is never
// split across a page boundary (a single group too large for a page is the lone
// exception). The pages are allocated from the pager's freelist or the file tail
// and left dirty for the next checkpoint to fold, the same path every engine write
// takes. bitsPerKey sizes the filter: the caller passes the Monkey budget for the level
// the segment is written at, so a deep segment carries a smaller filter than a shallow
// one. A non-positive value falls back to the flat default. kind selects the filter
// structure: filterBloom is the default, filterRibbon builds the opt-in Ribbon filter,
// which falls back to Bloom when its construction cannot be seeded.
func writeSegment(pgr *pager.Pager, bitsPerKey int, kind filterKind, cdc codecID, src func(emit func(internalKey, value []byte) bool)) (*segment, error) {
	usable := pgr.Header().UsablePageSize()
	// The most a single cell can occupy: two length varints plus the bytes. A page
	// must hold at least one whole cell, so a cell larger than the usable area minus
	// the header cannot be stored inline and is rejected until value separation lands.
	maxCell := usable - segDataHeaderSize
	// With compression on, a page may hold more raw cell bytes than the usable area as
	// long as the compressed frame fits; maxRawBlock caps how far the packer lets a page's
	// raw size run so the read-time decompress buffer and the in-flight candidate copies
	// stay bounded. Four usable pages is generous headroom for a real compression ratio
	// without letting one page's raw block grow without limit.
	maxRawBlock := 4 * usable

	// encodeCell appends one length-prefixed (ik, val) cell onto dst, the on-page cell
	// shape, so the packer can build a prospective page body to size or compression-test it
	// before committing the cells to the current page.
	encodeCell := func(dst, ik, val []byte) []byte {
		dst = format.AppendUvarint(dst, uint64(len(ik)))
		dst = append(dst, ik...)
		dst = format.AppendUvarint(dst, uint64(len(val)))
		dst = append(dst, val...)
		return dst
	}

	// storable reports whether a prospective page body (its reserved header plus cells)
	// can be stored on one page: directly when it already fits the usable area, or, with
	// compression on and the raw block within maxRawBlock, when its compressed frame fits
	// the usable area. A page whose raw cells overrun the usable area is admitted only when
	// it compresses back under a page, which is where the space win comes from in a
	// fixed-page store: more raw cell bytes packed into one physical page.
	storable := func(body []byte) bool {
		if len(body) <= usable {
			return true
		}
		if cdc == codecNone || len(body) > maxRawBlock {
			return false
		}
		frame := compressBlock(cdc, body[segDataHeaderSize:])
		return segDataHeaderSize+len(frame) <= usable
	}

	var (
		pages      []pendingPage
		cur        pendingPage
		minKey     []byte
		maxKey     []byte
		maxVersion uint64
		numCells   int
		rangeDels  []format.RangeDel
		filterKeys [][]byte // one entry per distinct user key, for the Bloom filter
		emitErr    error
	)
	startPage := func(firstUser []byte) {
		cur = pendingPage{
			body:  make([]byte, segDataHeaderSize, usable),
			first: append([]byte(nil), firstUser...),
		}
	}
	flushPage := func() {
		if cur.cells > 0 {
			pages = append(pages, cur)
			cur = pendingPage{}
		}
	}
	appendCell := func(ik, val []byte) {
		cur.body = encodeCell(cur.body, ik, val)
		cur.cells++
	}
	// maxCellsPerPage caps a page's cell count below the uint16 the header records, so the
	// dense packing a compressible block allows never overflows the count field. Without
	// compression the packer fills a page to the usable area long before this many cells
	// fit, so the cap only ever binds on a heavily compressed page.
	const maxCellsPerPage = 60000

	// A version group is buffered whole, then committed onto a page that has room
	// for all of it, so a group never straddles a page boundary unless it alone
	// exceeds a page.
	type cell struct{ ik, val []byte }
	var (
		group     []cell
		groupUser []byte
		groupLen  int
	)
	commitGroup := func() {
		if len(group) == 0 {
			return
		}
		// One filter entry per distinct user key, recorded as the group commits.
		filterKeys = append(filterKeys, append([]byte(nil), groupUser...))
		// Start the group on a fresh page when it will not fit on the current one, so the
		// whole group stays together. With compression on, "fit" means the prospective body
		// either stays within the usable area or compresses back under it, so a page packs
		// more raw cells before it spills; storable decides both. The cell-count ceiling
		// forces a spill regardless, so a densely packed page never overruns the count field.
		if cur.cells > 0 {
			over := false
			if cur.cells+len(group) > maxCellsPerPage {
				over = true
			} else if len(cur.body)+groupLen > usable {
				cand := append([]byte(nil), cur.body...)
				for _, c := range group {
					cand = encodeCell(cand, c.ik, c.val)
				}
				over = !storable(cand)
			}
			if over {
				flushPage()
			}
		}
		if cur.cells == 0 {
			startPage(groupUser)
		}
		for _, c := range group {
			cellLen := uvarintLen(uint64(len(c.ik))) + len(c.ik) + uvarintLen(uint64(len(c.val))) + len(c.val)
			// A group larger than a page spills onto continuation pages that repeat its
			// first user key, the only case a group spans pages. The raw fit test is free
			// until the body passes the usable area, then storable decides, so a compressible
			// run keeps packing past usable while an incompressible one spills at usable.
			if cur.cells > 0 {
				spill := false
				if cur.cells >= maxCellsPerPage {
					spill = true
				} else if len(cur.body)+cellLen > usable {
					cand := encodeCell(append([]byte(nil), cur.body...), c.ik, c.val)
					spill = !storable(cand)
				}
				if spill {
					flushPage()
					startPage(groupUser)
				}
			}
			appendCell(c.ik, c.val)

			uk := format.UserKey(c.ik)
			if minKey == nil {
				minKey = append([]byte(nil), uk...)
			}
			maxKey = append(maxKey[:0], uk...)
			if v := format.Version(c.ik); v > maxVersion {
				maxVersion = v
			}
			if format.KindOf(c.ik) == format.KindRangeBegin {
				rangeDels = append(rangeDels, format.RangeDel{
					Lo:      append([]byte(nil), uk...),
					Hi:      append([]byte(nil), c.val...),
					Version: format.Version(c.ik),
				})
			}
			numCells++
		}
		group = group[:0]
		groupUser = nil
		groupLen = 0
	}

	src(func(ik, val []byte) bool {
		cellLen := uvarintLen(uint64(len(ik))) + len(ik) + uvarintLen(uint64(len(val))) + len(val)
		if cellLen > maxCell {
			emitErr = fmt.Errorf("lsm: cell of %d bytes exceeds the usable page area of %d; value separation is required", cellLen, maxCell)
			return false
		}
		uk := format.UserKey(ik)
		if groupUser != nil && !bytes.Equal(uk, groupUser) {
			commitGroup()
		}
		if groupUser == nil {
			groupUser = append([]byte(nil), uk...)
		}
		group = append(group, cell{
			ik:  append([]byte(nil), ik...),
			val: append([]byte(nil), val...),
		})
		groupLen += cellLen
		return true
	})
	if emitErr != nil {
		return nil, emitErr
	}
	commitGroup()
	flushPage()

	seg := &segment{
		head:       format.NoPage,
		indexHead:  format.NoPage,
		rdHead:     format.NoPage,
		minKey:     minKey,
		maxKey:     append([]byte(nil), maxKey...),
		maxVersion: maxVersion,
		numCells:   numCells,
		rangeDels:  rangeDels,
	}
	if numCells == 0 {
		seg.maxKey = nil
	}

	// Reserve every data page number up front so each page's next-pointer can name its
	// successor before any page is written, then materialize the pages one at a time.
	// Reserving the numbers without pinning a frame bounds the in-flight pin count to a
	// single page, so a segment larger than the buffer pool flushes without exhausting
	// it (perf/05 F2). GetAllocated hands back a zeroed frame, so the unused tail past
	// the body is already deterministic without a separate clearing pass.
	pgnos := make([]format.PageNo, len(pages))
	for i := range pages {
		pgnos[i] = pgr.AllocateNumber()
	}
	for i := range pages {
		next := format.NoPage
		if i+1 < len(pages) {
			next = pgnos[i+1]
		}
		body := pages[i].body
		flags := byte(0)
		// A page whose raw cells overran the usable area was let through only by storable,
		// which proved its frame fits, so compress it now and mark it. A page within the
		// usable area is stored raw: compressing it would not shrink the fixed-size page on
		// disk, so it would only cost decode CPU on every read for no space back.
		if cdc != codecNone && len(body) > usable {
			frame := compressBlock(cdc, body[segDataHeaderSize:])
			out := make([]byte, segDataHeaderSize, segDataHeaderSize+len(frame))
			out = append(out, frame...)
			body = out
			flags = compressedFlag
		}
		header := format.CommonHeader{
			Type:      format.PageLSMBlock,
			Flags:     flags,
			CellCount: uint16(pages[i].cells),
			Overflow:  next,
		}
		header.Encode(body)
		fr, err := pgr.GetAllocated(pgnos[i])
		if err != nil {
			return nil, err
		}
		copy(fr.Data(), body)
		pgr.Unpin(fr, true)
	}
	if len(pgnos) > 0 {
		seg.head = pgnos[0]
	}

	// Build the in-memory block index from the data-page first keys, then persist it.
	seg.index = make([]indexEntry, len(pages))
	indexRecords := make([][]byte, len(pages))
	for i := range pages {
		seg.index[i] = indexEntry{firstUser: pages[i].first, page: pgnos[i]}
		rec := format.AppendUvarint(nil, uint64(len(pages[i].first)))
		rec = append(rec, pages[i].first...)
		var u32 [4]byte
		binary.BigEndian.PutUint32(u32[:], pgnos[i])
		rec = append(rec, u32[:]...)
		indexRecords[i] = rec
	}
	indexHead, indexPages, err := writeRecordPages(pgr, indexFlag, indexRecords)
	if err != nil {
		return nil, err
	}
	seg.indexHead = indexHead

	// Persist the range-delete intervals so a point read learns which deletes cover a
	// key without scanning the run for their markers.
	rdRecords := make([][]byte, len(rangeDels))
	for i, rd := range rangeDels {
		rec := format.AppendUvarint(nil, uint64(len(rd.Lo)))
		rec = append(rec, rd.Lo...)
		rec = format.AppendUvarint(rec, uint64(len(rd.Hi)))
		rec = append(rec, rd.Hi...)
		var u64 [8]byte
		binary.BigEndian.PutUint64(u64[:], rd.Version)
		rec = append(rec, u64[:]...)
		rdRecords[i] = rec
	}
	rdHead, rdPages, err := writeRecordPages(pgr, rangeDelFlag, rdRecords)
	if err != nil {
		return nil, err
	}
	seg.rdHead = rdHead

	// Build the membership filter over the distinct user keys and persist it, so a point
	// read can skip this whole segment when the filter says a key was never here. The
	// kind is the engine's configured choice; a Ribbon build that cannot be seeded falls
	// back to Bloom, so the writer never fails for want of a filter.
	var filterBlob []byte
	if len(filterKeys) > 0 {
		if bitsPerKey < 1 {
			bitsPerKey = bloomBitsPerKey
		}
		seg.filter = buildSegFilter(kind, filterKeys, bitsPerKey)
		filterBlob = seg.filter.encode()
	}
	filterHead, filterPages, err := writeBlobPages(pgr, filterFlag, filterBlob)
	if err != nil {
		return nil, err
	}
	seg.filterHead = filterHead

	seg.pages = len(pgnos) + indexPages + rdPages + filterPages + 1 // data + index + range-delete + filter + footer
	footer, err := writeFooter(pgr, seg)
	if err != nil {
		return nil, err
	}
	seg.footer = footer
	return seg, nil
}

// writeRecordPages packs the records into a chain of pages tagged with flag and
// returns the head page number and the number of pages written. Records are laid
// out in order, each page filled until the next record would overflow, then chained
// through the common header's overflow slot. No records yields no pages and a
// NoPage head.
func writeRecordPages(pgr *pager.Pager, flag byte, records [][]byte) (format.PageNo, int, error) {
	if len(records) == 0 {
		return format.NoPage, 0, nil
	}
	usable := pgr.Header().UsablePageSize()
	var (
		bodies [][]byte
		counts []int
		cur    = make([]byte, segDataHeaderSize, usable)
		count  int
	)
	flush := func() {
		bodies = append(bodies, cur)
		counts = append(counts, count)
		cur = make([]byte, segDataHeaderSize, usable)
		count = 0
	}
	for _, rec := range records {
		if len(rec) > usable-segDataHeaderSize {
			return 0, 0, fmt.Errorf("lsm: record of %d bytes exceeds the usable page area of %d", len(rec), usable-segDataHeaderSize)
		}
		if count > 0 && len(cur)+len(rec) > usable {
			flush()
		}
		cur = append(cur, rec...)
		count++
	}
	if count > 0 {
		flush()
	}

	// Reserve every page number first, then write one page at a time so at most one frame
	// is pinned at once and the chain fits any pool (perf/05 F2). GetAllocated returns a
	// zeroed frame, so the tail past the body needs no separate clearing.
	pgnos := make([]format.PageNo, len(bodies))
	for i := range bodies {
		pgnos[i] = pgr.AllocateNumber()
	}
	for i := range bodies {
		next := format.NoPage
		if i+1 < len(bodies) {
			next = pgnos[i+1]
		}
		format.CommonHeader{Type: format.PageLSMBlock, Flags: flag, CellCount: uint16(counts[i]), Overflow: next}.Encode(bodies[i])
		fr, err := pgr.GetAllocated(pgnos[i])
		if err != nil {
			return 0, 0, err
		}
		copy(fr.Data(), bodies[i])
		pgr.Unpin(fr, true)
	}
	return pgnos[0], len(bodies), nil
}

// writeBlobPages packs an opaque byte blob into a chain of pages tagged with flag
// and returns the head page number and the number of pages written. Unlike
// writeRecordPages, which stores a count of length-prefixed records, a blob page
// stores a raw slice of the blob and records its byte length in the common header's
// cell-count slot, so the blob is reassembled by concatenating each page's payload.
// The Bloom filter's bit array is stored this way: it is a flat array, not a run of
// records. An empty blob yields no pages and a NoPage head.
func writeBlobPages(pgr *pager.Pager, flag byte, blob []byte) (format.PageNo, int, error) {
	if len(blob) == 0 {
		return format.NoPage, 0, nil
	}
	usable := pgr.Header().UsablePageSize()
	chunk := usable - segDataHeaderSize

	var chunks [][]byte
	for off := 0; off < len(blob); off += chunk {
		end := off + chunk
		if end > len(blob) {
			end = len(blob)
		}
		chunks = append(chunks, blob[off:end])
	}

	// Reserve every page number first, then materialize one page at a time so the blob
	// chain pins a single frame at a time and fits any pool (perf/05 F2). GetAllocated
	// zeroes the frame, so the unused tail is already clear.
	pgnos := make([]format.PageNo, len(chunks))
	for i := range chunks {
		pgnos[i] = pgr.AllocateNumber()
	}
	for i := range chunks {
		next := format.NoPage
		if i+1 < len(chunks) {
			next = pgnos[i+1]
		}
		body := make([]byte, segDataHeaderSize, usable)
		body = append(body, chunks[i]...)
		format.CommonHeader{Type: format.PageLSMBlock, Flags: flag, CellCount: uint16(len(chunks[i])), Overflow: next}.Encode(body)
		fr, err := pgr.GetAllocated(pgnos[i])
		if err != nil {
			return 0, 0, err
		}
		copy(fr.Data(), body)
		pgr.Unpin(fr, true)
	}
	return pgnos[0], len(chunks), nil
}

// loadBlobPages walks a blob-page chain from head and returns the reassembled blob.
// A NoPage head yields a nil blob.
func loadBlobPages(pgr *pager.Pager, head format.PageNo, flag byte) ([]byte, error) {
	var blob []byte
	for pgno := head; pgno != format.NoPage; {
		fr, err := pgr.Get(pgno, pager.Read)
		if err != nil {
			return nil, err
		}
		data := fr.Data()
		h := format.DecodeCommonHeader(data)
		if h.Type != format.PageLSMBlock || h.Flags != flag {
			pgr.Unpin(fr, false)
			return nil, fmt.Errorf("lsm: page %d in blob chain has the wrong flag", pgno)
		}
		n := int(h.CellCount)
		blob = append(blob, data[segDataHeaderSize:segDataHeaderSize+n]...)
		next := h.Overflow
		pgr.Unpin(fr, false)
		pgno = next
	}
	return blob, nil
}

// loadFilter reconstructs the segment's membership filter from its page chain, decoding
// the blob by the kind the footer recorded. A NoPage head, or an empty or undecodable
// blob, leaves seg.filter nil, so mayContain conservatively passes and the segment is
// always read.
func (s *segment) loadFilter(pgr *pager.Pager, filterK uint32, kind filterKind) error {
	if s.filterHead == format.NoPage {
		return nil
	}
	blob, err := loadBlobPages(pgr, s.filterHead, filterFlag)
	if err != nil {
		return err
	}
	if len(blob) == 0 {
		return nil
	}
	switch kind {
	case filterRibbon:
		if rf := decodeRibbon(blob); rf != nil {
			s.filter = rf
		}
	default:
		s.filter = &bloomFilter{bits: blob, k: filterK}
	}
	return nil
}

// writeFooter writes the segment's footer page and returns its number. The footer
// is small and fixed in shape, so it always fits one page.
func writeFooter(pgr *pager.Pager, seg *segment) (format.PageNo, error) {
	pgno, fr, err := pgr.Allocate()
	if err != nil {
		return 0, err
	}
	body := make([]byte, segDataHeaderSize)
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
	binary.BigEndian.PutUint32(u32[:], seg.indexHead)
	body = append(body, u32[:]...)
	binary.BigEndian.PutUint32(u32[:], seg.rdHead)
	body = append(body, u32[:]...)
	binary.BigEndian.PutUint32(u32[:], seg.filterHead)
	body = append(body, u32[:]...)
	// The filter kind discriminates how open decodes the filter blob; filterK carries the
	// Bloom probe count and is unused (zero) for a Ribbon filter, whose parameters are
	// self-describing in its blob.
	var (
		filterK uint32
		kind    = filterBloom
	)
	switch f := seg.filter.(type) {
	case *bloomFilter:
		filterK = f.k
	case *ribbonFilter:
		kind = filterRibbon
	}
	body = format.AppendUvarint(body, uint64(filterK))
	body = format.AppendUvarint(body, uint64(kind))
	body = format.AppendUvarint(body, uint64(seg.pages))

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

// openSegment reads a segment's footer page and the index and range-delete chains
// it names, returning a handle ready to seek, the path recovery takes to rebuild a
// segment from the page number the MANIFEST recorded.
func openSegment(pgr *pager.Pager, footer format.PageNo) (*segment, error) {
	fr, err := pgr.Get(footer, pager.Read)
	if err != nil {
		return nil, err
	}
	data := fr.Data()
	h := format.DecodeCommonHeader(data)
	if h.Type != format.PageLSMBlock || h.Flags != footerFlag {
		pgr.Unpin(fr, false)
		return nil, fmt.Errorf("lsm: page %d is not a segment footer", footer)
	}
	off := segDataHeaderSize
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
		off += int(maxLen)
	}
	seg.indexHead = binary.BigEndian.Uint32(data[off:])
	off += 4
	seg.rdHead = binary.BigEndian.Uint32(data[off:])
	off += 4
	seg.filterHead = binary.BigEndian.Uint32(data[off:])
	off += 4
	filterK, n := format.Uvarint(data[off:])
	off += n
	kind, n := format.Uvarint(data[off:])
	off += n
	pageCount, n := format.Uvarint(data[off:])
	seg.pages = int(pageCount)
	pgr.Unpin(fr, false)

	if err := seg.loadIndex(pgr); err != nil {
		return nil, err
	}
	if err := seg.loadRangeDels(pgr); err != nil {
		return nil, err
	}
	if err := seg.loadFilter(pgr, uint32(filterK), filterKind(kind)); err != nil {
		return nil, err
	}
	return seg, nil
}

// loadIndex walks the index-page chain into seg.index.
func (s *segment) loadIndex(pgr *pager.Pager) error {
	for pgno := s.indexHead; pgno != format.NoPage; {
		fr, err := pgr.Get(pgno, pager.Read)
		if err != nil {
			return err
		}
		data := fr.Data()
		h := format.DecodeCommonHeader(data)
		if h.Type != format.PageLSMBlock || h.Flags != indexFlag {
			pgr.Unpin(fr, false)
			return fmt.Errorf("lsm: page %d in index chain is not an index page", pgno)
		}
		off := segDataHeaderSize
		for i := 0; i < int(h.CellCount); i++ {
			klen, m := format.Uvarint(data[off:])
			off += m
			key := append([]byte(nil), data[off:off+int(klen)]...)
			off += int(klen)
			page := binary.BigEndian.Uint32(data[off:])
			off += 4
			s.index = append(s.index, indexEntry{firstUser: key, page: page})
		}
		next := h.Overflow
		pgr.Unpin(fr, false)
		pgno = next
	}
	return nil
}

// loadRangeDels walks the range-delete-page chain into seg.rangeDels.
func (s *segment) loadRangeDels(pgr *pager.Pager) error {
	for pgno := s.rdHead; pgno != format.NoPage; {
		fr, err := pgr.Get(pgno, pager.Read)
		if err != nil {
			return err
		}
		data := fr.Data()
		h := format.DecodeCommonHeader(data)
		if h.Type != format.PageLSMBlock || h.Flags != rangeDelFlag {
			pgr.Unpin(fr, false)
			return fmt.Errorf("lsm: page %d in range-delete chain is not a range-delete page", pgno)
		}
		off := segDataHeaderSize
		for i := 0; i < int(h.CellCount); i++ {
			loLen, m := format.Uvarint(data[off:])
			off += m
			lo := append([]byte(nil), data[off:off+int(loLen)]...)
			off += int(loLen)
			hiLen, m := format.Uvarint(data[off:])
			off += m
			hi := append([]byte(nil), data[off:off+int(hiLen)]...)
			off += int(hiLen)
			version := binary.BigEndian.Uint64(data[off:])
			off += 8
			s.rangeDels = append(s.rangeDels, format.RangeDel{Lo: lo, Hi: hi, Version: version})
		}
		next := h.Overflow
		pgr.Unpin(fr, false)
		pgno = next
	}
	return nil
}

// seekPage returns the index of the first data page that may hold userKey's version
// group. Because a group is never split across pages, the group (if present) lies
// wholly on this page, or on the run of continuation pages that begins here when the
// group is too large for one page.
func (s *segment) seekPage(userKey []byte) int {
	n := len(s.index)
	if n == 0 {
		return 0
	}
	// First separator whose user key is >= the target.
	idx := sort.Search(n, func(i int) bool {
		return format.CompareUser(s.index[i].firstUser, userKey) >= 0
	})
	if idx < n && format.CompareUser(s.index[idx].firstUser, userKey) == 0 {
		return idx // the group starts a page; take its leftmost page
	}
	if idx == 0 {
		return 0 // the target is below the segment's first key
	}
	return idx - 1 // the group starts mid-page on the page before the first larger separator
}

// dataPageBody returns the cell bytes of a data page, the slice the readers iterate over.
// An uncompressed page returns its bytes past the header directly; a compressed page (its
// header carries compressedFlag) returns the decoded cells, so a reader walks the same cell
// layout either way and needs no knowledge of which codec, if any, packed the page. The
// returned slice for a compressed page is a fresh buffer; for an uncompressed page it aliases
// the pinned page and is valid only while the frame stays pinned.
func dataPageBody(data []byte, h format.CommonHeader) ([]byte, error) {
	if h.Flags&compressedFlag == 0 {
		return data[segDataHeaderSize:], nil
	}
	return decompressBlock(data[segDataHeaderSize:])
}

// getGroup calls fn for every cell of userKey's version group, in ascending
// internal-key order (newest version first), using the block index to seek the data
// page that holds the group rather than scanning the run. The cell slices alias the
// pinned page and are valid only for the duration of the call, so fn copies what it
// keeps. It stops early if fn returns false.
func (s *segment) getGroup(pgr *pager.Pager, userKey []byte, fn func(internalKey, value []byte) bool) error {
	if len(s.index) == 0 {
		return nil
	}
	// A key outside the segment's range cannot be present, so skip the read entirely.
	if s.minKey != nil && (format.CompareUser(userKey, s.minKey) < 0 || format.CompareUser(userKey, s.maxKey) > 0) {
		return nil
	}
	for i := s.seekPage(userKey); i < len(s.index); i++ {
		fr, err := pgr.Get(s.index[i].page, pager.Read)
		if err != nil {
			return err
		}
		data := fr.Data()
		h := format.DecodeCommonHeader(data)
		if h.Type != format.PageLSMBlock {
			pgr.Unpin(fr, false)
			return fmt.Errorf("lsm: page %d in segment chain is not an LSM block", s.index[i].page)
		}
		body, derr := dataPageBody(data, h)
		if derr != nil {
			pgr.Unpin(fr, false)
			return derr
		}
		off := 0
		stop := false
		var lastUser []byte
		for c := 0; c < int(h.CellCount); c++ {
			klen, m := format.Uvarint(body[off:])
			off += m
			ik := body[off : off+int(klen)]
			off += int(klen)
			vlen, m := format.Uvarint(body[off:])
			off += m
			val := body[off : off+int(vlen)]
			off += int(vlen)
			lastUser = format.UserKey(ik)
			cmp := format.CompareUser(lastUser, userKey)
			if cmp < 0 {
				continue
			}
			if cmp > 0 {
				stop = true
				break
			}
			if !fn(ik, val) {
				stop = true
				break
			}
		}
		pgr.Unpin(fr, false)
		if stop {
			return nil
		}
		// Continue onto the next page only when the group reaches the page's end,
		// which means a group too large for one page spills onto the next.
		if lastUser == nil || format.CompareUser(lastUser, userKey) != 0 {
			return nil
		}
	}
	return nil
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
		body, derr := dataPageBody(data, h)
		if derr != nil {
			pgr.Unpin(fr, false)
			return derr
		}
		off := 0
		stop := false
		for i := 0; i < int(h.CellCount); i++ {
			klen, n := format.Uvarint(body[off:])
			off += n
			key := body[off : off+int(klen)]
			off += int(klen)
			vlen, n := format.Uvarint(body[off:])
			off += n
			val := body[off : off+int(vlen)]
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
