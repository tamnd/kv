package lsm

import (
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// The range and iteration path resolves a key range by merging every source (the
// active memtable and each on-disk segment) in ascending internal-key order, then
// folding each user key's version group with the shared MVCC resolution. Earlier this
// gathered every cell from every source and sorted the whole set, which read the
// entire database to answer even a narrow scan. This file replaces that with a
// streaming k-way merge: each source is seeked to the range's lower bound and yields
// its cells in order, a small binary heap picks the next-smallest across sources, and
// the fold consumes the merged stream one group at a time until the upper bound. A
// scan now touches only the pages its range covers, and the same merge is the
// primitive a later leveled compaction reuses to combine segments.

// mergeSource is one ordered cell stream feeding the k-way merge: the memtable or one
// segment. A source yields cells in ascending format.CompareInternal order, the order
// both the skip list and a segment's data-page chain already store them in. The key
// and value slices a source returns are valid until its next advance, so the merge
// copies anything it retains past that point.
type mergeSource interface {
	valid() bool                // positioned on a cell
	key() []byte                // current internal key
	value() []byte              // current value
	next() error                // advance to the following cell
	seekGE(target []byte) error // position at the first cell whose key >= target; nil target means from the start
}

// memSource iterates the active memtable through its skip list. The node bytes alias
// the arena and are stable for the memtable's life, and the reader holds the engine
// read lock across the whole merge, so the slices stay valid without copying here.
type memSource struct {
	sl  *skiplist
	off uint32
}

func (m *memSource) valid() bool   { return m.off != 0 }
func (m *memSource) key() []byte   { return m.sl.nodeKey(m.off) }
func (m *memSource) value() []byte { return m.sl.nodeValue(m.off) }
func (m *memSource) next() error   { m.off = m.sl.next(m.off); return nil }

func (m *memSource) seekGE(target []byte) error {
	if target == nil {
		m.off = m.sl.first()
		return nil
	}
	m.off = m.sl.seek(target)
	return nil
}

// segSource iterates one on-disk segment's data-page chain. It decodes one page at a
// time into owned cells, so the slices it returns survive only until it crosses to the
// next page; the merge copies what it keeps. Positioning uses the block index to land
// near the lower bound rather than walking the run from its head.
type segSource struct {
	pgr   *pager.Pager
	seg   *segment
	idx   int       // index of the loaded data page in seg.index, len(index) when exhausted
	cells []srcCell // the loaded page's cells, owned copies
	pos   int       // current cell within cells
	err   error
}

func (s *segSource) valid() bool   { return s.err == nil && s.pos < len(s.cells) }
func (s *segSource) key() []byte   { return s.cells[s.pos].ik }
func (s *segSource) value() []byte { return s.cells[s.pos].val }

// loadPage decodes the data page at index i into s.cells, copying every cell so the
// slices outlive the pin. An index past the end leaves the source exhausted.
func (s *segSource) loadPage(i int) {
	s.cells = s.cells[:0]
	s.pos = 0
	s.idx = i
	if i >= len(s.seg.index) {
		return
	}
	fr, err := s.pgr.Get(s.seg.index[i].page, pager.Read)
	if err != nil {
		s.err = err
		return
	}
	data := fr.Data()
	h := format.DecodeCommonHeader(data)
	off := segDataHeaderSize
	for c := 0; c < int(h.CellCount); c++ {
		klen, n := format.Uvarint(data[off:])
		off += n
		ik := append([]byte(nil), data[off:off+int(klen)]...)
		off += int(klen)
		vlen, n := format.Uvarint(data[off:])
		off += n
		val := append([]byte(nil), data[off:off+int(vlen)]...)
		off += int(vlen)
		s.cells = append(s.cells, srcCell{ik: ik, val: val})
	}
	s.pgr.Unpin(fr, false)
}

func (s *segSource) next() error {
	if s.err != nil {
		return s.err
	}
	s.pos++
	for s.pos >= len(s.cells) && s.idx+1 <= len(s.seg.index)-1 {
		s.loadPage(s.idx + 1)
	}
	return s.err
}

func (s *segSource) seekGE(target []byte) error {
	if len(s.seg.index) == 0 {
		s.idx = 0
		return nil
	}
	start := 0
	if target != nil {
		start = s.seg.seekPage(format.UserKey(target))
	}
	s.loadPage(start)
	if s.err != nil {
		return s.err
	}
	// Walk forward to the first cell at or after the target, crossing pages if the
	// starting page holds only smaller cells.
	for s.valid() && (target != nil && format.CompareInternal(s.key(), target) < 0) {
		if err := s.next(); err != nil {
			return err
		}
	}
	return nil
}

// mergeIter is a forward k-way merge over its sources, yielding cells in ascending
// internal-key order. It keeps the valid sources in a binary min-heap keyed by current
// cell, so each step is logarithmic in the source count, which matters once leveled
// compaction merges many segments at once.
type mergeIter struct {
	heap []mergeSource // a binary min-heap of the currently valid sources
	err  error
}

// newMergeIter seeks every source to target (nil for the start) and builds the heap
// from those that have a cell.
func newMergeIter(sources []mergeSource, target []byte) (*mergeIter, error) {
	mi := &mergeIter{}
	for _, s := range sources {
		if err := s.seekGE(target); err != nil {
			return nil, err
		}
		if s.valid() {
			mi.heap = append(mi.heap, s)
		}
	}
	mi.build()
	return mi, nil
}

func (mi *mergeIter) less(i, j int) bool {
	return format.CompareInternal(mi.heap[i].key(), mi.heap[j].key()) < 0
}

func (mi *mergeIter) build() {
	for i := len(mi.heap)/2 - 1; i >= 0; i-- {
		mi.down(i)
	}
}

func (mi *mergeIter) down(i int) {
	n := len(mi.heap)
	for {
		l := 2*i + 1
		if l >= n {
			return
		}
		small := l
		if r := l + 1; r < n && mi.less(r, l) {
			small = r
		}
		if !mi.less(small, i) {
			return
		}
		mi.heap[i], mi.heap[small] = mi.heap[small], mi.heap[i]
		i = small
	}
}

// valid reports whether the merge is positioned on a cell.
func (mi *mergeIter) valid() bool { return mi.err == nil && len(mi.heap) > 0 }

// key and value return the current smallest cell across sources.
func (mi *mergeIter) key() []byte   { return mi.heap[0].key() }
func (mi *mergeIter) value() []byte { return mi.heap[0].value() }

// next advances the source that produced the current cell and re-heaps. A source that
// runs dry is dropped from the heap.
func (mi *mergeIter) next() error {
	if mi.err != nil {
		return mi.err
	}
	top := mi.heap[0]
	if err := top.next(); err != nil {
		mi.err = err
		return err
	}
	if top.valid() {
		mi.down(0)
		return nil
	}
	// The top source is exhausted: move the last source into its slot and re-heap.
	last := len(mi.heap) - 1
	mi.heap[0] = mi.heap[last]
	mi.heap = mi.heap[:last]
	mi.down(0)
	return nil
}
