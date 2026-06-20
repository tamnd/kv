package lsm

import (
	"encoding/binary"
	"fmt"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// The MANIFEST is the embedded catalog of the segment set (spec 06 §4.2). A classic
// LSM keeps it as a separate file; kv keeps the whole tree in one file, so the
// MANIFEST is a chain of pages inside it, anchored at the header's engine-root slot,
// exactly where the B-tree core anchors its tree root. It is an append-only log of
// edits: each flush adds a segment, each compaction (a later slice) adds and removes
// several at once. Replaying the log from oldest edit to newest reconstructs the live
// set, the same way a redo log reconstructs state.
//
// One page holds a run of fixed-width edits and an overflow pointer to the next-older
// page, so the chain runs newest to oldest and the engine root names the newest page.
// A flush appends its one edit into the head page when it has room and allocates a
// fresh head when it does not, the same allocate-and-dirty path every engine write
// takes; the next checkpoint folds the dirtied pages and the updated header together,
// so the catalog advances atomically with the segments it names.

const (
	// manifestHeaderSize is a MANIFEST page's header: the common 8-byte preamble,
	// whose overflow slot points at the next-older MANIFEST page (0 ends the chain).
	manifestHeaderSize = format.CommonHeaderSize
	// manifestEntrySize is one edit: a tag byte, the level byte the segment belongs to,
	// and the segment footer page number. The level lets the replay rebuild the level
	// structure, not just the flat set, and lets a compaction record an input's old
	// level and an output's new one.
	manifestEntrySize = 1 + 1 + 4
)

const (
	// manifestAdd records a segment entering the live set. A flush appends one.
	manifestAdd byte = 1
	// manifestRemove records a segment leaving it, the edit compaction will append
	// when it retires an input run. No writer emits one yet; the replay honors it so
	// the format need not change when compaction lands.
	manifestRemove byte = 2
)

// manifestEntriesPerPage reports how many edits fit on a page of the given usable
// size, the point past which a new head page is allocated.
func manifestEntriesPerPage(usable int) int {
	return (usable - manifestHeaderSize) / manifestEntrySize
}

// appendEditLocked records one MANIFEST edit, allocating a new head page only when the
// current head is full. The caller holds l.mu. The dirtied page and the header's
// updated engine root are folded by the next checkpoint, so the edit becomes durable
// at the same moment the segment it names does. level is the segment's level: an add
// records where the segment enters, a remove records where it left (the replay ignores
// a remove's level, but it is stored for symmetry and debugging).
func (l *LSM) appendEditLocked(tag byte, level uint8, footer format.PageNo) error {
	usable := l.pgr.Header().UsablePageSize()
	maxEntries := manifestEntriesPerPage(usable)
	head := l.pgr.Header().EngineRoot

	writeEntry := func(data []byte, off int) {
		data[off] = tag
		data[off+1] = level
		binary.BigEndian.PutUint32(data[off+2:], footer)
	}

	if head != format.NoPage {
		fr, err := l.pgr.Get(head, pager.Write)
		if err != nil {
			return err
		}
		data := fr.Data()
		h := format.DecodeCommonHeader(data)
		if h.Type == format.PageLSMManifest && int(h.CellCount) < maxEntries {
			writeEntry(data, manifestHeaderSize+int(h.CellCount)*manifestEntrySize)
			h.CellCount++
			h.Encode(data)
			l.pgr.Unpin(fr, true)
			return nil
		}
		l.pgr.Unpin(fr, false)
	}

	pgno, fr, err := l.pgr.Allocate()
	if err != nil {
		return err
	}
	data := fr.Data()
	format.CommonHeader{Type: format.PageLSMManifest, CellCount: 1, Overflow: head}.Encode(data)
	writeEntry(data, manifestHeaderSize)
	l.pgr.Unpin(fr, true)
	l.pgr.Header().EngineRoot = pgno
	return nil
}

// loadManifestLocked walks the MANIFEST chain from the engine root, applies every edit
// in chronological order, and opens each surviving segment into its level in l.levels.
// It runs at Open, before any redo, so the level structure the last checkpoint recorded
// is in place before the WAL tail replays the batches committed after it. The caller
// holds l.mu.
func (l *LSM) loadManifestLocked() error {
	// Collect each page's edits, walking the chain newest page to oldest.
	var pages [][]manifestEdit
	for pgno := l.pgr.Header().EngineRoot; pgno != format.NoPage; {
		fr, err := l.pgr.Get(pgno, pager.Read)
		if err != nil {
			return err
		}
		data := fr.Data()
		h := format.DecodeCommonHeader(data)
		if h.Type != format.PageLSMManifest {
			l.pgr.Unpin(fr, false)
			return fmt.Errorf("lsm: page %d in MANIFEST chain is not a MANIFEST page", pgno)
		}
		edits := make([]manifestEdit, 0, h.CellCount)
		off := manifestHeaderSize
		for i := 0; i < int(h.CellCount); i++ {
			edits = append(edits, manifestEdit{
				tag:    data[off],
				level:  data[off+1],
				footer: binary.BigEndian.Uint32(data[off+2:]),
			})
			off += manifestEntrySize
		}
		next := h.Overflow
		l.pgr.Unpin(fr, false)
		pages = append(pages, edits)
		pgno = next
	}

	// Apply edits oldest first: the last page in the walk is the oldest, and within a
	// page entries were appended left to right. An add records a footer at its level, a
	// remove drops it, and a later add at a different level moves it (the level-aware
	// compaction's trivial move), so the final state is each live footer at the level of
	// its most recent add. order preserves first-seen order so a level's segments load in
	// a stable sequence; level tracks the current level of each live footer.
	var order []format.PageNo
	at := make(map[format.PageNo]int)      // footer -> 1-based slot in order, 0 when absent
	level := make(map[format.PageNo]uint8) // footer -> current level, for live footers
	for i := len(pages) - 1; i >= 0; i-- {
		for _, e := range pages[i] {
			switch e.tag {
			case manifestAdd:
				if at[e.footer] == 0 {
					order = append(order, e.footer)
					at[e.footer] = len(order)
				}
				level[e.footer] = e.level
			case manifestRemove:
				if slot := at[e.footer]; slot != 0 {
					order[slot-1] = format.NoPage
					at[e.footer] = 0
					delete(level, e.footer)
				}
			default:
				return fmt.Errorf("lsm: unknown MANIFEST edit tag %d", e.tag)
			}
		}
	}

	for _, footer := range order {
		if footer == format.NoPage {
			continue
		}
		seg, err := openSegment(l.pgr, footer)
		if err != nil {
			return err
		}
		l.addSegmentLocked(int(level[footer]), seg)
	}
	return nil
}

// manifestEdit is one decoded MANIFEST entry.
type manifestEdit struct {
	tag    byte
	level  uint8
	footer format.PageNo
}
