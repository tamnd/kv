package lsm

import (
	"errors"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// errCorruptPointer is returned when a KindSetSep cell's value field does not decode to a
// value pointer, the same fault the read path reports when it cannot dereference a cell.
var errCorruptPointer = errors.New("lsm: corrupt value pointer in cell")

// Value-log garbage collection reclaims the space dead values leave behind (spec 06 §7).
// A separated value is written once and never moved, so when its key is overwritten or
// compacted away the value bytes stay in the log as dead weight. Without a collector the
// log only grows, the space-amplification tax value separation trades for its write-
// amplification win. The collector pays the tax down, budgeted through Maintain the same
// way compaction is: when no segment compaction is due, Maintain spends the budget here.
//
// The collector is a page-granular mark and sweep over the one overflow-linked chain the
// log forms. It marks every page any live pointer still references and frees the pages no
// live pointer touches, re-linking the chain around them. It does not move values, so a
// pointer in a segment stays valid: the cell is never rewritten and compaction need not
// be involved. The win this gives up is the partially-dead page, where a few live values
// pin a page that is mostly dead; that page is kept until its last live value dies. For
// the blob-ish workloads value separation targets, where a separated value usually spans
// whole pages, a dead value frees its whole span, so page-granular reclaim recovers
// essentially all the dead space. Value-moving compaction of partially-dead pages, which
// would rewrite the referencing cells too, is a later refinement.
//
// Liveness is decided against the live segment set. A value is live exactly when some
// live segment holds a KindSetSep cell whose pointer names it; once every such cell is
// gone (the key was overwritten and the old version compacted out, or the key was
// deleted and the tombstone reached the bottom) the value is unreachable. The memtable
// never references the log (it holds literal values, not pointers), so only segments are
// scanned. The append tail is always kept, live or not, since it is where the next value
// lands.

// runVLogGCLocked reclaims dead value-log pages within the budget and re-roots the chain.
// It walks the chain once to enumerate its pages, marks the pages every live segment
// pointer references, then sweeps the chain freeing dead pages up to the budget and
// re-linking the survivors. The new head is recorded in the MANIFEST so the shorter chain
// survives a reopen. The caller holds l.mu.
func (l *LSM) runVLogGCLocked(budget engine.MaintBudget) (engine.MaintReport, error) {
	v := l.vlog
	if v.head == format.NoPage {
		return engine.MaintReport{}, nil
	}

	// How many pages this call may free, derived from whichever budget the host set.
	pageSize := int64(l.pgr.PageSize())
	maxFree := budget.MaxPages
	if maxFree <= 0 && budget.MaxBytes > 0 {
		maxFree = int(budget.MaxBytes / pageSize)
	}
	if maxFree <= 0 {
		return engine.MaintReport{}, nil
	}

	chain, err := v.walkChain()
	if err != nil {
		return engine.MaintReport{}, err
	}
	if len(chain) == 0 {
		return engine.MaintReport{}, nil
	}

	// Mark the pages live pointers reference. pos maps a page to its position in the chain
	// so a pointer's span can be marked by arithmetic over consecutive chain slots, which
	// works because the chain order is exactly the overflow order a value spills along.
	pos := make(map[format.PageNo]int, len(chain))
	for i, p := range chain {
		pos[p] = i
	}
	live := make([]bool, len(chain))
	live[len(chain)-1] = true // the tail is the append cursor, never freed

	var scanErr error
	for _, seg := range l.allLiveSegmentsLocked() {
		err := seg.scan(l.pgr, func(ik, val []byte) bool {
			if format.KindOf(ik) != format.KindSetSep {
				return true
			}
			ptr, ok := format.DecodeValuePointer(val)
			if !ok {
				scanErr = errCorruptPointer
				return false
			}
			start, ok := pos[format.PageNo(ptr.Page)]
			if !ok {
				return true // a pointer into some other chain; nothing here to mark
			}
			span := v.spanPages(int(ptr.Offset), int(ptr.Length))
			for i := start; i < start+span && i < len(chain); i++ {
				live[i] = true
			}
			return true
		})
		if err != nil {
			return engine.MaintReport{}, err
		}
		if scanErr != nil {
			return engine.MaintReport{}, scanErr
		}
	}

	// Sweep the chain in order. A live page, or a dead one the budget can no longer cover,
	// stays in the chain; a dead page within budget is freed. Keeping over-budget dead
	// pages linked leaves the chain valid every call, and they are revisited next time.
	var survivors []format.PageNo
	freed := 0
	deadTotal := 0
	for i, p := range chain {
		if !live[i] {
			deadTotal++
		}
		if live[i] || freed >= maxFree {
			survivors = append(survivors, p)
			continue
		}
		l.pgr.Free(p)
		freed++
	}
	if freed == 0 {
		return engine.MaintReport{}, nil
	}

	if err := v.relink(survivors); err != nil {
		return engine.MaintReport{}, err
	}
	v.head = survivors[0]
	if err := l.persistVLogHeadLocked(); err != nil {
		return engine.MaintReport{}, err
	}

	return engine.MaintReport{
		BytesReclaimed: int64(freed) * pageSize,
		More:           deadTotal > freed,
	}, nil
}
