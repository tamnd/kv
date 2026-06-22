// Package btree is kv's default storage core: an in-place B+tree over the pager
// (spec 05). It is the engine you get unless you ask for the LSM core, and the one
// the conformance oracle treats as the read-semantics reference.
//
// Keys are stored as full internal keys (user_key || ^version || kind), so every
// version of a user key sorts together, newest first. A point read scans the
// version group for the newest version visible at the snapshot; a range scan
// resolves one visible version per user key. The tree carries a B-link
// right-sibling pointer on every leaf so an ordered scan walks leaves without
// re-descending (spec 05 §2).
//
// M1 scope: search, insert, leaf and interior split, and the Engine SPI end to end
// through the real pager, verified against the conformance oracle. Deletes are
// tombstone cells folded at read time -- no separate delete path. Whole-node
// decode/re-encode stands in for the in-place slotted edit the final layout wants;
// Bε write buffers, optimistic lock coupling, prefix compression, overflow values,
// and lazy node merge are later milestones. None of them change this SPI.
package btree

import (
	"errors"
	"fmt"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// BTree is an opened B-tree core over a pager.
type BTree struct {
	pgr      *pager.Pager
	pageSize int
	// usable is the page bytes a node body may occupy: the full page minus the
	// reserved trailer the pager stamps the per-page checksum into (spec 02 §3.2). Every
	// split and fit test bounds the body by usable, not pageSize, so the checksum never
	// overwrites a cell. It equals pageSize when the file carries no checksums.
	usable int
	merge  func(existing, operand []byte) []byte
	// rangeDels is the live set of range-delete intervals. It is rebuilt from the
	// marker cells at Open and extended on Apply, so a read can fold a range delete
	// whose marker cell lives in a leaf the read never visits (spec 11 §4).
	rangeDels []format.RangeDel

	// gc* carry a budget-bounded version-GC pass across calls (see gc.go). gcActive
	// is set while a pass is mid-chain, gcResume is the next leaf to collapse, and
	// gcResumeW is the watermark that pass adopted; a call at a different watermark
	// restarts the pass from the leftmost leaf, since mixing watermarks across a
	// resumed pass would be unsafe when marker cells are finally dropped.
	gcActive  bool
	gcResume  format.PageNo
	gcResumeW uint64

	// buffered turns on the Bε write path (spec 05 §4, buffer.go): inserts park as
	// messages in interior buffers and flush down in batches instead of descending to
	// their leaf one at a time. Off by default, set from EngineOptions.BufferedInserts
	// at Open. When off, interior buffers stay empty and the engine behaves exactly as
	// the in-place tree always has.
	buffered bool
}

// New returns a B-tree core bound to pgr. Call Open to finish wiring it to the
// shared substrate and to materialize an empty root for a fresh database.
func New(pgr *pager.Pager) *BTree {
	return &BTree{pgr: pgr, pageSize: pgr.PageSize(), usable: pgr.UsablePageSize()}
}

// Kind implements engine.Engine.
func (t *BTree) Kind() engine.Kind { return engine.BTree }

// SetMergeFunc installs the merge resolver used during version resolution. The
// conformance harness and the library's merge registry (spec 15) call it.
func (t *BTree) SetMergeFunc(f func(existing, operand []byte) []byte) { t.merge = f }

// Open implements engine.Engine. It records engine options and ensures the tree
// has a root: a fresh database has none, so Open allocates an empty leaf and points
// the header's engine-root field at it.
func (t *BTree) Open(env *engine.Env) error {
	if env != nil && env.Options.PageSize != 0 {
		reserved := t.pageSize - t.usable
		t.pageSize = env.Options.PageSize
		t.usable = t.pageSize - reserved
	}
	if env != nil && env.Options.BufferedInserts {
		t.buffered = true
	}
	if t.root() == format.NoPage {
		pgno, err := t.storeLeafNew(&leaf{})
		if err != nil {
			return err
		}
		t.setRoot(pgno)
	}
	return t.rebuildRangeDels()
}

// rebuildRangeDels reconstructs the in-memory range-delete interval set by scanning
// the marker cells in the tree. It runs at Open so a database reopened after a crash
// or clean close resolves range deletes the same way it did before (spec 11 §4).
//
// A corrupt page encountered during the scan does not fail the open: the database still
// opens so the integrity checker (`kv check`) and recovery tools can run on it, with a
// best-effort (possibly empty) range-delete set. Reads that later touch the corrupt page
// still fail fast with format.ErrCorrupt, so tolerating the open never serves torn data
// silently (spec 02 §3.2, spec 16 §4).
func (t *BTree) rebuildRangeDels() error {
	t.rangeDels = nil
	entries, err := t.collectRange(nil, nil)
	if err != nil {
		if errors.Is(err, format.ErrCorrupt) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if format.KindOf(e.ik) == format.KindRangeBegin {
			t.rangeDels = append(t.rangeDels, format.RangeDel{
				Lo:      append([]byte(nil), format.UserKey(e.ik)...),
				Hi:      append([]byte(nil), e.val...),
				Version: format.Version(e.ik),
			})
		}
	}
	return nil
}

// Close implements engine.Engine. It flushes nothing; the host checkpoints first.
func (t *BTree) Close() error { return nil }

// root reads the engine-root page number from the header.
func (t *BTree) root() format.PageNo { return t.pgr.Header().EngineRoot }

// setRoot points the header's engine-root field at pgno. It is persisted by the
// next checkpoint (the header's validity trick, spec 02 §2).
func (t *BTree) setRoot(pgno format.PageNo) { t.pgr.Header().EngineRoot = pgno }

// Apply implements engine.Engine: it installs every entry of an already-durable
// batch into the tree. A crash mid-Apply is harmless -- recovery replays the same
// committed batch and the versioned-key inserts are idempotent (spec 04 §3).
func (t *BTree) Apply(batch *engine.WriteBatch, commitVersion uint64) error {
	for _, e := range batch.Entries() {
		if t.buffered {
			if err := t.bufferedApply(e.InternalKey, e.Value); err != nil {
				return err
			}
		} else if err := t.insertOne(e.InternalKey, e.Value); err != nil {
			return err
		}
		if format.KindOf(e.InternalKey) == format.KindRangeBegin {
			t.rangeDels = append(t.rangeDels, format.RangeDel{
				Lo:      append([]byte(nil), format.UserKey(e.InternalKey)...),
				Hi:      append([]byte(nil), e.Value...),
				Version: format.Version(e.InternalKey),
			})
		}
	}
	return nil
}

// insertOne descends to the target leaf, inserts the internal-key cell, and splits
// upward as far as needed.
func (t *BTree) insertOne(ik, value []byte) error {
	if nodeHeaderSize+len(ik)+len(value)+8 > t.usable {
		return fmt.Errorf("btree: entry of %d bytes exceeds page (overflow values are deferred)", len(ik)+len(value))
	}
	uk := format.UserKey(ik)

	// Descend, recording the interior pages on the path so a split can post its
	// separator to the parent without re-descending.
	var path []format.PageNo
	pgno := t.root()
	for {
		typ, err := t.typeOf(pgno)
		if err != nil {
			return err
		}
		if typ == format.PageBTreeLeaf {
			break
		}
		in, err := t.loadInterior(pgno)
		if err != nil {
			return err
		}
		path = append(path, pgno)
		pgno = in.children[in.childFor(uk)]
	}

	l, err := t.loadLeaf(pgno)
	if err != nil {
		return err
	}
	l.insert(ik, value)
	if len(marshalLeaf(l)) <= t.usable {
		return t.storeLeaf(pgno, l)
	}

	// The leaf overflowed: split it, keeping version groups intact.
	sp := l.splitPoint()
	if sp == 0 {
		sp = len(l.keys) / 2
		if sp == 0 {
			sp = 1
		}
	}
	right := &leaf{keys: l.keys[sp:], vals: l.vals[sp:], next: l.next}
	left := &leaf{keys: l.keys[:sp], vals: l.vals[:sp]}
	rpgno, err := t.storeLeafNew(right)
	if err != nil {
		return err
	}
	left.next = rpgno
	if err := t.storeLeaf(pgno, left); err != nil {
		return err
	}
	sep := format.UserKey(right.keys[0])
	return t.propagateSplit(path, uk, pgno, sep, rpgno)
}

// propagateSplit posts a (separator, newChild) pair into the parent chain, splitting
// interior nodes as needed and growing a new root when the split reaches the top.
func (t *BTree) propagateSplit(path []format.PageNo, uk []byte, leftChild format.PageNo, sep []byte, newChild format.PageNo) error {
	for i := len(path) - 1; i >= 0; i-- {
		ppgno := path[i]
		in, err := t.loadInterior(ppgno)
		if err != nil {
			return err
		}
		p := in.childFor(uk) // the index we descended through == leftChild's slot
		in.insertChild(p, sep, newChild)
		if len(marshalInterior(in)) <= t.usable {
			return t.storeInterior(ppgno, in)
		}
		// Interior overflowed: split, pushing the middle separator up.
		mid := len(in.seps) / 2
		upSep := append([]byte(nil), in.seps[mid]...)
		rightIn := &interior{seps: in.seps[mid+1:], children: in.children[mid+1:]}
		leftIn := &interior{seps: in.seps[:mid], children: in.children[:mid+1]}
		rpgno, err := t.storeInteriorNew(rightIn)
		if err != nil {
			return err
		}
		if err := t.storeInterior(ppgno, leftIn); err != nil {
			return err
		}
		leftChild, newChild, sep = ppgno, rpgno, upSep
	}
	// The split reached above the root: grow the tree by one level.
	newRoot := &interior{seps: [][]byte{append([]byte(nil), sep...)}, children: []format.PageNo{leftChild, newChild}}
	rpgno, err := t.storeInteriorNew(newRoot)
	if err != nil {
		return err
	}
	t.setRoot(rpgno)
	return nil
}

// --- page load/store helpers ---

func (t *BTree) typeOf(pgno format.PageNo) (format.PageType, error) {
	fr, err := t.pgr.Get(pgno, pager.Read)
	if err != nil {
		return 0, err
	}
	typ := format.PageType(fr.Data()[0])
	t.pgr.Unpin(fr, false)
	return typ, nil
}

func (t *BTree) loadLeaf(pgno format.PageNo) (*leaf, error) {
	fr, err := t.pgr.Get(pgno, pager.Read)
	if err != nil {
		return nil, err
	}
	data := fr.Data()
	// Guard against type confusion: a checksum-valid page reached as a leaf must actually be one, or a
	// corrupt root pointer would have us decode an interior (or any other page) with the leaf decoder.
	if len(data) < format.CommonHeaderSize || format.DecodeCommonHeader(data).Type != format.PageBTreeLeaf {
		t.pgr.Unpin(fr, false)
		return nil, format.ErrCorrupt
	}
	l, err := unmarshalLeaf(data)
	t.pgr.Unpin(fr, false)
	return l, err
}

func (t *BTree) loadInterior(pgno format.PageNo) (*interior, error) {
	fr, err := t.pgr.Get(pgno, pager.Read)
	if err != nil {
		return nil, err
	}
	data := fr.Data()
	if len(data) < format.CommonHeaderSize || format.DecodeCommonHeader(data).Type != format.PageBTreeInterior {
		t.pgr.Unpin(fr, false)
		return nil, format.ErrCorrupt
	}
	in, err := unmarshalInterior(data)
	t.pgr.Unpin(fr, false)
	return in, err
}

// viewLeaf returns the decoded leaf for a read-only caller. It reuses the decoded
// node cached on the pager frame when the page is resident and unchanged since the
// last read, decoding the bytes only on a miss and caching the result for the next
// read (spec 01 Finding 1: stop re-decoding a node that is already in the buffer
// pool). The returned *leaf is shared and immutable: read callers only ever read its
// keys and values and copy out before returning, and the mutating path uses loadLeaf,
// which decodes a private copy, so no caller mutates the cached instance. The pager
// invalidates the cached view before any write-intent pin or frame rebind, so a hit
// always describes the page's current bytes.
func (t *BTree) viewLeaf(pgno format.PageNo) (*leaf, error) {
	fr, err := t.pgr.Get(pgno, pager.Read)
	if err != nil {
		return nil, err
	}
	if l, ok := fr.Decoded().(*leaf); ok {
		t.pgr.Unpin(fr, false)
		return l, nil
	}
	data := fr.Data()
	if len(data) < format.CommonHeaderSize || format.DecodeCommonHeader(data).Type != format.PageBTreeLeaf {
		t.pgr.Unpin(fr, false)
		return nil, format.ErrCorrupt
	}
	l, err := unmarshalLeaf(data)
	if err == nil {
		fr.SetDecoded(l)
	}
	t.pgr.Unpin(fr, false)
	return l, err
}

// viewNode fetches a node for a read-only descent in a single pager Get, returning
// its page type and whichever of the decoded leaf or interior it is. The descent code
// needs the type to decide whether it has reached a leaf and needs the decoded body to
// continue, and doing both from one Get halves the per-node buffer-pool traffic versus
// a typeOf probe followed by a separate view fetch (spec 01 Finding 1 follow-on: the
// node-decode cache left the read path bound on the per-node shard-lock acquisitions, so
// fetching each node once rather than twice is the next cut). Like viewLeaf it serves
// the frame-cached decode on a hit and caches a fresh decode on a miss, and the returned
// node is shared and immutable.
func (t *BTree) viewNode(pgno format.PageNo) (format.PageType, *leaf, *interior, error) {
	fr, err := t.pgr.Get(pgno, pager.Read)
	if err != nil {
		return 0, nil, nil, err
	}
	switch n := fr.Decoded().(type) {
	case *leaf:
		t.pgr.Unpin(fr, false)
		return format.PageBTreeLeaf, n, nil, nil
	case *interior:
		t.pgr.Unpin(fr, false)
		return format.PageBTreeInterior, nil, n, nil
	}
	data := fr.Data()
	if len(data) < format.CommonHeaderSize {
		t.pgr.Unpin(fr, false)
		return 0, nil, nil, format.ErrCorrupt
	}
	switch typ := format.DecodeCommonHeader(data).Type; typ {
	case format.PageBTreeLeaf:
		l, err := unmarshalLeaf(data)
		if err == nil {
			fr.SetDecoded(l)
		}
		t.pgr.Unpin(fr, false)
		return format.PageBTreeLeaf, l, nil, err
	case format.PageBTreeInterior:
		in, err := unmarshalInterior(data)
		if err == nil {
			fr.SetDecoded(in)
		}
		t.pgr.Unpin(fr, false)
		return format.PageBTreeInterior, nil, in, err
	default:
		t.pgr.Unpin(fr, false)
		return typ, nil, nil, format.ErrCorrupt
	}
}

func (t *BTree) storeLeaf(pgno format.PageNo, l *leaf) error {
	return t.writePage(pgno, marshalLeaf(l))
}

func (t *BTree) storeInterior(pgno format.PageNo, in *interior) error {
	return t.writePage(pgno, marshalInterior(in))
}

func (t *BTree) writePage(pgno format.PageNo, body []byte) error {
	if len(body) > t.usable {
		return fmt.Errorf("btree: node body %d exceeds usable page size %d", len(body), t.usable)
	}
	fr, err := t.pgr.Get(pgno, pager.Write)
	if err != nil {
		return err
	}
	data := fr.Data()
	copy(data, body)
	for i := len(body); i < len(data); i++ {
		data[i] = 0
	}
	t.pgr.Unpin(fr, true)
	return nil
}

func (t *BTree) storeLeafNew(l *leaf) (format.PageNo, error) {
	pgno, fr, err := t.pgr.Allocate()
	if err != nil {
		return 0, err
	}
	body := marshalLeaf(l)
	copy(fr.Data(), body)
	t.pgr.Unpin(fr, true)
	return pgno, nil
}

func (t *BTree) storeInteriorNew(in *interior) (format.PageNo, error) {
	pgno, fr, err := t.pgr.Allocate()
	if err != nil {
		return 0, err
	}
	body := marshalInterior(in)
	copy(fr.Data(), body)
	t.pgr.Unpin(fr, true)
	return pgno, nil
}

// --- remaining Engine SPI ---

// Stats implements engine.Engine with a best-effort page-count footprint. It
// reports the physical size and freelist depth cheaply; live key/byte counts that
// would need a full tree walk are left zero, since Stats is meant to be O(1) for the
// observability surface (spec 09 §4).
func (t *BTree) Stats() engine.EngineStats {
	return engine.EngineStats{
		PhysicalBytes: int64(t.pgr.DBSize()) * int64(t.pageSize),
		FreePages:     int64(t.pgr.FreeCount()),
		Amplification: 1,
	}
}

// Reclaim implements engine.Engine. Page reclamation rides on lazy merge, deferred.
func (t *BTree) Reclaim(budget int) (int, error) { return 0, nil }

// RecoverFinished implements engine.Engine: a no-op. The whole tree lives in pages
// and is already correct after WAL replay, so open is O(1) past redo (spec 05 §7).
func (t *BTree) RecoverFinished(lastVersion uint64) error { return nil }

// compile-time check that BTree satisfies the SPI.
var _ engine.Engine = (*BTree)(nil)
