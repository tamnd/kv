package betree

// This file decouples a tree's root page from the single pager-header EngineRoot field, the
// next slice of M7's deferred integration after the durable shard directory (sharddir.go). A
// sharded core splits the keyspace into N independent sub-trees, each rooted at its own page,
// but every sub-tree cannot record its root in the one header field the file has. So a tree's
// root storage moves behind a small rootStore seam: the single-shard core still records its
// root in the header exactly as before, while a sharded sub-tree records its root in its slot
// of the in-memory shard directory the sharded core persists to the directory page.
//
// What this slice lands, and what it leaves. It lands the seam and the sub-tree constructor,
// proven by mounting several sub-trees in one file at independent roots, growing each into a
// multi-level tree, and reading every key back across a reopen through the directory. What it
// does not land is the sharded core itself: the Engine SPI wrapper that routes a write to its
// shard by the partition function, merges reads across the sub-trees, and persists the
// directory at checkpoint. That wrapper is the next slice; this one is the mount mechanism it
// stands on, the same build-the-substrate-first discipline the directory codec, the WAL frame,
// and the OLC primitives each followed before anything live depended on them.

import (
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// rootStore abstracts where a tree records its root page number durably. The seam exists so
// the single-shard core can keep its root in the pager header while a sharded sub-tree keeps
// its root in the shard directory, without the descent, split, and reopen paths knowing which.
// load reports the recorded root (format.NoPage on a fresh tree, which makes Open materialize
// an empty root); store records a new root when a growRoot installs one.
type rootStore interface {
	load() format.PageNo
	store(p format.PageNo)
}

// headerRootStore records the root in the pager header's EngineRoot field, the durable home the
// single-shard core has always used so a reopen finds the run with no side file. It is the
// default the exported New installs, so the core's on-disk behavior is unchanged by the seam:
// store writes the same header field setRoot wrote directly before, and the pager checkpoint
// flushes it the same way.
type headerRootStore struct{ pgr *pager.Pager }

func (h headerRootStore) load() format.PageNo   { return h.pgr.Header().EngineRoot }
func (h headerRootStore) store(p format.PageNo) { h.pgr.Header().EngineRoot = p }

// dirRootStore records a sub-tree's root in one slot of an in-memory shard directory (sharddir.go).
// A sub-tree that grows a new root updates its own slot here; the sharded core writes the
// directory page back at checkpoint so the new roots become durable, exactly as the header
// store's field is made durable by the pager checkpoint. Each sub-tree owns a distinct slot, so
// two sub-trees' stores never touch the same slice element, and a sub-tree's store runs under
// that sub-tree's writer lock; the directory is read whole only when persisting it, which the
// sharded core does at a checkpoint barrier when no sub-tree is mid-write.
type dirRootStore struct {
	dir  *shardDir
	slot int
}

func (d dirRootStore) load() format.PageNo   { return d.dir.roots[d.slot] }
func (d dirRootStore) store(p format.PageNo) { d.dir.roots[d.slot] = p }

// newTreeWithRoot is the shared constructor both the exported New and the sub-tree constructor
// call, so the two cannot drift in how they wire the reclaimer, the encode-buffer pool, and the
// root store. Callers pass the root store that decides where this tree's root lives.
func newTreeWithRoot(pgr *pager.Pager, rs rootStore) *Tree {
	return &Tree{pgr: pgr, rstore: rs, recl: newReclaimer(), encBuf: newScratchPool(pgr.UsablePageSize())}
}

// newSubTree returns a Bε-tree core that roots at slot of dir rather than at the pager header,
// the per-shard sub-tree a sharded core mounts. Call Open to materialize or rebind the root the
// same way the single-shard core does: a fresh slot (format.NoPage) makes Open build an empty
// root and record its page in the slot, an existing slot makes Open rebind it. It is the
// directory-backed counterpart to New, unwired until the sharded SPI wrapper mounts it.
func newSubTree(pgr *pager.Pager, dir *shardDir, slot int) *Tree {
	return newTreeWithRoot(pgr, dirRootStore{dir: dir, slot: slot})
}
