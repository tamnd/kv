package betree

// This file is M2's optimistic lock coupling primitive (doc 05 section 1, decision
// D4). It is the latch that replaces the single RWMutex the M0 skeleton wraps around
// every tree access. A reader never excludes a writer and never excludes another
// reader: it reads a node's version word, reads the node, then reads the version word
// again, and trusts what it read only if the word did not move and the node was not
// locked the whole time. A writer takes a real exclusive lock on the one node it is
// about to change, mutates it, and bumps the version on release so any reader that
// overlapped the change fails its second check and starts over. Lock coupling is the
// rule that a descent validates the parent's version after it has read the child, so a
// structural change a writer makes to the parent while the reader is below cannot slip
// past unseen.
//
// The version word. One 64-bit word per node, touched only through sync/atomic so the
// Go race detector sees the happens-before that carries a writer's published change to
// the next reader. The two low bits are flags and the rest is a monotonic counter:
//
//	bit 0   locked    a writer holds the exclusive lock right now
//	bit 1   obsolete  the node has been retired and routed to epoch reclamation
//	bit 2.. version   bumped on every writeUnlock so readers detect any change
//
// A reader that observes either flag set on its first load cannot read safely, so it
// restarts (locked) or abandons the path (obsolete). The counter increment on unlock is
// what makes the second load differ from the first when a writer slipped in between, so
// a reader that saw a clean word, read the node, and finds the same clean word again
// knows no writer published a change to that node across its read.
//
// Where the word lives before the arena. Doc 05 stores the version word in the node
// header inside the off-heap arena, which M6 builds. Until then nodes live on the
// pager's frame pool and the persisted page bytes carry no lock state, so the word's
// pre-arena home is an in-memory table keyed by page number (versionTable below). The
// lock is a property of the node's identity, not of its bytes, so a page read in from
// disk and a page already resident share the one version word the table hands out for
// their page number, and the word survives eviction and reload of the frame underneath
// it.

import (
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	// lockedFlag is bit 0: a writer holds the exclusive lock on the node.
	lockedFlag uint64 = 0b01
	// obsoleteFlag is bit 1: the node has been retired and its page routed to the
	// epoch reclaimer. A reader that reaches an obsolete node followed a pointer that a
	// writer has since unlinked, so it abandons and restarts from a fresh root.
	obsoleteFlag uint64 = 0b10
	// versionStep is the increment added to the version word on every writeUnlock. It
	// is 4, not 1, so the add lands above the two flag bits and never disturbs them.
	versionStep uint64 = 0b100
)

// version is the per-node optimistic lock word. The zero value is a valid, unlocked,
// version-zero node, so a freshly created node needs no initialization.
type version struct {
	w atomic.Uint64
}

// readBegin loads the version word for an optimistic read. It returns the observed word
// and whether the node is in a readable state. ok is false when a writer holds the lock
// (the caller should spin and retry) or the node is obsolete (the caller followed a
// stale pointer and should restart from the root); the caller tells the two apart with
// obsolete(w).
func (v *version) readBegin() (w uint64, ok bool) {
	w = v.w.Load()
	return w, w&(lockedFlag|obsoleteFlag) == 0
}

// validate reports whether the node is unchanged since readBegin returned w0. It is the
// reader's second check: a true result means no writer took the lock and published a
// change to this node across the read, so everything the reader pulled from the node is
// a consistent snapshot. A false result means the reader must discard what it read and
// restart. The load carries the writer's release-store happens-before, which is what
// lets the non-atomic content reads between the two checks be the writer's last
// published bytes rather than a torn mix.
func (v *version) validate(w0 uint64) bool {
	return v.w.Load() == w0
}

// obsolete reports whether the word names a retired node.
func obsolete(w uint64) bool { return w&obsoleteFlag != 0 }

// locked reports whether the word names a node a writer holds right now.
func locked(w uint64) bool { return w&lockedFlag != 0 }

// tryWriteLock attempts to take the exclusive lock from the version the caller already
// observed as w0. It fails if w0 already shows the node locked or obsolete, or if
// another writer changed the word between the caller's load and this CAS. A caller that
// fails re-observes the word and decides whether to retry or abandon. On success the
// caller owns the lock and must release it with writeUnlock or writeUnlockObsolete.
func (v *version) tryWriteLock(w0 uint64) bool {
	if w0&(lockedFlag|obsoleteFlag) != 0 {
		return false
	}
	return v.w.CompareAndSwap(w0, w0|lockedFlag)
}

// writeLock blocks until it owns the exclusive lock. It is the writer's entry point
// when it has not already observed a version to couple from: it loads, tries to take an
// unlocked word, and spins with a scheduler yield until it wins. It does not lock an
// obsolete node; a writer that reaches one has followed a stale pointer and the boolean
// reports that so the caller restarts from the root rather than mutating a retired node.
func (v *version) writeLock() (ok bool) {
	for {
		w := v.w.Load()
		if w&obsoleteFlag != 0 {
			return false
		}
		if w&lockedFlag == 0 && v.w.CompareAndSwap(w, w|lockedFlag) {
			return true
		}
		runtime.Gosched()
	}
}

// writeUnlock releases the exclusive lock and bumps the version so any optimistic
// reader that overlapped the locked section fails its validate and restarts. The lock
// holder is the only goroutine that writes the word while it is locked (readers only
// load, other writers cannot take the lock), so a plain load-then-store is safe here and
// needs no CAS.
func (v *version) writeUnlock() {
	w := v.w.Load()
	v.w.Store((w &^ lockedFlag) + versionStep)
}

// writeUnlockObsolete releases the exclusive lock, marks the node obsolete, and bumps
// the version. The writer calls it instead of writeUnlock when it has unlinked the node
// from the tree and handed its page to the epoch reclaimer: the obsolete flag turns away
// any reader still descending through the stale pointer, and the version bump fails any
// reader that was mid-read of the node, so both the in-flight reader and the next
// arriving reader take the restart path.
func (v *version) writeUnlockObsolete() {
	w := v.w.Load()
	v.w.Store(((w &^ lockedFlag) | obsoleteFlag) + versionStep)
}

// versionTable is the pre-arena home of the per-node version words: a map from page
// number to the one version that guards that node, created on first touch and shared by
// every access to the page thereafter. It is the stand-in for the in-header version word
// M6's arena will carry; until then the lock state cannot live in the page bytes, so it
// lives here, attached to the node's identity (its page number) rather than to whichever
// frame currently holds its bytes.
//
// The table is read on every node access and written only when a never-seen page number
// first appears, so it uses sync.Map: the steady state is the read-mostly Load path with
// no lock, and the one-time LoadOrStore on a new page number is the only contended write.
type versionTable struct {
	m sync.Map // uint32 page number -> *version
}

// of returns the version word guarding the node at pgno, creating it on first touch. The
// same *version comes back for every later call with the same page number, so all
// readers and writers of a node couple on one word.
func (t *versionTable) of(pgno uint32) *version {
	if v, ok := t.m.Load(pgno); ok {
		return v.(*version)
	}
	v, _ := t.m.LoadOrStore(pgno, &version{})
	return v.(*version)
}

// forget drops the version word for a page number. A page number is forgotten only after
// its node is obsolete and its page has been physically reclaimed, so no live reader or
// writer can still hold the word; the next allocation that reuses the page number gets a
// fresh version starting at zero, which is correct because it is a different node. Until
// page reclamation exists in the betree (structural changes currently keep the original
// page number and only allocate new pages, never freeing one), nothing calls forget; it
// is the table's half of the retirement path the epoch reclaimer drives.
func (t *versionTable) forget(pgno uint32) {
	t.m.Delete(pgno)
}
