package betree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// TestRootStoresLoadStore pins the two root stores' contract directly: a load reports what the
// last store recorded, the header store reaches the pager header field, and the directory store
// reaches only its own slot and leaves the others alone.
func TestRootStoresLoadStore(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "rs.kv", pager.Options{PageSize: 512, CacheFrames: 8, Engine: format.EngineBeta})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer p.Close()

	h := headerRootStore{pgr: p}
	h.store(42)
	if got := h.load(); got != 42 {
		t.Fatalf("header store load = %d, want 42", got)
	}
	if p.Header().EngineRoot != 42 {
		t.Fatalf("header store did not reach the header field: EngineRoot = %d", p.Header().EngineRoot)
	}

	dir := &shardDir{kind: pkHash, roots: make([]format.PageNo, 3)}
	d0 := dirRootStore{dir: dir, slot: 0}
	d2 := dirRootStore{dir: dir, slot: 2}
	d0.store(7)
	d2.store(9)
	if d0.load() != 7 || d2.load() != 9 {
		t.Fatalf("dir store load mismatch: slot0 = %d, slot2 = %d, want 7 and 9", d0.load(), d2.load())
	}
	if dir.roots[1] != format.NoPage {
		t.Fatalf("dir store touched an unrelated slot: roots[1] = %d", dir.roots[1])
	}
}

// TestSubTreeRootsAreIndependent mounts several Bε-tree sub-trees in one pager file at
// independent roots, each rooted at its own slot of a shard directory, grows each into a
// multi-level tree over a disjoint key space, then persists the directory, reopens the file,
// remounts the sub-trees from the reread directory, and reads every key back. It proves three
// things the sharded mount rests on: the sub-trees do not share a root (a grown root lands in
// its own slot, distinct from the others), they are isolated (no sub-tree sees another's keys),
// and the whole arrangement survives a reopen through the directory rather than the single
// header root.
func TestSubTreeRootsAreIndependent(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "shard.kv", pager.Options{PageSize: 512, CacheFrames: 64, Engine: format.EngineBeta})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	const n = 3
	const per = 1500
	// A fresh directory: every root is NoPage, so each sub-tree's Open materializes an empty
	// root and records its page back in the slot through the directory store.
	dir := &shardDir{kind: pkHash, roots: make([]format.PageNo, n)}
	subs := make([]*Tree, n)
	for i := range subs {
		subs[i] = newSubTree(p, dir, i)
		if err := subs[i].Open(&engine.Env{}); err != nil {
			t.Fatalf("open sub-tree %d: %v", i, err)
		}
	}

	// Each sub-tree gets a disjoint key space (its index is the key prefix), enough keys at a
	// small page to force leaf splits, interior splits, and a new root, so the growRoot path
	// that calls setRoot through the directory store is genuinely exercised per sub-tree.
	for s := range subs {
		b := engine.NewWriteBatch(1)
		for i := 0; i < per; i++ {
			b.Set([]byte(fmt.Sprintf("s%dk%06d", s, i)), []byte(fmt.Sprintf("v%06d", i)))
		}
		if err := subs[s].Apply(b, 1); err != nil {
			t.Fatalf("apply to sub-tree %d: %v", s, err)
		}
		// Drain the tail onto pages: the reopen below drives the pager directly with no logical
		// WAL to replay a tail-resident write from.
		if err := subs[s].Flush(); err != nil {
			t.Fatalf("flush sub-tree %d: %v", s, err)
		}
	}

	// Every sub-tree grew its own root: the slot is set, non-null, distinct from the others, and
	// matches the live root mirror. A shared or aliased root would collide here.
	seen := map[format.PageNo]bool{}
	for i := range subs {
		r := dir.roots[i]
		if r == format.NoPage {
			t.Fatalf("sub-tree %d left its directory slot at NoPage", i)
		}
		if seen[r] {
			t.Fatalf("sub-tree %d shares root page %d with another sub-tree", i, r)
		}
		seen[r] = true
		if r != subs[i].root() {
			t.Fatalf("sub-tree %d directory slot %d diverged from its live root %d", i, r, subs[i].root())
		}
		if d := subs[i].depth(t); d < 2 {
			t.Fatalf("sub-tree %d depth = %d, want >= 2 (a grown root, not a single leaf)", i, d)
		}
	}

	// Persist the directory, point the engine root at it, checkpoint, and close. This is what the
	// sharded core's checkpoint will do; here the trees are quiescent so the directory read is clean.
	dirPgno, err := writeShardDir(p, dir)
	if err != nil {
		t.Fatalf("write directory: %v", err)
	}
	p.Header().EngineRoot = dirPgno
	if err := p.Checkpoint(0, 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen, read the directory back from the engine root, and remount each sub-tree from its
	// recorded root. Open now takes the existing-root path, seeding the mirror from the slot.
	p2, err := pager.Open(fs, "shard.kv", pager.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()
	d2, err := readShardDir(p2, p2.Header().EngineRoot)
	if err != nil {
		t.Fatalf("read directory after reopen: %v", err)
	}
	if len(d2.roots) != n {
		t.Fatalf("reopened directory names %d roots, want %d", len(d2.roots), n)
	}
	subs2 := make([]*Tree, n)
	for i := range subs2 {
		subs2[i] = newSubTree(p2, d2, i)
		if err := subs2[i].Open(&engine.Env{}); err != nil {
			t.Fatalf("remount sub-tree %d: %v", i, err)
		}
	}

	for s := range subs2 {
		rd, err := subs2[s].NewReader(engine.Snapshot{Version: 1})
		if err != nil {
			t.Fatalf("reader on sub-tree %d: %v", s, err)
		}
		for i := 0; i < per; i++ {
			k := []byte(fmt.Sprintf("s%dk%06d", s, i))
			v, err := rd.Get(k)
			if err != nil {
				t.Fatalf("key %q missing from sub-tree %d after reopen: %v", k, s, err)
			}
			if want := fmt.Sprintf("v%06d", i); string(v) != want {
				t.Fatalf("key %q in sub-tree %d = %q, want %q", k, s, v, want)
			}
		}
		// Isolation: a key from a neighbor sub-tree's space must be absent here, which can only
		// hold if the sub-trees really descend from independent roots.
		other := []byte(fmt.Sprintf("s%dk%06d", (s+1)%n, 0))
		if _, err := rd.Get(other); err != engine.ErrNotFound {
			t.Fatalf("sub-tree %d returned neighbor key %q (err %v); roots are not independent", s, other, err)
		}
		rd.Close()
	}
}
