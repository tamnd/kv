package betree

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// sameDir reports whether two decoded directories carry identical state: kind, roots in order, and
// splits in order. It is the round-trip oracle the codec tests compare against.
func sameDir(a, b *shardDir) bool {
	if a.kind != b.kind || len(a.roots) != len(b.roots) || len(a.splits) != len(b.splits) {
		return false
	}
	for i := range a.roots {
		if a.roots[i] != b.roots[i] {
			return false
		}
	}
	for i := range a.splits {
		if !bytes.Equal(a.splits[i], b.splits[i]) {
			return false
		}
	}
	return true
}

// TestShardDirRoundTripHash encodes and decodes a hash directory and checks every field survives and
// the rebuilt partitioner routes a spread of keys exactly as a fresh hash partitioner over the same
// shard count, which is the determinism a reopen relies on.
func TestShardDirRoundTripHash(t *testing.T) {
	roots := []format.PageNo{7, 11, 13, 17}
	in := newShardDir(newHashPartitioner(len(roots)), roots)

	dst := make([]byte, 512)
	if _, err := encodeShardDir(dst, in); err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := decodeShardDir(dst)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !sameDir(in, out) {
		t.Fatalf("round trip mismatch: in=%+v out=%+v", in, out)
	}
	if out.kind != pkHash || len(out.splits) != 0 {
		t.Fatalf("hash directory decoded with kind %d, %d splits", out.kind, len(out.splits))
	}

	ref := newHashPartitioner(len(roots))
	got := out.partitioner()
	if got.shards() != ref.shards() {
		t.Fatalf("rebuilt shard count = %d, want %d", got.shards(), ref.shards())
	}
	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if got.route(key) != ref.route(key) {
			t.Fatalf("key %q routes to %d, rebuilt routes to %d", key, ref.route(key), got.route(key))
		}
	}
}

// TestShardDirRoundTripRange encodes and decodes a range directory, checks the splits and roots
// survive, and checks the rebuilt range partitioner routes identically to a fresh one over the same
// splits.
func TestShardDirRoundTripRange(t *testing.T) {
	splits := [][]byte{[]byte("d"), []byte("k"), []byte("r")}
	roots := []format.PageNo{2, 3, 5, 8} // one more root than splits
	rp := newRangePartitioner(splits)
	if rp.shards() != len(roots) {
		t.Fatalf("range partitioner has %d shards, set up %d roots", rp.shards(), len(roots))
	}
	in := newShardDir(rp, roots)

	dst := make([]byte, 512)
	if _, err := encodeShardDir(dst, in); err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := decodeShardDir(dst)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !sameDir(in, out) {
		t.Fatalf("round trip mismatch: in=%+v out=%+v", in, out)
	}
	if out.kind != pkRange || len(out.splits) != len(roots)-1 {
		t.Fatalf("range directory decoded with kind %d, %d splits (roots %d)", out.kind, len(out.splits), len(roots))
	}

	ref := newRangePartitioner(splits)
	got := out.partitioner()
	for _, key := range [][]byte{nil, []byte("a"), []byte("d"), []byte("dz"), []byte("k"), []byte("q"), []byte("r"), []byte("zzz")} {
		if got.route(key) != ref.route(key) {
			t.Fatalf("key %q routes to %d, rebuilt routes to %d", key, ref.route(key), got.route(key))
		}
	}
}

// TestShardDirReopen writes a directory through a real pager, installs it as the engine root,
// checkpoints, closes, reopens the file, and reads the directory back, so the durable path (allocate,
// copy into a frame, checkpoint to disk, reopen, decode) is proven end to end, not just the in-memory
// codec.
func TestShardDirReopen(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "dir.kv", pager.Options{PageSize: 512, CacheFrames: 16, Engine: format.EngineBeta})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	splits := [][]byte{[]byte("m")}
	roots := []format.PageNo{4, 9}
	in := newShardDir(newRangePartitioner(splits), roots)
	pgno, err := writeShardDir(p, in)
	if err != nil {
		t.Fatalf("write directory: %v", err)
	}
	p.Header().EngineRoot = pgno
	if err := p.Checkpoint(0, 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := pager.Open(fs, "dir.kv", pager.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()
	if p2.Header().EngineRoot != pgno {
		t.Fatalf("engine root after reopen = %d, want %d", p2.Header().EngineRoot, pgno)
	}
	out, err := readShardDir(p2, p2.Header().EngineRoot)
	if err != nil {
		t.Fatalf("read directory after reopen: %v", err)
	}
	if !sameDir(in, out) {
		t.Fatalf("directory changed across reopen: in=%+v out=%+v", in, out)
	}
}

// TestShardDirTooBig checks the encoder reports ErrPageFull, not a silent truncation, when the splits
// do not fit the page, so a caller can respond rather than write a short directory.
func TestShardDirTooBig(t *testing.T) {
	// Enough roots and wide splits to overflow a tiny page.
	var splits [][]byte
	var roots []format.PageNo
	roots = append(roots, 1)
	for i := 0; i < 40; i++ {
		splits = append(splits, bytes.Repeat([]byte{byte('a' + i%26)}, 20))
		roots = append(roots, format.PageNo(i+2))
	}
	in := newShardDir(newRangePartitioner(splits), roots)
	// newRangePartitioner sorts and dedups, so resync roots to the resulting shard count.
	rp := newRangePartitioner(splits)
	in = newShardDir(rp, make([]format.PageNo, rp.shards()))

	if _, err := encodeShardDir(make([]byte, 128), in); err != ErrPageFull {
		t.Fatalf("encode into a tiny page: err = %v, want ErrPageFull", err)
	}
}

// TestShardDirRejectsBadHeader checks the decoder fails closed on a wrong tag, an unknown kind, a zero
// shard count, and a split count that disagrees with the kind, rather than returning a bogus directory.
func TestShardDirRejectsBadHeader(t *testing.T) {
	good := make([]byte, 512)
	in := newShardDir(newHashPartitioner(3), []format.PageNo{1, 2, 3})
	if _, err := encodeShardDir(good, in); err != nil {
		t.Fatalf("encode baseline: %v", err)
	}

	cases := []struct {
		name   string
		mangle func(p []byte)
	}{
		{"wrong tag", func(p []byte) { p[0] = byte(format.PageBTreeLeaf) }},
		{"unknown kind", func(p []byte) { p[1] = 9 }},
		{"zero shards", func(p []byte) { p[2], p[3] = 0, 0 }},
		{"hash with splits", func(p []byte) { p[8], p[9] = 0, 1 }}, // split count 1 on a hash dir
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := append([]byte(nil), good...)
			tc.mangle(p)
			if _, err := decodeShardDir(p); err != ErrCorruptNode {
				t.Fatalf("decode mangled (%s): err = %v, want ErrCorruptNode", tc.name, err)
			}
		})
	}
}

// TestShardDirEncodeShape checks the encoder rejects an inconsistent directory (a range directory whose
// split count does not match its root count) with the shape error rather than writing a directory that
// would decode wrong.
func TestShardDirEncodeShape(t *testing.T) {
	bad := &shardDir{kind: pkRange, roots: []format.PageNo{1, 2, 3}, splits: [][]byte{[]byte("m")}} // needs 2 splits
	if _, err := encodeShardDir(make([]byte, 512), bad); err != errShardDirShape {
		t.Fatalf("encode inconsistent range dir: err = %v, want errShardDirShape", err)
	}
}

// FuzzDecodeShardDir asserts the decoder never panics or reads past the slice on arbitrary input and
// always either fails closed with ErrCorruptNode or returns a directory that re-encodes and decodes
// back to the same thing, the same fail-closed-and-stable contract the node codec fuzz holds.
func FuzzDecodeShardDir(f *testing.F) {
	seedDir := func(d *shardDir) []byte {
		b := make([]byte, 512)
		if _, err := encodeShardDir(b, d); err != nil {
			return nil
		}
		return b
	}
	f.Add(seedDir(newShardDir(newHashPartitioner(4), []format.PageNo{1, 2, 3, 4})))
	f.Add(seedDir(newShardDir(newRangePartitioner([][]byte{[]byte("d"), []byte("k")}), []format.PageNo{1, 2, 3})))
	f.Add(make([]byte, 64))
	f.Add([]byte("not a directory at all"))

	f.Fuzz(func(t *testing.T, src []byte) {
		d, err := decodeShardDir(src)
		if err != nil {
			if d != nil {
				t.Fatalf("error return with non-nil directory")
			}
			return
		}
		// A decoded directory must be internally consistent and must re-encode and decode back
		// identically. Encode needs a page at least as large as the source it came from.
		re := make([]byte, len(src))
		if _, err := encodeShardDir(re, d); err != nil {
			t.Fatalf("re-encode of a decoded directory failed: %v (dir %+v)", err, d)
		}
		d2, err := decodeShardDir(re)
		if err != nil {
			t.Fatalf("decode of re-encoded directory failed: %v", err)
		}
		if !sameDir(d, d2) {
			t.Fatalf("decode/encode/decode not stable: %+v vs %+v", d, d2)
		}
	})
}
