package betree

// This file is the first integration slice of M7's durable side (doc 05 section 6, decision D9):
// the on-disk shard directory. M7's substance, the partition function, the cross-shard merge, and
// the cross-shard commit coordinator, landed and was proven in isolation; what it left for M8 is the
// integration, and the first piece of that is making the partitioning durable. A sharded core splits
// the keyspace into N independent sub-trees, each rooted at its own page, and the partitioner routes
// a key to one of them. None of that survives a reopen unless the file records two things: how many
// shards there are and the root page of each, and enough of the partitioner's own state to rebuild
// the exact routing rule, because a key written into shard 3 becomes unreachable if a later open
// rebuilds a partitioner that routes it to shard 5 (the determinism discipline partition.go is built
// around). The shard directory is the page that records both.
//
// Format first, like every layout slice before it. This lands the directory page codec and its
// durable round-trip through the pager, proven in isolation, with nothing mounting a sharded tree on
// it yet, exactly the discipline M0.1 used for the node codec and M5.1 used for the WAL frame: the
// envelope is built and fuzzed before any live path depends on it. The single-root, single-shard
// default core is untouched; it still roots directly at the header's EngineRoot. A sharded core
// instead points EngineRoot at a directory page and mounts one sub-tree per root the directory
// names; the sub-tree that roots at a directory slot rather than the header is the rootStore seam
// in root.go, and the SPI wrapper that routes and merges across those sub-trees is the slice after.
//
// What the directory must persist to rebuild the partitioner. A hash partitioner is fully described
// by its shard count, since the FNV-1a hash is a fixed pure function and the count is the only
// per-database parameter, so the directory's root count is enough to rebuild it. A range partitioner
// is described by its N-1 split keys, the boundaries that define the contiguous bands, so the
// directory stores those splits verbatim; the shard count is then the split count plus one and must
// match the root count, which the decoder checks. Storing the splits rather than recomputing them is
// the only safe choice: the splits are an operator's partition boundaries, not derivable from the
// data, so the file is their only durable home.

import (
	"encoding/binary"

	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
)

// partitionKind tags which partitioner a directory describes, so a reopen rebuilds the matching rule.
// It is stored in the directory page's common-header Flags byte.
type partitionKind byte

const (
	pkHash  partitionKind = 0 // hashPartitioner: FNV-1a modulo shard count, no stored boundaries
	pkRange partitionKind = 1 // rangePartitioner: N-1 stored split keys define the bands
)

// shardDir is the decoded form of a shard directory: the partitioner kind, the per-shard sub-tree
// root pages in shard order, and, for a range partitioner, the N-1 split keys. roots always has the
// shard count as its length; splits is empty for a hash directory and len(roots)-1 for a range one.
type shardDir struct {
	kind   partitionKind
	roots  []format.PageNo
	splits [][]byte
}

// Shard directory page layout, after the 8-byte common header (Type PageBetreeShardDir, Flags = the
// partition kind, CellCount = the shard count):
//
//	+8  split count       (2)   N-1 for a range directory, 0 for a hash one
//	+10 reserved          (6)   zeroed
//	+16 root array        shard count x 4-byte page number, big-endian
//	    split region            split count cells of [uvarint klen][key], strictly ascending (range only)
const shardDirHeaderEnd = 16

// maxShards bounds the shard count to what the common header's 16-bit cell count can name, the same
// ceiling the leaf codec puts on its record count. A sharded core never approaches it (the useful
// shard count is on the order of the core count), so this is a corruption guard, not a real limit.
const maxShards = 0xFFFF

// newShardDir builds a directory from a partitioner and its per-shard roots, capturing whatever state
// the partitioner needs to be rebuilt: nothing beyond the root count for a hash partitioner, the split
// keys for a range one. It type-switches on the concrete partitioner because the directory has to
// persist the partitioner's identity, not just its routing behavior, and the two concrete kinds carry
// different durable state. The roots and splits are copied so the directory owns stable bytes.
func newShardDir(p partitioner, roots []format.PageNo) *shardDir {
	d := &shardDir{roots: append([]format.PageNo(nil), roots...)}
	switch pp := p.(type) {
	case rangePartitioner:
		d.kind = pkRange
		d.splits = make([][]byte, len(pp.splits))
		for i, s := range pp.splits {
			d.splits[i] = append([]byte(nil), s...)
		}
	default:
		d.kind = pkHash
	}
	return d
}

// partitioner rebuilds the routing rule the directory describes. A hash directory rebuilds a hash
// partitioner over its root count; a range directory rebuilds a range partitioner from its stored
// splits. Because the hash constants are fixed and the splits are stored verbatim, the rebuilt
// partitioner routes every key identically to the one the directory was written from, which is the
// determinism a reopen depends on.
func (d *shardDir) partitioner() partitioner {
	if d.kind == pkRange {
		return newRangePartitioner(d.splits)
	}
	return newHashPartitioner(len(d.roots))
}

// encodeShardDir writes d into dst, the usable page area, and returns the number of bytes used. It
// returns ErrPageFull if the roots and splits do not fit. A directory whose kind and split count are
// inconsistent (a hash directory with splits, or a range directory whose split count is not one less
// than its root count) is a caller programming error, not an overflow, so it returns a plain error;
// only a genuine shortage of page space is ErrPageFull, the signal a caller would respond to.
func encodeShardDir(dst []byte, d *shardDir) (int, error) {
	n := len(d.roots)
	if n < 1 {
		return 0, errShardDirShape
	}
	if n > maxShards {
		return 0, ErrPageFull // more shards than the 16-bit count can name
	}
	switch d.kind {
	case pkHash:
		if len(d.splits) != 0 {
			return 0, errShardDirShape
		}
	case pkRange:
		if len(d.splits) != n-1 {
			return 0, errShardDirShape
		}
	default:
		return 0, errShardDirShape
	}

	end := shardDirHeaderEnd + n*4
	if end > len(dst) {
		return 0, ErrPageFull
	}
	// Header.
	format.CommonHeader{Type: format.PageBetreeShardDir, Flags: byte(d.kind), CellCount: uint16(n)}.Encode(dst)
	binary.BigEndian.PutUint16(dst[8:10], uint16(len(d.splits)))
	for i := 10; i < shardDirHeaderEnd; i++ {
		dst[i] = 0 // reserved
	}
	// Root array.
	for i, p := range d.roots {
		binary.BigEndian.PutUint32(dst[shardDirHeaderEnd+i*4:], p)
	}
	// Split region: length-prefixed keys in ascending order.
	off := end
	for _, s := range d.splits {
		need := off + format.UvarintLen(uint64(len(s))) + len(s)
		if need > len(dst) {
			return 0, ErrPageFull
		}
		buf := format.AppendUvarint(dst[:off], uint64(len(s)))
		off = len(buf)
		off += copy(dst[off:], s)
	}
	return off, nil
}

// decodeShardDir reads a directory from src, the usable page area, failing closed with ErrCorruptNode
// on anything that is not a well-formed generation-2 shard directory: a wrong tag, an unknown kind, a
// zero shard count, a split count that disagrees with the kind or the root count, a length that runs
// past the slice, or splits that are not strictly ascending. It never reads past src, the property the
// directory fuzz depends on, and it copies every split out so the result owns its bytes after the page
// is unpinned.
func decodeShardDir(src []byte) (*shardDir, error) {
	if len(src) < shardDirHeaderEnd {
		return nil, ErrCorruptNode
	}
	h := format.DecodeCommonHeader(src)
	if h.Type != format.PageBetreeShardDir {
		return nil, ErrCorruptNode
	}
	kind := partitionKind(h.Flags)
	if kind != pkHash && kind != pkRange {
		return nil, ErrCorruptNode
	}
	n := int(h.CellCount)
	if n < 1 {
		return nil, ErrCorruptNode
	}
	splitCount := int(binary.BigEndian.Uint16(src[8:10]))
	switch kind {
	case pkHash:
		if splitCount != 0 {
			return nil, ErrCorruptNode
		}
	case pkRange:
		if splitCount != n-1 {
			return nil, ErrCorruptNode
		}
	}

	end := shardDirHeaderEnd + n*4
	if end > len(src) {
		return nil, ErrCorruptNode
	}
	roots := make([]format.PageNo, n)
	for i := 0; i < n; i++ {
		roots[i] = binary.BigEndian.Uint32(src[shardDirHeaderEnd+i*4:])
	}

	var splits [][]byte
	if splitCount > 0 {
		splits = make([][]byte, 0, splitCount)
	}
	off := end
	var prev []byte
	for i := 0; i < splitCount; i++ {
		l, m := format.Uvarint(src[off:])
		// m <= 0 is a truncated or overlong varint; bound the length by the slice size before the int
		// conversion so a huge encoded length cannot overflow int negative and slip past the bound check,
		// the decode hazard the node codec fuzz already burned in. format.Uvarint pairs with the
		// format.AppendUvarint the encoder writes; the stdlib binary varint is a different encoding.
		if m <= 0 || l > uint64(len(src)) {
			return nil, ErrCorruptNode
		}
		off += m
		if off+int(l) > len(src) {
			return nil, ErrCorruptNode
		}
		key := append([]byte(nil), src[off:off+int(l)]...)
		off += int(l)
		// Splits must be strictly ascending: a non-ascending or duplicate boundary would name an empty or
		// out-of-order band no key routes to, so it is corruption, not a directory a reopen should trust.
		if i > 0 && format.CompareUser(prev, key) >= 0 {
			return nil, ErrCorruptNode
		}
		splits = append(splits, key)
		prev = key
	}
	return &shardDir{kind: kind, roots: roots, splits: splits}, nil
}

// writeShardDir allocates a fresh page, encodes d into it, and returns the page number, the durable
// home a reopen reads the directory back from. It runs at database create or a shard reconfigure under
// the caller's exclusive control, off the latch-free read path, so it copies into the frame without the
// fillGate dance the live node writes need; nothing mounts the directory yet and no reader reaches the
// page during this write. The caller installs the returned page as the engine root.
func writeShardDir(pgr *pager.Pager, d *shardDir) (format.PageNo, error) {
	dst := make([]byte, pgr.UsablePageSize())
	if _, err := encodeShardDir(dst, d); err != nil {
		return 0, err
	}
	pgno, fr, err := pgr.Allocate()
	if err != nil {
		return 0, err
	}
	copy(fr.Data(), dst)
	pgr.Unpin(fr, true)
	return pgno, nil
}

// readShardDir reads and decodes the directory at pgno. It is the reopen-side counterpart to
// writeShardDir: a sharded core's Open reads the engine root as a directory page and rebuilds its
// partitioner and per-shard roots from it. The decode copies its splits, so the directory is valid
// after the page is unpinned.
func readShardDir(pgr *pager.Pager, pgno format.PageNo) (*shardDir, error) {
	fr, err := pgr.Get(pgno, pager.Read)
	if err != nil {
		return nil, err
	}
	d, derr := decodeShardDir(fr.Data()[:pgr.UsablePageSize()])
	pgr.Unpin(fr, false)
	return d, derr
}
