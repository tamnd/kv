package hlog

import (
	"testing"
)

// This file is the step-three technique decision, committed with both candidates so the
// choice is reproducible. The question: once part of the log lives on disk, how does a
// logical address turn into a file offset?
//
// Candidate A, direct mapping, the one the hybrid log uses: the file IS the address space,
// so logical address a lives at file offset a. Resolution is the identity, nothing to look
// up. This is the tax FASTER drops and the design keeps.
//
// Candidate B, block table, the classic pager approach the old engine carried: the file is
// cut into blocks, a record's address is split into a block number and an in-block offset,
// and a table maps block number to the block's base offset on disk. Resolution is a divide,
// a table load, and an add. The table grows with the file, so once the file is larger than
// cache the table load is a cache miss, the same random scatter the index Put profile flagged
// in note 177. This is the indirection the direct mapping removes.
//
// To measure the resolution path and only the resolution path, both candidates touch the
// same small resident buffer, so the per-op memory access is identical, and the only
// difference is how the address is resolved. The logical space is made large so the block
// table outgrows cache, which is the larger-than-memory regime this engine targets, so the
// benchmark shows the table miss rather than asserting it.

const benchBlockSize = 1 << 16 // 64 KiB blocks, the kvbench page geometry

// logicalSpace is the size of the address space the table must cover. At 16 GiB over 64 KiB
// blocks the base table is 256K entries, 2 MiB, already past L2 on the boxes here; the point
// is that it scales with the file while the direct map never grows.
const logicalSpace = int64(1) << 34

// touchBufBits sizes the small resident buffer both candidates read from, so the access cost
// is the same for each and the divergence is purely resolution.
const touchBufBits = 26 // 64 MiB

// blockTable is the loser kept checked in: a block-number to file-base map. The base is the
// natural layout, block i at offset i*blockSize, so the resolved offset matches the direct
// path and the comparison isolates the lookup cost, not two different access patterns.
type blockTable struct {
	base []int64
}

func newBlockTable(blocks int) *blockTable {
	t := &blockTable{base: make([]int64, blocks)}
	for i := range t.base {
		t.base[i] = int64(i) * benchBlockSize
	}
	return t
}

func (t *blockTable) resolve(addr int64) int64 {
	blk := addr / benchBlockSize
	off := addr % benchBlockSize
	return t.base[blk] + off
}

// mappingAddrs builds a fixed set of addresses scattered across the whole logical space, so
// the block-table load lands on an unpredictable entry and misses cache the way real
// out-of-cache reads do. A multiplicative hash gives a deterministic scatter without
// Math.rand, which the harness forbids.
func mappingAddrs(n int, space int64) []int64 {
	a := make([]int64, n)
	const stride = 2654435761
	for i := range a {
		a[i] = (int64(i) * stride) & (space - 1)
	}
	return a
}

func BenchmarkMappingDirect(b *testing.B) {
	const bufMask = (int64(1) << touchBufBits) - 1
	buf := make([]byte, int64(1)<<touchBufBits)
	addrs := mappingAddrs(1<<20, logicalSpace)
	mask := len(addrs) - 1
	var sink byte
	b.ResetTimer()
	for i := range b.N {
		off := addrs[i&mask] // direct: offset == address, no lookup
		sink ^= buf[off&bufMask]
	}
	_ = sink
}

func BenchmarkMappingBlockTable(b *testing.B) {
	const bufMask = (int64(1) << touchBufBits) - 1
	buf := make([]byte, int64(1)<<touchBufBits)
	t := newBlockTable(int(logicalSpace / benchBlockSize))
	addrs := mappingAddrs(1<<20, logicalSpace)
	mask := len(addrs) - 1
	var sink byte
	b.ResetTimer()
	for i := range b.N {
		off := t.resolve(addrs[i&mask]) // block table: divide, table load (a miss at scale), add
		sink ^= buf[off&bufMask]
	}
	_ = sink
}
