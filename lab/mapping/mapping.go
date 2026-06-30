// Package mapping is a frozen experiment: once part of the log lives on disk, how does a
// logical address turn into a file offset?
//
// Verdict: direct mapping, offset == address. The file IS the address space, so resolution is
// the identity, nothing to look up. A block table (split the address into a block number and
// an in-block offset, map the block number to a file base) costs a divide, a table load, and
// an add, and the table grows with the file so its load misses cache at scale. Direct mapping
// beats it ~3x on amd64 and has no table to miss as the file grows, which is why the design
// drops the pager. The full board is in impl note 178.
package mapping

const benchBlockSize = 1 << 16 // 64 KiB blocks, the kvbench page geometry

// logicalSpace is large so the block table outgrows cache, the larger-than-memory regime the
// engine targets, while the direct map never grows.
const logicalSpace = int64(1) << 34

// touchBufBits sizes the small resident buffer both candidates read from, so the per-op memory
// access is identical and the only difference measured is the resolution path.
const touchBufBits = 26 // 64 MiB

// blockTable is the loser kept for the comparison: a block-number to file-base map. The base is
// the natural layout so the resolved offset matches the direct path and the comparison isolates
// the lookup cost, not two different access patterns.
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

// mappingAddrs scatters addresses across the whole logical space so the block-table load lands
// on an unpredictable entry, the way real out-of-cache reads do. A multiplicative hash gives a
// deterministic scatter without rand.
func mappingAddrs(n int, space int64) []int64 {
	a := make([]int64, n)
	const stride = 2654435761
	for i := range a {
		a[i] = (int64(i) * stride) & (space - 1)
	}
	return a
}
