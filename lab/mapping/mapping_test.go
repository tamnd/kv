package mapping

import "testing"

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
