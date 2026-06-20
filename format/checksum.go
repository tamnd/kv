package format

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// ErrCorrupt reports that on-disk bytes failed an integrity check: a page whose
// stored checksum does not match its contents (a torn write or bit rot, spec 02
// §3.2), or any structurally impossible image the lower layers refuse to trust.
// It is the sentinel the read path returns so a caller can tell corruption from a
// transient I/O error and surface it as the corrupt-file condition (spec 16 §4,
// exit code 4).
var ErrCorrupt = errors.New("kv/format: corrupt page (checksum mismatch)")

// StampPageChecksum writes the page's checksum into its trailer, the last
// ChecksumSize bytes of the full page, computed over everything before it (the
// usable area, spec 02 §3.2). It is a no-op when the algorithm is none, so a file
// created without checksums is written byte-for-byte as before. The page slice is
// the whole physical page; the engine's usable area is page[:len(page)-size] and
// the reserved trailer page[len(page)-size:] is exactly this checksum.
func StampPageChecksum(page []byte, algo ChecksumAlgo) {
	size := algo.ChecksumSize()
	if size == 0 || len(page) <= size {
		return
	}
	sum := algo.Sum(page[:len(page)-size])
	putChecksum(page[len(page)-size:], sum, size)
}

// VerifyPageChecksum recomputes the page's checksum over its usable area and
// compares it to the stored trailer, returning ErrCorrupt on a mismatch. It is a
// no-op (returns nil) when the algorithm is none, so an un-checksummed file always
// verifies. The caller passes the whole physical page.
func VerifyPageChecksum(page []byte, algo ChecksumAlgo) error {
	size := algo.ChecksumSize()
	if size == 0 || len(page) <= size {
		return nil
	}
	want := readChecksum(page[len(page)-size:], size)
	got := algo.Sum(page[:len(page)-size])
	if got != want {
		return ErrCorrupt
	}
	return nil
}

// putChecksum stores a checksum into the trailer big-endian: CRC32C in the low 4
// bytes, xxHash64 in 8. A 4-byte trailer holds only the low 32 bits of the sum.
func putChecksum(trailer []byte, sum uint64, size int) {
	if size == 4 {
		binary.BigEndian.PutUint32(trailer[:4], uint32(sum))
		return
	}
	binary.BigEndian.PutUint64(trailer[:8], sum)
}

// readChecksum reads a checksum from the trailer written by putChecksum.
func readChecksum(trailer []byte, size int) uint64 {
	if size == 4 {
		return uint64(binary.BigEndian.Uint32(trailer[:4]))
	}
	return binary.BigEndian.Uint64(trailer[:8])
}

// ChecksumAlgo selects the page/WAL checksum function (header offset 23,
// spec 02 §3.2). Algorithm 2 (xxHash64-truncated) is reserved for a later
// milestone; generation-1 files default to CRC32C.
type ChecksumAlgo byte

const (
	ChecksumNone   ChecksumAlgo = 0
	ChecksumCRC32C ChecksumAlgo = 1
	ChecksumXXH64  ChecksumAlgo = 2
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ChecksumSize reports how many trailing bytes the algorithm occupies in a page's
// usable area: 0 for none, 4 for CRC32C, 8 for xxHash64.
func (a ChecksumAlgo) ChecksumSize() int {
	switch a {
	case ChecksumCRC32C:
		return 4
	case ChecksumXXH64:
		return 8
	default:
		return 0
	}
}

// Sum computes the checksum of data under the selected algorithm, returned as a
// uint64 (CRC32C occupies the low 32 bits).
func (a ChecksumAlgo) Sum(data []byte) uint64 {
	switch a {
	case ChecksumCRC32C:
		return uint64(crc32.Checksum(data, castagnoli))
	case ChecksumXXH64:
		return xxh64(data)
	default:
		return 0
	}
}

// xxh64 is a self-contained implementation of the xxHash64 digest (seed 0). It is
// pure Go and dependency-free; the WAL's chained checksum and the optional page
// checksum both use it when ChecksumXXH64 is selected.
// xxHash64 primes. Declared as runtime variables (not constants) so the
// wrapping additions below compile; Go constant arithmetic does not wrap.
var (
	xxhPrime1 uint64 = 11400714785074694791
	xxhPrime2 uint64 = 14029467366897019727
	xxhPrime3 uint64 = 1609587929392839161
	xxhPrime4 uint64 = 9650029242287828579
	xxhPrime5 uint64 = 2870177450012600261
)

func xxh64(b []byte) uint64 {
	prime1, prime2, prime3, prime4, prime5 := xxhPrime1, xxhPrime2, xxhPrime3, xxhPrime4, xxhPrime5
	var h uint64
	n := len(b)
	if n >= 32 {
		v1 := prime1 + prime2
		v2 := prime2
		v3 := uint64(0)
		v4 := -prime1
		for len(b) >= 32 {
			v1 = xxhRound(v1, le64(b[0:8]))
			v2 = xxhRound(v2, le64(b[8:16]))
			v3 = xxhRound(v3, le64(b[16:24]))
			v4 = xxhRound(v4, le64(b[24:32]))
			b = b[32:]
		}
		h = rol(v1, 1) + rol(v2, 7) + rol(v3, 12) + rol(v4, 18)
		h = xxhMergeRound(h, v1)
		h = xxhMergeRound(h, v2)
		h = xxhMergeRound(h, v3)
		h = xxhMergeRound(h, v4)
	} else {
		h = prime5
	}
	h += uint64(n)
	for len(b) >= 8 {
		k := xxhRound(0, le64(b[0:8]))
		h ^= k
		h = rol(h, 27)*prime1 + prime4
		b = b[8:]
	}
	if len(b) >= 4 {
		h ^= uint64(le32(b[0:4])) * prime1
		h = rol(h, 23)*prime2 + prime3
		b = b[4:]
	}
	for _, c := range b {
		h ^= uint64(c) * prime5
		h = rol(h, 11) * prime1
	}
	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime3
	h ^= h >> 32
	return h
}

func xxhRound(acc, input uint64) uint64 {
	acc += input * 14029467366897019727
	acc = rol(acc, 31)
	acc *= 11400714785074694791
	return acc
}

func xxhMergeRound(acc, val uint64) uint64 {
	val = xxhRound(0, val)
	acc ^= val
	acc = acc*11400714785074694791 + 9650029242287828579
	return acc
}

func rol(x uint64, r uint) uint64 { return (x << r) | (x >> (64 - r)) }

func le64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

func le32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
