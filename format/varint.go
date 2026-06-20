// Package format defines the byte-level on-disk layout shared by both storage
// cores: the database header, the page-type taxonomy and common page header, the
// freelist linkage, the internal-key (MVCC) encoding, and the varint primitive.
// It is the container described in spec 02; it knows nothing about what a B-tree
// cell or an LSM block means, only how the bytes that frame them are laid out.
//
// All multi-byte integers are big-endian unless a field says otherwise, so files
// are byte-identical across architectures and fixed-width big-endian keys sort in
// page order.
package format

// PutUvarint encodes x into buf using the SQLite varint scheme and returns the
// number of bytes written (1..9). buf must have room for MaxVarintLen bytes.
//
// The encoding is big-endian, high-bit-continuation: each of the first eight
// bytes carries seven payload bits with the high bit signalling "more follows",
// and a ninth byte, when present, carries all eight of its bits, covering the
// full 64-bit range. This is byte-for-byte the encoding SQLite uses.
func PutUvarint(buf []byte, x uint64) int {
	if x <= 0x7f {
		buf[0] = byte(x)
		return 1
	}
	// Nine-byte form: the value needs more than 56 bits, so the low eight bits
	// go raw into the last byte and the remaining 56 bits fill bytes 0..7.
	if x&(uint64(0xff000000)<<32) != 0 {
		buf[8] = byte(x)
		x >>= 8
		for i := 7; i >= 0; i-- {
			buf[i] = byte(x&0x7f) | 0x80
			x >>= 7
		}
		return 9
	}
	// General case: collect 7-bit groups least-significant first, clear the
	// continuation bit on the terminating group, then emit most-significant first.
	var tmp [9]byte
	n := 0
	for {
		tmp[n] = byte(x&0x7f) | 0x80
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	tmp[0] &^= 0x80
	for i, j := 0, n-1; j >= 0; i, j = i+1, j-1 {
		buf[i] = tmp[j]
	}
	return n
}

// Uvarint decodes a varint from buf and returns the value and the number of bytes
// consumed. It returns n == 0 if buf is too short to hold a complete varint.
func Uvarint(buf []byte) (uint64, int) {
	var v uint64
	for i := 0; i < 8; i++ {
		if i >= len(buf) {
			return 0, 0
		}
		c := buf[i]
		v = (v << 7) | uint64(c&0x7f)
		if c&0x80 == 0 {
			return v, i + 1
		}
	}
	// Eight continuation bytes seen; the ninth byte carries all eight bits.
	if len(buf) < 9 {
		return 0, 0
	}
	v = (v << 8) | uint64(buf[8])
	return v, 9
}

// UvarintLen reports how many bytes PutUvarint would use for x.
func UvarintLen(x uint64) int {
	if x <= 0x7f {
		return 1
	}
	if x&(uint64(0xff000000)<<32) != 0 {
		return 9
	}
	n := 0
	for x != 0 {
		n++
		x >>= 7
	}
	return n
}

// MaxVarintLen is the largest number of bytes a varint can occupy.
const MaxVarintLen = 9

// AppendUvarint appends the varint encoding of x to dst and returns the extended
// slice.
func AppendUvarint(dst []byte, x uint64) []byte {
	var buf [MaxVarintLen]byte
	n := PutUvarint(buf[:], x)
	return append(dst, buf[:n]...)
}

// zigzag maps a signed integer to an unsigned one so that small-magnitude values
// of either sign stay short under the varint encoding.
func zigzag(n int64) uint64   { return uint64((n << 1) ^ (n >> 63)) }
func unzigzag(u uint64) int64 { return int64(u>>1) ^ -int64(u&1) }

// PutVarint encodes a signed integer via zigzag then varint.
func PutVarint(buf []byte, n int64) int { return PutUvarint(buf, zigzag(n)) }

// Varint decodes a signed integer written by PutVarint.
func Varint(buf []byte) (int64, int) {
	u, k := Uvarint(buf)
	if k == 0 {
		return 0, 0
	}
	return unzigzag(u), k
}
