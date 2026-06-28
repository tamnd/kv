package f2

// hash64 maps a key to a 64-bit hash whose bits are well mixed in every region,
// because three different parts of the store slice different bit ranges out of
// it: the high byte picks the shard, middle bits form the index tag, and low bits
// pick the home slot. A weak hash that left any of those regions correlated would
// cluster one of the three. This is FNV-1a for the byte mixing followed by a
// splitmix64 finalizer that avalanches every input bit across the whole word.
func hash64(b []byte) uint64 {
	const (
		offset = 1469598103934665603
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	// splitmix64 finalizer
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

// bytesEqual is a length-checked compare. It is the standard library's bytes.Equal
// behavior inlined here so the hot read path carries no import beyond this package.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
