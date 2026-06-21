package format

import "encoding/binary"

// Kind is the one-byte tag appended to an internal key. The values are frozen
// for format generation 1 (spec 02 §8.4).
type Kind byte

const (
	KindDelete     Kind = 0x00 // deletion tombstone
	KindSet        Kind = 0x01 // value set
	KindMerge      Kind = 0x02 // merge operand
	KindRangeBegin Kind = 0x03 // range-delete start
	KindRangeEnd   Kind = 0x04 // range-delete end
	KindSetWithTTL Kind = 0x05 // value set with an expiry prefix on the value (spec 15 §6)
	KindSetSep     Kind = 0x06 // value set whose bytes live in the value log; the cell holds a value pointer (spec 06 §7)
)

// String renders a Kind for diagnostics.
func (k Kind) String() string {
	switch k {
	case KindDelete:
		return "del"
	case KindSet:
		return "set"
	case KindMerge:
		return "merge"
	case KindRangeBegin:
		return "rangebegin"
	case KindRangeEnd:
		return "rangeend"
	case KindSetWithTTL:
		return "setttl"
	case KindSetSep:
		return "setsep"
	default:
		return "kind?"
	}
}

// internalSuffixLen is the number of bytes appended to a user key to form an
// internal key: an 8-byte inverted version plus a one-byte kind.
const internalSuffixLen = 9

// MaxVersion is the largest assignable commit version. Inverting it yields zero,
// which sorts first; SeekGE uses it to land on the newest version of a key.
const MaxVersion uint64 = 1<<64 - 1

// AppendInternalKey appends user_key || BE64(^version) || kind to dst and returns
// the extended slice. Inverting the version makes the newest version of a user
// key sort first within its group, so a forward seek lands on the freshest
// version. Both cores store keys this exact way, which is what keeps MVCC and
// iteration engine-agnostic (spec 02 §8.4, spec 04 §1).
func AppendInternalKey(dst, userKey []byte, version uint64, kind Kind) []byte {
	dst = append(dst, userKey...)
	var suffix [internalSuffixLen]byte
	binary.BigEndian.PutUint64(suffix[:8], ^version)
	suffix[8] = byte(kind)
	return append(dst, suffix[:]...)
}

// EncodeInternalKey returns a freshly allocated internal key.
func EncodeInternalKey(userKey []byte, version uint64, kind Kind) []byte {
	return AppendInternalKey(make([]byte, 0, len(userKey)+internalSuffixLen), userKey, version, kind)
}

// ParseInternalKey splits an internal key into its user key, version, and kind.
// It returns ok == false if ik is too short to carry the 9-byte suffix.
func ParseInternalKey(ik []byte) (userKey []byte, version uint64, kind Kind, ok bool) {
	if len(ik) < internalSuffixLen {
		return nil, 0, 0, false
	}
	n := len(ik) - internalSuffixLen
	userKey = ik[:n]
	version = ^binary.BigEndian.Uint64(ik[n : n+8])
	kind = Kind(ik[n+8])
	return userKey, version, kind, true
}

// UserKey returns the user-key prefix of an internal key without copying. It
// returns ik unchanged if ik is too short to be a valid internal key.
func UserKey(ik []byte) []byte {
	if len(ik) < internalSuffixLen {
		return ik
	}
	return ik[:len(ik)-internalSuffixLen]
}

// Version extracts the commit version from an internal key.
func Version(ik []byte) uint64 {
	if len(ik) < internalSuffixLen {
		return 0
	}
	n := len(ik) - internalSuffixLen
	return ^binary.BigEndian.Uint64(ik[n : n+8])
}

// KindOf extracts the kind byte from an internal key.
func KindOf(ik []byte) Kind {
	if len(ik) < internalSuffixLen {
		return KindSet
	}
	return Kind(ik[len(ik)-1])
}

// CompareInternal orders two internal keys: user key ascending, then version
// descending (newest first), then kind ascending. It compares the user-key
// portions first and only then the fixed-width 9-byte trailer (inverted version
// + kind). A plain memcmp of the whole key would be wrong when one user key is a
// prefix of another, because the trailer bytes of the shorter key would compare
// against the continuation of the longer key. Comparing user parts first is the
// standard internal-key comparator (RocksDB/Pebble).
func CompareInternal(a, b []byte) int {
	ua, ub := UserKey(a), UserKey(b)
	if c := compareBytes(ua, ub); c != 0 {
		return c
	}
	// User keys equal: order by the trailer, which sorts the inverted version
	// (newest first) then the kind.
	return compareBytes(a[len(ua):], b[len(ub):])
}

// CompareUser orders two user keys bytewise (the default memcmp collation).
func CompareUser(a, b []byte) int {
	return compareBytes(a, b)
}

func compareBytes(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

// PrefixSuccessor returns the smallest key strictly greater than every key with
// the given prefix, or nil if the prefix is all 0xff (meaning "no upper bound").
// A prefix scan over p is the range [p, PrefixSuccessor(p)).
func PrefixSuccessor(prefix []byte) []byte {
	out := make([]byte, len(prefix))
	copy(out, prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
