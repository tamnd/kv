package format

import "encoding/binary"

// TTL gives a key an expiry: a SetWithTTL stores the value under KindSetWithTTL with
// an 8-byte big-endian expiry stamped in front of it (spec 15 §6). A read past the
// expiry treats the version as absent even before the background sweep tombstones it,
// so expiry is a read-time predicate layered on the existing version machinery rather
// than a new resolution rule. The expiry is wall-clock nanoseconds; zero means no
// expiry, which a well-formed TTL value never carries.

// EncodeTTLValue frames value with expiry as its leading 8 bytes.
func EncodeTTLValue(expiry uint64, value []byte) []byte {
	out := make([]byte, 8+len(value))
	binary.BigEndian.PutUint64(out[:8], expiry)
	copy(out[8:], value)
	return out
}

// DecodeTTLValue splits a framed TTL value into its expiry and the user value. A value
// too short to carry the 8-byte prefix is treated as a non-expiring empty value, so a
// malformed cell reads as live rather than panicking.
func DecodeTTLValue(raw []byte) (expiry uint64, value []byte) {
	if len(raw) < 8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(raw[:8]), raw[8:]
}

// OpFromCell decodes a stored internal-key/value cell into the Op the shared fold
// should see at wall clock now. It is the one place expiry and value framing are
// understood, so every fold (both engine cores and the conformance oracle) expands an
// expired TTL set into a synthetic tombstone identically and the shared Fold stays
// oblivious to TTL.
func OpFromCell(ik, value []byte, now uint64) (Op, bool) {
	return OpFromParts(Version(ik), KindOf(ik), value, now)
}

// OpFromParts is OpFromCell for callers that already hold the decoded version, kind,
// and stored value, such as the conformance oracle's per-version records. It returns
// ok == false for range-delete markers, which resolve through RangeDel rather than as
// ops. An expired TTL set (now != 0 and expiry <= now) becomes a synthetic delete at
// the same version; now == 0 disables expiry entirely, which is what GC and any
// deliberately non-expiring read pass so a value the sweep has not yet removed still
// folds as live.
func OpFromParts(version uint64, kind Kind, value []byte, now uint64) (Op, bool) {
	switch kind {
	case KindRangeBegin, KindRangeEnd:
		return Op{}, false
	case KindSetWithTTL:
		expiry, uv := DecodeTTLValue(value)
		if now != 0 && expiry != 0 && expiry <= now {
			return Op{Version: version, Kind: KindDelete}, true
		}
		return Op{Version: version, Kind: KindSet, Value: uv}, true
	default:
		return Op{Version: version, Kind: kind, Value: value}, true
	}
}
