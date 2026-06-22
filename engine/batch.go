package engine

import (
	"bytes"

	"github.com/tamnd/kv/format"
)

// BatchEntry is one internal-key mutation. The kind (set/delete/merge/range) is
// encoded in the internal key's trailing byte (spec 02 §8.4); Value is the
// payload for sets and merges, empty for deletes.
type BatchEntry struct {
	// InternalKey is user_key || ^version || kind, already version-stamped.
	InternalKey []byte
	// Value is the inline value, or empty for tombstones. Large values may be
	// routed to a value pointer by the engine on Apply.
	Value []byte
}

// WriteBatch is the unit of mutation handed to Engine.Apply (spec 04 §2.4). It is
// built above the seam by the transaction layer from the user's puts, deletes,
// and merges, already carrying commit versions. The same batch bytes are what the
// WAL logs, so "what is durable" and "what is applied" are identical.
type WriteBatch struct {
	entries []BatchEntry
	version uint64
	size    int
}

// NewWriteBatch returns an empty batch for the given commit version.
func NewWriteBatch(version uint64) *WriteBatch {
	return &WriteBatch{version: version}
}

// Version reports the commit version stamped on every entry.
func (b *WriteBatch) Version() uint64 { return b.version }

// Len reports the number of entries.
func (b *WriteBatch) Len() int { return len(b.entries) }

// Size reports the accounted byte size, used for write-stall pacing.
func (b *WriteBatch) Size() int { return b.size }

// Entries exposes the ordered entries for the engine to consume.
func (b *WriteBatch) Entries() []BatchEntry { return b.entries }

// Add appends a pre-encoded internal-key mutation.
func (b *WriteBatch) Add(internalKey, value []byte) {
	b.entries = append(b.entries, BatchEntry{InternalKey: internalKey, Value: value})
	b.size += len(internalKey) + len(value)
}

// Set appends a set mutation for userKey at the batch's version.
func (b *WriteBatch) Set(userKey, value []byte) {
	b.Add(format.EncodeInternalKey(userKey, b.version, format.KindSet), value)
}

// SetWithTTL appends a set for userKey whose value carries an absolute expiry in wall
// clock nanoseconds (spec 15 §6). It is stored under KindSetWithTTL with the expiry
// framed in front of the value, so a redo of the same committed batch re-installs an
// identical cell and a read past the expiry resolves the key absent.
func (b *WriteBatch) SetWithTTL(userKey, value []byte, expiry uint64) {
	b.Add(format.EncodeInternalKey(userKey, b.version, format.KindSetWithTTL), format.EncodeTTLValue(expiry, value))
}

// Delete appends a tombstone for userKey at the batch's version.
func (b *WriteBatch) Delete(userKey []byte) {
	b.Add(format.EncodeInternalKey(userKey, b.version, format.KindDelete), nil)
}

// Merge appends a merge operand for userKey at the batch's version.
func (b *WriteBatch) Merge(userKey, operand []byte) {
	b.Add(format.EncodeInternalKey(userKey, b.version, format.KindMerge), operand)
}

// DeleteRange appends a range deletion of the half-open interval [lo, hi) at the
// batch's version. It is stored as a single marker cell keyed at lo (kind
// KindRangeBegin) carrying hi as its value, so a redo of the same committed batch
// re-installs the identical cell and stays idempotent. Resolution treats every key
// in [lo, hi) older than this version as absent; the marker itself never surfaces
// as a user key (spec 11 §4, spec 02 §8.4).
func (b *WriteBatch) DeleteRange(lo, hi []byte) {
	ik := format.EncodeInternalKey(lo, b.version, format.KindRangeBegin)
	// Every entry in a batch shares the batch version, so two range deletes with the same lo encode to
	// the identical internal key (lo, version, KindRangeBegin). The engine stores a range delete as one
	// marker cell keyed there, and a cell key is unique, so a second marker would overwrite the first and
	// only the last hi would survive a checkpoint and reopen. Their union over any covered key is
	// [lo, max(hi)), so fold a repeat into the existing marker by keeping the larger hi rather than
	// appending a duplicate the store cannot keep. Range deletes are rare and batches small, so the scan
	// is cheap.
	for i := range b.entries {
		if bytes.Equal(b.entries[i].InternalKey, ik) {
			if bytes.Compare(hi, b.entries[i].Value) > 0 {
				b.size += len(hi) - len(b.entries[i].Value)
				b.entries[i].Value = hi
			}
			return
		}
	}
	b.Add(ik, hi)
}

// Encode serializes the batch to its wire form. This is the exact payload the WAL
// logs as a kv-batch frame (spec 07 §2.2), so "what is durable" and "what is
// applied" are byte-identical. The layout is varint version, varint entry count,
// then per entry: varint internal-key length, key bytes, varint value length,
// value bytes. Lengths are varints because keys and values are small and the
// common case packs into one byte each.
func (b *WriteBatch) Encode() []byte {
	dst := make([]byte, 0, 16+b.size+2*len(b.entries))
	dst = format.AppendUvarint(dst, b.version)
	dst = format.AppendUvarint(dst, uint64(len(b.entries)))
	for _, e := range b.entries {
		dst = format.AppendUvarint(dst, uint64(len(e.InternalKey)))
		dst = append(dst, e.InternalKey...)
		dst = format.AppendUvarint(dst, uint64(len(e.Value)))
		dst = append(dst, e.Value...)
	}
	return dst
}

// DecodeBatch reconstructs a batch from its wire form (the inverse of Encode). It
// returns ErrBatchCorrupt if the bytes are truncated or internally inconsistent,
// so a torn WAL frame is rejected rather than half-applied.
func DecodeBatch(p []byte) (*WriteBatch, error) {
	version, n := format.Uvarint(p)
	if n <= 0 {
		return nil, ErrBatchCorrupt
	}
	p = p[n:]
	count, n := format.Uvarint(p)
	if n <= 0 {
		return nil, ErrBatchCorrupt
	}
	p = p[n:]
	b := NewWriteBatch(version)
	for i := uint64(0); i < count; i++ {
		klen, n := format.Uvarint(p)
		if n <= 0 || uint64(len(p)-n) < klen {
			return nil, ErrBatchCorrupt
		}
		p = p[n:]
		key := append([]byte(nil), p[:klen]...)
		p = p[klen:]
		vlen, n := format.Uvarint(p)
		if n <= 0 || uint64(len(p)-n) < vlen {
			return nil, ErrBatchCorrupt
		}
		p = p[n:]
		var val []byte
		if vlen > 0 {
			val = append([]byte(nil), p[:vlen]...)
		}
		p = p[vlen:]
		b.Add(key, val)
	}
	return b, nil
}
