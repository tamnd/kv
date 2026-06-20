package engine

import "github.com/tamnd/kv/format"

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

// Delete appends a tombstone for userKey at the batch's version.
func (b *WriteBatch) Delete(userKey []byte) {
	b.Add(format.EncodeInternalKey(userKey, b.version, format.KindDelete), nil)
}

// Merge appends a merge operand for userKey at the batch's version.
func (b *WriteBatch) Merge(userKey, operand []byte) {
	b.Add(format.EncodeInternalKey(userKey, b.version, format.KindMerge), operand)
}
