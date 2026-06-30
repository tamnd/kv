package hlog

import (
	"bytes"
	"encoding/binary"
	"hash/maphash"
)

// Store is the first end-to-end point path of the clean-room engine: the log holds the
// records, the index maps key fingerprints to their addresses, and Store joins them so a
// caller works in keys and values instead of addresses. This is the in-memory hot tier
// standing alone, before the disk spill and the cold tier. It exists so the point Get
// and Set paths can be measured for what they cost end to end, with nothing under them
// but the two pieces already benchmarked on their own.
//
// A Set frames the record straight into the log's reserved span and publishes the
// address through the index, both allocation-free. A Get hashes the key, finds the
// address, and reads the record back, verifying the stored key so a fingerprint
// collision returns a miss rather than the wrong value. The read path takes no lock.

// keyLenSize is the width of the per-record key-length prefix, a little-endian uint16,
// so the value that follows can be found without scanning. It caps a key at 64 KiB,
// far above any key the engine stores.
const keyLenSize = 2

// Store is the hot-tier point store: a log and its fingerprint index, plus the per-store
// hash seed mixed into every key. The seed is fixed for the store's life so a fingerprint
// is stable across the run.
type Store struct {
	log  *Log
	ix   *Index
	seed maphash.Seed
}

// NewStore returns a hot-tier store backed by capBytes of log memory and an index sized
// for capKeys keys. The caller sizes both to the working set, the same staging the log
// and index use on their own until the disk spill and resize steps land.
func NewStore(capBytes int64, capKeys int) *Store {
	return &Store{
		log:  New(capBytes),
		ix:   NewIndex(capKeys),
		seed: maphash.MakeSeed(),
	}
}

// Set stores value under key. It frames the record, a key-length prefix then the key then
// the value, directly into the log's reserved span with no temporary buffer, then points
// the index at the new address. An overwrite of an existing key updates the index in
// place to the new record and leaves the old record as garbage for a later compaction
// step to reclaim.
func (s *Store) Set(key, value []byte) {
	addr, dst := s.log.Reserve(keyLenSize + len(key) + len(value))
	binary.LittleEndian.PutUint16(dst, uint16(len(key)))
	copy(dst[keyLenSize:], key)
	copy(dst[keyLenSize+len(key):], value)
	s.ix.Put(maphash.Bytes(s.seed, key), addr)
}

// Get returns the value stored under key and whether it was found. It hashes the key to a
// fingerprint, asks the index for the address, reads the record there, and confirms the
// record's stored key matches before returning the value. The key check is what makes a
// fingerprint collision safe: a different key that hashed to the same fingerprint fails
// the compare and reports a miss instead of returning a stranger's value. No lock is
// taken on this path. The returned value aliases the log buffer and must not be mutated.
func (s *Store) Get(key []byte) ([]byte, bool) {
	addr, ok := s.ix.Get(maphash.Bytes(s.seed, key))
	if !ok {
		return nil, false
	}
	rec := s.log.At(addr)
	klen := int(binary.LittleEndian.Uint16(rec))
	recKey := rec[keyLenSize : keyLenSize+klen]
	if !bytes.Equal(recKey, key) {
		return nil, false
	}
	return rec[keyLenSize+klen:], true
}
