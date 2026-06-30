package hlog

import "encoding/binary"

// A stored record in the durable engine (DB, TieredDB) carries a one-byte op ahead of the key
// so a delete is a real record, a tombstone, not the absence of one. A tombstone shadows any
// older value for its key across every tier, which is what makes a delete durable and correct in
// a log-structured store: you cannot erase the old record in place, so you write a newer record
// that says "gone" and let it win by being indexed last, exactly as an overwrite does.
//
// The frozen in-memory Store (store.go) predates deletes and keeps its leaner op-less framing;
// these helpers are only for the durable engine, so the two never share a record layout.
const (
	opSize = 1
	opSet  = 0 // a normal key to value mapping
	opDel  = 1 // a tombstone: the key is deleted as of this record
)

// recordLen is the framed size of a record for key and value: the op, the key-length prefix,
// the key, and the value.
func recordLen(key, value []byte) int {
	return opSize + keyLenSize + len(key) + len(value)
}

// frameRecord writes [op][klen][key][value] into dst, which must be recordLen(key, value) bytes.
func frameRecord(dst []byte, op byte, key, value []byte) {
	dst[0] = op
	binary.LittleEndian.PutUint16(dst[opSize:], uint16(len(key)))
	copy(dst[opSize+keyLenSize:], key)
	copy(dst[opSize+keyLenSize+len(key):], value)
}

// parseRecord splits a framed payload into its op, key, and value. ok is false for a record too
// short to hold its own header, which a torn or stray read can produce.
func parseRecord(rec []byte) (op byte, key, value []byte, ok bool) {
	if len(rec) < opSize+keyLenSize {
		return 0, nil, nil, false
	}
	op = rec[0]
	klen := int(binary.LittleEndian.Uint16(rec[opSize:]))
	if opSize+keyLenSize+klen > len(rec) {
		return 0, nil, nil, false
	}
	key = rec[opSize+keyLenSize : opSize+keyLenSize+klen]
	value = rec[opSize+keyLenSize+klen:]
	return op, key, value, true
}
