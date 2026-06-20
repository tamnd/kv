package kv

import "github.com/tamnd/kv/db"

// Iterator is a snapshot-consistent, version-resolved iterator over a range of user
// keys (spec 11). Every key it yields is already resolved to one value at the
// transaction's snapshot, with the transaction's own buffered writes overlaid. It must
// be Closed; it pins versions for its lifetime.
type Iterator struct {
	it *db.Iterator
}

// SeekGE positions at the first visible key >= key in the iteration direction and
// reports whether the iterator is valid.
func (it *Iterator) SeekGE(key []byte) bool { return it.it.SeekGE(key) }

// SeekLT positions at the last visible key < key in the iteration direction.
func (it *Iterator) SeekLT(key []byte) bool { return it.it.SeekLT(key) }

// First positions at the first key in the iteration direction.
func (it *Iterator) First() bool { return it.it.First() }

// Last positions at the last key in the iteration direction.
func (it *Iterator) Last() bool { return it.it.Last() }

// Next advances one key in the iteration direction.
func (it *Iterator) Next() bool { return it.it.Next() }

// Prev steps back one key in the iteration direction.
func (it *Iterator) Prev() bool { return it.it.Prev() }

// Valid reports whether the iterator is positioned on a key.
func (it *Iterator) Valid() bool { return it.it.Valid() }

// Key returns the user key at the cursor. The bytes are owned by the iterator.
func (it *Iterator) Key() []byte { return it.it.Key() }

// Value returns the value at the cursor, resolving a lazy value on demand, or nil for a
// key-only iterator.
func (it *Iterator) Value() ([]byte, error) {
	v, err := it.it.Value()
	return v, wrap(err)
}

// Error reports any error that ended iteration.
func (it *Iterator) Error() error { return wrap(it.it.Error()) }

// Close releases the iterator and unpins the versions it held.
func (it *Iterator) Close() error { return wrap(it.it.Close()) }
