package hlog

import (
	"bytes"
	"encoding/binary"
	"hash/maphash"
	"sync"
)

// DB is the larger-than-memory point store: the hybrid log holds the records with its hot
// window in RAM and its cold prefix on disk in one file, and the index maps key
// fingerprints to addresses. It is the in-memory Store from note 176 grown past memory,
// the same two pieces with the log replaced by one that spills. This is the engine the
// later steps, the cold tier and the read cache, build on.
//
// A Set frames its record into a pooled buffer and appends it, so the write path does not
// allocate per op. A Get hashes the key, finds the address, reads the record from the ring
// or the file, and verifies the stored key. The read path takes no lock; the value is
// copied into the caller's scratch buffer because a ring slot can be reused under a reader
// and a disk record is not in memory at all.
type DB struct {
	log     *HybridLog
	ix      *Index
	seed    maphash.Seed
	bufPool sync.Pool
}

// OpenDB creates a store at path whose log keeps ringBytes of the recent tail resident and
// whose index is sized for capKeys keys. Both are sized to the working set, which is the
// point: the profiler showed the write cost is an index and log sized to the whole
// keyspace, and bounding them to the working set is the fix (note 177).
func OpenDB(path string, ringBytes int64, capKeys int) (*DB, error) {
	log, err := OpenHybridLog(path, ringBytes)
	if err != nil {
		return nil, err
	}
	d := &DB{
		log:  log,
		ix:   NewIndex(capKeys),
		seed: maphash.MakeSeed(),
		bufPool: sync.Pool{
			New: func() any { return make([]byte, 0, 256) },
		},
	}
	if err := d.replay(); err != nil {
		log.Close()
		return nil, err
	}
	return d, nil
}

// replay rebuilds the index from the recovered log so a reopen serves every durable record. It
// walks the records in address order and points the index at each, so a later overwrite of a
// key wins by being indexed last, the same latest-wins order the live write path produces. The
// fingerprints are recomputed with this process's seed, so the seed need not be persisted. A
// fresh file replays nothing.
func (d *DB) replay() error {
	if d.log.Tail() == 0 {
		return nil
	}
	return d.log.Range(func(addr int64, rec []byte) bool {
		if len(rec) < keyLenSize {
			return true
		}
		klen := int(binary.LittleEndian.Uint16(rec))
		if keyLenSize+klen > len(rec) {
			return true
		}
		key := rec[keyLenSize : keyLenSize+klen]
		d.ix.Put(maphash.Bytes(d.seed, key), addr)
		return true
	})
}

// Set stores value under key. It frames the record, a key-length prefix then the key then
// the value, into a pooled buffer, appends it to the log, and points the index at the new
// address. The pooled buffer keeps the write path allocation-free across calls.
func (d *DB) Set(key, value []byte) {
	fp := maphash.Bytes(d.seed, key)
	need := keyLenSize + len(key) + len(value)
	b := d.bufPool.Get().([]byte)
	if cap(b) < need {
		b = make([]byte, need)
	} else {
		b = b[:need]
	}
	binary.LittleEndian.PutUint16(b, uint16(len(key)))
	copy(b[keyLenSize:], key)
	copy(b[keyLenSize+len(key):], value)
	addr := d.log.Append(b)
	d.ix.Put(fp, addr)
	d.bufPool.Put(b[:0])
}

// Get returns the value stored under key. scratch is a caller-owned buffer the value is
// read into and may be reused across calls for allocation-free reads; the returned slice
// aliases it. It hashes the key, asks the index for the address, reads the record from the
// ring or the file, and verifies the stored key so a fingerprint collision is a clean miss.
func (d *DB) Get(key, scratch []byte) ([]byte, bool, error) {
	addr, ok := d.ix.Get(maphash.Bytes(d.seed, key))
	if !ok {
		return nil, false, nil
	}
	rec, err := d.log.At(addr, scratch)
	if err != nil {
		return nil, false, err
	}
	klen := int(binary.LittleEndian.Uint16(rec))
	if keyLenSize+klen > len(rec) {
		return nil, false, nil
	}
	if !bytes.Equal(rec[keyLenSize:keyLenSize+klen], key) {
		return nil, false, nil
	}
	return rec[keyLenSize+klen:], true, nil
}

// Sync forces every set so far durable before returning. Callers that need a crash-safe
// barrier use it; the normal path relies on the background group commit.
func (d *DB) Sync() error { return d.log.Sync() }

// Close flushes and closes the backing file. After Close every set is on disk.
func (d *DB) Close() error { return d.log.Close() }
