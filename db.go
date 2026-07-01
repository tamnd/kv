package kv

import (
	"bytes"
	"hash/maphash"
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
	log  *HybridLog
	ix   *Index
	seed maphash.Seed
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
		_, key, _, ok := parseRecord(rec)
		if !ok {
			return true
		}
		// Index every record, tombstone included, so a delete that came after a value wins by
		// being indexed last, the same latest-wins order the live write path produces.
		d.ix.Put(maphash.Bytes(d.seed, key), addr)
		return true
	})
}

// Set stores value under key. It frames the record into a pooled buffer, appends it to the
// log, and points the index at the new address. The pooled buffer keeps the write path
// allocation-free across calls.
func (d *DB) Set(key, value []byte) { d.write(opSet, key, value) }

// Delete writes a tombstone for key. The key reads as absent afterward, and the tombstone
// shadows any older value for the key, including one already on disk, because it is indexed
// last and Get stops at it. The old record's space is reclaimed by a later compaction.
func (d *DB) Delete(key []byte) { d.write(opDel, key, nil) }

// write frames a record with the given op straight into the log and repoints the index. It is
// the shared body of Set and Delete. Framing into the log rather than into a staging buffer it
// then copies saves a copy of the value per write, which the profiler showed was a third of the
// write path on a 1 KiB value (note 182).
func (d *DB) write(op byte, key, value []byte) {
	fp := maphash.Bytes(d.seed, key)
	addr := d.log.AppendFrame(op, key, value)
	d.ix.Put(fp, addr)
}

// Get returns the value stored under key. scratch is a caller-owned buffer the value is
// read into and may be reused across calls for allocation-free reads; the returned slice
// aliases it. It hashes the key, asks the index for the address, reads the record from the
// ring or the file, and verifies the stored key so a fingerprint collision is a clean miss. A
// tombstone reads as not found.
func (d *DB) Get(key, scratch []byte) ([]byte, bool, error) {
	v, ok, _, err := d.get(key, scratch)
	return v, ok, err
}

// get is Get plus whether the value came from disk rather than the resident ring. The tiered
// store uses the flag to cache only disk-sourced reads, so a read already served from memory does
// not pay a cache write. The public Get drops the flag.
func (d *DB) get(key, scratch []byte) (val []byte, ok, fromDisk bool, err error) {
	addr, ok := d.ix.Get(maphash.Bytes(d.seed, key))
	if !ok {
		return nil, false, false, nil
	}
	rec, fromDisk, err := d.log.AtSource(addr, scratch)
	if err != nil {
		return nil, false, false, err
	}
	op, recKey, value, ok := parseRecord(rec)
	if !ok || op == opDel || !bytes.Equal(recKey, key) {
		return nil, false, false, nil
	}
	return value, true, fromDisk, nil
}

// Sync forces every set so far durable before returning. Callers that need a crash-safe
// barrier use it; the normal path relies on the background group commit.
func (d *DB) Sync() error { return d.log.Sync() }

// Close flushes and closes the backing file. After Close every set is on disk.
func (d *DB) Close() error { return d.log.Close() }
