// Package kv is an embeddable key/value database for Go, in one file on disk with
// no dependencies outside the standard library.
//
// A database is a single file you open with a path and a line of code. The store is
// a sharded resident hash index over a hybrid log: every key's fingerprint lives in
// an in-memory index, and the values spill to the file, so a lookup is a hash and a
// read rather than a tree descent, and read latency stays flat as the database grows
// past memory. Writes land in an in-memory hot tier and migrate to the cold log a
// segment at a time, so the write path does not wait on a page rewrite.
//
// The whole surface is Open, one Options struct, and five methods on the returned
// *DB:
//
//	db, err := kv.Open("app.kv", kv.Options{})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
//	db.Set([]byte("greeting"), []byte("hello"))
//	val, ok, err := db.Get([]byte("greeting"), nil)
//	db.Delete([]byte("greeting"))
//	db.Sync()
//
// Get takes a scratch buffer to decode into and returns a slice aliased to it, so a
// hot read path can reuse one buffer and allocate nothing. Pass nil to let the engine
// allocate.
//
// The store is unordered. It answers point operations (set, get, delete) and does not
// range or scan, which is the trade the hash index makes for a flat point lookup.
//
// # Durability
//
// Options.SyncWrites picks the durability contract, and both settings are durable.
//
// The default, SyncWrites false, is background group commit: a write lands in the hot
// tier and returns, and a background flusher fsyncs it a moment later. A crash in the
// window between the ack and the next flush loses at most the un-flushed hot records,
// bounded to two segments, the same bounded-loss contract Redis gives with
// appendfsync everysec. This is the fast default, and it is where the engine's
// throughput lead lives, because the ack does not wait on the disk.
//
// SyncWrites true is synchronous group commit: a Set does not return until the
// group-commit fsync has persisted its record, so an acked write survives a crash with
// zero loss, the same contract Redis gives with appendfsync always. Concurrent writers
// coalesce onto one shared fsync, so a burst pays one flush between them rather than
// hitting the disk's per-flush ceiling on every write; a lone sequential writer pays
// one fsync per commit, the honest floor of durable-on-return.
//
// Sync forces a durability barrier on demand under either setting, and Close syncs
// before it returns, so a clean shutdown leaves nothing unflushed.
package kv
