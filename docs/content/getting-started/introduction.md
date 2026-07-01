---
title: "Introduction"
description: "The model behind kv: one file, zero dependencies, a hash-indexed point store built for flat lookups over datasets larger than memory, with two durability modes."
weight: 10
---

A key/value store is the simplest useful database: keys map to values, and you `Set`, `Get`, and `Delete`.
kv is that, done as a real database you import rather than run.
It is an embedded key/value store for Go, in the lineage of SQLite: not a server you connect to, but a library you link and a file on disk.
The whole thing is built on a few ideas.

## One file, no dependencies

A kv database is a single file, by convention named `app.kv`, plus a small sibling commit-watermark file kept alongside it.
You can copy it, back it up, or commit a small one to a repository.
There is no directory of segments to keep together and no server process to supervise.

The module depends on nothing outside the Go standard library.
`go get github.com/tamnd/kv` pulls in no third-party packages, so it adds no transitive supply-chain surface and compiles into one static binary.
That constraint holds all the way down: the hash index, the hash-log core, and the server are all written against the standard library and nothing else.

## A point store, and only that

kv answers three operations: `Set`, `Get`, and `Delete`, each addressing one key.
That is the whole data surface.

kv is unordered.
It does not keep keys in sorted order, so there is no range scan, no cursor, no prefix walk, and no ordered iteration.
If your workload needs to enumerate keys in order or walk a range, model that in your own keys or reach for an ordered store instead.
Choosing kv is choosing to trade ordered iteration for a flat, contention-light point-lookup path.

## Built for point lookups

What you get in exchange is a read path that does not slow down as the database grows.
A key's fingerprint lives in an in-memory hash index; the value spills to the file.
A get hashes the key, finds the slot, and reads one record.
There is no tree to descend, so a lookup costs about the same at a thousand keys or a billion: point-read latency stays flat as the database grows past memory.

On the hot path a read allocates nothing.
`Get` decodes into a scratch buffer you pass in and returns a slice aliased to it, so a hot loop can reuse one buffer and allocate zero:

```go
scratch := make([]byte, 0, 256)
v, ok, err := db.Get([]byte("user:1"), scratch)
if err != nil {
	log.Fatal(err)
}
if !ok {
	// the key is not present
}
_ = v
```

A missing key is `ok == false`, not an error.
Pass `nil` as the scratch buffer to let the engine allocate a fresh slice for you.

The shape rewards workloads that are mostly keyed reads and writes: session stores, durable caches, per-entity records, counters, and queues addressed by id.
Compose keys however you like, but design for lookups by exact key rather than range.

## Larger than memory

A kv database does not have to fit in RAM.
The hash-log core keeps a bounded resident working set, the hot tier where writes land plus a recently-migrated cold tail, and leaves the rest in the file.
So the file can be many times the size of the memory you give it, and a read that misses the resident set pays one disk read while a hot read pays none.
The [storage engine guide](/guides/storage-engine/) covers how that works.

## Two durability modes

Every write goes to disk, but you choose when the acknowledgement waits on the fsync.
The default is background group commit: a write lands in the in-memory hot tier and returns, and a background flusher fsyncs it a moment later.
A crash between the ack and the next flush loses at most the un-flushed hot records, a bounded sub-second window, the same contract Redis gives with `appendfsync everysec`.

Set `SyncWrites` to true and a write does not return until its record is fsynced, so an acked write survives a crash with zero loss.
Concurrent writers coalesce onto one shared fsync, so a burst pays one flush rather than one per write.
The [durability guide](/guides/durability/) explains both modes and when to pick which.

Next: [install kv](/getting-started/installation/).
