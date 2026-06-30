---
title: "Introduction"
description: "The model behind kv: one file, ACID transactions, and a hash-indexed storage core built for fast point lookups over datasets larger than memory."
weight: 10
---

A key/value store is the simplest useful database: keys map to values, and you `Get`, `Set`, and `Delete`. The simplicity is the trap. The moment two writers touch the same data, or a process dies mid-write, the bare map stops being enough. You start bolting on locks and recovery, and you are writing a database without having decided to.

kv decides to. It is an embedded key/value database for Go, in the lineage of SQLite: not a server you run and connect to, but a library you import and a file on disk. The whole thing is built on three ideas.

## One file, no dependencies

A kv database is a single file, by convention named `app.kv`, with a write-ahead log `app.kv-wal` kept alongside it while the database is open. You can copy it, back it up, or commit a small one to a repository. There is no directory of segments to keep together and no server process to supervise.

The module depends on nothing outside the Go standard library. `go get github.com/tamnd/kv` pulls in no third-party packages, so it adds no transitive supply-chain surface and compiles into one static binary. That constraint is deliberate and it holds all the way down: the hash index, the hybrid log, the write-ahead log, the encryption, and the server are all written against `os` and `crypto` and nothing else.

## Transactions, not just operations

Every read and write in kv happens inside a transaction. The two you reach for most are closures:

```go
// A read-only transaction at a consistent snapshot.
db.View(func(txn *kv.Txn) error {
	v, err := txn.Get([]byte("user:1"))
	// ...
	return err
})

// A read-write transaction that commits atomically, or not at all.
db.Update(func(txn *kv.Txn) error {
	txn.Set([]byte("user:1"), []byte("alice"))
	txn.Set([]byte("user:2"), []byte("bob"))
	return nil
})
```

A `View` sees a stable snapshot of the database: nothing another writer does while the closure runs changes what it reads. An `Update` either commits every write together or, if it returns an error or hits a conflict, leaves the database untouched. When two `Update`s race and one would violate isolation, kv detects the conflict and retries the closure for you, up to a bound you control.

For a single key that does not need to agree with any other read, `db.Get([]byte("user:1"))` skips the transaction entirely and hands back a copy you own. Reach for a `View` the moment two reads have to see the same version.

The default isolation level is snapshot isolation, which is fast and correct for almost everything. When you need the strongest guarantee, open with `WithIsolation(kv.Serializable)` and kv validates read sets at commit, closing the one anomaly (write skew) that snapshot isolation permits. [The transactions guide](/guides/transactions/) goes deeper.

## Built for point lookups

kv is a point-lookup store. The operations are `Get`, `Set`, `Delete`, `Exists`, and `Merge`, each addressing one key at a time. There is no range scan or ordered iteration: keys are not kept in sorted order, so kv does not promise to walk them in order, and if you need that you keep an index in your own keys or reach for an ordered store instead.

What you get in exchange is a read path that does not slow down as the database grows. Keys live in a sharded hash index over a hybrid log: a get hashes the key, looks it up in the index, and reads one record. There is no tree to descend, so a lookup costs about the same at a thousand keys or a billion. In memory on an Apple M4, a random read across a million keys takes about 60 nanoseconds and allocates nothing; the previous B-tree core took several microseconds for the same read. The index is split into many shards so concurrent readers and writers rarely contend, and most reads take no lock at all, so they scale across cores.

The hybrid log is also what lets a database outgrow RAM. The working set you touch most stays resident, bounded by the cache size you set, and the cold tail lives in the file; a read that misses the resident set faults its page in from disk, so you pay for a disk read only on the cold keys you actually reach. The index stays small while this happens, around 10 to 13 bytes per key whatever the key length, because a slot holds a fingerprint and a log offset rather than the key itself, so a billion keys cost roughly 15 GiB of index. The [storage engine guide](/guides/engines/) covers both in depth.

```go
v, err := db.Get([]byte("user:1"))
if errors.Is(err, kv.ErrNotFound) {
	// the key is not present
}
```

The shape rewards workloads that are mostly keyed reads and writes: session stores, caches with durability, per-entity records, counters, and queues addressed by id. Compose keys however you like, but design for lookups by exact key rather than range.

## More than a library

Because the CLI and the server are written on top of the same public API, they are not separate products that can drift. The [`kv` command](/reference/cli/) opens a database file and calls the library, so it is the natural way to inspect, script, back up, and repair a database from a shell. The [server](/guides/server/) wraps a database in an HTTP and a pure-Go binary protocol, with authentication, TLS, request limits, and a change feed, for when a database needs to be reachable over a socket rather than linked into one process.

Next: [install kv](/getting-started/installation/).
