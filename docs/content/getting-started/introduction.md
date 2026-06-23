---
title: "Introduction"
description: "The model behind kv: one file, ACID transactions over ordered keys, and a storage engine you choose rather than inherit."
weight: 10
---

A key/value store is the simplest useful database: keys map to values, and you `Get`, `Set`, and `Delete`. The simplicity is the trap. The moment two writers touch the same data, or a process dies mid-write, or you want every key under a prefix in order, the bare map stops being enough. You start bolting on locks, recovery, and iteration, and you are writing a database without having decided to.

kv decides to. It is an embedded ordered key/value database for Go, in the lineage of SQLite: not a server you run and connect to, but a library you import and a file on disk. The whole thing is built on four ideas.

## One file, no dependencies

A kv database is a single file, by convention named `app.kv`, with a write-ahead log `app.kv-wal` kept alongside it while the database is open. You can copy it, back it up, or commit a small one to a repository. There is no directory of segments to keep together and no server process to supervise.

The module depends on nothing outside the Go standard library. `go get github.com/tamnd/kv` pulls in no third-party packages, so it adds no transitive supply-chain surface and compiles into one static binary. That constraint is deliberate and it holds all the way down: the B-tree, the LSM tree, the write-ahead log, the encryption, and the server are all written against `os` and `crypto` and nothing else.

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

The default isolation level is snapshot isolation, which is fast and correct for almost everything. When you need the strongest guarantee, open with `WithIsolation(kv.Serializable)` and kv validates read sets at commit, closing the one anomaly (write skew) that snapshot isolation permits. [The transactions guide](/guides/transactions/) goes deeper.

## Keys are ordered

kv keeps keys in sorted byte order, and that order is part of the contract. It is what makes range and prefix scans natural:

```go
db.View(func(txn *kv.Txn) error {
	it, err := txn.NewIterator(kv.IterOptions{Prefix: []byte("user:")})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("%s\n", it.Key())
	}
	return it.Error()
})
```

Because keys are ordered, you can model a lot on top of kv: secondary indexes as key prefixes, time series as sortable timestamps, composite keys that scan hierarchically. An iterator reads from the same kind of stable snapshot a `View` does, so a long scan is never torn by concurrent writes.

## You choose the engine

Storage engines trade off in a way no single design escapes: a B-tree updates data in place, which keeps reads cheap and the file compact but makes random writes do more work, while a log-structured merge (LSM) tree turns writes into fast sequential appends and pays for it later with background compaction and more work per read. Most databases pick one and that is that.

kv makes it a per-database choice behind one option:

```go
// The default: a read-optimised B-tree.
db, _ := kv.Open("reads.kv")

// A write-optimised LSM tree, same API.
db, _ := kv.Open("writes.kv", kv.WithEngine(kv.LSM))
```

The engine is fixed when the file is created and recorded in its header, so reopening is automatic. Everything above the engine, transactions, iterators, the CLI, the server, is identical either way. [The engines guide](/guides/engines/) explains when to pick which, and the tuning knobs each one exposes.

## More than a library

Because the CLI and the server are written on top of the same public API, they are not separate products that can drift. The [`kv` command](/reference/cli/) opens a database file and calls the library, so it is the natural way to inspect, script, back up, and repair a database from a shell. The [server](/guides/server/) wraps a database in an HTTP and a pure-Go binary protocol, with authentication, TLS, request limits, and a change feed, for when a database needs to be reachable over a socket rather than linked into one process.

Next: [install kv](/getting-started/installation/).
