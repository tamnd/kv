---
title: "kv"
description: "kv is an embeddable key/value database for Go: one file on disk, zero external dependencies, full ACID transactions, and a sharded hash-log storage core that keeps point lookups flat as the database grows and runs over datasets larger than memory."
heroTitle: "A key/value store in one Go import"
heroLead: "kv gives a Go program a durable, transactional key/value database that lives in a single file and pulls in nothing but the standard library. Snapshot-isolated transactions, a sharded hash index over a self-durable log that keeps point reads flat and runs larger than memory, crash-safe writes through a write-ahead log, and a CLI and HTTP server built on the same API. It is what SQLite is for relations, for keys and values."
heroPrimaryURL: "/getting-started/quick-start/"
heroPrimaryText: "Get started"
---

Most embedded key/value stores hand you a `Get`/`Put` API with no transactions and leave consistency to you. Many also drag in a tree of dependencies or store a database as a directory of files you cannot move atomically.

kv takes a different line. A database is one file. The import graph is the Go standard library and nothing else. Reads and writes run inside real transactions with snapshot isolation by default and serializable on request. And every key lookup goes through a sharded hash index over a self-durable log, so a get is a hash and a read.

```go
db, err := kv.Open("app.kv")
if err != nil {
	log.Fatal(err)
}
defer db.Close()

db.Update(func(txn *kv.Txn) error {
	return txn.Set([]byte("greeting"), []byte("hello"))
})
```

## What it is

- **One file, zero dependencies.** A database is a single `.kv` file plus a write-ahead log alongside it. The module imports only the standard library, so `go get github.com/tamnd/kv` adds nothing else to your build.
- **Real transactions.** `View` and `Update` run closures inside ACID transactions. Snapshot isolation is the default; ask for [serializable](/guides/transactions/) and write skew is closed too. Conflicts retry automatically.
- **Fast point lookups that stay flat.** Keys live in a sharded, mostly lock-free hash index over a hybrid log. A get hashes the key and reads one record, with no tree to descend, so read latency does not grow with the database. In memory a random read across a million keys is about 60 ns and allocates nothing, and reads take no lock on the common path, so they scale across cores. Set, get, exists, delete, and merge are the core operations.
- **[Larger than memory](/guides/engines/#larger-than-memory).** The hybrid log keeps the working set in RAM and the cold tail in the file, so a database can be many times the size of the cache. A read that misses the resident set faults its page in from disk, and the index stays compact at around 10 to 13 bytes per key, so a billion keys cost roughly 15 GiB of index.
- **Crash-safe by construction.** Every commit goes through a write-ahead log with checksummed frames. After a crash, the next open replays the log and brings the file back to its last committed state. Durability is [tunable](/guides/durability/) from one fsync per commit down to none.
- **More than a library.** The same surface ships as a [command-line tool](/reference/cli/) for scripting and operations, and an [HTTP and binary server](/guides/server/) with auth, TLS, and a change feed when you need the database over a socket.

## A taste of the CLI

The `kv` command is a thin layer over the library, so anything the API does, a shell script can do too:

```bash
kv create app.kv
kv set app.kv user:1 alice
kv set app.kv user:2 bob
kv get app.kv user:1
```

```
alice
```

## Where to go next

- New here? Start with the [introduction](/getting-started/introduction/) for the model, then the [quick start](/getting-started/quick-start/) for a first database in a minute.
- Installing it? See [installation](/getting-started/installation/) for `go get`, Homebrew, Scoop, Linux packages, and the container image.
- Building on it? The [guides](/guides/) cover transactions, durability, encryption, backup and replication, and running the server.
- Need every detail? The [library API](/reference/library/), [CLI](/reference/cli/), and [configuration](/reference/configuration/) references are the full surface.
