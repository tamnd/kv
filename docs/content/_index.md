---
title: "kv"
description: "kv is an embeddable ordered key/value database for Go: one file on disk, zero external dependencies, full ACID transactions, and a choice of B-tree or LSM storage behind a single API."
heroTitle: "An ordered key/value store in one Go import"
heroLead: "kv gives a Go program a durable, transactional, ordered key/value database that lives in a single file and pulls in nothing but the standard library. Snapshot-isolated transactions, a B-tree or LSM core chosen with one option, crash-safe writes through a write-ahead log, and a CLI and HTTP server built on the same API. It is what SQLite is for relations, for keys and values."
heroPrimaryURL: "/getting-started/quick-start/"
heroPrimaryText: "Get started"
---

Most embedded key/value stores ask you to choose early and live with it: a B-tree that is fast to read but slow to write, or an LSM tree that is the other way round. Many also drag in a tree of dependencies, store a database as a directory of files you cannot move atomically, or hand you a `Get`/`Put` API with no transactions and leave consistency to you.

kv takes a different line. A database is one file. The import graph is the Go standard library and nothing else. Reads and writes run inside real transactions with snapshot isolation by default and serializable on request. And the storage engine is a choice you make per database with a single option, not a fork in the road you cannot walk back.

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
- **Two engines, one API.** The default [B-tree](/guides/engines/) is read-optimised and updates in place. Open with `WithEngine(kv.LSM)` and the same API runs on a write-optimised log-structured core. Nothing else in your code changes.
- **Crash-safe by construction.** Every commit goes through a write-ahead log with checksummed frames. After a crash, the next open replays the log and brings the file back to its last committed state. Durability is [tunable](/guides/durability/) from one fsync per commit down to none.
- **Ordered, so you can scan.** Keys are sorted, so range scans, prefix scans, and reverse iteration are first-class, not bolted on. Iterators see a stable snapshot.
- **More than a library.** The same surface ships as a [command-line tool](/reference/cli/) for scripting and operations, and an [HTTP and binary server](/guides/server/) with auth, TLS, and a change feed when you need the database over a socket.

## A taste of the CLI

The `kv` command is a thin layer over the library, so anything the API does, a shell script can do too:

```bash
kv create app.kv
kv set app.kv user:1 alice
kv set app.kv user:2 bob
kv scan app.kv --prefix user:
```

```
user:1	alice
user:2	bob
```

## Where to go next

- New here? Start with the [introduction](/getting-started/introduction/) for the model, then the [quick start](/getting-started/quick-start/) for a first database in a minute.
- Installing it? See [installation](/getting-started/installation/) for `go get`, Homebrew, Scoop, Linux packages, and the container image.
- Building on it? The [guides](/guides/) cover transactions, picking an engine, durability, encryption, backup and replication, and running the server.
- Need every detail? The [library API](/reference/library/), [CLI](/reference/cli/), and [configuration](/reference/configuration/) references are the full surface.
