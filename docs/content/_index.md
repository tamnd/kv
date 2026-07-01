---
title: "kv"
description: "kv is an embeddable key/value database for Go: one file on disk, zero dependencies outside the standard library, and a sharded hash-log storage core that keeps point lookups flat as the database grows past memory. Two durability modes and a Redis-protocol server."
heroTitle: "A key/value store in one Go import"
heroLead: "kv gives a Go program a durable key/value database that lives in a single file and pulls in nothing but the standard library. A sharded hash index over a hash-log core keeps point reads flat and runs larger than memory, two durability modes let you trade a bounded loss window for raw speed, and a Redis-protocol server puts the same engine on a socket. It is what SQLite is for relations, for keys and values."
heroPrimaryURL: "/getting-started/quick-start/"
heroPrimaryText: "Get started"
---

Most embedded key/value stores hand you a `Get`/`Put` API and drag in a tree of dependencies, or store a database as a directory of files you cannot move atomically.

kv takes a different line.
A database is one file.
The import graph is the Go standard library and nothing else.
Every key lookup goes through a sharded hash index over a hash-log core, so a get is a hash and a read, not a tree descent.

```go
db, err := kv.Open("app.kv", kv.Options{})
if err != nil {
	log.Fatal(err)
}
defer db.Close()

db.Set([]byte("greeting"), []byte("hello"))
```

## What it is

- **One file, zero dependencies.** A database is a single file on disk plus a small sibling commit-watermark file. The module imports only the standard library, so `go get github.com/tamnd/kv` adds nothing else to your build.
- **A point store.** The operations are `Set`, `Get`, and `Delete`, each addressing one key. kv is unordered: it does not range, scan, or iterate in key order. If you need ordered iteration, reach for a different engine.
- **Fast point lookups that stay flat.** A key's fingerprint lives in an in-memory hash index and its value spills to the file. A get hashes the key and reads one record, with no tree to descend, so read latency does not grow as the database grows. On the hot path a read allocates nothing, and you can hand `Get` a scratch buffer to reuse across a loop.
- **[Larger than memory](/guides/storage-engine/#larger-than-memory).** The hash-log core keeps a hot tier and a recently-migrated cold tail in RAM and leaves the rest in the file, so a database can be many times the size of the resident memory you give it. Point reads stay flat while the file grows past the cache.
- **[Two durability modes](/guides/durability/).** The default is background group commit, a bounded sub-second loss window, the same contract Redis gives with `appendfsync everysec`. Flip one option and every commit is fsynced before it returns, so an acked write survives a crash with zero loss.
- **A Redis-protocol server.** The `kv` binary serves one store over the [Redis wire protocol](/guides/server/), so `redis-cli` or any Redis client library can drive the same engine over a socket.

## A taste of the server

The `kv` binary is the network face of the same engine, spoken over RESP:

```bash
kv --port 6379 --dir ./data &
redis-cli -p 6379 set user:1 alice
redis-cli -p 6379 get user:1
```

```
alice
```

## Where to go next

- New here? Start with the [introduction](/getting-started/introduction/) for the model, then the [quick start](/getting-started/quick-start/) for a first database in a minute.
- Installing it? See [installation](/getting-started/installation/) for `go get`, Homebrew, Scoop, Linux packages, and the container image.
- Building on it? The [guides](/guides/) cover the storage engine, durability, sizing a store, and running the server.
- Need every detail? The [library API](/reference/library/), [server](/reference/server/), and [configuration](/reference/configuration/) references are the full surface.
