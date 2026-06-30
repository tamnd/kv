# kv

An embeddable key/value database for Go.
One file on disk, zero external dependencies, full ACID transactions, and a sharded hash-log storage core built for fast point lookups over datasets larger than memory.

[![Go Reference](https://pkg.go.dev/badge/github.com/tamnd/kv.svg)](https://pkg.go.dev/github.com/tamnd/kv)
[![Go Report Card](https://goreportcard.com/badge/github.com/tamnd/kv)](https://goreportcard.com/report/github.com/tamnd/kv)
[![Release](https://img.shields.io/github/v/release/tamnd/kv)](https://github.com/tamnd/kv/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-kv.tamnd.com-2d7ff9)](https://kv.tamnd.com)

kv is what SQLite is for relational data, for keys and values.
A database is a single file you open with a path and a line of code.
The import graph is the Go standard library and nothing else.
Reads and writes run inside real transactions, and every key lookup goes through a sharded hash index over a self-durable log, so a get is a hash and a read, not a tree descent.

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

## Why kv

Most embedded key/value stores hand you a `Get`/`Put` API with no transactions and leave consistency to you, drag in a tree of dependencies, or store a database as a directory of files you cannot move atomically.

kv takes a different line:

- **One file, zero dependencies.** A database is a single `.kv` file plus a write-ahead log alongside it. The module imports only the standard library, so `go get` adds nothing else to your build.
- **Real transactions.** `View` and `Update` run closures inside ACID transactions. Snapshot isolation is the default, serializable is one option away, and conflicts retry automatically.
- **Fast point lookups that stay flat.** Keys live in a sharded, mostly lock-free hash index over a hybrid log. A get hashes the key and reads one record, with no tree to descend, so latency does not grow with the database. In memory on an Apple M4 a random read across a million keys is about 60 ns and allocates nothing, where the previous B-tree core took several microseconds for the same read. Reads take no lock on the common path, so they scale across cores. Set, get, exists, delete, and merge are the core operations.
- **Larger than memory.** The hybrid log keeps the working set you touch in RAM and leaves the cold tail in the file. The resident memory is bounded by `WithCacheSize`, so the database can be many times larger than the cache; a read that misses the resident set faults its page in from the file, and you pay a disk read only for the cold keys you actually reach. The index itself is compact, around 10 to 13 bytes per key whatever the key length, so a billion keys cost roughly 15 GiB of index.
- **Crash-safe by construction.** Every commit goes through a checksummed write-ahead log. After a crash, the next open replays the log and brings the file back to its last committed state. Durability is tunable from one fsync per commit down to none, and group commit batches concurrent committers so write throughput holds up at the safe level.
- **More than a library.** The same surface ships as a command-line tool and an HTTP and binary server with auth, TLS, and a change feed, for when you need the database over a socket.

## Install

As a library:

```bash
go get github.com/tamnd/kv@latest
```

The command-line tool, through any channel:

```bash
go install github.com/tamnd/kv/cmd/kv@latest   # Go
brew install tamnd/tap/kv                       # Homebrew
scoop install kv                                # Scoop (after adding the bucket)
docker run ghcr.io/tamnd/kv version             # Container
```

Signed apt and dnf repositories, release archives, and `.deb`/`.rpm`/`.apk` packages are on the [installation page](https://kv.tamnd.com/getting-started/installation/).
kv requires Go 1.23 or newer.

## Quick start

### In Go

```go
package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/tamnd/kv"
)

func main() {
	db, err := kv.Open("app.kv")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Write two keys atomically.
	err = db.Update(func(txn *kv.Txn) error {
		if err := txn.Set([]byte("user:1"), []byte("alice")); err != nil {
			return err
		}
		return txn.Set([]byte("user:2"), []byte("bob"))
	})
	if err != nil {
		log.Fatal(err)
	}

	// Read one key back. Get skips the transaction for a lone read.
	v, err := db.Get([]byte("user:1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("user:1 = %s\n", v)

	// A missing key is an error you can test for, not a silent zero value.
	if _, err := db.Get([]byte("user:3")); errors.Is(err, kv.ErrNotFound) {
		fmt.Println("user:3 not found")
	}
}
```

### At the shell

The `kv` command is a thin layer over the same library, so a shell script reaches everything the API does:

```bash
kv create app.kv
kv set app.kv user:1 alice
kv set app.kv user:2 bob
kv get app.kv user:1
```

```
alice
```

Run `kv app.kv` with no subcommand to drop into an interactive shell on the open file, the way `sqlite3 app.db` does.

## Transactions and isolation

Everything happens inside a transaction.
`Update` commits atomically if and only if the closure returns nil, and retries automatically on conflict.

```go
db.Update(func(txn *kv.Txn) error {
	v, err := txn.Get([]byte("counter"))
	if err != nil && !errors.Is(err, kv.ErrNotFound) {
		return err
	}
	return txn.Set([]byte("counter"), increment(v))
})
```

Snapshot isolation is the default and gives every transaction a stable view of the database.
Open with `kv.WithIsolation(kv.Serializable)` to close write skew too.
See the [transactions guide](https://kv.tamnd.com/guides/transactions/).

## Durability

Commits go through a write-ahead log and recover automatically after a crash.
The synchronous level is the durability-versus-speed dial:

| Level | Guarantee |
| --- | --- |
| `SyncOff` | No fsync; fastest, loses recent commits on power failure. |
| `SyncNormal` | fdatasync at checkpoint and periodically. The default: no corruption on power loss, only the last sub-second of commits at risk. |
| `SyncBarrier` | A write-ordering barrier on every commit. |
| `SyncFull` | fdatasync on every commit; no acked commit is ever lost. |
| `SyncExtra` | `SyncFull` plus a directory sync on growth. |

See the [durability guide](https://kv.tamnd.com/guides/durability/).

## More

- **Encryption at rest** with AES-256-GCM and in-place key rotation.
- **Backup and replication**: consistent online backup, WAL shipping to read replicas, and point-in-time recovery.
- **A server**: `kv serve` over HTTP/JSON, a pure-Go binary protocol, and a Redis (RESP) face an existing Redis client can drive, with token and JWT/OIDC auth, per-prefix authorization, TLS and mTLS, and a change feed.
- **Observability**: Prometheus metrics, structured logging, and tracing hooks.

The full story is in the [guides](https://kv.tamnd.com/guides/) and the [reference](https://kv.tamnd.com/reference/).

## Documentation

The complete documentation lives at **[kv.tamnd.com](https://kv.tamnd.com)**:

- [Getting started](https://kv.tamnd.com/getting-started/): introduction, installation, and a first database in a minute.
- [Guides](https://kv.tamnd.com/guides/): transactions, durability, encryption, backup and replication, the server, and the command line.
- [Reference](https://kv.tamnd.com/reference/): the library API, the CLI, and every configuration option and pragma.

## License

Apache License 2.0. See [LICENSE](LICENSE).
