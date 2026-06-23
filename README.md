# kv

An embeddable ordered key/value database for Go.
One file on disk, zero external dependencies, full ACID transactions, and a choice of B-tree or LSM storage behind a single API.

[![Go Reference](https://pkg.go.dev/badge/github.com/tamnd/kv.svg)](https://pkg.go.dev/github.com/tamnd/kv)
[![Go Report Card](https://goreportcard.com/badge/github.com/tamnd/kv)](https://goreportcard.com/report/github.com/tamnd/kv)
[![Release](https://img.shields.io/github/v/release/tamnd/kv)](https://github.com/tamnd/kv/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-kv.tamnd.com-2d7ff9)](https://kv.tamnd.com)

kv is what SQLite is for relational data, for keys and values.
A database is a single file you open with a path and a line of code.
The import graph is the Go standard library and nothing else.
Reads and writes run inside real transactions, and you pick a read-optimized B-tree or a write-optimized LSM tree per database with one option, not a fork in the road you cannot walk back.

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

Most embedded key/value stores ask you to choose early and live with it.
A B-tree is fast to read and slow to write, or an LSM tree is the other way round.
Many also drag in a tree of dependencies, store a database as a directory of files you cannot move atomically, or hand you a `Get`/`Put` API with no transactions and leave consistency to you.

kv takes a different line:

- **One file, zero dependencies.** A database is a single `.kv` file plus a write-ahead log alongside it. The module imports only the standard library, so `go get` adds nothing else to your build.
- **Real transactions.** `View` and `Update` run closures inside ACID transactions. Snapshot isolation is the default, serializable is one option away, and conflicts retry automatically.
- **Two engines, one API.** The default B-tree updates in place and is tuned for low read latency. Open with `WithEngine(kv.LSM)` and the same API runs on a log-structured core tuned for write throughput. Nothing else in your code changes.
- **Crash-safe by construction.** Every commit goes through a checksummed write-ahead log. After a crash, the next open replays the log and brings the file back to its last committed state. Durability is tunable from one fsync per commit down to none.
- **Ordered, so you can scan.** Keys are sorted, so range scans, prefix scans, and reverse iteration are first-class. Iterators see a stable snapshot.
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

	// Read one back at a consistent snapshot.
	db.View(func(txn *kv.Txn) error {
		v, err := txn.Get([]byte("user:1"))
		if err != nil {
			return err
		}
		fmt.Printf("user:1 = %s\n", v)
		return nil
	})

	// Scan every key under a prefix.
	db.View(func(txn *kv.Txn) error {
		it, err := txn.NewIterator(kv.IterOptions{Prefix: []byte("user:")})
		if err != nil {
			return err
		}
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
			v, _ := it.Value()
			fmt.Printf("%s = %s\n", it.Key(), v)
		}
		return it.Error()
	})
}
```

### At the shell

The `kv` command is a thin layer over the same library, so a shell script reaches everything the API does:

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

Run `kv app.kv` with no subcommand to drop into an interactive shell on the open file, the way `sqlite3 app.db` does.

## Choosing an engine

The engine is fixed when a database is created and remembered in the file, so reopening is automatic.

```go
db, _ := kv.Open("app.kv")                            // B-tree, the default
db, _ := kv.Open("ingest.kv", kv.WithEngine(kv.LSM))  // LSM
```

Reach for the **B-tree** when reads dominate, when you want the smallest file, or when you are not sure.
Reach for the **LSM tree** when you ingest large volumes of writes, absorb bursts, or run write-heavy and scan-heavy workloads like logs and time series.
The transactions, iterators, CLI, and server are identical on both.
See the [engine guide](https://kv.tamnd.com/guides/engines/).

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
| `SyncNormal` | fdatasync at checkpoint and periodically. |
| `SyncBarrier` | A write-ordering barrier on every commit. |
| `SyncFull` | fdatasync on every commit. The safe default. |
| `SyncExtra` | `SyncFull` plus a directory sync on growth. |

See the [durability guide](https://kv.tamnd.com/guides/durability/).

## More

- **Encryption at rest** with AES-256-GCM and in-place key rotation.
- **Backup and replication**: consistent online backup, WAL shipping to read replicas, and point-in-time recovery.
- **A server**: `kv serve` over HTTP/JSON and a pure-Go binary protocol, with token and JWT/OIDC auth, per-prefix authorization, TLS and mTLS, and a change feed.
- **Observability**: Prometheus metrics, structured logging, and tracing hooks.

The full story is in the [guides](https://kv.tamnd.com/guides/) and the [reference](https://kv.tamnd.com/reference/).

## Documentation

The complete documentation lives at **[kv.tamnd.com](https://kv.tamnd.com)**:

- [Getting started](https://kv.tamnd.com/getting-started/): introduction, installation, and a first database in a minute.
- [Guides](https://kv.tamnd.com/guides/): transactions, engines, durability, encryption, backup and replication, the server, and the command line.
- [Reference](https://kv.tamnd.com/reference/): the library API, the CLI, and every configuration option and pragma.

## License

Apache License 2.0. See [LICENSE](LICENSE).
