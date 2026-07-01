# kv

An embeddable key/value database for Go.
One file on disk, zero dependencies outside the standard library, and a sharded hash-log storage core built for fast point lookups over datasets larger than memory.

[![Go Reference](https://pkg.go.dev/badge/github.com/tamnd/kv.svg)](https://pkg.go.dev/github.com/tamnd/kv)
[![Go Report Card](https://goreportcard.com/badge/github.com/tamnd/kv)](https://goreportcard.com/report/github.com/tamnd/kv)
[![Release](https://img.shields.io/github/v/release/tamnd/kv)](https://github.com/tamnd/kv/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-kv.tamnd.com-2d7ff9)](https://kv.tamnd.com)

kv is what SQLite is for relational data, for keys and values.
A database is a single file you open with a path and a line of code.
The import graph is the Go standard library and nothing else.
Every key lives in a sharded hash index over a self-durable log, so a get is a hash and a read, not a tree descent, and the latency stays flat as the database grows past memory.

```go
db, err := kv.Open("app.kv", kv.Options{})
if err != nil {
	log.Fatal(err)
}
defer db.Close()

db.Set([]byte("greeting"), []byte("hello"))

val, ok, err := db.Get([]byte("greeting"), nil)
```

## Why kv

Most embedded key/value stores drag in a tree of dependencies, or store a database as a directory of files you cannot move atomically, or make you descend a B-tree for every point read.

kv takes a different line:

- **One file, zero dependencies.** A database is a single `.kv` file plus a small sibling commit-watermark file. The module imports only the standard library, so `go get` adds nothing else to your build.
- **Fast point lookups that stay flat.** Keys live in a sharded, mostly lock-free hash index over a hybrid log. A get hashes the key and reads one record, with no tree to descend, so latency does not grow with the database. Reads take no lock on the common path, so they scale across cores. The whole surface is `Set`, `Get`, `Delete`, `Sync`, and `Close`.
- **Larger than memory.** The design keeps a compact key index resident in RAM while the values spill to the file. Writes land in an in-memory hot tier and migrate to the cold log a segment at a time; a read that misses the resident set reads its record from the file, so you pay a disk read only for the cold keys you actually reach.
- **Two durability modes, both durable.** The default is background group commit: a write returns as soon as it is in the hot tier and a flusher fsyncs it a moment later, a bounded sub-second loss window, the same contract Redis gives with `appendfsync everysec`. Set `SyncWrites` and a write does not return until it is fsynced, so an acked write survives a crash with zero loss.
- **A Redis-protocol server too.** The same engine ships as a `kv` binary that speaks RESP over TCP or a unix socket, so any Redis client can drive it, for when you need the store over a socket.

kv is a point store: it answers set, get, and delete, and it does not range or scan. That is the trade the hash index makes for a flat point lookup. If you need ordered iteration, reach for an ordered engine instead.

## Install

As a library:

```bash
go get github.com/tamnd/kv@latest
```

The server binary, through any channel:

```bash
go install github.com/tamnd/kv/cmd/kv@latest   # Go
brew install tamnd/tap/kv                       # Homebrew
scoop install kv                                # Scoop (after adding the bucket)
docker run ghcr.io/tamnd/kv --help              # Container
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
	db, err := kv.Open("app.kv", kv.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Set([]byte("user:1"), []byte("alice"))
	db.Set([]byte("user:2"), []byte("bob"))

	// Get decodes into the scratch buffer you pass and returns a slice aliased
	// to it, so a hot loop can reuse one buffer and allocate nothing. Pass nil
	// to let the engine allocate.
	val, ok, err := db.Get([]byte("user:1"), nil)
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		fmt.Printf("user:1 = %s\n", val)
	}

	// A missing key is the ok=false return, not an error.
	if _, ok, _ := db.Get([]byte("user:3"), nil); !ok {
		fmt.Println("user:3 not found")
	}
}
```

### Over the wire

The `kv` binary serves one store over the Redis protocol, so `redis-cli` and every Redis client library work unchanged:

```bash
kv --addr :6379 --dir ./data &
redis-cli -p 6379 set user:1 alice
redis-cli -p 6379 get user:1
```

```
alice
```

It speaks the point-operation subset of RESP: `GET`, `SET`, `DEL`, `EXISTS`, `PING`, the `HELLO` handshake, and the handful of introspection commands a client issues at connect. A unix socket (`--unixsocket /path`) is the fast local path.

## Durability

`Options.SyncWrites` picks the durability contract, and both settings are durable.

| `SyncWrites` | Guarantee |
| --- | --- |
| `false` (default) | Background group commit. A write returns once it is in the hot tier and the flusher fsyncs it a moment later. A crash loses at most the un-flushed hot records, bounded to two segments, the same contract as Redis `appendfsync everysec`. This is where the throughput lead lives. |
| `true` | Per-commit fsync. A `Set` does not return until its record is on the disk, so an acked write survives a crash with zero loss. Concurrent writers coalesce onto one shared fsync, so a burst pays one flush between them. |

`Sync` forces a durability barrier on demand under either setting, and `Close` syncs before it returns, so a clean shutdown leaves nothing unflushed.
See the [durability guide](https://kv.tamnd.com/guides/durability/).

## Performance

kv is built for read-heavy and update-heavy workloads whose working set fits in memory.
On an Apple M4, a random read across a million cache-resident keys runs in the millions of ops per second and allocates nothing on the hot path, several times faster than the LSM and B-tree engines in the same harness, and a read-update mix (YCSB-A) leads the field because a hot key takes its update straight into memory.
Its per-commit durable write throughput is mid-pack, because it fsyncs per commit rather than batching many commits into one flush the way a group-committing LSM does.

The numbers, the methodology, and the honest places kv loses are all published at **[kvbench](https://github.com/tamnd/kvbench)**, which measures kv against badger, pebble, bbolt, buntdb, pogreb, goleveldb, and sqlite through one adapter with no home-field advantage.
Run it on your own hardware; the whole point is that you do not have to take ours.

## Documentation

The complete documentation lives at **[kv.tamnd.com](https://kv.tamnd.com)**:

- [Getting started](https://kv.tamnd.com/getting-started/): introduction, installation, and a first database in a minute.
- [Guides](https://kv.tamnd.com/guides/): durability, the server, and sizing a store.
- [Reference](https://kv.tamnd.com/reference/): the library API and the server.

## License

Apache License 2.0. See [LICENSE](LICENSE).
