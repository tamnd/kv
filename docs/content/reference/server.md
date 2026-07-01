---
title: "Server reference"
description: "Every flag of the kv server, the supported RESP commands, and the store file layout on disk."
weight: 20
---

The `kv` binary serves one store over the Redis wire protocol (RESP2 and RESP3).
It is the network face of the same engine the library exposes.
For a task-oriented walkthrough, see [running the server](/guides/server/).

```
kv [flags]
```

One of `--addr` or `--unixsocket` is required.
Run `kv --version` to print the build and version.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--addr` | none | TCP listen address, for example `:6379`. Empty disables the TCP listener. |
| `--unixsocket` | none | Unix socket path, the faster local path. Wins if both this and `--addr` are set. |
| `--dir` | `.` | Data directory. The store lives at `<dir>/kv.db`. |
| `--synchronous` | `default` | Durability: `off`, `normal`, `full`, or `default`. Any value but `off` keeps the synced contract. |
| `--cardinality` | engine default | Expected distinct key count, mirroring `Options.KeyCapacity`. |
| `--value-bytes` | engine default | Typical value size hint, used to size hot segments. |
| `--cache-bytes` | engine default | Resident read window size, mirroring the cold-log resident window. |

One of `--addr` or `--unixsocket` must be set.
If both are set, the unix socket is used.
`--synchronous off` selects background group commit, the fast bounded-loss default; any other value fsyncs every commit before acknowledging it.

## Supported commands

The server implements the string commands a client and a benchmark use, plus the introspection commands a client issues at connect:

| Command | Purpose |
| --- | --- |
| `GET`, `SET`, `DEL` | Read, write, and delete a key. |
| `EXISTS` | Report whether a key is present. |
| `PING` | Liveness check. |
| `HELLO` | The RESP2/RESP3 handshake. |
| `CONFIG`, `COMMAND`, `INFO`, `DBSIZE`, `CLIENT`, `SELECT`, `FLUSHALL` | Introspection and session commands issued at connect. |
| `BGREWRITEAOF` | Forces a `Sync` in a synced mode: the "make it durable now" hook. |

It is the string keyspace only.
kv is an unordered point store, so there are no lists, hashes, sorted sets, expiry, or sorted iteration.

## Store file layout

The store lives under `--dir`:

| File | Role |
| --- | --- |
| `<dir>/kv.db` | The main store file, where the hash-indexed log lives. |
| commit-watermark sibling | A small file next to `kv.db` recording how far the durable commit point has advanced. |

On the next start, an existing store is opened and its log is replayed forward, dropping any record left torn by a crash, so recovery is automatic and there is no repair step to run.

## Next

- The [server guide](/guides/server/) walks the flags and shows `redis-cli` and Go client usage.
- The [library reference](/reference/library/) covers the same engine as an in-process API.
