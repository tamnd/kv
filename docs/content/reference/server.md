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

The flags follow `redis-server`, so the binary is close to a drop-in.
Bare `kv` starts on `127.0.0.1:6379` with the store at `./dump.kv`, the way `redis-server` with no config does.
Run `kv --version` to print the build and version.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--port` | `6379` | TCP port to serve RESP on. `0` disables the TCP listener. |
| `--bind` | `127.0.0.1` | Address the TCP listener binds to. |
| `--unixsocket` | none | Unix socket path, the faster local path. Binds alongside the TCP port, not instead of it. |
| `--dir` | `.` | Data directory. The store lives at `<dir>/<dbfilename>`. |
| `--dbfilename` | `dump.kv` | Store file name within `--dir`. |
| `--appendonly` | `yes` | Keep the append log: `yes` or `no`. kv is always log-backed, so this is accepted for compatibility and does not turn persistence off. |
| `--appendfsync` | `everysec` | Durability: `no`, `everysec`, or `always`. `always` waits for the group-commit fsync before acknowledging a write; `everysec` and `no` ack from memory and fsync in the background. |
| `--maxmemory` | engine default | Resident memory budget, a redis-style size like `512mb` or `1gb`, mirroring `Options.ResidentBytes`. `0` uses the engine default. |
| `--cardinality` | engine default | kv-specific: expected distinct key count, mirroring `Options.KeyCapacity`. |
| `--value-bytes` | engine default | kv-specific: typical value size hint, used to size the hot tier. |
| `--version` | | Print the build version and exit. |

At least one of `--port` (non-zero) or `--unixsocket` must be set; a TCP port and a unix socket can bind at once, as they can in `redis-server`.
`--appendfsync` picks the [durability contract](/guides/durability/): `always` is synchronous group commit, where a write waits for the group-commit fsync before it returns for zero acked-write loss, and `everysec` (the default) is background group commit, a bounded sub-second loss window. `no` behaves like `everysec` here, since kv's background flusher runs either way. `--cardinality` and `--value-bytes` have no redis equivalent; they let a benchmark shape the tiers the way the in-process adapter does.

## Supported commands

The server implements the string commands a client and a benchmark use, plus the introspection commands a client issues at connect:

| Command | Purpose |
| --- | --- |
| `GET`, `SET`, `DEL` | Read, write, and delete a key. |
| `EXISTS` | Report whether a key is present. |
| `PING` | Liveness check. |
| `HELLO` | The RESP2/RESP3 handshake. |
| `CONFIG`, `COMMAND`, `INFO`, `DBSIZE`, `CLIENT`, `SELECT`, `FLUSHALL` | Introspection and session commands issued at connect. |
| `BGREWRITEAOF` | Forces a durability barrier now: the "make it durable now" hook. Waits for the flusher to fsync everything acked so far. |

It is the string keyspace only.
kv is an unordered point store, so there are no lists, hashes, sorted sets, expiry, or sorted iteration.

## Store file layout

The store lives under `--dir`:

| File | Role |
| --- | --- |
| `<dir>/dump.kv` | The main store file, where the hash-indexed log lives. The name comes from `--dbfilename`. |
| commit-watermark sibling | A small file next to the store recording how far the durable commit point has advanced. |

On the next start, an existing store is opened and its log is replayed forward, dropping any record left torn by a crash, so recovery is automatic and there is no repair step to run.

## Next

- The [server guide](/guides/server/) walks the flags and shows `redis-cli` and Go client usage.
- The [library reference](/reference/library/) covers the same engine as an in-process API.
