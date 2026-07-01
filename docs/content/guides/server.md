---
title: "Running the server"
description: "Serving one kv store over the Redis wire protocol: the flags, unix socket versus TCP, the supported commands, and driving it with redis-cli and Redis client libraries."
weight: 60
---

kv is an embedded database first, but the `kv` binary can also serve one store over the network so other processes can reach it.
It speaks the Redis wire protocol (RESP2 and RESP3), so any Redis client, library, or `redis-cli` can drive it without special code.
It is the network face of the same engine, not a second storage model.

## Starting it

The server serves one store, and one of `--addr` or `--unixsocket` is required:

```bash
kv --addr :6379 --dir ./data
```

`--dir` is the data directory, and the store lives at `<dir>/kv.db` inside it (default `.`).
The server opens that store on start and serves it over the address you gave.

### Unix socket versus TCP

`--addr` is a TCP listen address, for example `:6379`.
`--unixsocket` is a path to a unix domain socket, which is the faster local path when the client is on the same box.
If you set both, the unix socket wins.
Set `--addr` alone for a server other machines reach over TCP, `--unixsocket` alone for a local-only server, or both to offer each.

```bash
# local-only, over a unix socket
kv --unixsocket /tmp/kv.sock --dir ./data
```

## Supported commands

The server implements the string commands a client and a benchmark use, plus the introspection commands a client issues at connect:

| Command | Purpose |
| --- | --- |
| `GET`, `SET`, `DEL` | The point operations: read, write, delete a key. |
| `EXISTS` | Report whether a key is present. |
| `PING` | Liveness check. |
| `HELLO` | The RESP2/RESP3 handshake. |
| `CONFIG`, `COMMAND`, `INFO`, `DBSIZE`, `CLIENT`, `SELECT`, `FLUSHALL` | Introspection and session commands clients issue at connect. |
| `BGREWRITEAOF` | The "make it durable now" hook: forces a `Sync` in a synced mode. |

It is the string keyspace only.
There are no lists, hashes, sorted sets, expiry, or sorted iteration, because kv is an unordered point store.

## Durability

`--synchronous` maps to the [durability modes](/guides/durability/):

| Value | Contract |
| --- | --- |
| `off` | Background group commit, the fast default, a bounded sub-second loss window. |
| `normal`, `full`, `default` | The synced contract: every commit is fsynced before it is acknowledged. |

Any value other than `off` keeps the synced contract, so an acked write survives a crash with zero loss.
`BGREWRITEAOF` forces a `Sync` on demand in a synced mode.

The workload sizing hints mirror the library `Options`: `--cardinality` is the expected distinct key count (`KeyCapacity`), `--value-bytes` is the typical value size, and `--cache-bytes` sizes the resident read window.
Set them for a large store the same way you would set the `Options` fields.

## Driving it

Any Redis client works. From the shell with `redis-cli`:

```bash
kv --addr :6379 --dir ./data &
redis-cli -p 6379 set user:1 alice
redis-cli -p 6379 get user:1
redis-cli -p 6379 exists user:1
redis-cli -p 6379 del user:1
```

From a Go program with a Redis client library, point it at the same address:

```go
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
rdb.Set(ctx, "user:1", "alice", 0)
v, err := rdb.Get(ctx, "user:1").Result()
```

Over a unix socket, give the client the socket path instead of a host and port.

## Next

- The [server reference](/reference/server/) documents every flag and the store file layout.
- The [durability guide](/guides/durability/) explains what each `--synchronous` value guarantees.
