---
title: "Running the server"
description: "Serving one kv store over the Redis wire protocol: the flags, unix socket versus TCP, the supported commands, and driving it with redis-cli and Redis client libraries."
weight: 60
---

kv is an embedded database first, but the `kv` binary can also serve one store over the network so other processes can reach it.
It speaks the Redis wire protocol (RESP2 and RESP3), so any Redis client, library, or `redis-cli` can drive it without special code.
It is the network face of the same engine, not a second storage model.

## Starting it

The flags follow `redis-server`, so bare `kv` starts on `127.0.0.1:6379` with the store at `./dump.kv`:

```bash
kv --port 6379 --dir ./data
```

`--dir` is the data directory and `--dbfilename` is the store file name within it, so the store lives at `<dir>/<dbfilename>`, `./data/dump.kv` here.
The server opens that store on start and serves it over the port you gave.

### Unix socket versus TCP

`--port` sets the TCP port and `--bind` the address it listens on, `127.0.0.1` by default.
`--unixsocket` is a path to a unix domain socket, the faster local path when the client is on the same box.
Unlike a redis where you often pick one, kv binds both when you set both, so you can offer a local socket and a TCP port at once.
Set `--port 0` to turn the TCP listener off and serve the socket alone.

```bash
# a TCP port and a local socket at the same time
kv --port 6379 --unixsocket /tmp/kv.sock --dir ./data

# local-only, over a unix socket
kv --port 0 --unixsocket /tmp/kv.sock --dir ./data
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
| `BGREWRITEAOF` | The "make it durable now" hook: forces a durability barrier, waiting for the flusher to fsync everything acked so far. |

It is the string keyspace only.
There are no lists, hashes, sorted sets, expiry, or sorted iteration, because kv is an unordered point store.

## Durability

`--appendfsync` maps to the [durability modes](/guides/durability/), the same three values redis uses:

| Value | Contract |
| --- | --- |
| `everysec` (default) | Background group commit, the fast path, a bounded sub-second loss window. |
| `always` | Synchronous group commit: a write waits for the group-commit fsync before it is acknowledged, for zero acked-write loss. |
| `no` | Behaves like `everysec` here: the write acks from memory and the background flusher fsyncs it. |

`--appendfsync always` is the zero-loss contract; `everysec` is the fast default.
`BGREWRITEAOF` forces a durability barrier on demand under either.

`--maxmemory` sets the resident memory budget, a redis-style size like `512mb`. The kv-specific `--cardinality` (expected distinct key count, `KeyCapacity`) and `--value-bytes` (typical value size) shape the tiers for a large store the way you would set the `Options` fields.

## Driving it

Any Redis client works. From the shell with `redis-cli`:

```bash
kv --port 6379 --dir ./data &
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
- The [durability guide](/guides/durability/) explains what each `--appendfsync` value guarantees.
