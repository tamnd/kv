---
title: "Running the server"
description: "Serving a kv database over HTTP/JSON and a binary protocol, with authentication, TLS and mTLS, rate and connection limits, and a change feed."
weight: 60
---

kv is an embedded database first, but it can also serve a single file over the network so other processes, on the same box or across a fleet, can reach it. This guide covers starting the server, locking it down, and what it exposes.

## Starting it

`kv serve` opens a database and serves it:

```bash
kv serve app.kv
# listening on :8480
```

The default address is `:8480`. The server speaks HTTP with JSON bodies: unary requests for the point operations, and server-sent events for the change feed. Every operation the library offers is available over the wire, so a remote client gets the same get, set, transaction, batch, and watch surface as an in-process one.

Alongside HTTP, the server can speak a compact binary protocol, which is opt-in because it listens on a second address:

```bash
kv serve app.kv --addr :8480 --binary-addr :8481
```

The binary protocol is pure Go with no external dependencies and adds interactive transactions, a session that interleaves reads and writes across several round trips, which the stateless HTTP path does not. Use HTTP for reach and tooling, the binary protocol for throughput and interactive sessions.

## The Redis face

kv can also speak the Redis wire protocol, so an existing Redis client, library, or benchmark can drive it without code changes. It is opt-in, on its own address or a unix socket:

```bash
kv serve app.kv --addr "" --resp-addr :6380
redis-cli -p 6380 set greeting hello
redis-cli -p 6380 get greeting
```

Setting `--addr ""` turns the HTTP face off, for a server meant to speak only Redis; leave it set to run both at once over one database. The Redis face is a front end over the same writer as HTTP and the binary protocol, so the three share one keyspace: a `SET` over Redis is readable as a `get` over HTTP. Each write is a full kv transaction, so the Redis face inherits kv's durability and MVCC rather than adding a second storage model.

It implements the string commands a client and a benchmark use: `GET`, `SET`, `DEL`, `EXISTS`, `PING`, the `HELLO` handshake (RESP2 and RESP3), `DBSIZE`, and the introspection commands a client issues at connect. It is the string keyspace only, with no sorted iteration, lists, hashes, or expiry. `--synchronous` overrides the WAL durability for the run, which a benchmark uses to compare write paths with the per-commit fsync removed:

```bash
kv serve app.kv --addr "" --resp-unixsocket /tmp/kv.sock --synchronous off
```

The wire loop is adapted from the minimal RESP front end in [tamnd/aki](https://github.com/tamnd/aki), reworked over kv's transactional API.

## Securing it

The server defaults to safe: it refuses to serve a non-loopback address in plaintext, so you cannot accidentally expose an unencrypted, unauthenticated database to the network. To serve off-host you either turn on TLS or, for a deliberately open setup on a trusted network, pass `--insecure`.

### Transport security

Turn on TLS with a certificate and key, and require client certificates (mTLS) by adding a client CA:

```bash
kv serve app.kv --addr :8480 \
  --tls-cert server.pem --tls-key server-key.pem \
  --tls-client-ca clients-ca.pem \
  --mtls-identity-file identities.txt
```

With `--tls-client-ca` set, the server verifies client certificates, and `--mtls-identity-file` maps each certificate's common name to an identity with its own per-prefix grants.

### Authentication

Two authentication modes are available, and they are mutually exclusive. A static token file maps tokens to identities and per-prefix grants:

```bash
kv serve app.kv --auth-file tokens.txt
```

Or verify JWT bearer tokens, validated against a shared secret, a public key, or an OIDC JWKS endpoint, with optional issuer and audience checks:

```bash
kv serve app.kv --jwt-jwks-url https://issuer.example/.well-known/jwks.json \
  --jwt-issuer https://issuer.example --jwt-audience kv-api
```

Either way, authorization is per-prefix: an identity is granted read or write on key prefixes, so you can hand one caller read-only access to `metrics:` and another full access to `tenant-7:` from the same database.

### Limits

The server can bound both the size of what it accepts and the load it takes:

| Flag | Bounds |
| --- | --- |
| `--max-key-size`, `--max-value-size` | The largest key and value it will accept. |
| `--max-batch-ops` | Operations in one batch or transaction. |
| `--max-conns` | Open connections per listener. |
| `--max-in-flight` | Concurrent in-progress requests. |
| `--rate`, `--rate-burst` | Per-caller request rate and burst. |

These default to off (unbounded). Set the ones that matter for a server facing untrusted callers; leave them open for a trusted internal one.

## The change feed

`watch` streams committed changes as they happen, which is how a downstream process tails the database without polling:

```bash
kv watch app.kv --prefix orders:
```

Over HTTP the same feed arrives as server-sent events. A subscriber that falls too far behind is dropped with `ErrSubscriberLagged` rather than being allowed to stall writers, so a slow consumer never backs up the database; reconnect and resume.

## Graceful shutdown

On `SIGINT` or `SIGTERM` the server stops accepting new work and drains what is in flight. Add `--checkpoint-on-shutdown` to fold the WAL into the main file on the way out, which leaves the file fully checkpointed and makes the next open fast:

```bash
kv serve app.kv --checkpoint-on-shutdown
```

## Embedding the server

If you need more control than the flags give, the `server` package exposes the same machinery to a Go program. `server.New(db, opts)` builds a server over an already-open database, and `Serve`, `ServeBinary`, and `Shutdown` drive its lifecycle, with `server.Options` carrying the same limits, auth, and TLS configuration the flags set. This is the path to take when the process also needs to hold an encryption key or run other work alongside the listener.

## Next

- The [CLI reference](/reference/cli/) documents `serve` and every other command in full.
- [Backup and replication](/guides/backup-and-replication/) covers keeping a remote replica current.
