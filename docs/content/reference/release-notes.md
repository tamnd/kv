---
title: "Release notes"
description: "What shipped in each kv release, and what the version numbers promise."
weight: 40
---

The authoritative, full changelog lives in [CHANGELOG.md](https://github.com/tamnd/kv/blob/main/CHANGELOG.md) in the repository. This page summarizes releases and explains the versioning.

## Versioning

kv follows semantic versioning. While the project is in its 0.x series, the API stays broadly stable as the surface settles toward a 1.0 commitment, and any breaking change is called out in the release notes. The on-disk format written by 0.1.0 is fixed and forward-compatible, so a database created now opens in later releases.

## 0.2.0

A performance and ergonomics release. It is a drop-in upgrade: the on-disk format is unchanged, so an existing database opens as-is, and the API is purely additive, so existing code keeps compiling.

- **A faster hot path.** Reads, scans, and writes were tightened across the board, with most of the work in the engine's point-read and iteration paths and in the per-operation overhead the transaction layer adds. Cold-start and out-of-cache behavior also improved as the buffer pool spends fewer cycles per page touched.
- **Single-key reads.** `db.Get(key)` reads one key at the latest committed state without opening a transaction, returning a copy you own. It is the lightest way to read a lone key, for when a read does not need to agree with other reads. Use a `View` or `Snapshot` when several reads must see one consistent version. See [single-key reads](/reference/library/#single-key-reads).
- **Correctness fix.** The B-tree interior-node decoder now verifies its checksum trailer on the path it previously skipped, closing a gap where a corrupt interior page could go unnoticed instead of returning `ErrCorrupt`.

Everything from 0.1.0 carries forward unchanged.

## 0.1.0

The first public release. The library, CLI, and server are feature-complete and the on-disk format is fixed. It lands the full build from skeleton to hardening:

- **Two engines behind one API.** A read-optimized B-tree (the default) and a write-optimized LSM tree, chosen at creation and remembered in the file. Both expose identical transactions, iterators, CLI, and server.
- **ACID transactions.** Snapshot isolation by default with optional serializable isolation, automatic conflict retry on the closure form, MVCC throughout, and ordered iteration forward or reverse over ranges and prefixes.
- **Durability you can dial.** A checksummed write-ahead log with automatic crash recovery, five synchronous levels from `SyncOff` to `SyncExtra`, group commit, background checkpointing, and incremental vacuum.
- **Encryption at rest.** AES-256-GCM with a 32-byte master key, envelope-wrapped data keys, and lazy in-place key rotation.
- **Backup and replication.** Consistent online backup and restore, WAL shipping to read replicas, and point-in-time recovery from an archived log.
- **A server.** `kv serve` exposes a database over HTTP/JSON and a pure-Go binary protocol, with token and JWT/OIDC authentication, per-prefix authorization, TLS and mTLS, and rate and connection limits.
- **A full CLI.** Point operations, scans, CSV/TSV/JSONL import and export, a change feed, maintenance, inspection, and an interactive shell.
- **Observability.** Prometheus metrics, structured logging with a slow-op log, and tracing hooks.
- **Hardening.** Fuzzing of the file format, the operation stream against a model oracle, the WAL recovery path, and LSM compaction, plus a concurrency stress and soak campaign. The fuzzing found and fixed a B-tree decoder panic on malformed pages and a range-delete persistence bug, both before release.

kv is pure Go with zero external dependencies and builds against Go 1.23.

## Next

- The [library reference](/reference/library/) lists the API this release exposes.
- The [CLI reference](/reference/cli/) lists every command.
