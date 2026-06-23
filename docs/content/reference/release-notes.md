---
title: "Release notes"
description: "What shipped in each kv release, and what the version numbers promise."
weight: 40
---

The authoritative, full changelog lives in [CHANGELOG.md](https://github.com/tamnd/kv/blob/main/CHANGELOG.md) in the repository. This page summarizes releases and explains the versioning.

## Versioning

kv follows semantic versioning. While the project is in its 0.x series, the API stays broadly stable as the surface settles toward a 1.0 commitment, and any breaking change is called out in the release notes. The on-disk format written by 0.1.0 is fixed and forward-compatible, so a database created now opens in later releases.

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
