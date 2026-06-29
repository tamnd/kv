---
title: "Release notes"
description: "What shipped in each kv release, and what the version numbers promise."
weight: 40
---

The authoritative, full changelog lives in [CHANGELOG.md](https://github.com/tamnd/kv/blob/main/CHANGELOG.md) in the repository. This page summarizes releases and explains the versioning.

## Versioning

kv follows semantic versioning. While the project is in its 0.x series, the API and on-disk format are still settling toward a 1.0 commitment, and any breaking change is called out in the release notes. 0.3.0 consolidated the storage engine and is a clean break: a database created by 0.1.0 or 0.2.0 does not open in 0.3.0 or later. From 0.3.0 on, the f2 on-disk format is the one the project carries forward.

## 0.3.0

A consolidation release, and a deliberate breaking change. kv had shipped two storage cores and let you pick one per database; benchmarking settled the question, so the project now has a single engine, f2, a sharded hash index over a self-durable log, and the rest was removed to keep the surface small and the code honest.

- **One engine.** The B-tree and LSM cores are gone, and so is the engine selector. Every database uses f2. `WithEngine`, `kv.BTree`, and `kv.LSM` are removed, along with the per-core tuning options (`WithMemtableSize`, `WithLevelRatio`, `WithFilter`, `WithRangeIndex`, `WithValueSeparation`, `WithCompression`, `WithColdCompression`, `WithFillFactor`, `WithMaxInlineValue`, `WithBtreeBuffers`).
- **Point lookups only.** f2 does not keep keys in order, so range scan and ordered iteration are gone: `Txn.NewIterator`, `IterOptions`, the `Iterator` type, and `Txn.DeleteRange` are removed, and the CLI loses `scan`, `count`, `dump`, and `export`. The operations are the point ones: get, set, exists, delete, and merge. A get is a hash and a read, and read latency stays flat as the database grows.
- **Same everything else.** Transactions with snapshot and serializable isolation, the tunable write-ahead log and crash recovery, encryption at rest, backup and replication, the server, and the observability surface all carry forward unchanged.
- **A new on-disk file.** f2 keeps its durable state in an `app.kv-f2` sidecar next to the main file and the WAL. A database created before 0.3.0 cannot be opened; move data across with the source you loaded from.

If you were running the default B-tree and using only point operations, the upgrade is recreating the database on 0.3.0 and reloading it. If you relied on scans or the LSM core, 0.2.0 remains available, and the pre-consolidation code is preserved in the `tamnd/kv-v1` repository.

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
