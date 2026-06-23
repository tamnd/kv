---
title: "Configuration"
description: "Every open option and its default, every pragma the CLI exposes, and the files kv writes on disk."
weight: 30
---

This page collects the dials in one place: the Go options you pass to `Open`, the pragmas you read and set from the CLI, and what kv leaves on disk.

## Defaults at a glance

| Setting | Default |
| --- | --- |
| Engine | B-tree |
| Page size | 4096 bytes |
| Synchronous level | `SyncFull` |
| Isolation | Snapshot |
| Filter (LSM) | Bloom |
| Level ratio (LSM) | 10 |
| Memtable size (LSM) | 64 MiB |
| Auto-vacuum | off |
| Encryption | off |
| Server address | `:8480` |

## Open options

Options come in two kinds. Create-time options are baked into the file when it is created and cannot change afterward; open-time options are applied fresh on every `Open` and can differ between runs.

### Create-time

These are recorded in the file header and fixed for its lifetime.

| Option | Default | Effect |
| --- | --- | --- |
| `WithEngine(EngineKind)` | `BTree` | B-tree (read-optimized) or LSM (write-optimized). |
| `WithPageSize(int)` | 4096 | Page size in bytes for the main file. |
| `WithEncryptionKey([]byte)` | none | 32-byte AES-256-GCM master key; turns on encryption at rest. |
| `WithMergeOperator(name, fn)` | none | The associative merge operator; must be re-supplied on every open. |

### Open-time

These apply per run and may differ each time.

| Option | Default | Effect |
| --- | --- | --- |
| `WithCacheSize(int)` | engine default | Buffer-pool capacity in bytes. |
| `WithSynchronous(Sync)` | `SyncFull` | WAL sync level (see below). |
| `WithAutoCheckpoint(int)` | engine default | WAL frame backlog before a background checkpoint; negative disables. |
| `WithMaxRetries(int)` | small default | Bound on automatic `Update` conflict retries. |
| `WithIsolation(Isolation)` | `SnapshotIsolation` | `SnapshotIsolation` or `Serializable`. |
| `WithReadReplica()` | off | Open read-only; only `ApplyWAL` advances state. |
| `WithWALArchive(sink)` | none | Sink for each WAL generation before checkpoint. |
| `WithLogger(*slog.Logger)` | none | Structured operational logging. |
| `WithSlowOpThreshold(time.Duration)` | off | WARN log for ops at or above the threshold; needs `WithLogger`. |
| `WithTracer(Tracer)` | none | Tracing hooks. |
| `WithFillFactor(float64)` | ~0.7 | B-tree target leaf occupancy, in (0, 1]. |
| `WithMaxInlineValue(int)` | engine default | B-tree inline value cap; larger values overflow to dedicated pages. |
| `WithBtreeBuffers(bool)` | off | B-tree Bε buffered write path. |
| `WithMemtableSize(int)` | 64 MiB | LSM memtable flush threshold. |
| `WithLevelRatio(int)` | 10 | LSM size multiplier between levels. |
| `WithFilter(FilterKind)` | `FilterBloom` | LSM per-segment filter: Bloom or Ribbon. |
| `WithRangeIndex(bool)` | off | LSM REMIX scan index. |
| `WithValueSeparation(int)` | off | LSM WiscKey value-log threshold in bytes. |
| `WithCompression(bool)` | off | LSM heat-tiered block compression. |
| `WithColdCompression(bool)` | off | LSM cold-levels-only compression; overrides `WithCompression`. |

## Synchronous levels

The `synchronous` setting is the durability-versus-speed dial, set with `WithSynchronous` or the `synchronous` pragma:

| Level | Pragma value | Guarantee |
| --- | --- | --- |
| `SyncOff` | `off` | No fsync; the OS flushes on its own schedule. |
| `SyncNormal` | `normal` | fdatasync at checkpoint and periodically. |
| `SyncBarrier` | `barrier` | A write-ordering barrier on every commit. |
| `SyncFull` | `full` | fdatasync on every commit (the default). |
| `SyncExtra` | `extra` | `SyncFull` plus a directory sync on file growth. |

The [durability guide](/guides/durability/) explains what each one loses on a power failure.

## Pragmas

Pragmas read and set knobs on a file from the CLI or the interactive shell:

```bash
kv pragma app.kv synchronous          # read
kv pragma app.kv synchronous=normal   # set
kv pragma app.kv help                 # list all
```

### Create-time (read-only)

| Pragma | Meaning |
| --- | --- |
| `engine` | Storage core, `btree` or `lsm`. |
| `page_size` | Page size in bytes. |

### Settable

| Pragma | Values | Meaning |
| --- | --- | --- |
| `application_id` | uint32 | Application-defined file tag. |
| `user_version` | uint32 | Application-defined version counter. |
| `synchronous` | `off`/`normal`/`barrier`/`full`/`extra` | WAL sync level. |
| `wal_autocheckpoint` | frames | WAL backlog before auto-checkpoint; 0 disables. |
| `full_page_writes` | `on`/`off` | Log page pre-images before checkpoint (torn-write protection). |
| `auto_vacuum` | `none`/`incremental`/`full` | Automatic space reclamation after checkpoint. |
| `commit_linger_us` | microseconds | Group-commit leader wait window; 0 disables. |
| `incremental_vacuum` | pages | Return up to N trailing free pages now; 0 or empty means all. |

### Read-only counters

| Pragma | Meaning |
| --- | --- |
| `page_count` | File size in pages. |
| `freelist_count` | Pages on the freelist. |
| `physical_bytes` | On-disk footprint in bytes. |
| `live_keys`, `live_bytes` | Live key count and data bytes (zero if not tracked). |
| `amplification` | Space amplification, physical over live. |
| `commit_version` | Latest committed version. |
| `wal_frames`, `wal_backlog` | Frames written, and frames committed but not yet checkpointed. |
| `syncs` | Fsyncs since open. |
| `cache_size` | Buffer-pool capacity in frames. |
| `wal_checkpoint` | Checkpoint the WAL; the value selects the mode. |

## On-disk layout

A kv database is a single main file plus a sidecar log while it is open:

| File | Role |
| --- | --- |
| `app.kv` | The database: header, pages, and engine data. |
| `app.kv-wal` | The write-ahead log, holding committed frames not yet folded into the main file. |

The `.kv` extension is a convention, not a requirement; the file is identified by its header, so any path works. The `-wal` sidecar appears next to the main file while the database is open and is consumed by checkpointing. After a clean close with the WAL checkpointed, the main file stands alone. On the next open, an existing `-wal` is replayed automatically as part of crash recovery.
