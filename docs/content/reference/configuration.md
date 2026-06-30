---
title: "Configuration"
description: "Every open option and its default, every pragma the CLI exposes, and the files kv writes on disk."
weight: 30
---

This page collects the dials in one place: the Go options you pass to `Open`, the pragmas you read and set from the CLI, and what kv leaves on disk.

## Defaults at a glance

| Setting | Default |
| --- | --- |
| Engine | f2 |
| Page size | 4096 bytes |
| Synchronous level | `SyncNormal` |
| Isolation | Snapshot |
| Auto-vacuum | off |
| Encryption | off |
| Server address | `:8480` |

## Open options

Options come in two kinds. Create-time options are baked into the file when it is created and cannot change afterward; open-time options are applied fresh on every `Open` and can differ between runs.

### Create-time

These are recorded in the file header and fixed for its lifetime.

| Option | Default | Effect |
| --- | --- | --- |
| `WithPageSize(int)` | 4096 | Page size in bytes for the main file. |
| `WithEncryptionKey([]byte)` | none | 32-byte AES-256-GCM master key; turns on encryption at rest. |
| `WithMergeOperator(name, fn)` | none | The associative merge operator; must be re-supplied on every open. |

### Open-time

These apply per run and may differ each time.

| Option | Default | Effect |
| --- | --- | --- |
| `WithCacheSize(int)` | engine default | The bound on the engine's resident memory in bytes, the dial behind running [larger than memory](/guides/engines/#larger-than-memory). Size it to your working set. |
| `WithSynchronous(Sync)` | `SyncNormal` | WAL sync level (see below). |
| `WithAutoCheckpoint(int)` | engine default | WAL frame backlog before a background checkpoint; negative disables. |
| `WithMaxRetries(int)` | small default | Bound on automatic `Update` conflict retries. |
| `WithIsolation(Isolation)` | `SnapshotIsolation` | `SnapshotIsolation` or `Serializable`. |
| `WithReadReplica()` | off | Open read-only; only `ApplyWAL` advances state. |
| `WithWALArchive(sink)` | none | Sink for each WAL generation before checkpoint. |
| `WithLogger(*slog.Logger)` | none | Structured operational logging. |
| `WithSlowOpThreshold(time.Duration)` | off | WARN log for ops at or above the threshold; needs `WithLogger`. |
| `WithTracer(Tracer)` | none | Tracing hooks. |

## Synchronous levels

The `synchronous` setting is the durability-versus-speed dial, set with `WithSynchronous` or the `synchronous` pragma:

| Level | Pragma value | Guarantee |
| --- | --- | --- |
| `SyncOff` | `off` | No fsync; the OS flushes on its own schedule. |
| `SyncNormal` | `normal` | fdatasync at checkpoint and periodically (the default). |
| `SyncBarrier` | `barrier` | A write-ordering barrier on every commit. |
| `SyncFull` | `full` | fdatasync on every commit; no acked commit is ever lost. |
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
| `engine` | Storage core; always `f2`. |
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
| `cache_size` | Resident memory bound, in frames. The larger-than-memory dial; see the [storage engine guide](/guides/engines/#larger-than-memory). |
| `wal_checkpoint` | Checkpoint the WAL; the value selects the mode. |

## On-disk layout

A kv database is a single main file plus a sidecar log while it is open:

| File | Role |
| --- | --- |
| `app.kv` | The main file: the header that records the engine and page size, plus the database metadata pages. |
| `app.kv-f2` | The f2 core's self-durable data file, where the hash-indexed log lives. |
| `app.kv-wal` | The write-ahead log, holding committed frames past f2's durable point not yet folded in. |

The `.kv` extension is a convention, not a requirement; the file is identified by its header, so any path works. The `-f2` and `-wal` sidecars appear next to the main file while the database is open. The `-f2` file holds the engine's durable state; the `-wal` is consumed by checkpointing. On the next open, an existing `-wal` is replayed automatically as part of crash recovery, and the f2 core replays its own log forward, dropping any record left torn by a crash.
