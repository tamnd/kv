---
title: "Configuration"
description: "Every Options field with its default and effect, and the files kv writes on disk."
weight: 30
---

This page collects the dials in one place: the `Options` fields you pass to `Open`, and what kv leaves on disk.

## Defaults at a glance

| Setting | Default |
| --- | --- |
| `KeyCapacity` | `1 << 20` (~1M keys) |
| `HotBytes` | 8 MiB |
| `HotKeys` | heuristic from `HotBytes` |
| `ResidentBytes` | 64 MiB |
| `ReadCacheCells` | `1 << 16` |
| `SyncWrites` | `false` (background group commit) |

The zero value `Options{}` is valid: leave a field at its zero value and it falls back to the default above.

## Options

| Field | Default | Range | Effect |
| --- | --- | --- | --- |
| `KeyCapacity` | `1 << 20` | any positive count | Expected distinct key count. Sizes the resident key index, which holds an entry per key while values spill to disk, so it is the memory floor and the main knob for a large store. |
| `HotBytes` | 8 MiB | any positive size | Size of one in-memory hot segment where writes land. Bounds the resident write buffer (at most two segments live) and, under the default durability, the crash-loss window. |
| `HotKeys` | heuristic | zero or a positive count | Records one hot segment's index is sized for. Set it when the value size is known. Too small only causes an earlier seal, never a lost write; zero derives it from `HotBytes`. |
| `ResidentBytes` | 64 MiB | any positive size | Cold log's resident tail window: how much recently-migrated cold data stays in RAM for fast reads. Larger keeps more resident, smaller holds a bigger database in less RAM. |
| `ReadCacheCells` | `1 << 16` | rounded up to a power of two | Cells in the read cache over cold reads. Raise it when repeated reads hit a set of cold keys larger than the resident window. |
| `SyncWrites` | `false` | `true` or `false` | Durability contract. `false` is background group commit, a bounded sub-second loss window; `true` is synchronous group commit, where a write waits for the group-commit fsync before it returns, for zero acked-commit loss. See [durability](/guides/durability/). |

The [sizing guide](/guides/sizing/) walks the memory knobs in the order they matter, and the [durability guide](/guides/durability/) covers `SyncWrites`.

## On-disk layout

A kv store is one main file plus a small sibling watermark file:

| File | Role |
| --- | --- |
| `app.kv` (or `<dir>/dump.kv` under the server) | The main store file: the hash-indexed log where keys and values live. |
| commit-watermark sibling | A small file next to the main file recording how far the durable commit point has advanced, so recovery knows where the last durable state ends. |

The `.kv` extension is a convention, not a requirement; any path works.
On the next open, the store's log is replayed forward as part of recovery, dropping any record left torn by a crash, so the store comes back at its last durable state with no repair step to run.

## Next

- The [library reference](/reference/library/) gives the exact method signatures.
- The [server reference](/reference/server/) covers the Redis-protocol server and its flags.
