---
title: "Storage engine"
description: "The storage core behind kv: a sharded hash index over a self-durable hybrid log, why it makes point lookups flat, and what it does not do."
weight: 20
---

kv has one storage engine, and you never choose it. This guide explains how it works, what it is good at, and the one thing it deliberately does not do, so you can tell whether kv fits your workload before you build on it.

## The shape

The engine keeps every key in a hash index that maps a key to the location of its newest record in a log. A get hashes the key, finds the slot, and reads one record. There is no tree to descend, so a lookup costs about the same whether the database holds a thousand keys or a billion: read latency stays flat as the database grows.

The index is split into many shards, each guarded independently, so concurrent readers and writers touch different shards and rarely contend. Most lookups take no lock at all. That is what lets reads scale across cores instead of serializing on one structure.

Records live in a **hybrid log**: a single append-only log with a hot region held in memory and a cold tail on disk. New writes append to the in-memory region, where a key written again can be updated in place while it is still hot. As the log grows, older records age out to disk. The working set you touch most stays in memory; the long cold tail costs a disk read only when you reach for it.

## What it is good at

The engine rewards workloads that are mostly keyed reads and writes:

- Session and token stores, where every access is by id.
- Durable caches, where you want a fast point read but also crash recovery.
- Per-entity records, counters, and feature flags addressed by an exact key.
- Queues and work logs addressed by a generated id.

For these, the hash index gives you a read path that does not degrade with size and a write path that appends rather than rewrites.

## What it does not do

The engine does not keep keys in sorted order, so kv has **no range scan and no ordered iteration**. There is no cursor, no prefix walk, and no reverse scan. The operations are all point operations: `Get`, `Set`, `Delete`, `Exists`, and `Merge`, each addressing one key.

If your workload needs to enumerate keys in order, range over a prefix, or walk a time series, model that in your own keys (keep an explicit index value you read and update), or use an ordered store instead. Choosing kv is choosing to trade ordered iteration for a flat, contention-light point-lookup path.

## Durability and compaction

The log is self-durable: every committed write is on the log before the commit returns, and a [write-ahead log](/guides/durability/) records the same commits so a crash recovers to the last committed state. Recovery replays the log forward and drops any record torn by a crash, so a half-written tail heals itself rather than corrupting the database.

Because writes append, superseded versions of a key pile up in the cold log over time. Compaction runs in the background to fold the live records forward and reclaim the space the dead ones hold. `Stats` reports `CompactionScore`, the urgency of the most-pending compaction (1.0 at its trigger, 0 when idle), and `Amplification`, physical bytes over live bytes, which is how much space the dead tail is currently costing you. The [CLI](/reference/cli/) surfaces the same numbers through `kv stats` and `kv metrics`.

## Tuning

The engine needs little tuning. Two options matter most:

| Option | Effect |
| --- | --- |
| `WithPageSize(bytes)` | The unit the file grows and is read in. The default suits general use; raise it when values are large so fewer pages back each record. It is fixed when the file is created and recorded in it. |
| `WithCacheSize(bytes)` | How much memory the buffer pool holds for the on-disk tail of the log. A larger cache keeps more of the cold region resident, so reads that miss the hot region still avoid a disk seek. |

## Next

- [Durability](/guides/durability/) covers the write-ahead log the engine commits through.
- The [configuration reference](/reference/configuration/) lists every option with its default.
