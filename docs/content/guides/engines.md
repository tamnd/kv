---
title: "Storage engine"
description: "The storage core behind kv: a sharded hash index over a self-durable hybrid log, why it makes point lookups fast and flat, how it runs larger than memory, and what it does not do."
weight: 20
---

kv has one storage engine, and you never choose it. This guide explains how it works, why it is fast, how it runs over a dataset larger than memory, and the one thing it deliberately does not do, so you can tell whether kv fits your workload before you build on it.

## The shape

The engine keeps every key in a hash index that maps a key to the location of its newest record in a log. A get hashes the key, finds the slot, and reads one record. There is no tree to descend, so a lookup costs about the same whether the database holds a thousand keys or a billion: read latency stays flat as the database grows.

The index is split into 256 shards by default, each guarded independently, so concurrent readers and writers touch different shards and rarely contend. Most lookups take no lock at all: a reader probes the index with plain atomic loads, and only a writer on the same shard takes a lock. That is what lets reads scale across cores instead of serializing on one structure.

A slot in the index is one 64-bit word, not a copy of the key. It packs a short fingerprint of the key with the offset of the key's newest record in the log. The fingerprint rejects keys that do not match without touching the log, and the offset points straight at the record the read returns. Because the slot never holds the key bytes, the index costs about the same per key no matter how long the keys are, around 10 to 13 bytes each, which is what puts a billion keys in roughly 15 GiB of index and keeps that index small enough to stay resident while the data does not have to.

Records live in a **hybrid log**: a single append-only log with a hot region held in memory and a cold tail on disk. New writes append to the in-memory region, where a key written again can be updated in place while it is still hot. As the log grows, older records age out to disk. The working set you touch most stays in memory; the long cold tail costs a disk read only when you reach for it.

## Performance

The point of the shape above is a read path with almost nothing in it. A get is a hash, one index probe, and one record read, and on the common path it takes no lock and allocates nothing. In memory on an Apple M4, a random read across a million keys is about 60 nanoseconds with zero allocations, and a repeated hot key is a few nanoseconds; the B-tree core kv shipped before took several microseconds for the same million-key read, because a tree descent gets deeper as the database grows while a hash probe does not.

Writes are cheap for the same reason the index is small: a write appends one record to its shard's log and repoints one index slot, rather than rewriting a tree page. Under concurrency the shards spread writers apart, and at the durable levels group commit batches the fsyncs of everyone committing at once into one device flush, so write throughput holds up instead of collapsing to one fsync per write.

The cross-engine comparison against other embedded stores lives in [kvbench](https://github.com/tamnd/kvbench), which runs kv next to pebble, badger, bbolt and the Rust-rail engines under the same harness. The short version is that the flat, lock-light point path wins the keyed read and write workloads by a wide margin in the in-memory regime; the ordered stores win the scans kv does not do at all.

## Larger than memory

A kv database does not have to fit in RAM. The hybrid log keeps a bounded resident working set and leaves the rest in the file, so the file can be many times the size of memory you give it.

`WithCacheSize` sets that bound: it is how much memory the engine holds resident for the log. The keys and records you touch most stay in that resident set; everything colder lives in the file. A read that misses the resident set faults the page it needs in from the file by offset, with no extra copy, because an evicted page is simply a page already written to disk. So the cost you pay for going larger than memory is one disk read on a cold key, and nothing on a hot one, and the index that finds either stays resident the whole time because it is small.

This is what makes kv usable as a durable store for a dataset that does not fit in memory, not only as an in-memory cache: size the cache to your working set, let the file hold the long tail, and reads stay flat while writes keep appending.

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
| `WithCacheSize(bytes)` | The bound on the engine's resident memory, the dial behind [larger than memory](#larger-than-memory). Size it to your working set: a larger cache keeps more of the log resident so fewer reads touch the disk, a smaller one holds a bigger database in less RAM at the cost of more cold-key disk reads. |

## Next

- [Durability](/guides/durability/) covers the write-ahead log the engine commits through.
- The [configuration reference](/reference/configuration/) lists every option with its default.
