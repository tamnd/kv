---
title: "Storage engine"
description: "The storage core behind kv: a sharded hash index over a hybrid hot-tier and cold-log core, why it makes point lookups fast and flat, how it runs larger than memory, and what it does not do."
weight: 20
---

kv has one storage engine, and it is not swappable.
This guide explains how it works, why it is fast, how it runs over a dataset larger than memory, and the one thing it deliberately does not do, so you can tell whether kv fits your workload before you build on it.

## The shape

The engine keeps a resident hash index that maps a key to the location of its newest record.
A get hashes the key, finds the slot, and reads one record.
There is no tree to descend, so a lookup costs about the same whether the database holds a thousand keys or a billion: read latency stays flat as the database grows past memory.

The index is sharded, and each shard is guarded independently, so concurrent readers and writers touch different shards and rarely contend.
Most lookups take no lock at all: a reader probes the index with plain atomic loads, and only a writer on the same shard takes a lock.
That is what lets reads scale across cores instead of serializing on one structure.

A slot in the index holds a short fingerprint of the key and the offset of the key's newest record, not the key bytes themselves.
The fingerprint rejects keys that do not match without touching the log, and the offset points straight at the record the read returns.
Because the slot never holds the key bytes, the index costs about the same per key no matter how long the keys are, which is what keeps it small enough to stay resident while the values do not have to.

Records live in a single append-only log with a hot region held in memory and a cold tail on disk.
New writes append to the in-memory hot tier, where a key written again can be updated straight in memory while it is still hot.
As the log grows, older records migrate out to the file.
The working set you touch most stays in memory; the long cold tail costs a disk read only when you reach for it.

## Performance

The point of the shape above is a read path with almost nothing in it.
A get is a hash, one index probe, and one record read, and on the common path it takes no lock and allocates nothing, because `Get` decodes into the scratch buffer you pass in.
On an Apple M4, a random read across a million cache-resident keys runs in the millions of ops per second and allocates nothing on the hot path, several times faster than the LSM engines (badger, pebble, goleveldb) and the B-tree engine (bbolt) in the same harness.

Update-heavy mixes lead the field too.
A read-update mix (YCSB-A) and a read-modify-write mix (YCSB-F) both win, because a hot key takes its update straight into memory and the resident index is repointed rather than a tree page being rewritten.

The honest limits are worth stating.
Per-commit durable write throughput, with `SyncWrites` true so every commit is fsynced, is mid-pack: kv fsyncs per commit and does not batch many commits into one flush the way a group-committing LSM (badger) or sqlite does, so a group-committer beats it on that one workload.
And kv is a store for working sets that fit in memory: on out-of-cache uniform random reads where nothing is resident, it does not win, and an LSM with a block cache can do better there.
Frame kv for read-heavy and update-heavy workloads whose working set fits in memory.

The cross-engine comparison lives in [kvbench](https://github.com/tamnd/kvbench), which runs kv next to pebble, badger, bbolt, goleveldb, and others under one harness.
The measured board and the methodology live there rather than here, so the numbers do not drift out of sync with a copy.

## Larger than memory

A kv database does not have to fit in RAM.
The engine keeps a bounded resident set, the hot tier where writes land plus a window of recently-migrated cold data, and leaves the rest in the file, so the file can be many times the size of memory you give it.

`Options.ResidentBytes` sets how much recently-migrated cold data stays resident for fast reads, and `Options.HotBytes` sizes the hot tier.
The keys and records you touch most stay resident; everything colder lives in the file.
A read that misses the resident set faults the record it needs in from the file by offset.
So the cost of going larger than memory is one disk read on a cold key and nothing on a hot one, and the index that finds either stays resident the whole time because it holds fingerprints and offsets, not values.

This is what makes kv usable as a durable store for a dataset that does not fit in memory, not only as an in-memory cache: size the resident windows to your working set, let the file hold the long tail, and point reads stay flat while writes keep appending.
The [sizing guide](/guides/sizing/) covers the knobs.

## What it is good at

The engine rewards workloads that are mostly keyed reads and writes:

- Session and token stores, where every access is by id.
- Durable caches, where you want a fast point read but also crash recovery.
- Per-entity records, counters, and feature flags addressed by an exact key.
- Queues and work logs addressed by a generated id.

For these, the resident hash index gives you a read path that does not degrade with size and a write path that appends rather than rewrites.

## What it does not do

The engine does not keep keys in sorted order, so kv has no range scan and no ordered iteration.
There is no cursor, no prefix walk, and no reverse scan.
The operations are all point operations: `Set`, `Get`, and `Delete`, each addressing one key.

If your workload needs to enumerate keys in order, range over a prefix, or walk a time series, model that in your own keys, or use an ordered store instead.
Choosing kv is choosing to trade ordered iteration for a flat, contention-light point-lookup path.

## Durability and the file

Every write goes through the log, and a small sibling commit-watermark file records how far the durable point has advanced, so a crash recovers to the last durable state.
Recovery replays the log forward and drops any record left torn by a crash, so a half-written tail heals itself rather than corrupting the database.
How much a crash can lose depends on the [durability mode](/guides/durability/) you chose.

## Next

- [Durability](/guides/durability/) covers the two modes the engine commits through.
- [Sizing a store](/guides/sizing/) covers the `Options` that set the resident footprint.
- The [configuration reference](/reference/configuration/) lists every option with its default.
