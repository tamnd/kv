---
title: "Choosing an engine"
description: "The B-tree and LSM storage cores: how each one trades read cost against write cost, when to pick which, and the tuning knobs each exposes."
weight: 20
---

kv ships two storage engines behind one API. The choice is made once, when a database file is created, and recorded in its header so reopening is automatic. This guide explains the trade-off and how to tune each side.

## The trade-off

Every ordered storage engine answers the same question, "where do I put a new write," and the answer drives everything else.

A **B-tree** keeps keys in a balanced tree of pages and updates them in place. A write finds the leaf for its key and modifies it. Reads are cheap: a point lookup touches one page per level, a handful of pages total, and the data is never duplicated, so the file stays compact. The cost is on the write side: a random write may have to read, modify, and write back a page, and a steady stream of random writes scatters that work across the file.

An **LSM tree** (log-structured merge tree) never updates in place. A write goes into an in-memory table and, when that fills, is flushed as a new sorted segment on disk. Writes are fast because they are sequential appends. The cost is deferred: a key can exist in several segments at once, so a read may have to check more than one, and a background process called compaction continually merges segments to keep reads bounded and reclaim the space that superseded versions take.

Neither is universally better. The B-tree wins when reads dominate or the working set is read-then-update. The LSM tree wins when writes dominate, especially bursts of inserts, and you can spend background CPU on compaction.

## Picking one

The default is the B-tree, which is the right call for most embedded workloads:

```go
db, _ := kv.Open("app.kv")                       // B-tree
db, _ := kv.Open("ingest.kv", kv.WithEngine(kv.LSM)) // LSM
```

From the CLI:

```bash
kv create app.kv                 # B-tree
kv create ingest.kv --engine lsm # LSM
```

A rough guide:

- **Reach for the B-tree** when your reads outnumber your writes, when you want the smallest file, when you do random point lookups, or when you simply are not sure. It has the fewest moving parts.
- **Reach for the LSM tree** when you ingest large volumes of writes, when writes come in bursts you need to absorb quickly, or when your access pattern is write-heavy and scan-heavy (logs, events, time series).

Because the engine is fixed at creation, switching means creating a new database and copying the data across (a `kv dump` piped into a `kv load`, or a `Load` in Go). The transactions, iterators, CLI, and server are identical on both, so nothing else in your program changes.

## Tuning the B-tree

These options take effect when the file is created and are recorded in it.

| Option | Effect |
| --- | --- |
| `WithPageSize(bytes)` | Size of a B-tree page. Larger pages mean shallower trees and fewer seeks on scans, at the cost of more write amplification per change. The default suits general use. |
| `WithFillFactor(f)` | Target leaf occupancy before a split, between 0 and 1. A high fill factor packs leaves tighter (smaller file, better scan locality); a lower one leaves room for in-place inserts without splitting. |
| `WithMaxInlineValue(bytes)` | Values up to this size live inside the page next to their key; larger values overflow to dedicated pages. Tune it to keep your common values inline without bloating index pages. |
| `WithBtreeBuffers(on)` | Turn on the Bε buffered write path, which batches writes in interior nodes before pushing them to leaves. It trades a little read work for markedly better random-write throughput. |

## Tuning the LSM tree

| Option | Effect |
| --- | --- |
| `WithMemtableSize(bytes)` | How large the in-memory table grows before it is flushed to a segment. Bigger memtables mean fewer, larger segments and less compaction, at the cost of more memory and a longer crash-recovery replay. |
| `WithLevelRatio(n)` | The size multiplier between adjacent levels of the tree. A larger ratio means fewer levels (cheaper reads) but more work per compaction. |
| `WithFilter(kind)` | The per-segment membership filter that lets a read skip a segment that cannot hold its key. `FilterBloom` is the default; `FilterRibbon` is roughly 30% smaller for the same false-positive rate, trading a little CPU for memory. |
| `WithRangeIndex(on)` | Build a REMIX ordered index across segments, which speeds up scans on a tree with many segments. Worth it for scan-heavy LSM workloads. |
| `WithValueSeparation(threshold)` | Store values larger than `threshold` in a separate value log (the WiscKey design) so the tree itself stays small and compaction moves less data. Helps when values are large relative to keys. |
| `WithCompression(on)` | Compress segment blocks with a heat-tiered policy: leave hot, recently written data raw and compress cold, settled data. |
| `WithColdCompression(on)` | A stricter policy that keeps the hottest levels (L0 and L1) raw and compresses only deeper, colder levels, so no hot-path read ever pays decompression. |

## Reading the engine's state

`Stats` reports engine-specific numbers so you can see how the tree is behaving. For the LSM tree, `Levels` breaks down segments and bytes per level and `CompactionScore` shows how much compaction work is pending; for both engines, `Amplification` shows physical bytes over live bytes, which is how much space superseded data and free pages are costing you. The [CLI](/reference/cli/) surfaces the same numbers through `kv stats` and `kv metrics`.

## Next

- [Durability](/guides/durability/) covers the write-ahead log both engines commit through.
- The [configuration reference](/reference/configuration/) lists every option with its default.
