---
title: "Durability"
description: "The two durability modes kv offers, the bounded loss window under the default, when to pick which, and what Sync and Close guarantee."
weight: 30
---

A database is only as good as its promise that a committed write survives a crash.
kv makes that promise in one of two ways, and you pick which with a single option.
Both modes are durable.
The difference is when the acknowledgement waits on the disk.

## The two modes

`Options.SyncWrites` selects the contract:

| `SyncWrites` | Contract | Loses on a crash |
| --- | --- | --- |
| `false` (default) | Background group commit. A write lands in the in-memory hot tier and returns; a background flusher fsyncs it a moment later. | At most the un-flushed hot records, bounded to two segments. |
| `true` | Per-commit fsync. A `Set` does not return until its record is fsynced. | Nothing acknowledged. |

### The default: background group commit

By default a write returns as soon as it lands in the hot tier, and a background flusher fsyncs it a moment later.
A crash between the ack and the next flush loses at most the records that had not been flushed yet, bounded to two segments.

This is a bounded sub-second loss window, the same contract Redis gives with `appendfsync everysec`.
It is durable on a short delay, not "not durable".
This is the fast default, and it is where the throughput lead lives, because the ack does not wait on the disk.

```go
// the default, background group commit
db, _ := kv.Open("app.kv", kv.Options{})
```

### Zero loss: per-commit fsync

Set `SyncWrites` to true and a `Set` does not return until its record is fsynced, so an acked write survives a crash with zero loss.
This is the same guarantee bbolt, pebble, and per-commit sqlite give.

```go
// every commit fsynced before it returns
db, _ := kv.Open("app.kv", kv.Options{SyncWrites: true})
```

Concurrent writers coalesce onto one shared fsync, so a burst pays one flush between them rather than hitting the disk's per-flush ceiling on every write.
That is what keeps write throughput reasonable under concurrency even at the strict setting.

## Choosing a mode

Leave `SyncWrites` false when a bounded sub-second loss window is acceptable, which it is for most caches, session stores, and derived data you can rebuild.
This is the fast path.

Set `SyncWrites` true when you cannot lose any acknowledged write: a system of record, a ledger, anything where "the client was told it committed" has to mean it is on the platter.
You pay for it on the write path, since the ack now waits on the fsync, though concurrent writers share one flush.

## Sync and Close

`Sync()` forces a durability barrier on demand under either mode.
Call it when you want everything written so far to be on disk before you continue, for example at the end of a batch load under the default mode.

```go
db.Set([]byte("k"), []byte("v"))
if err := db.Sync(); err != nil {
	log.Fatal(err)
}
```

`Close()` syncs before it returns, so a clean shutdown never sits in the loss window.
Always close the database on the way out, typically with `defer db.Close()` right after `Open`.

## Next

- [Sizing a store](/guides/sizing/) covers the `Options` knobs that set the resident memory footprint.
- The [configuration reference](/reference/configuration/) lists every option with its default.
