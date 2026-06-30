---
title: "Durability and recovery"
description: "How kv survives a crash through a write-ahead log, what each synchronous level guarantees, and how checkpointing and vacuum keep the file in shape."
weight: 30
---

A database is only as good as its promise that a committed write survives a crash. This guide explains how kv keeps that promise, and the dial you can turn to trade durability for speed.

## The write-ahead log

While a database is open, kv keeps a write-ahead log (WAL) in a file next to the main one, `app.kv-wal` beside `app.kv`. Every commit is written to the WAL first, as a sequence of checksummed frames, before the change is considered durable. The main file is updated later, in the background, by a process called checkpointing.

This ordering is what makes a crash survivable. If the process dies at any point, the next `Open` reads the WAL, verifies each frame's checksum, replays every committed transaction it finds, and discards any torn or partial tail. The database comes back at exactly its last committed state. Recovery is automatic; there is no repair step to run.

A frame that fails its checksum marks the end of the valid log: kv stops there, because anything past a corrupt frame cannot be trusted. A transaction whose commit frame never made it to disk is simply not replayed, which is correct, it was never durable, so it never committed.

## The synchronous levels

"Written to the WAL" can mean several things, depending on how hard kv leans on the operating system to actually put the bytes on the platter. That is the `synchronous` setting, and it is the main durability-versus-speed dial:

| Level | What it does | Loses on power failure |
| --- | --- | --- |
| `SyncOff` | Never fsyncs. The OS flushes on its own schedule. | Recent commits, possibly many. |
| `SyncNormal` | fdatasyncs at checkpoint and on a short timer, not on every commit. The shipped default. | The last sub-second of commits. |
| `SyncBarrier` | A write-ordering barrier on every commit (`F_BARRIERFSYNC` on macOS, fdatasync on Linux). | Nothing reordered past the barrier, though the very last commit may not be flushed. |
| `SyncFull` | fdatasyncs on every commit, batched across concurrent committers. | Nothing acknowledged. |
| `SyncExtra` | `SyncFull` plus a directory sync when the file grows. | Nothing, including the file's existence after growth. |

`SyncNormal` is the shipped default. It is group commit: kv fdatasyncs the WAL at each checkpoint and on a short timer rather than on every individual commit, so out-of-box write throughput is tens of thousands of commits per second instead of the few hundred per second an fsync-on-every-commit default would give you. This is the same trade SQLite's WAL mode with `PRAGMA synchronous=NORMAL`, badger, pebble and rocksdb all default to.

The guarantee is honest about its one gap. A power failure never corrupts the file: the checksum chain still holds and recovery comes back at a consistent committed state. What it can lose is the last sub-second of commits, the ones that were acknowledged but had not yet been fsynced when the power went. If you cannot tolerate losing any acknowledged commit, ask for `SyncFull`, which fdatasyncs on every commit so once `Commit` returns the data is on the platter and nothing after a power cut can lose it. `SyncFull` is one option away, no format change involved.

Set the level at open time or change it at runtime:

```go
// the default, group commit, is SyncNormal; pass nothing to get it
db, _ := kv.Open("app.kv")

// ask for zero acked-commit loss
db, _ := kv.Open("app.kv", kv.WithSynchronous(kv.SyncFull))

// or change it later, for example to bulk-load fast then tighten back up
db.SetSynchronous(kv.SyncOff)
// ... load a million keys ...
db.SetSynchronous(kv.SyncFull)
db.Checkpoint()
```

That bulk-load pattern, drop to `SyncOff`, load, restore the level, and checkpoint, is the standard way to ingest a large dataset quickly while ending in a fully durable state. The risk window is only during the load, and if it crashes you just start the load over.

## Group commit

Even when you ask for `SyncFull`, kv does not fsync once per commit when many commits arrive at once. It batches concurrent committers so that one fsync covers a whole group, which is why write throughput under concurrency stays high even at the strictest level. You can widen the batching window deliberately with the `commit_linger_us` setting, which makes a commit wait a few microseconds for others to join its group, trading a little latency for fewer fsyncs under heavy write load.

## Checkpointing

The WAL grows as commits accumulate. Checkpointing folds the committed frames back into the main file and resets the log, keeping both the WAL bounded and reads fast (a read never has to consult an unbounded log). kv checkpoints automatically in the background once the WAL passes a threshold you set with `WithAutoCheckpoint`, and you can trigger one yourself:

```go
db.Checkpoint() // passive: fold what is committed, do not block writers
```

The checkpoint modes give finer control when you need it: passive folds without blocking, full and restart additionally reset the log for reuse, and truncate shrinks the `-wal` file back down. The [CLI](/reference/cli/) exposes them through `kv checkpoint --mode`. For almost all uses, the automatic background checkpointer is enough and you never call this.

## Reclaiming space

Checkpointing keeps the WAL in check; vacuum keeps the main file in check. As keys are deleted and updated, pages free up inside the file. `Vacuum` returns trailing free pages to the operating system, shrinking the file on disk:

```go
reclaimed, err := db.Vacuum(0) // 0 = reclaim everything available
```

It is incremental: pass a page budget to bound how much work one call does, so a large reclaim can be spread across several calls without a long pause. The space that superseded versions hold inside the f2 log is reclaimed separately, by the engine's background compaction, which folds live records forward and drops the dead tail; `Stats().CompactionScore` reports how much of that work is pending. You can also set an automatic policy with the `auto_vacuum` setting so trailing free pages are returned without an explicit call.

## When durability is fenced off

If an fsync ever fails, the operating system is telling kv that a write it promised is durable might not be. kv treats that as fatal: it fences the database into a needs-recovery state and fails subsequent operations with `ErrNeedsRecovery`, rather than carrying on as if the write succeeded. The fix is to close and reopen, which runs recovery from the WAL and brings the database back to its last known-good committed state. This is deliberate; silently continuing after a durability failure is how databases lose data without anyone noticing.

## Next

- [Backup and replication](/guides/backup-and-replication/) covers getting a consistent copy off the machine.
- The [configuration reference](/reference/configuration/) lists the durability settings and their defaults.
