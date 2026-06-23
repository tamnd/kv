---
title: "Backup and replication"
description: "Taking a consistent online backup, restoring it, and shipping the write-ahead log to keep a read replica or an archive for point-in-time recovery."
weight: 50
---

This guide covers getting data safely off the machine: a one-shot consistent backup, and the streaming primitives that keep a follower current or build an archive you can replay to any point in time.

## A consistent backup

`Backup` writes a complete, consistent physical image of the database to any `io.Writer`, returning the committed version the image captures:

```go
f, _ := os.Create("snapshot.kvbak")
defer f.Close()

version, err := db.Backup(f)
// version is the commit version this backup is faithful to
```

It folds the write-ahead log into the image first, takes the write lock briefly, and streams the result, so the backup is a single point-in-time view with no torn writes, even while the database is open and serving reads. If the database is encrypted, the backup is encrypted too, so it is safe to send to storage you trust less than the live machine.

Restoring rebuilds a database from that stream:

```go
err := kv.RestoreBackup("restored.kv", f)
```

`RestoreBackup` refuses to overwrite an existing file, so a restore never silently clobbers a live database; restore to a fresh path and swap it in. The restored file is byte-faithful to the source, down to its engine and settings.

From the CLI the same two operations are `backup` and `restore`, which stream through files or standard in and out:

```bash
kv backup app.kv --output snapshot.kvbak
kv restore restored.kv --input snapshot.kvbak

# or piped, with the version printed to stderr
kv backup app.kv | gzip > snapshot.kvbak.gz
```

## Shipping the log to a replica

A backup is a snapshot in time. To keep a second copy continuously current, ship the write-ahead log instead of re-imaging the whole database. The primary streams its current WAL generation as a delta, and a follower applies it.

Open the follower as a read replica, which refuses writes through the normal API so the only way its state advances is by applying shipped log:

```go
replica, _ := kv.Open("replica.kv", kv.WithReadReplica())
```

On the primary, capture the current generation; on the replica, apply it:

```go
// primary
version, _ := primary.ShipWAL(out) // stream the current WAL generation, no checkpoint

// replica
applied, err := replica.ApplyWAL(in) // replay it; idempotent over versions already applied
```

`ApplyWAL` is idempotent over versions it has already seen, so re-shipping an overlapping range is safe. If the stream instead begins past the version the replica has applied, leaving a hole, it returns `kv.ErrReplicaGap` rather than applying a discontinuous log; the fix is to reseed the replica from a fresh `Backup` and resume shipping from there. Promote a replica to a writable primary by reopening it without `WithReadReplica`.

The CLI mirrors this with `ship` and `replay`:

```bash
kv ship primary.kv --output delta.wal
kv replay replica.kv --input delta.wal
```

## Point-in-time recovery

To recover to an exact moment rather than the latest, you need every generation archived and the ability to stop replaying at a chosen version. `WithWALArchive` registers a sink that receives each WAL generation before it is checkpointed away, which is how you build that archive:

```go
db, _ := kv.Open("app.kv", kv.WithWALArchive(func(delta []byte) error {
	return uploadToObjectStore(delta) // called under the write lock, before checkpoint
}))
```

The sink is called synchronously before checkpoint, so returning an error fails the checkpoint and stops the log from being discarded before you have stored it. Keep the sink fast and offload slow work; an error here is a backpressure signal, not a place to retry forever.

To recover, restore the nearest backup and replay archived generations up to your target version with `ApplyWALUntil`, which stops after the version you name and leaves later commits unapplied:

```go
applied, err := db.ApplyWALUntil(archivedDelta, targetVersion)
```

On the CLI, `kv replay --until V` does the same, stopping after version `V`.

## Choosing an approach

- A periodic `Backup` is the simplest disaster-recovery story and all many applications need.
- Add `ShipWAL` and a read replica when you want a warm standby or a read-only copy that stays close to current.
- Add `WithWALArchive` and `ApplyWALUntil` when you need to rewind to an exact version, for example to recover from a bad write at a known time.

## Next

- [Running the server](/guides/server/) covers exposing the database over a socket, including to replicas on other hosts.
- The [library reference](/reference/library/) lists the backup and replication methods.
