# kv

A fast embedded ordered key/value database for Go. One file, a write-ahead log,
transactions, two pluggable storage engines, and an operator CLI — the familiar
SQLite shape applied to a key/value problem.

```
go get github.com/tamnd/kv
```

[![Go Reference](https://pkg.go.dev/badge/github.com/tamnd/kv.svg)](https://pkg.go.dev/github.com/tamnd/kv)

---

## What it is

`kv` is a pure-Go, zero-dependency embedded key/value store with:

- **One file per database.** A `.kv` file plus an optional `-wal` sidecar. Copy,
  move, or back it up with `cp`. No daemon, no network, no configuration directory.
- **Two storage engines.** A B-tree core (default) for low read latency, and an LSM
  core (opt-in) for high write throughput and large values. Both share the same file
  format, WAL, MVCC layer, and API.
- **Snapshot isolation by default, serializable on request.** Every `View` and
  `Update` sees a consistent snapshot; serializable adds commit-time read-set
  validation.
- **Durable group commit.** Concurrent writers batch their fsyncs, so throughput
  scales with concurrency without multiplying I/O.
- **A real CLI.** `kv get`, `kv scan`, `kv checkpoint`, `kv pragma`, and more — the
  SQLite-shell interface for operational tasks and scripting.
- **An HTTP/binary server.** `kv serve` exposes the full API over HTTP/JSON and a
  pure-Go binary protocol with streaming, subscriptions, and interactive transactions.

---

## Quick start

```go
package main

import (
    "fmt"
    "log"

    "github.com/tamnd/kv"
)

func main() {
    db, err := kv.Open("data.kv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Write
    err = db.Update(func(txn *kv.Txn) error {
        return txn.Set([]byte("hello"), []byte("world"))
    })
    if err != nil {
        log.Fatal(err)
    }

    // Read
    err = db.View(func(txn *kv.Txn) error {
        val, err := txn.Get([]byte("hello"))
        if err != nil {
            return err
        }
        fmt.Printf("%s\n", val)
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

---

## Core API

### Opening a database

```go
db, err := kv.Open("data.kv")           // open or create with defaults

db, err := kv.Open("data.kv",
    kv.WithEngine(kv.LSM),               // use LSM engine (create-time)
    kv.WithSynchronous(kv.SyncFull),     // fsync on every commit (default)
    kv.WithCacheSize(256 << 20),         // 256 MiB buffer pool
    kv.WithAutoCheckpoint(1000),         // fold WAL after 1000 frames
)
```

`Open` creates the file if it does not exist, or opens an existing file (engine is
detected from the header). The returned `*DB` is safe for concurrent use.

### Transactions

```go
// Read-only snapshot (never retried)
err = db.View(func(txn *kv.Txn) error {
    val, err := txn.Get(key)
    if err != nil {
        return err
    }
    _ = val // slice valid until txn ends
    return nil
})

// Read-write (retried on write-write conflict up to WithMaxRetries)
err = db.Update(func(txn *kv.Txn) error {
    val, err := txn.Get(key)
    if err != nil && !kv.IsNotFound(err) {
        return err
    }
    return txn.Set(key, newVal)
})

// Explicit begin/commit
txn := db.Begin(true) // writable
if err := txn.Set(key, val); err != nil {
    txn.Discard()
    return err
}
if err := txn.Commit(); err != nil {
    return err
}
```

`View` takes a stable snapshot for the duration of the call; concurrent writes do not
affect it. `Update` auto-retries on conflict up to `WithMaxRetries` (default: a small
bounded count); the closure must be idempotent.

### Key/value operations

```go
val, err := txn.Get(key)            // ErrNotFound when absent
ok, err  := txn.Exists(key)
err       = txn.Set(key, val)
err       = txn.Delete(key)
err       = txn.SetWithTTL(key, val, expiry) // expiry as Unix nanos
err       = txn.DeleteRange(lo, hi) // half-open [lo, hi)
err       = txn.Merge(key, operand) // apply the registered merge operator
```

Values returned from `Get` reference internal memory and are valid until the
transaction ends. Call `append(nil, val...)` to retain them beyond the transaction.

### Scanning

```go
it, err := txn.NewIterator(engine.IterOptions{
    Lower: []byte("a"),
    Upper: []byte("z"),
})
if err != nil {
    return err
}
defer it.Close()

for it.First(); it.Valid(); it.Next() {
    key := it.Key() // valid until next iterator call
    val, err := it.Value()
    if err != nil {
        return err
    }
    _ = key
    _ = val
}
if err := it.Error(); err != nil {
    return err
}
```

`SeekGE(key)` and `SeekLT(key)` position the cursor. `Prev()` and `Last()` for
reverse iteration.

### Bulk load

```go
pairs := []struct{ k, v []byte }{ ... }
i := 0
_, err = db.Load(func() (key, value []byte, ok bool) {
    if i >= len(pairs) {
        return nil, nil, false
    }
    p := pairs[i]
    i++
    return p.k, p.v, true
})
```

`Load` uses a bulk-load fast path when the engine supports it (LSM's direct
memtable insert, B-tree's sequential insert). Keys must arrive in order. For
incremental bulk writes, `WriteBatch` auto-flushes every N operations:

```go
wb := db.NewWriteBatch(500)
for _, p := range pairs {
    if err := wb.Set(p.k, p.v); err != nil {
        wb.Discard()
        return err
    }
}
if err := wb.Close(); err != nil {
    return err
}
```

### Snapshots

```go
snap := db.Snapshot()       // freeze a read version
defer snap.Release()

val, err := snap.Get(key)   // reads from the frozen version
```

A snapshot is a lightweight stable-read handle that doesn't allocate a transaction.
It is pinned until `Release`; avoid holding it across checkpoints in write-heavy
workloads.

### Subscribe (change feed)

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

err = db.Subscribe(ctx, []byte("orders/"), func(changes []kv.Change) error {
    for _, c := range changes {
        fmt.Printf("%s -> %s\n", c.Key, c.Value)
    }
    return nil
})
```

The callback receives batches of committed changes under the prefix in version
order. It is the hook for CDC pipelines and read-model materializers.

### Maintenance

```go
err = db.Checkpoint()                          // fold WAL (passive)
err = db.CheckpointMode(kv.CheckpointFull)     // fold + confirm no backlog
err = db.CheckpointMode(kv.CheckpointTruncate) // fold + truncate WAL file
n, err := db.Vacuum(budget)                    // return trailing free pages to OS

rep, err := db.Verify()                        // structural integrity check
stats := db.Stats()                            // space and durability stats
```

---

## Choosing an engine

```go
// B-tree (default): read-optimized, in-place, low latency
db, _ = kv.Open("data.kv", kv.WithEngine(kv.BTree))

// LSM: write-optimized, compaction-based, good for large values or heavy writes
db, _ = kv.Open("data.kv", kv.WithEngine(kv.LSM))
```

The engine is fixed at create time and stored in the file header. An existing file
ignores `WithEngine`; the engine from the header is used automatically.

**B-tree** wins when reads dominate, key space is bounded, or write amplification
budget is low (embedded analytics, config stores, indexes).

**LSM** wins when writes dominate, values are large, or the working set does not fit
in cache (event ingestion, time-series, append-heavy logs).

---

## Durability

The WAL durability level is set with `WithSynchronous`:

| Level | Behavior | When to use |
|-------|----------|-------------|
| `SyncFull` (default) | `fdatasync` on every group commit | production |
| `SyncBarrier` | ordering barrier, no full flush | NVMe with power-loss protection |
| `SyncNormal` | `fdatasync` at checkpoint only | batch ingestion, lower latency tail |
| `SyncOff` | no fsync | in-memory ephemeral workloads |

`SyncFull` groups concurrent commits into one shared fsync, so write throughput scales
with concurrency even at the default level.

---

## Isolation levels

| Level | Behavior |
|-------|----------|
| `SnapshotIsolation` (default) | stable snapshot per transaction; write skew possible |
| `Serializable` | read-set validation at commit; serializable history; higher abort rate |

```go
db, _ = kv.Open("data.kv", kv.WithIsolation(kv.Serializable))
```

---

## Encryption

```go
key := make([]byte, 32)
if _, err := rand.Read(key); err != nil {
    log.Fatal(err)
}
db, err := kv.Open("data.kv", kv.WithEncryptionKey(key))
```

`WithEncryptionKey` encrypts every page and WAL frame with AES-256-GCM. The master
key is never stored on disk; the caller must supply it on every open. Key rotation is
online via `db.RotateEncryptionKey(newKey)`.

---

## Replication and backup

```go
// Physical backup (consistent snapshot stream)
_, err = db.Backup(w)

// Restore from a backup stream
db, err = kv.RestoreBackup(fs, "new.kv", r)

// WAL shipping (primary side): archive each generation
db, _ = kv.Open("primary.kv",
    kv.WithWALArchive(func(delta []byte) error {
        return sendToReplica(delta)
    }),
)

// Follower side: apply generations in order
db, _ = kv.Open("replica.kv", kv.WithReadReplica())
_, err = db.ApplyWAL(r)
```

---

## HTTP/binary server

```
kv serve data.kv --addr :8080
```

The server exposes the full API over HTTP/JSON (unary and streaming NDJSON) and a
pure-Go binary protocol. It supports TLS, token/mTLS/JWT authentication, per-prefix
ACLs, rate limits, connection limits, and graceful shutdown.

---

## CLI

```
kv <command> <db> [flags]

  create       create a new database file
  get          print the value for a key
  set          upsert a key to a value
  del          delete one key
  del-range    range-delete [lo, hi)
  exists       exit 0 if present, 1 if absent
  merge        apply the merge operator to a key
  scan         range or prefix scan
  count        count keys in a range or prefix
  dump         stream all key/value pairs as JSONL
  load         bulk-load from stdin or a file
  export       export keys as CSV, TSV, or JSONL
  import       import from CSV, TSV, or JSONL
  checkpoint   fold the WAL into the main file
  backup       stream a physical backup to a file or stdout
  restore      rebuild a database from a backup stream
  ship         stream the current WAL generation
  replay       apply a WAL delta onto a read-only follower
  vacuum       return trailing free pages to the OS
  pragma       read or set a configuration knob
  check        verify structural integrity; exit 4 on any violation
  info         print a human summary of the database
  stats        print space and durability accounting as JSON
  metrics      print Prometheus text metrics
  serve        serve the database over HTTP/JSON
```

Exit codes mirror library error types (0 OK, 1 not found, 2 usage, 3 open failed,
4 corrupt, 5 locked, 6 conflict, 7 crypto, 8 I/O).

### Pragmas

```
kv pragma <db> synchronous          # read current sync level
kv pragma <db> synchronous=FULL     # set sync level: OFF|NORMAL|FULL|EXTRA
kv pragma <db> wal_autocheckpoint   # read current WAL backlog threshold
kv pragma <db> wal_autocheckpoint=2000
kv pragma <db> full_page_writes     # on|off
kv pragma <db> auto_vacuum          # NONE|INCREMENTAL|FULL
kv pragma <db> commit_linger_us=100 # group-commit window in microseconds
kv pragma <db> page_count           # total pages in the file
kv pragma <db> freelist_count       # free pages
kv pragma <db> engine               # btree|lsm
kv pragma <db> application_id       # 32-bit tag for the application
kv pragma <db> user_version         # 32-bit user-managed version field
```

---

## Configuration reference

| Option | Default | Description |
|--------|---------|-------------|
| `WithEngine(e)` | `BTree` | storage core (create-time) |
| `WithPageSize(n)` | 4096 | page size in bytes (create-time) |
| `WithCacheSize(n)` | 32 MiB | buffer pool capacity |
| `WithSynchronous(s)` | `SyncFull` | WAL fsync level |
| `WithAutoCheckpoint(n)` | 1000 frames | background checkpoint trigger |
| `WithIsolation(l)` | `SnapshotIsolation` | transaction isolation level |
| `WithMaxRetries(n)` | small | conflict auto-retry bound |
| `WithMergeOperator(name, fn)` | nil | associative merge operator |
| `WithEncryptionKey(k)` | nil | 32-byte AES-256-GCM master key |
| `WithLogger(l)` | nil | structured log sink |
| `WithSlowOpThreshold(d)` | off | slow-operation log threshold |
| `WithTracer(t)` | nil | OpenTelemetry-compatible span hook |
| `WithReadReplica()` | off | open as a read-only WAL follower |
| `WithWALArchive(fn)` | nil | WAL generation sink for replication/PITR |
| `WithMemtableSize(n)` | 64 MiB | LSM memtable flush threshold |
| `WithCompression(c)` | fast DEFLATE | hot-level compression |
| `WithColdCompression(c)` | high DEFLATE | cold-level (deep LSM) compression |
| `WithFilter(f)` | Bloom | per-level filter policy |
| `WithRangeIndex(bool)` | off | REMIX range index for scan-heavy LSM |
| `WithValueSeparation(n)` | off | LSM vLog threshold for large values |

---

## Error types

```go
kv.IsNotFound(err)    // ErrNotFound: key absent
kv.IsConflict(err)    // ErrConflict: write-write conflict after retries
kv.IsCorrupt(err)     // ErrCorrupt: structural integrity violation
kv.IsLocked(err)      // ErrLocked: another writer holds the file lock
```

---

## Performance notes

- Pure Go, zero CGO: no cgo call overhead, single static binary, one GC.
- Group commit: concurrent writers share one fsync per commit batch.
- Lock-free reads: non-transactional `Get` never touches the MVCC oracle mutex.
- Arena-backed memtable: a 64 MiB LSM memtable is a few allocations, not millions.
- Cache-sharded buffer pool: 2Q eviction, per-shard locks, decoded-node cache.
- B-tree OLC (Optimistic Latch Coupling): read path mostly lock-free.

---

## Stability

v1.0. The public API (all exported identifiers in this package) is stable. Internal
packages (`db/`, `btree/`, `lsm/`, `pager/`, `wal/`, `engine/`, `format/`) are not
part of the stable surface and may change without notice.

The on-disk format is stable and forward-compatible. A file written by v1.0 can be
opened by any future v1.x release.

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
