---
title: "Library API"
description: "Every exported type, method, option, and error in the github.com/tamnd/kv package, grouped by what it does."
weight: 10
---

This is the complete public surface of the `kv` package. Import it as `github.com/tamnd/kv`. For task-oriented walkthroughs, see the [guides](/guides/); this page is the index.

## Opening and closing

```go
func Open(path string, opts ...Option) (*DB, error)
func (db *DB) Close() error
```

`Open` creates the file if absent and runs crash recovery if present. `Close` flushes, stops background work, and releases the file. The package-level `Version` constant holds the library version as a string.

## Single-key reads

```go
func (db *DB) Get(key []byte) ([]byte, error)
```

`Get` returns a copy of the newest committed value for `key`, or `ErrNotFound` if the key is absent or tombstoned. It reads at the latest committed state through the engine's point-read path, with no transaction to open and discard, so it is the lightest way to read one key and the right call when a read does not need to agree with other reads. The bytes are yours to keep; they are not tied to a transaction lifetime.

Reach for a transaction instead when several reads must see one consistent state: two back-to-back `Get` calls can land on either side of a concurrent commit, while reads inside a `View` or against a `Snapshot` all see the same version. The example below is correct only if a counter and its label never need to move together; if they do, read both inside one `View`.

```go
v, err := db.Get([]byte("user:1"))
if errors.Is(err, kv.ErrNotFound) {
	// not there
}
```

## Transactions

```go
func (db *DB) View(fn func(txn *Txn) error) error
func (db *DB) Update(fn func(txn *Txn) error) error
func (db *DB) UpdateVersion(fn func(txn *Txn) error) (uint64, error)
func (db *DB) Begin(writable bool) *Txn
```

`View` runs a read-only closure at a consistent snapshot. `Update` runs a read-write closure and commits it atomically if the closure returns nil, retrying on conflict up to the configured bound. `UpdateVersion` is `Update` that also returns the commit version. `Begin` opens an explicit transaction you commit or discard yourself.

### Transaction methods

```go
func (txn *Txn) Get(key []byte) ([]byte, error)
func (txn *Txn) GetCopy(key []byte) ([]byte, error)
func (txn *Txn) Exists(key []byte) (bool, error)
func (txn *Txn) Set(key, value []byte) error
func (txn *Txn) SetWithTTL(key, value []byte, ttl time.Duration) error
func (txn *Txn) Delete(key []byte) error
func (txn *Txn) Merge(key, operand []byte) error
func (txn *Txn) Commit() error
func (txn *Txn) Discard()
```

Every method addresses one key: kv is a point-lookup store with no range scan or ordered iteration. `Get` returns bytes valid only until the transaction ends; `GetCopy` returns a copy you own past it. `Exists` reports presence without materializing the value. `SetWithTTL` writes a value that expires after the given duration. `Merge` records an operand the registered merge operator folds into the value. `Commit` applies an explicit transaction and may return `ErrConflict`. `Discard` releases the snapshot and is a no-op after a successful `Commit`; always call it, typically with `defer`.

## Snapshots

```go
func (db *DB) Snapshot() *Snapshot
func (snap *Snapshot) View(fn func(txn *Txn) error) error
func (snap *Snapshot) Close() error
```

A `Snapshot` pins one read version across many separate read transactions. Close it as soon as the work is done, since a pinned version holds back space reclamation.

## Batches and bulk load

```go
func (db *DB) NewWriteBatch(maxOps int) *WriteBatch
func (b *WriteBatch) Set(key, value []byte) error
func (b *WriteBatch) Delete(key []byte) error
func (b *WriteBatch) Flush() error
func (b *WriteBatch) Count() int
func (b *WriteBatch) Pending() int
func (b *WriteBatch) Close() error

func (db *DB) Load(next func() (key, value []byte, ok bool)) (uint64, error)
```

A `WriteBatch` accumulates writes and flushes them in committed chunks of at most `maxOps` operations, the efficient path for bulk loading. `Flush` commits what is pending; `Count` and `Pending` report total and uncommitted operations; `Close` flushes and releases it. `Load` pulls key/value pairs from a generator and writes them in batches, returning the final commit version.

## Maintenance

```go
func (db *DB) Checkpoint() error
func (db *DB) CheckpointMode(m CheckpointMode) error
func (db *DB) Vacuum(budget int) (int, error)
func (db *DB) Stats() Stats
func (db *DB) Check() (*CheckReport, error)
```

`Checkpoint` folds the WAL into the main file; `CheckpointMode` does the same with an explicit passive, full, restart, or truncate mode. `Vacuum` returns up to `budget` trailing free pages to the OS (0 means all) and reports how many it reclaimed. `Stats` reports space and durability accounting, including `CompactionScore`, the urgency of the most-pending compaction, and `Amplification`, physical bytes over live bytes. `Check` walks the structure and returns a report on its integrity.

## Backup and replication

```go
func (db *DB) Backup(w io.Writer) (uint64, error)
func RestoreBackup(path string, r io.Reader) error
func (db *DB) ShipWAL(w io.Writer) (uint64, error)
func (db *DB) ApplyWAL(r io.Reader) (uint64, error)
func (db *DB) ApplyWALUntil(r io.Reader, target uint64) (uint64, error)
```

`Backup` streams a consistent physical image and returns its version; `RestoreBackup` rebuilds from that stream and refuses to overwrite. `ShipWAL` streams the current WAL generation as a replication delta; `ApplyWAL` replays it on a follower (idempotent over applied versions, `ErrReplicaGap` on a hole); `ApplyWALUntil` stops after `target` for point-in-time recovery. See the [backup guide](/guides/backup-and-replication/).

## Encryption

```go
func (db *DB) RotateEncryptionKey() error
```

Supply the 32-byte master key at open time with `WithEncryptionKey`. `RotateEncryptionKey` advances the internal data-encryption key to a new epoch and re-encrypts lazily as pages are written, without changing the master key you open with. See the [encryption guide](/guides/encryption/).

## Change feed

```go
func (db *DB) Subscribe(ctx context.Context, prefix []byte, fn func([]Change) error) error
```

`Subscribe` calls `fn` with batches of committed changes under `prefix` until `ctx` is cancelled or `fn` returns an error. A subscriber that lags is dropped with `ErrSubscriberLagged` rather than stalling writers.

## Options

Every option is a function passed to `Open`. Create-time options are recorded in the file and take effect only when it is created; open-time options apply on every open.

| Option | When | Effect |
| --- | --- | --- |
| `WithPageSize(int)` | create | Page size in bytes for a fresh file. |
| `WithEncryptionKey([]byte)` | create | 32-byte AES-256-GCM master key. |
| `WithMergeOperator(name, fn)` | create + every open | Registers the associative merge operator. |
| `WithCacheSize(int)` | open | Resident memory bound in bytes, the [larger-than-memory](/guides/engines/#larger-than-memory) dial. |
| `WithSynchronous(Sync)` | open | `SyncOff`, `SyncNormal` (default), `SyncBarrier`, `SyncFull`, `SyncExtra`. |
| `WithAutoCheckpoint(int)` | open | WAL frame backlog before background checkpoint; negative disables. |
| `WithMaxRetries(int)` | open | Bound on `Update` conflict retries. |
| `WithIsolation(Isolation)` | open | `SnapshotIsolation` (default) or `Serializable`. |
| `WithReadReplica()` | open | Open read-only; only `ApplyWAL` advances state. |
| `WithWALArchive(sink)` | open | Sink for each WAL generation before checkpoint. |
| `WithLogger(*slog.Logger)` | open | Structured operational logging. |
| `WithSlowOpThreshold(time.Duration)` | open | WARN log for ops at or above the threshold (needs `WithLogger`). |
| `WithTracer(Tracer)` | open | Tracing hooks for OpenTelemetry. |

## Errors

Match these with `errors.Is`.

| Error | Meaning |
| --- | --- |
| `ErrNotFound` | Key absent or tombstoned. |
| `ErrConflict` | Write-write or serializable conflict; retry the transaction. |
| `ErrReadOnly` | Write attempted on a read-only transaction or database. |
| `ErrClosed` | Operation on a closed database or finished transaction. |
| `ErrTxnTooBig` | A single transaction exceeded its size bound. |
| `ErrCorrupt` | Checksum or authentication failure on a page or frame. |
| `ErrNeedsRecovery` | A prior fatal fsync error fenced the database; reopen to recover. |
| `ErrUnsupported` | The engine lacks an optional capability. |
| `ErrSnapshotClosed` | Snapshot used after `Close`. |
| `ErrBatchClosed` | Write batch used after `Close`. |
| `ErrSubscriberLagged` | A change-feed subscriber lagged and was dropped. |
| `ErrWrongKey` | Wrong encryption key, or a corrupt descriptor. |
| `ErrEncryptionKeyRequired` | File is encrypted but opened with no key. |
| `ErrKeyOnPlaintext` | Key supplied for a file created unencrypted. |
| `ErrNotEncrypted` | Key rotation requested on an unencrypted database. |
| `ErrBackupFormat` | `RestoreBackup` was handed a stream that is not a backup. |
| `ErrReplicaGap` | `ApplyWAL` stream begins past the applied version. |

## Next

- The [configuration reference](/reference/configuration/) covers every option's default and range.
- The [CLI reference](/reference/cli/) covers the command-line client.
