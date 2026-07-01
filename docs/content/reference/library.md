---
title: "Library API"
description: "Every exported type, method, and Options field in the github.com/tamnd/kv package, with exact signatures and semantics."
weight: 10
---

This is the complete public surface of the `kv` package.
Import it as `github.com/tamnd/kv`.
The whole API is `Open`, an `Options` struct, and five methods on `*DB`.
For task-oriented walkthroughs, see the [guides](/guides/); this page is the exact reference.

## Opening

```go
func Open(path string, opts Options) (*DB, error)
```

`Open` opens the store at `path`, creating it if it does not exist and running recovery if it does.
It takes two arguments: the path and an `Options` value.
The zero value `Options{}` is valid, and every field falls back to a default, so `kv.Open(path, kv.Options{})` just works.
The store is one file at `path` plus a small sibling commit-watermark file.

```go
db, err := kv.Open("app.kv", kv.Options{})
if err != nil {
	log.Fatal(err)
}
defer db.Close()
```

## Options

```go
type Options struct {
	KeyCapacity    int   // expected distinct key count; sizes the resident key index. Default 1<<20.
	HotBytes       int64 // size of one in-memory hot segment; writes land here. Default 8 MiB.
	HotKeys        int   // records one hot segment's index is sized for. Zero uses a heuristic from HotBytes.
	ResidentBytes  int64 // cold log's resident tail window in bytes. Default 64 MiB.
	ReadCacheCells int   // cells in the read cache over cold reads; rounded up to a power of two. Default 1<<16.
	SyncWrites     bool  // durability contract. Default false (background group commit).
}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `KeyCapacity` | `1 << 20` (~1M) | Expected distinct key count. Sizes the resident key index, which holds an entry per key, so it is the memory floor and the one knob worth setting for a large store. |
| `HotBytes` | 8 MiB | Size of one in-memory hot segment, where writes land. Bounds the resident write buffer (at most two segments live) and, under the default durability, the crash-loss window. |
| `HotKeys` | heuristic from `HotBytes` | Records one hot segment's index is sized for; set it when the value size is known. Too small only causes an earlier seal, never a lost write. Zero uses a heuristic. |
| `ResidentBytes` | 64 MiB | Cold log's resident tail window: how much recently-migrated cold data stays in RAM for fast reads. |
| `ReadCacheCells` | `1 << 16` | Cells in the read cache over cold reads, rounded up to a power of two. |
| `SyncWrites` | `false` | Durability contract. `false` is background group commit; `true` fsyncs every commit before it returns. See [Durability](/guides/durability/). |

The [sizing guide](/guides/sizing/) walks these in the order they matter.

## Methods

The whole method surface is five methods on `*DB`.

```go
func (d *DB) Set(key, value []byte)
func (d *DB) Delete(key []byte)
func (d *DB) Get(key, scratch []byte) ([]byte, bool, error)
func (d *DB) Sync() error
func (d *DB) Close() error
```

### Set

```go
func (d *DB) Set(key, value []byte)
```

`Set` writes `value` under `key`, overwriting any existing value.
It does not return an error.
The write lands in the in-memory hot tier and returns; under the default durability a background flusher fsyncs it a moment later, and with `SyncWrites` true it does not return until the record is fsynced.

### Delete

```go
func (d *DB) Delete(key []byte)
```

`Delete` removes `key`.
It does not return an error.
A later `Get` on that key returns `ok == false`.
Deleting a key that is not present is a no-op.

### Get

```go
func (d *DB) Get(key, scratch []byte) ([]byte, bool, error)
```

`Get` looks up `key` and returns three values: the value, whether the key was found, and an error.

It decodes the value into `scratch` and returns a slice aliased to it, so a hot loop can reuse one buffer and allocate nothing.
Pass `nil` as `scratch` to let the engine allocate a fresh slice for you.

A missing key is `ok == false`, not an error.
Check `ok`, not the error, to tell present from absent; the error is reserved for a real read failure.

```go
scratch := make([]byte, 0, 256)
v, ok, err := db.Get([]byte("user:1"), scratch)
if err != nil {
	log.Fatal(err)
}
if !ok {
	// the key is not present
}
_ = v
```

### Sync

```go
func (d *DB) Sync() error
```

`Sync` forces a durability barrier now, under either durability mode.
After it returns, everything written so far is on disk.

### Close

```go
func (d *DB) Close() error
```

`Close` syncs and then releases the store.
It syncs before returning, so a clean shutdown never sits in the loss window.
Always close the store on the way out, typically with `defer db.Close()`.

## Durability contract

`Options.SyncWrites` picks the contract, and both modes are durable.

With `SyncWrites` false, the default, a write returns as soon as it lands in the hot tier and a background flusher fsyncs it a moment later.
A crash between the ack and the next flush loses at most the un-flushed hot records, bounded to two segments, the same bounded sub-second window Redis gives with `appendfsync everysec`.

With `SyncWrites` true, a `Set` does not return until its record is fsynced, so an acked write survives a crash with zero loss.
Concurrent writers coalesce onto one shared fsync, so a burst pays one flush rather than one per write.

`Sync()` forces a barrier on demand under either mode, and `Close()` syncs before returning.
The [durability guide](/guides/durability/) covers when to pick which.

## Next

- The [configuration reference](/reference/configuration/) covers every option's default and the files kv writes on disk.
- The [server reference](/reference/server/) covers the Redis-protocol server.
