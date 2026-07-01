---
title: "Sizing a store"
description: "How to size a kv store with Options: KeyCapacity as the main memory knob, HotBytes and HotKeys for the write buffer, ResidentBytes for the cold read window, and ReadCacheCells for the read cache."
weight: 40
---

The zero value `kv.Options{}` is valid, so you can open a store with no tuning and every field falls back to a sensible default.
For a small or medium store, that is the right call: open it and move on.
This guide is for when the store is large enough that the defaults leave performance on the table, and it walks the knobs in the order they matter.

## KeyCapacity: the main knob

`KeyCapacity` is the expected count of distinct keys, and it sizes the resident hash index.
It is the one knob worth setting for a large store.

The reason it matters most is that the index is the memory floor.
The index holds an entry for every key, a fingerprint and an offset, while the values spill to the file.
So the resident index scales with the number of keys, not the size of the values, and it stays resident for the life of the store.
Budget for it: if you expect fifty million keys, set `KeyCapacity` to fifty million so the index is sized up front rather than growing under you.

```go
db, _ := kv.Open("app.kv", kv.Options{
	KeyCapacity: 50_000_000,
})
```

The default is `1 << 20`, about a million keys.
Setting it too low is not a correctness problem, but a too-small index costs you as the store grows past the size it was built for, so size it to the distinct key count you actually expect.

## HotBytes and HotKeys: the write buffer

`HotBytes` is the size of one in-memory hot segment, the buffer writes land in before they migrate to the file.
The default is 8 MiB.
It bounds the resident write buffer, since at most two segments are live at once, and under the default durability it bounds the crash-loss window, since a crash loses at most the un-flushed hot records.
Raise it for a write-heavy store that can spare the RAM and tolerate a slightly larger loss window; lower it to tighten both.

`HotKeys` records how many keys one hot segment's index is sized for.
Set it when you know the average value size, so the segment index is right-sized for how many records fit in `HotBytes`.
A too-small `HotKeys` only causes the segment to seal earlier, never a lost write, so it is safe to leave alone.
Zero uses a heuristic derived from `HotBytes`.

```go
db, _ := kv.Open("app.kv", kv.Options{
	HotBytes: 32 << 20, // 32 MiB hot segments
	HotKeys:  200_000,  // sized for ~200k records per segment
})
```

## ResidentBytes: the cold read window

`ResidentBytes` is the cold log's resident tail window: how much recently-migrated cold data stays in RAM for fast reads after it leaves the hot tier.
The default is 64 MiB.
A larger window keeps more of the recently-touched cold data resident, so fewer reads fault a record in from the file; a smaller one holds a bigger database in less RAM at the cost of more cold-key disk reads.
Size it to how much of your working set lives just past the hot tier.

## ReadCacheCells: the read cache

`ReadCacheCells` is the number of cells in the read cache over cold reads, rounded up to a power of two.
The default is `1 << 16`.
It caches records read from the cold file so a repeat read of the same cold key does not fault it in again.
Raise it when your reads repeatedly hit a set of cold keys larger than the resident window; the default suits most stores.

## Putting it together

For most stores, set `KeyCapacity` to your expected distinct key count and leave the rest at their defaults:

```go
db, _ := kv.Open("app.kv", kv.Options{
	KeyCapacity: 50_000_000,
})
```

Reach for `HotBytes`, `HotKeys`, `ResidentBytes`, and `ReadCacheCells` only when a large or write-heavy store shows you a reason to, and change one at a time so you can see what each does.

## Next

- The [storage engine guide](/guides/storage-engine/) explains the hot tier, cold log, and resident index these knobs size.
- The [configuration reference](/reference/configuration/) lists every option with its default in one table.
