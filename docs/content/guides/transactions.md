---
title: "Transactions and isolation"
description: "How kv runs reads and writes inside ACID transactions, what snapshot isolation guarantees, when to reach for serializable, and how to handle conflicts."
weight: 10
---

Everything in kv happens inside a transaction. This guide covers the two closure forms, the explicit form, what isolation you get, and how conflicts surface and retry.

## The two closures

Most code never constructs a transaction by hand. `View` and `Update` take a closure and run it inside one:

```go
// Read-only, at a consistent snapshot.
err := db.View(func(txn *kv.Txn) error {
	v, err := txn.Get([]byte("balance:alice"))
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", v)
	return nil
})

// Read-write, committed atomically on a nil return.
err = db.Update(func(txn *kv.Txn) error {
	return txn.Set([]byte("balance:alice"), []byte("100"))
})
```

The rule that makes this safe: an `Update` commits if and only if the closure returns nil. Return an error and every write the closure made is discarded. Panic and the transaction is rolled back as the stack unwinds. There is no half-applied state to clean up.

`UpdateVersion` is `Update` that also returns the commit version, the monotonically increasing number kv stamps on each committed write, useful when you want to record or compare what version a change landed at:

```go
version, err := db.UpdateVersion(func(txn *kv.Txn) error {
	return txn.Set([]byte("k"), []byte("v"))
})
```

## What a transaction can do

Inside the closure, `txn` is the handle for all data access:

| Method | Purpose |
| --- | --- |
| `Get(key)` | Value at a key, or `ErrNotFound`. The bytes are valid until the transaction ends. |
| `GetCopy(key)` | Like `Get` but returns a copy you own past the transaction. |
| `Exists(key)` | Presence check without fetching the value. |
| `Set(key, value)` | Upsert. |
| `SetWithTTL(key, value, ttl)` | Upsert that expires after `ttl`. |
| `Delete(key)` | Remove one key. |
| `DeleteRange(lo, hi)` | Remove every key in `[lo, hi)` in one operation. |
| `Merge(key, operand)` | Fold an operand into a key through the registered merge operator. |
| `NewIterator(opts)` | A snapshot-consistent iterator over a range or prefix. |

`Get` returns bytes that point into the database's buffers and stay valid only until the transaction ends. If you need to keep a value after the closure returns, use `GetCopy`, or copy it yourself.

## Snapshot isolation, the default

A kv transaction reads from a snapshot: the committed state of the database at the moment the transaction began. Nothing another transaction commits while yours runs changes what yours sees. That is snapshot isolation, and it gives you a clean, intuitive guarantee for free: a read-only transaction always sees a single consistent point in time, however long it runs and however much write traffic is happening around it.

Snapshot isolation permits exactly one anomaly, called write skew: two transactions read an overlapping set of keys, each writes a different key based on what it read, and both commit, because neither wrote what the other read. The classic example is two on-call schedulers each checking "at least one person is on call" and each removing a different person. Both see two people, both remove one, and the invariant is violated.

If your writes do not have that read-decides-a-disjoint-write shape, snapshot isolation is all you need, and it is the faster choice.

## Serializable, when you need it

Open the database with `WithIsolation(kv.Serializable)` and kv closes write skew too:

```go
db, err := kv.Open("app.kv", kv.WithIsolation(kv.Serializable))
```

Under serializable isolation, kv tracks each read-write transaction's read set and validates at commit that nothing it read was changed by a transaction that committed in the meantime. If something was, the commit fails with `ErrConflict` rather than allowing the anomaly. The result is as if the transactions ran one at a time, in some order. The cost is the read-set tracking and a higher conflict rate under contention, which is why it is opt-in.

## Conflicts and retries

When two read-write transactions genuinely conflict, one of them must lose. The loser's commit returns `kv.ErrConflict`. With the closure form, you do not handle this yourself: `Update` catches the conflict and re-runs your closure on a fresh snapshot, up to a bound you set with `WithMaxRetries`. Because the closure runs again from the top, it must be idempotent in the sense of recomputing its writes from what it reads, which is the natural way to write one anyway:

```go
// Safe to retry: it reads the current value and writes a derived one.
db.Update(func(txn *kv.Txn) error {
	v, err := txn.Get([]byte("counter"))
	if err != nil && !errors.Is(err, kv.ErrNotFound) {
		return err
	}
	n := parse(v) + 1
	return txn.Set([]byte("counter"), format(n))
})
```

If you exhaust the retry bound, `Update` returns the last `ErrConflict` so you can back off or surface it.

## Explicit transactions

When control flow does not fit a closure, for example an interactive session that interleaves reads and writes across several steps, use `Begin`:

```go
txn := db.Begin(true) // true = writable
defer txn.Discard()   // a no-op once Commit succeeds

v, err := txn.Get([]byte("k"))
// ... arbitrary logic ...
if err := txn.Set([]byte("k"), next(v)); err != nil {
	return err
}
return txn.Commit()
```

You own the lifecycle: `Commit` applies the writes (and may return `ErrConflict`, which you retry yourself), and `Discard` releases the snapshot. Always `Discard`, typically with `defer`; it is harmless after a successful `Commit` and essential on every other path, because an abandoned transaction holds its snapshot open and prevents space from being reclaimed.

## Long-lived snapshots

A `Snapshot` pins a read version you can reuse across many separate read transactions, so a batch job sees one stable view for its whole run without holding a single transaction open:

```go
snap := db.Snapshot()
defer snap.Close()

snap.View(func(txn *kv.Txn) error { /* reads at the pinned version */ return nil })
snap.View(func(txn *kv.Txn) error { /* same version, later */ return nil })
```

A pinned snapshot, like an open transaction, holds back the versions it can see, so close it as soon as the job is done.

## Next

- [Choosing an engine](/guides/engines/) covers how the B-tree and LSM cores differ under your transaction load.
- [Durability](/guides/durability/) covers what "committed" means when the power goes out.
