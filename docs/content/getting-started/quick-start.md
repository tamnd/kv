---
title: "Quick start"
description: "From an empty editor to a transactional, ordered key/value database: open it, write in a transaction, read it back, and scan a prefix, in both Go and the CLI."
weight: 30
---

This walks the core loop twice, once from Go and once from the shell, so you can see that the CLI is the same database the library is. By the end you will have written keys in a transaction, read them back, and scanned a range.

## In Go

### 1. Open a database

```go
package main

import (
	"fmt"
	"log"

	"github.com/tamnd/kv"
)

func main() {
	db, err := kv.Open("app.kv")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
```

`Open` creates the file if it does not exist and runs crash recovery if it does, so the same call works the first time and every time after. The default engine is the read-optimised B-tree; pass `kv.WithEngine(kv.LSM)` to create a write-optimised one instead. The choice is fixed at creation and remembered in the file.

### 2. Write in a transaction

```go
err = db.Update(func(txn *kv.Txn) error {
	if err := txn.Set([]byte("user:1"), []byte("alice")); err != nil {
		return err
	}
	return txn.Set([]byte("user:2"), []byte("bob"))
})
if err != nil {
	log.Fatal(err)
}
```

`Update` runs the closure inside a read-write transaction and commits it atomically when the closure returns nil. Both `Set`s land together or, if anything returns an error, neither does. If a concurrent writer causes a conflict, kv retries the closure automatically.

### 3. Read it back

```go
err = db.View(func(txn *kv.Txn) error {
	v, err := txn.Get([]byte("user:1"))
	if err != nil {
		return err
	}
	fmt.Printf("user:1 = %s\n", v)
	return nil
})
```

`View` runs a read-only transaction at a consistent snapshot. A missing key returns `kv.ErrNotFound`, which you match with `errors.Is`:

```go
v, err := txn.Get([]byte("absent"))
if errors.Is(err, kv.ErrNotFound) {
	// not there
}
```

### 4. Scan a prefix

Because keys are ordered, every key under `user:` is a contiguous range:

```go
db.View(func(txn *kv.Txn) error {
	it, err := txn.NewIterator(kv.IterOptions{Prefix: []byte("user:")})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		v, _ := it.Value()
		fmt.Printf("%s = %s\n", it.Key(), v)
	}
	return it.Error()
})
```

```
user:1 = alice
user:2 = bob
```

## At the shell

The same four steps, with no code:

```bash
# 1. Create the database
kv create app.kv

# 2. Write two keys (each set is its own committed transaction)
kv set app.kv user:1 alice
kv set app.kv user:2 bob

# 3. Read one back
kv get app.kv user:1
# alice

# 4. Scan the prefix
kv scan app.kv --prefix user:
```

```
user:1	alice
user:2	bob
```

Run `kv app.kv` with no subcommand and you drop into an interactive shell on the open file, the way `sqlite3 app.db` does:

```
$ kv app.kv
kv 0.1.0  engine=btree  app.kv
kv> scan --prefix user:
user:1	alice
user:2	bob
kv> .exit
```

## Where to go next

- The [guides](/guides/) cover transactions, choosing an engine, durability, encryption, backup and replication, and running the server.
- The [library API reference](/reference/library/) lists every type and method; the [CLI reference](/reference/cli/) lists every command and flag.
