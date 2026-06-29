---
title: "Quick start"
description: "From an empty editor to a transactional key/value database: open it, write in a transaction, and read it back, in both Go and the CLI."
weight: 30
---

This walks the core loop twice, once from Go and once from the shell, so you can see that the CLI is the same database the library is. By the end you will have written keys in a transaction and read them back.

## In Go

### 1. Open a database

```go
package main

import (
	"errors"
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

`Open` creates the file if it does not exist and runs crash recovery if it does, so the same call works the first time and every time after. An empty path opens a memory-only database that is discarded on close, which is handy for tests.

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

For a single key, `Get` is the shortest path:

```go
v, err := db.Get([]byte("user:1"))
if err != nil {
	log.Fatal(err)
}
fmt.Printf("user:1 = %s\n", v)
```

A missing key returns `kv.ErrNotFound`, which you match with `errors.Is`:

```go
v, err := db.Get([]byte("absent"))
if errors.Is(err, kv.ErrNotFound) {
	// not there
}
```

When several reads have to agree on one state, do them inside a `View` instead, which runs a read-only transaction at a consistent snapshot. Nothing another writer commits while the closure runs changes what it reads:

```go
err = db.View(func(txn *kv.Txn) error {
	a, err := txn.Get([]byte("user:1"))
	if err != nil {
		return err
	}
	b, err := txn.Get([]byte("user:2"))
	if err != nil {
		return err
	}
	fmt.Printf("%s and %s\n", a, b)
	return nil
})
```

kv addresses one key at a time: there is no range scan or ordered iteration. To enumerate a set of keys, keep your own list of them under a known key, or track them in your application.

## At the shell

The same steps, with no code:

```bash
# 1. Create the database
kv create app.kv

# 2. Write two keys (each set is its own committed transaction)
kv set app.kv user:1 alice
kv set app.kv user:2 bob

# 3. Read one back
kv get app.kv user:1
```

```
alice
```

Run `kv app.kv` with no subcommand and you drop into an interactive shell on the open file, the way `sqlite3 app.db` does:

```
$ kv app.kv
kv 0.3.0  engine=f2  app.kv
kv> get user:1
alice
kv> .exit
```

## Where to go next

- The [guides](/guides/) cover transactions, durability, encryption, backup and replication, and running the server.
- The [library API reference](/reference/library/) lists every type and method; the [CLI reference](/reference/cli/) lists every command and flag.
