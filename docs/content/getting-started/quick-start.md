---
title: "Quick start"
description: "From an empty editor to a key/value database: open it, set a key, get it back with a scratch buffer, delete it, and close. Then serve the same engine over the Redis protocol."
weight: 30
---

This walks the core loop in Go: open a database, set a key, read it back, delete it, and close.
Then a short section shows the same engine over the wire with the `kv` server and `redis-cli`.

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
	db, err := kv.Open("app.kv", kv.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
```

`Open` takes a path and an `Options` value.
The zero value `kv.Options{}` is valid: every field falls back to a default, so `kv.Open(path, kv.Options{})` just works.
`Open` creates the file if it does not exist and runs recovery if it does, so the same call works the first time and every time after.
`Close` syncs before it returns.

### 2. Set some keys

```go
db.Set([]byte("user:1"), []byte("alice"))
db.Set([]byte("user:2"), []byte("bob"))
```

`Set` does not return an error.
The write lands in the in-memory hot tier and, under the default durability, a background flusher fsyncs it a moment later.

### 3. Read it back

`Get` takes the key and a scratch buffer and returns three values: the value, whether the key was found, and an error.

```go
scratch := make([]byte, 0, 256)
v, ok, err := db.Get([]byte("user:1"), scratch)
if err != nil {
	log.Fatal(err)
}
if !ok {
	fmt.Println("user:1 is not present")
	return
}
fmt.Printf("user:1 = %s\n", v)
```

`Get` decodes the value into `scratch` and returns a slice aliased to it, so a hot loop can reuse one buffer and allocate nothing.
Pass `nil` if you would rather the engine allocate a fresh slice each call.
A missing key is `ok == false`, not an error, so you check `ok` rather than matching a sentinel.

### 4. Delete a key

```go
db.Delete([]byte("user:2"))
```

`Delete` does not return an error either.
A later `Get` on that key comes back with `ok == false`.

### 5. Close

`defer db.Close()` from step 1 runs a final sync and releases the file when `main` returns.
Call `db.Sync()` any time you want to force a durability barrier before then.

kv addresses one key at a time: there is no range scan or ordered iteration.
To enumerate a set of keys, keep your own list of them under a known key, or track them in your application.

## Over the wire

The `kv` binary serves one store over the Redis wire protocol, so any Redis client or `redis-cli` can drive the same engine:

```bash
# start the server on a TCP port, store under ./data/kv.db
kv --addr :6379 --dir ./data &

# talk to it with redis-cli
redis-cli -p 6379 set user:1 alice
redis-cli -p 6379 get user:1
```

```
alice
```

One of `--addr` or `--unixsocket` is required.
A unix socket is the faster local path, and it wins if you set both.
The [server guide](/guides/server/) covers the flags, the supported commands, and driving it from Redis client libraries.

## Where to go next

- The [guides](/guides/) cover the storage engine, durability, sizing a store, and running the server.
- The [library API reference](/reference/library/) lists every type and method; the [server reference](/reference/server/) lists every flag and supported command.
