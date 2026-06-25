---
title: "Working from the command line"
description: "Driving kv from the shell: reading and writing keys, scanning, moving data in and out, the interactive shell, and inspecting a database's health."
weight: 70
---

The `kv` binary is a full client to the same database the library opens. This guide walks the everyday tasks from the shell; the [CLI reference](/reference/cli/) is the exhaustive command and flag list.

## The shape of a command

Almost every command takes the database file as its first argument:

```bash
kv <command> <db> [args] [flags]
```

So `kv get app.kv user:1` reads a key, `kv set app.kv user:1 alice` writes one. Keys and values are treated as raw bytes; when they are not printable text, the `--hex` and `--base64` flags let you pass and receive them in an encoding instead, and they work consistently across every command that takes a key or value.

## Reading and writing

The point operations are what you reach for first:

```bash
kv create app.kv               # make a new database
kv set app.kv k v              # upsert (its own committed transaction)
kv get app.kv k                # print the value
kv exists app.kv k             # exit 0 if present, 1 if absent
kv del app.kv k                # delete one key
kv del-range app.kv a m        # delete every key in [a, m)
kv merge app.kv counter 1      # fold an operand through the merge operator
```

`exists` setting its exit code rather than printing makes it drop straight into shell conditionals:

```bash
if kv exists app.kv user:1; then echo "present"; fi
```

For a value that is large or binary, read it from a file instead of the argument:

```bash
kv set app.kv blob --value-file ./payload.bin
```

## Scanning and counting

Because keys are ordered, you scan by prefix or by an explicit range:

```bash
kv scan app.kv --prefix user:           # everything under user:
kv scan app.kv --from a --to m          # the range [a, m)
kv scan app.kv --prefix user: --reverse --limit 10
kv count app.kv --prefix user:          # how many, without printing them
```

`scan` formats its output for whatever you are doing next: a readable table by default, or `-f jsonl`, `-f json`, or `-f raw` to feed a script. Add `--keys-only` when you only need the keys.

## Moving data in and out

For wholesale movement there are two pairs. `dump` and `load` are the lossless round trip, every pair as JSONL, which is how you copy a database or migrate it between engines:

```bash
# copy app.kv into a fresh LSM database
kv create ingest.kv --engine lsm
kv dump app.kv | kv load ingest.kv
```

`export` and `import` are for talking to other tools, in CSV, TSV, or JSONL:

```bash
kv export app.kv --format csv --output users.csv --prefix user:
kv import app.kv --format csv --input users.csv --key-col 1 --val-col 2 --batch 1000
```

`import` batches its writes so a large file loads in bounded transactions rather than one enormous one.

## The interactive shell

Run `kv` on a file with no subcommand and you get an interactive session on the open database, the way `sqlite3 app.db` does:

```
$ kv app.kv
kv 0.2.0  engine=btree  app.kv
kv> set user:1 alice
kv> scan --prefix user:
user:1	alice
kv> .pragma synchronous
full
kv> .help
kv> .quit
```

Inside the shell the data commands work without repeating the filename, and dot-commands like `.pragma`, `.help`, and `.quit` drive the session. Holding the file open across many commands is faster than re-opening it per invocation, so the shell is the right tool for an exploratory session.

## Checking on a database

A handful of commands report on health and accounting rather than data:

```bash
kv info app.kv      # human-readable summary: engine, size, version
kv stats app.kv     # space and durability accounting, as JSON
kv metrics app.kv   # the same numbers in Prometheus text format
kv check app.kv     # verify structural integrity
```

`check` walks the structure and confirms it is internally consistent, which is what you run if you suspect a problem or after recovering from one. `metrics` emitting Prometheus format means you can scrape a database file directly into a dashboard.

## Maintenance

Two commands keep a file in shape, both covered in depth in the [durability guide](/guides/durability/):

```bash
kv checkpoint app.kv --mode passive   # fold the WAL into the main file
kv vacuum app.kv                      # return free space to the OS
kv vacuum app.kv --full               # rebuild into a fresh, compact file
```

And `pragma` reads or sets a configuration knob on the file:

```bash
kv pragma app.kv synchronous          # read it
kv pragma app.kv synchronous=normal   # set it
kv pragma app.kv help                 # list every pragma
```

## Next

- The [CLI reference](/reference/cli/) lists every command and flag.
- The [configuration reference](/reference/configuration/) explains every pragma.
