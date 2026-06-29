---
title: "CLI reference"
description: "Every kv subcommand and its flags, grouped by what it does, plus the shared byte-encoding flags and the interactive shell."
weight: 20
---

The `kv` binary is a complete client to a database file. Every command except the interactive shell takes the database path as its first argument:

```
kv <command> <db> [args] [flags]
```

Run `kv --version` (or `kv version <db>`) to print the build and library version. For task-oriented walkthroughs, see [working from the command line](/guides/command-line/).

## Byte-encoding flags

Keys and values are raw bytes. Commands that take or print them accept `--hex` and `--base64` to pass and receive non-text bytes in an encoding; they apply consistently wherever a key or value crosses the command line.

## Data commands

| Command | Purpose | Notable flags |
| --- | --- | --- |
| `create <db>` | Create a new database file. | `--page-size N` |
| `get <db> <key>` | Print the value for a key. | `--hex`, `--base64`, `--raw` |
| `set <db> <key> [value]` | Upsert a key to a value. | `--hex`, `--base64`, `--value-file F` |
| `del <db> <key>` | Delete one key. | `--hex`, `--base64` |
| `exists <db> <key>` | Exit 0 if present, 1 if absent. | `--hex`, `--base64` |
| `merge <db> <key> <operand>` | Apply the registered merge operator. | `--hex`, `--base64` |

Every data command addresses one key: kv has no range scan or ordered iteration. `set` reads its value from `--value-file` when the value is large or binary, instead of from the argument. `get --raw` writes the value bytes with no formatting, for piping.

## Bulk loading

| Command | Purpose | Notable flags |
| --- | --- | --- |
| `load <db>` | Bulk-load JSONL pairs from stdin or a file. | `--input F`, `--hex`, `--base64` |
| `import <db>` | Import pairs from CSV, TSV, or JSONL. | `--format csv\|tsv\|jsonl`, `--input F`, `--key-col N`, `--val-col N`, `--batch N` |

`load` reads the JSONL pair stream kv writes and is the efficient path for restoring a known key set. `import` ingests CSV, TSV, or JSONL from other tools; `import --batch` bounds the transaction size so a large file loads incrementally.

## Maintenance and durability

| Command | Purpose | Notable flags |
| --- | --- | --- |
| `checkpoint <db>` | Fold the WAL into the main file. | `--mode passive\|full\|restart\|truncate` |
| `vacuum <db>` | Return trailing free pages to the OS. | `-n pages`, `--incremental` |
| `pragma <db> <name>[=<value>]` | Read or set a configuration knob. | `kv pragma <db> help` lists all |

`vacuum` returns trailing free pages to the OS; `-n` bounds how many it reclaims this round, and an empty or zero `-n` reclaims the whole trailing free run. See the [durability guide](/guides/durability/) and the [configuration reference](/reference/configuration/).

## Backup and replication

| Command | Purpose | Notable flags |
| --- | --- | --- |
| `backup <db>` | Stream a consistent physical image. | `--output F` |
| `restore <db>` | Rebuild from a backup stream. | `--input F` |
| `ship <db>` | Stream the current WAL generation as a delta. | `--output F` |
| `replay <db>` | Apply a shipped WAL delta to a follower. | `--input F`, `--until V` |

`backup`/`restore` are the one-shot copy; `ship`/`replay` keep a replica current, and `replay --until V` stops at a version for point-in-time recovery. See the [backup guide](/guides/backup-and-replication/).

## Inspection

| Command | Purpose | Notable flags |
| --- | --- | --- |
| `info <db>` | Human-readable summary: engine, size, commit version. | |
| `stats <db>` | Space and durability accounting, as JSON. | |
| `metrics <db>` | Observability metrics in Prometheus text format. | |
| `check <db>` | Verify structural integrity. | `-f table\|json` |
| `watch <db>` | Stream committed changes as JSONL. | `--prefix P` |

## Server

| Command | Purpose |
| --- | --- |
| `serve <db>` | Serve the database over HTTP/JSON and an optional binary protocol. |

`serve` carries an extensive set of flags for the listen addresses, authentication, TLS and mTLS, and rate and connection limits. The [server guide](/guides/server/) walks them; the highlights are `--addr` (default `:8480`), `--binary-addr`, `--auth-file` or the `--jwt-*` family, `--tls-cert`/`--tls-key`/`--tls-client-ca`, and the `--max-*` and `--rate*` limits.

## Interactive shell

Run `kv <db>` with no subcommand on an existing file to open an interactive session:

```
$ kv app.kv
kv 0.3.0  engine=f2  app.kv
kv> set user:1 alice
kv> get user:1
kv> .info
kv> .help
kv> .quit
```

Data commands work without repeating the filename, and dot-commands (`.info`, `.stats`, `.help`, `.quit`, and friends) drive the session.
