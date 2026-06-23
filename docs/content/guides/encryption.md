---
title: "Encryption at rest"
description: "Encrypting a kv database with AES-256-GCM, how the key is supplied and how pages are protected, and how to rotate the key without taking the database offline."
weight: 40
---

kv can encrypt a database so that the file on disk reveals nothing without the key. This guide covers turning it on, what is and is not protected, and rotating the key.

## Turning it on

Encryption is a create-time decision. You supply a 32-byte master key when the file is first created, and you must supply the same key on every subsequent `Open`:

```go
key := loadKey() // exactly 32 bytes, from a KMS, a file, an env var, wherever you keep secrets

// First time: creates an encrypted database.
db, err := kv.Open("secrets.kv", kv.WithEncryptionKey(key))

// Every time after: the same key is required.
db, err = kv.Open("secrets.kv", kv.WithEncryptionKey(key))
```

The cipher is AES-256-GCM, an authenticated cipher, so every page is both encrypted and tamper-checked. On modern CPUs this runs on the AES-NI hardware path, so the overhead is small. Each page carries 28 bytes of cipher metadata (a nonce, the authentication tag, and a key epoch), which is the only on-disk cost.

Because encryption is fixed at creation, the key handling has three guardrails that surface as distinct errors:

| Situation | Error |
| --- | --- |
| The file is encrypted and you opened it with no key | `kv.ErrEncryptionKeyRequired` |
| You opened an encrypted file with the wrong key | `kv.ErrWrongKey` |
| You supplied a key for a file that was created unencrypted | `kv.ErrKeyOnPlaintext` |

`ErrWrongKey` is the authenticated cipher doing its job: a wrong key cannot decrypt and authenticate a page, so kv refuses to open rather than handing back garbage.

## What is protected

Encryption covers the data at rest: the pages of the main file, including keys, values, and the internal index structure, plus the write-ahead log and backups taken with `Backup`. A backup of an encrypted database is itself encrypted, so it is safe to ship a backup to less-trusted storage.

Encryption does not change the in-memory picture. Once a page is read, it is decrypted in memory and your program sees plaintext, which is the point. It also does not authenticate the path the key took to reach your process; supplying the key safely (a KMS, a mounted secret, an environment variable scrubbed from logs) is your application's responsibility. kv only promises that without the key, the file is opaque and tamper-evident.

## Rotating the key

A long-lived encrypted database should be able to re-key without a full rewrite. kv uses envelope encryption: the master key you supply through `WithEncryptionKey` wraps an internal data-encryption key, and that inner key is the one pages are actually encrypted with. `RotateEncryptionKey` rotates the inner key:

```go
err := db.RotateEncryptionKey()
```

It advances the database to a new key epoch and re-encrypts lazily and incrementally as pages are written, rather than blocking to rewrite the whole file at once. Old pages remain readable under their original epoch until they are next written, at which point they move to the new epoch. From your code's perspective the call returns quickly and the database keeps serving; the migration happens in the background as normal write traffic touches pages.

Because rotation advances the inner key, the master key you pass to `Open` does not change. Rotating the key of a database that was never encrypted returns `kv.ErrNotEncrypted`, since there is nothing to rotate.

## A note on the CLI

Encryption is currently a library feature. The CLI and server operate on the database through the same engine, but supplying the key is done in Go through `WithEncryptionKey`, not through a command-line flag, so that the key never lands in shell history or a process listing. If you run encrypted databases, drive them from a small Go program or embed the server in one that holds the key.

## Next

- [Backup and replication](/guides/backup-and-replication/) covers moving an encrypted database safely off the box.
- The [library reference](/reference/library/) lists the encryption methods and their errors.
