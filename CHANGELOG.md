# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased]

### Changed

- The shipped default durability is now `SyncNormal` (group commit) instead of
  `SyncFull`. kv fdatasyncs the WAL at each checkpoint and on a short timer rather than on
  every commit, so out-of-box write throughput is tens of thousands of commits per second
  instead of the few hundred per second an fsync-on-every-commit default gives. This is the
  trade SQLite WAL with `synchronous=NORMAL`, badger, pebble and rocksdb all default to. A
  power failure never corrupts the file; the only thing it can lose is the last sub-second
  of acknowledged commits. If you need zero acked-commit loss, open with
  `WithSynchronous(kv.SyncFull)` or set `synchronous=full`; that path is unchanged and one
  option away. No on-disk format change.

---

## [0.3.0] — 2026-06-30

A consolidation release and a deliberate breaking change. kv had shipped two storage
cores and let you pick one per database; benchmarking settled the question, so the
project now runs a single engine, f2, a sharded hash index over a self-durable hybrid
log, and the rest was removed to keep the surface small. A database created by 0.1.0 or
0.2.0 does not open in 0.3.0; the f2 on-disk format is the one the project carries
forward.

### Added

- A Redis (RESP) face for `kv serve`, bound with `-resp-addr` or `-resp-unixsocket`, so a
  redis client, library, or benchmark drives the same database as the HTTP and binary
  faces. It serves the string keyspace (`GET`, `SET`, `DEL`, `EXISTS`, `PING`, the
  `HELLO` handshake for RESP2 and RESP3, and `DBSIZE`) over one writer shared with the
  other faces. The wire loop is adapted from `tamnd/aki`, reworked over kv's API.
- An in-memory mode: `kv.OpenMem` and `kv serve :memory:` run the whole stack in RAM with
  nothing written to disk, for a throwaway cache or a benchmark that wants the engine
  without the file.

### Changed

- f2 is the only engine and the default. A get is a hash and one record read, so read
  latency stays flat as the database grows, and the index stays compact at roughly 10 to
  13 bytes per key regardless of key length, which keeps a billion keys near 15 GiB.
- The hybrid log bounds resident memory with `WithCacheSize` and faults cold pages in from
  the file by offset, so a database can run many times larger than the memory it holds.

### Removed

- The B-tree and LSM cores and the engine selector: `WithEngine`, `kv.BTree`, `kv.LSM`,
  and the per-core tuning options (`WithMemtableSize`, `WithLevelRatio`, `WithFilter`,
  `WithRangeIndex`, `WithValueSeparation`, `WithCompression`, `WithColdCompression`,
  `WithFillFactor`, `WithMaxInlineValue`, `WithBtreeBuffers`).
- Ordered iteration, which f2 does not keep keys in order to provide: `Txn.NewIterator`,
  `IterOptions`, the `Iterator` type, and `Txn.DeleteRange`, plus the CLI's `scan`,
  `count`, `dump`, and `export`. The operations are the point ones: get, set, exists,
  delete, and merge.

### Upgrading

A database from before 0.3.0 cannot be opened; recreate it on 0.3.0 and reload from the
source you loaded it from. If you relied on scans or the LSM core, 0.2.0 remains
available, and the pre-consolidation code is preserved in the `tamnd/kv-v1` repository.

---

## [0.2.0] — 2026-06-25

A performance and ergonomics release. Drop-in over 0.1.0: the on-disk format is unchanged
and the API is purely additive, so an existing database and existing code both carry
forward untouched.

### Added

- `db.Get(key)` reads one key at the latest committed state without opening a transaction
  and returns a copy the caller owns. It is the lightest path for a single-key read that
  does not need to agree with other reads; use `View` or `Snapshot` when several reads must
  see one consistent version.

### Changed

- Tightened the read, scan, and write hot paths across the engine and transaction layers:
  lower per-operation overhead on point reads and iteration, and fewer cycles per page
  touched in the buffer pool, which also helps the out-of-cache regime.

### Fixed

- The B-tree interior-node decoder now verifies its checksum trailer on the path that
  previously skipped it, so a corrupt interior page returns `ErrCorrupt` instead of going
  unnoticed.

---

## [0.1.0] — 2026-06-23

First public release. The library, CLI, and server are feature-complete and the on-disk
format is fixed. The 0.x series keeps the API broadly stable while the surface settles
toward a 1.0 commitment; the file format written by 0.1.0 is forward-compatible.

This first release lands the full M0–M8 build: the two-engine core, MVCC transactions,
the WAL and crash recovery, the CLI, the HTTP/binary server, encryption, replication,
and the hardening campaign that closed it out. The sections below summarize each
milestone that shipped into this release.

### M8 — Hardening

- File-format fuzzing: feeds mutated `.kv` bytes through `Open` via a mem-VFS.
  Found and fixed a real B-tree decoder panic on type-confused pages (bounds-checked
  `nodeReader` + page-type guard in `loadLeaf`/`loadInterior`).
- Operation fuzzing vs model oracle: drives a randomized program of ops against the real
  DB and a Go-map oracle. Found and fixed a range-delete persistence bug where two
  `DeleteRange` calls sharing the same `lo` key in one transaction could collide on the
  marker-cell key, resurrecting the narrower range after checkpoint/reopen.
- WAL recovery fuzzing: feeds mutated WAL bytes into the recovery path. Hardened a
  checkpoint-frame over-read (`payload<8` torn tail).
- LSM compaction fuzzing: exercises flush + compaction + checkpoint + reopen cycles.
- Concurrency stress: 6 tests — `TestConcurrentReadWrite`, `TestConcurrentScanVsWrite`,
  `TestMaintenanceDuringWrites`, `TestMaintenanceDuringWritesLSM`,
  `TestConcurrentTransactionConflicts`, `TestSoak` (5 s, 727k writes, zero errors).
- Runtime engineering: pooled flate decompression reader via `flate.Resetter`
  (eliminates per-page inflate-state alloc on the read/compaction path); pooled
  `bytes.Buffer` on the compression side; 64-byte cache-line pad between
  `oracle.appliedVersion` (written under mutex) and `oracle.appliedPub` (lock-free
  atomic) to prevent cross-core false sharing.
- Durability pragmas: `full_page_writes` (page pre-image logging before checkpoint
  writes, now fully wired through `FramePageImage`), `auto_vacuum` (`TruncateTail` after
  checkpoint), `commit_linger_us` (group-commit leader sleep window). New header fields
  at offsets 108–113.
- `kv watch`: streams committed changes under a prefix as JSONL, powered by `Subscribe`.
- `kv import` / `kv export`: CSV, TSV, and JSONL interchange formats.
- Public engine-tuning options: `WithMemtableSize`, `WithCompression`,
  `WithColdCompression`, `WithFilter`, `WithRangeIndex`, `WithValueSeparation`.
- Runtime PRAGMAs: `synchronous`, `wal_autocheckpoint`, `cache_size` (live, no reopen).
- Benchmark acceptance results updated to reflect Phase 3 perf campaign.
- `kv.Version` constant added to the public package.
- Comprehensive README: quick start, full API reference, engine guide, durability table,
  isolation table, encryption, replication, server, CLI, pragma examples, configuration
  reference, error types, performance notes, and the stability statement.
- Documentation site at [kv.tamnd.com](https://kv.tamnd.com): getting-started, guides,
  and the full reference, built with the tago-doks theme and deployed to Cloudflare Pages
  and GitHub Pages.

### M7 — Server (v0.9)

- `kv serve`: HTTP/JSON + pure-Go binary protocol, single binary, zero external
  dependencies.
- HTTP: unary CRUD, streaming NDJSON scan, SSE watch, `/metrics`, `/healthz`.
- Binary: unary ops, streaming scan/watch, interactive multi-statement transactions.
- Auth: token, mTLS, JWT/OIDC (`JWTAuthenticator`, `StaticKeySet`, `RemoteKeySet`).
- Per-prefix ACL (`Identity.Grants`), rate/connection limits, graceful shutdown.
- TLS and mTLS transport.

### M6 — Operability (v0.8)

- Encryption at rest: AES-256-GCM page and WAL envelope, online key rotation via
  `db.RotateEncryptionKey`.
- Observability: Prometheus metrics, per-op counters, commit-latency histogram,
  engine-internal signals (LSM level stats, compaction backlog), reader-age metric,
  structured logging, slow-op log, tracing hooks (`Tracer`/`Span` seam for
  OpenTelemetry or any tracer).
- Physical backup: `db.Backup` / `kv.RestoreBackup` + `kv backup` / `kv restore`.
- WAL shipping: `WithWALArchive` sink on the primary, `WithReadReplica` + `ApplyWAL` on
  the follower.
- Point-in-time recovery: `WALArchive` + `ApplyWALUntil`.
- `db.Subscribe`: Badger-style change-feed callback (blocked goroutine, buffered channel,
  `ErrSubscriberLagged` on slow consumer).

### M5 — Engine maturity (v0.7)

- Ribbon filter option (`WithFilter(Ribbon)`): cache-efficient replacement for Bloom.
- Bε write buffers on the B-tree: interior-node pivot buffers defer child pages until
  the node's buffer fills, batching random writes into sequential I/O.
- Heat-tiered block compression: fast DEFLATE on hot LSM levels, high DEFLATE on cold.
  Codec self-described per page; LZ4/Zstd drop in without format change.
- Benchmark suite: YCSB-A/B/C/D workloads + recovery + durability sweep + out-of-cache
  regime; JSON report with regression compare; pprof profiling integration.
- Performance Phase 1: sharded buffer pool (2.42x read throughput at 8 readers),
  group-commit (one shared fsync/leader: 4.1x fillrandom at 8 writers, 14.2x at 32),
  lock-free read snapshot (oracle mutex contention 3.73%→0 at 32 readers), recycled
  conflict maps.
- Performance Phase 2: leaf binary search, decoded-node cache on buffer frame,
  single-fetch B-tree descent, lock-free cache-hit pin, zero-copy value on the read
  path, streaming iterators, LSM background flush + auto-compaction, immutable
  segment-list snapshot, slotted-page in-place B-tree edits, pooled WAL frame + fused
  batch encode, chunked arena, lock-free skiplist, parallel group apply, lock-free LSM
  read snapshot of the active memtable.
- Performance Phase 3: biased B-tree leaf split for sequential inserts, lazy TTL clock,
  F_BARRIERFSYNC intermediate sync level, cold-only compression policy, auto version GC
  after checkpoint, checkpoint I/O off the foreground write lock.

### M4 — LSM core (v0.6)

- Single-file segment model (SQLite4 lsm1 page-range extents), MANIFEST, embedded
  freelist.
- Arena skip-list memtable, seal/flush, L0..Ln levels, leveled compaction.
- Bloom filters + Monkey bit allocation; filter skipping on the read path.
- LSM `RecoverFinished`: MANIFEST replay, orphaned-compaction reclamation.
- Fluid leveled compaction policy (tiered/lazy-leveling default), lazy-leveling
  (tiered largest level).
- WiscKey value separation: vLog flush, value deref on reads, GC.
- REMIX range index for the scan path.
- Engine-equivalence-under-crash: both engines pass the same conformance + crash suites.

### M3 — Library API + CLI (v0.5)

- Merge operators, TTL (`SetWithTTL`), bulk-load builder (`WriteBatch`), long-lived
  `Snapshot`, typed errors, `Subscribe`.
- Full CLI: `get`/`set`/`del`/`scan`/`count`/`dump`/`load`/`checkpoint`/`vacuum`/
  `pragma`/`check`/`info`/`stats`/`serve` + interactive shell; exit codes 0–8.
- Checkpointing: passive/full/truncate modes, auto-checkpoint, freelist reuse,
  incremental/full vacuum.
- Verifier: corruption-class tests for all 6 B-tree fault classes.
- Crash-injection harness: `vfs.Mem.CrashAfterSync(n)` proves the durable-prefix
  property at every sync boundary.
- `db.ErrFatalSync` write fence (fsyncgate): poisons the DB on sync failure; maps to
  public `ErrNeedsRecovery`.
- Interrupted-checkpoint crash test.
- Change-feed `Subscribe` + `ErrSubscriberLagged` + `ErrClosed`.

### M2 — MVCC, transactions, iterators (v0.4)

- Versioned internal keys end-to-end; watermark oracle (Badger lineage).
- `View`/`Update` / explicit `Begin`/`Commit`; conflict retry.
- Cursor protocol: forward/reverse, bounds/prefix, range-delete visibility.
- Version GC tied to the readMark.
- Serializable isolation: read-set validation at commit.

### M1 — Pager, B-tree, WAL, crash recovery (v0.3)

- Pager + 2Q buffer pool (sharded, arena-backed, OLC on the B-link tree).
- B-tree core: in-place B-link B+tree, search/insert/split/delete, OLC.
- WAL: frame format, write-ahead rule, group commit, four `Sync` levels, full-page
  images, `fdatasync` group batching.
- Crash recovery: durable-tail scan, logical redo, page-LSN idempotency.

### M0 — Skeleton and seam (v0.2)

- Module (`github.com/tamnd/kv`), zero external dependencies, cross-compile
  (`CGO_ENABLED=0`), CI.
- File format: header (magic, version, page size, engine kind, freelist root,
  application_id, user_version), slotted pages, encoding primitives, round-trip + CRC
  tests.
- Engine SPI (`engine.Engine`, `engine.Txn`, `engine.Iterator`, `VerifyReport`).
- Model engine oracle (conformance harness from day one).
- VFS seam: `osfs` backend + fault-injecting `vfs.Mem` (sync counting, crash injection).
