# kv benchmark results

Apple Silicon developer machine, macOS 15.7.8, 2026-06-21. Not the NVMe reference machine; read these for tradeoff direction and honesty targets.

These numbers come from the in-repo harness (`bench/`) running every workload on both engines.
They are produced on the developer machine disclosed below, not the NVMe reference machine the spec 21 §6 absolute targets are stated against, so the figures to read here are the *shape* of the B-tree/LSM tradeoff and the honesty targets, both of which are machine-independent in direction.
Regenerate them with `go run ./cmd/bench`.

## Setup

- Machine: darwin/arm64, 10 CPU, go1.26.4
- Keys: 20000 at 24B key / 64B value; ops: 20000; concurrency: 1; batch: 100
- Durability: full; seed: 1

## Per-workload numbers

| workload | engine | throughput (ops/s) | read p99 | write p99 | space-amp | write-factor | read-ios/op |
|---|---|---:|---:|---:|---:|---:|---:|
| bulk-load | btree | 19107 | n/a | 96.1us | 1.00 | 3.38 | - |
| write-saturated | btree | 298 | n/a | 4.94ms | 1.00 | 4.57 | - |
| ycsb-a | btree | 546 | 59.3us | 7.95ms | 1.00 | 4.15 | 0.00 |
| ycsb-b | btree | 5984 | 40.5us | 4.05ms | 1.00 | 3.44 | 0.00 |
| ycsb-c | btree | 249124 | 10.3us | n/a | 1.00 | 3.38 | 0.00 |
| ycsb-d | btree | 6418 | 26.4us | 4.04ms | 1.00 | 3.50 | 0.00 |
| ycsb-e | btree | 198 | 8.33ms | n/a | 1.00 | 3.38 | 0.00 |
| ycsb-f | btree | 299 | 5.10ms | 5.10ms | 1.00 | 5.03 | 0.00 |
| bulk-load | lsm | 23639 | n/a | 53.5us | 1.00 | 1.14 | - |
| write-saturated | lsm | 307 | n/a | 4.59ms | 1.00 | 3.18 | - |
| ycsb-a | lsm | 585 | 461.7us | 5.06ms | 1.00 | 2.16 | 0.00 |
| ycsb-b | lsm | 5872 | 40.0us | 4.36ms | 1.00 | 1.24 | 0.00 |
| ycsb-c | lsm | 1240887 | 2.5us | n/a | 1.00 | 1.14 | 0.00 |
| ycsb-d | lsm | 5723 | 10.5us | 4.22ms | 1.00 | 1.24 | 0.00 |
| ycsb-e | lsm | 429 | 3.75ms | n/a | 1.00 | 1.14 | 0.00 |
| ycsb-f | lsm | 286 | 5.33ms | 5.33ms | 1.00 | 3.18 | 0.00 |

## Tradeoff and targets

- **Read latency reference (B-tree, YCSB-C cache-resident)** — yes
  - Claim: B-tree cache-resident read p99 is microsecond-class with no extra page I/O
  - Observed: btree p99 10.3us at 0.00 read-ios/op
  - Holds here: yes
- **Write amplification (LSM below B-tree, write-saturated)** — yes
  - Claim: LSM write-factor is at or below the B-tree's: log-structured writes cost less per op
  - Observed: lsm write-factor 3.18 vs btree 4.57
  - Holds here: yes
- **Bulk ingest (LSM at or above B-tree, un-fsync-pinned)** — yes
  - Claim: with batches not pinned to per-op fsync, LSM ingest is at or above the B-tree's
  - Observed: lsm 23639 ops/s vs btree 19107 ops/s
  - Holds here: yes
- **No silent drops (spec 21 §3)** — yes
  - Claim: every offered operation is accounted for across the whole suite
  - Observed: 0 dropped operations across 16 cells
  - Holds here: yes

## Garbage collection (process-global context)

Worst stop-the-world pause over every window: 4.49ms (btree/write-saturated).
This is a process-global figure. It includes the benchmark driver's own per-op key and value allocations, not just the engine, so it bounds the whole process rather than isolating the engine arena.
The engine's sub-millisecond arena claim (spec 20) is checked by the engine's own allocation tests, not by this counter.

