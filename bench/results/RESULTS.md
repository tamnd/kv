# kv benchmark results

These numbers come from the in-repo harness (`bench/`) running every workload on both engines.
They are produced on the developer machine disclosed below, not the NVMe reference machine the spec 21 §6 absolute targets are stated against, so the figures to read here are the *shape* of the B-tree/LSM tradeoff and the honesty targets, both of which are machine-independent in direction.
Regenerate them with `go run ./cmd/bench`.

## Setup

- Machine: darwin/arm64, 10 CPU, go1.26.4
- Keys: 50000 at 24B key / 64B value; ops: 50000; concurrency: 1; batch: 100
- Durability: full; seed: 1

## Per-workload numbers

| workload | engine | throughput (ops/s) | read p99 | write p99 | space-amp | write-factor | read-ios/op |
|---|---|---:|---:|---:|---:|---:|---:|
| bulk-load | btree | 19407 | n/a | 64.3us | 1.00 | 2.32 | - |
| write-saturated | btree | 138 | n/a | 12.12ms | 1.00 | 4.61 | - |
| ycsb-a | btree | 580 | 1.88ms | 10.93ms | 1.00 | 3.41 | 11.80 |
| ycsb-b | btree | 5315 | 20.4us | 4.64ms | 1.00 | 2.67 | 0.00 |
| ycsb-c | btree | 775620 | 1.5us | n/a | 1.00 | 2.32 | 0.00 |
| ycsb-d | btree | 5732 | 13.8us | 4.23ms | 1.00 | 2.38 | 0.00 |
| ycsb-e | btree | 1663 | 9.30ms | n/a | 1.00 | 2.32 | 0.00 |
| ycsb-f | btree | 157 | 15.22ms | 15.22ms | 1.00 | 3.47 | 5.38 |
| bulk-load | lsm | 27265 | n/a | 40.6us | 1.00 | 1.14 | - |
| write-saturated | lsm | 269 | n/a | 4.98ms | 1.00 | 3.19 | - |
| ycsb-a | lsm | 454 | 1.31ms | 7.29ms | 1.00 | 2.16 | 0.00 |
| ycsb-b | lsm | 4554 | 79.5us | 6.30ms | 1.00 | 1.24 | 0.00 |
| ycsb-c | lsm | 909846 | 3.9us | n/a | 1.00 | 1.14 | 0.00 |
| ycsb-d | lsm | 4701 | 17.8us | 8.63ms | 1.00 | 1.24 | 0.00 |
| ycsb-e | lsm | 685 | 22.34ms | n/a | 1.00 | 1.14 | 0.00 |
| ycsb-f | lsm | 270 | 7.08ms | 7.08ms | 1.00 | 3.19 | 0.00 |

## Tradeoff and targets

- **Read latency reference (B-tree, YCSB-C cache-resident)** — yes
  - Claim: B-tree cache-resident read p99 is microsecond-class with no extra page I/O
  - Observed: btree p99 1.5us at 0.00 read-ios/op
  - Holds here: yes
- **Write amplification (LSM below B-tree, write-saturated)** — yes
  - Claim: LSM write-factor is at or below the B-tree's: log-structured writes cost less per op
  - Observed: lsm write-factor 3.19 vs btree 4.61
  - Holds here: yes
- **Bulk ingest (LSM at or above B-tree, un-fsync-pinned)** — yes
  - Claim: with batches not pinned to per-op fsync, LSM ingest is at or above the B-tree's
  - Observed: lsm 27265 ops/s vs btree 19407 ops/s
  - Holds here: yes
- **No silent drops (spec 21 §3)** — yes
  - Claim: every offered operation is accounted for across the whole suite
  - Observed: 0 dropped operations across 16 cells
  - Holds here: yes

## Garbage collection (process-global context)

Worst stop-the-world pause over every window: 3.82ms (lsm/ycsb-a).
This is a process-global figure. It includes the benchmark driver's own per-op key and value allocations, not just the engine, so it bounds the whole process rather than isolating the engine arena.
The engine's sub-millisecond arena claim (spec 20) is checked by the engine's own allocation tests, not by this counter.

