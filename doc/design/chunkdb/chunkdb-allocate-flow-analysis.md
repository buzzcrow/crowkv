<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkDB Allocation Flow Analysis

## Scope

This analysis measures one-strip EC 8+4 chunk allocation on three co-located
logical nodes. Each node runs KV, DiskDB, and ChunkDB; the fixture has three
racks and three DiskDB data groups backed by three-replica KV groups. One
successful ChunkDB request allocates and commits twelve physical blocks.

The implementation keeps strips sequential. Within one strip it groups blocks
by data group, sends one DiskDB allocation request per group, and runs the
three group requests concurrently.

## Environment and Results

- Host: AMD Ryzen 9 5950X, 16 cores / 32 threads, 62 GiB RAM.
- Duration: 20 seconds per row.
- Capacity: four 4-TiB logical disks per DiskDB; 256-GiB zones.
- KV inflight/coalescing: 32/32.
- Command: `pixi run -- bash tools/bench-chunkdb-regression.sh`.

| Workload | Groups | Threads | Strips | EC | Client conn | ChunkDB conn | DiskDB conn | KV conn | Workers | Chunk/s | Block/s | p50 us | p99 us | Errors | Space |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| allocate | 3 | 1 | 1 | 8+4 | 2 | 2 | 2 | 2 | 2 | 436 | 5,232 | 2,386 | 2,777 | 0 | exact |
| allocate | 3 | 16 | 1 | 8+4 | 2 | 2 | 2 | 2 | 2 | 5,099 | 61,188 | 3,060 | 4,810 | 0 | exact |
| allocate | 3 | 128 | 1 | 8+4 | 4 | 4 | 4 | 4 | 4 | 8,259 | 99,108 | 14,849 | 27,332 | 0 | exact |
| allocate | 3 | 256 | 1 | 8+4 | 4 | 4 | 4 | 4 | 4 | 8,737 | 104,844 | 28,112 | 51,656 | 0 | exact |
| allocate | 3 | 512 | 1 | 8+4 | 4 | 4 | 4 | 4 | 4 | 8,548 | 102,576 | 57,402 | 103,077 | 0 | exact |

All rows stopped at the deadline and retained three KV, DiskDB, and ChunkDB
metrics/RPC log pairs plus the CLI pair. Clean artifacts are:

- `bench-log/chunkdb-regression-20260905-181843` (all rows).

## Bottleneck

The peak is 8,737 chunks/s, or 104,844 physical blocks/s, at 256 threads.
Increasing to 512 threads reduces throughput by 2.2% while p99 doubles from
52 ms to 103 ms, so 256 threads is the better operating point.

DiskDB now retains each freshly persisted tentative `BusyBlockValue` in a
bounded per-data-group cache keyed by `allocation_ts`. Commit validates the
cached physical identity and owner, changes the state, and overwrites the KV
record without a preceding read. A miss after restart or eviction safely
falls back to KV. At the 256-thread peak, the three DiskDB servers reported
2,098,704 cache hits, exactly 174,892 chunks times twelve blocks, and zero
misses.

At 512 threads, steady-state ChunkDB metrics show approximately:

- grouped DiskDB allocation: 18–22 ms average per metrics window;
- block commit: 13–17 ms;
- Init metadata KV persistence: 7–11 ms;
- Active metadata KV persistence: 7–10 ms;
- placement: below 0.02 ms.

The tentative cache removed the sequential busy-record lookup from the normal
commit path. Compared with the original matrix, peak physical throughput rose
12.8%, the 128-thread row rose 22.8%, and 512-thread p99 fell 68.5%.

The current bottleneck is the initial grouped DiskDB allocation round, with
commit second and the two ChunkDB metadata writes contributing similar KV
pressure. All four stages grow together at 512 threads, indicating KV/Paxos
queue saturation rather than placement CPU cost. More client concurrency is
not useful; the next flow optimization should reduce metadata transactions or
make DiskDB allocation idempotent so ambiguous failures can be recovered
without replaying physical allocation.

## Metrics

Every ChunkDB RPC exports `request.{method}.lh`, `.inflight.g`, and `.errors.c`.
Allocation also exports placement, DiskDB round/call/retry, block commit,
record-build, Init/Active KV persistence, rollback, strip/block count, and
inflight metrics. CLI, ChunkDB, DiskDB, KV, and crowdb-rpc logs share the
timestamped regression root.
