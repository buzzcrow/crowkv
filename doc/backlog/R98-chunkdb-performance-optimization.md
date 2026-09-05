<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R98: chunkdb — Allocation Performance Optimization

## Problem

ChunkDB allocation composes placement, parallel DiskDB allocation, block
commit, two durable chunk-metadata writes, and three RPC layers. The existing
regression uses a six-node topology and does not expose enough transport or
workflow metrics to attribute its EC allocation ceiling. Its recorded EC 8+4
sample is also limited to 16 client tasks.

The production target topology for this requirement is three nodes. Each node
runs KV, DiskDB, and ChunkDB, and owns one of three KV data groups. One EC 8+4
strip allocates twelve physical blocks. Blocks routed to the same DiskDB data
group are already sent in one `AllocateBlocks` request and the three group
requests run concurrently. Multiple strips in a chunk remain sequential:
R98 does not batch or overlap strip allocation.

## Solution

Use the DiskDB performance methodology to measure and tune one-strip EC 8+4
chunk allocation without changing its durability contract.

1. Add a three-node combined benchmark deployment with one KV, DiskDB, and
   ChunkDB instance per node, three racks, one data group per node, and enough
   logical disk capacity for every 20-second case.
2. Expose independent connection and crowdb-rpc worker controls for the
   benchmark-to-ChunkDB, ChunkDB-to-DiskDB, ChunkDB-to-KV, DiskDB-to-KV, and KV
   server legs. Start from the selected DiskDB regression settings.
3. Add uniform count, inflight, error, and latency metrics for every ChunkDB
   RPC request type. Add allocation-stage metrics for placement, each DiskDB
   allocation round, block commit, chunk-record construction, Init metadata
   persistence, Active metadata persistence, response construction, rollback,
   and retries.
4. Retain CLI, ChunkDB, DiskDB, KV, and crowdb-rpc logs under one timestamped
   run root. Record both chunk operations/s and physical block allocations/s;
   for EC 8+4 with one strip, physical block rate is twelve times successful
   chunk allocation rate.
5. Run one-strip EC 8+4 allocation at 1, 16, 128, 256, and 512 client tasks.
   Use two connections/workers by default and four for 128 tasks and above,
   with KV inflight/coalescing settings inherited from the DiskDB sentinel.
   Tune only the stage proven dominant by the collected metrics.

## Dependencies

- Depends on the completed DiskDB allocation metrics, tunables, regression
  fixture, and flow analysis.
- Uses the current one-request-per-data-group batching inside
  `ChunkAllocator::allocate_blocks_parallel`.
- Does not implement multi-strip batch allocation or deferred confirmation;
  those belong to R113.

## Acceptance

- The benchmark deploys exactly three KV nodes, three DiskDB instances, and
  three ChunkDB instances, with one co-located service set per node and three
  KV data groups.
- Every measured request is one EC 8+4 strip and returns exactly twelve unique
  physical segments with exact DiskDB busy-space accounting.
- The regression records workload, group count, client tasks, strip count,
  client/ChunkDB/DiskDB/KV connection counts, RPC workers, inflight window,
  coalescing width, chunk ops/s, block allocations/s, p50, p99, duration,
  errors, stop reason, and space verification.
- Each ChunkDB RPC method exposes completed-request latency/count, inflight,
  and errors. Allocation-stage metrics identify placement, DiskDB allocation,
  commit, both KV metadata writes, response, retry, and rollback costs.
- All process metrics and RPC logs are retained under the case run root and
  validated as non-empty for the workload window.
- The selected 20-second case completes at the deadline with zero errors,
  exact accounting, and throughput reasonably below the measured DiskDB/KV
  component ceilings. Three confirmation runs stay within 10% of their median.
- Rust format, clippy, affected tests, and the ChunkDB regression pass through
  `pixi run`.
