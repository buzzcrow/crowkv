<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkDB Allocation Performance (R98)

This draft implements [R98](../backlog/R98-chunkdb-performance-optimization.md)
within the allocation, placement, and durability contracts in the
[ChunkDB design](../design/chunkdb/design-crowdb-chunkdb.md). It reuses the
measurement method and selected transport region from the
[DiskDB allocation flow analysis](../design/diskdb/diskdb-allocate-flow-analysis.md).

## 1. Benchmark Contract

The primary workload repeatedly creates a new chunk containing exactly one
EC 8+4 strip. A successful operation therefore represents one ChunkDB RPC,
twelve physical block allocations, twelve block commits, and the existing
Init-to-Active metadata publication sequence. Report both `chunk_ops/s` and
`block_allocs/s = chunk_ops/s * 12`.

The benchmark topology is three logical nodes and three racks. Each node hosts
one KV server, one DiskDB server, and one ChunkDB server. Each node's DiskDB
owns one data group, for three data groups total. EC placement puts at most
four blocks on a node, satisfying the EC 8+4 node-failure bound.

`strip_count` stays one in the performance matrix. The existing
`ChunkAllocator::allocate_blocks_parallel` groups a strip's placement by data
group, sends one `AllocateBlocks` call for all blocks assigned to that group,
and runs the three calls concurrently. The `allocate_chunk` loop remains
sequential for requests with multiple strips. No multi-strip batch or
cross-strip pipeline is added.

## 2. Controls and Result Schema

Keep each transport leg independently configurable:

1. client-to-ChunkDB connections and client RPC workers;
2. ChunkDB server RPC workers;
3. ChunkDB-to-DiskDB connections and RPC workers;
4. ChunkDB-to-KV connections and RPC workers;
5. DiskDB server RPC workers and DiskDB-to-KV connections/workers;
6. KV server RPC workers, peer pool, inflight window, and coalescing width.

Start with two connections and two workers on service/client legs. Use four
connections and four workers for 128 tasks and above. Start KV inflight and
coalescing at 32, matching the current DiskDB regression matrix. The focused
task matrix is 1, 16, 128, 256, and 512. Neighbor sweeps change one leg at a
time only after metrics identify saturation.

The retained TSV columns are:

```text
workload grp thread strip ec client-connection chunkdb-connection
diskdb-connection kv-internal-connection client-worker chunkdb-worker
diskdb-worker kv-worker win coal chunk-ops/s block-allocs/s p50 p99 Dur Err Stop Space
```

Here `grp=3`, `strip=1`, and `ec=8+4` for the primary matrix.

## 3. Workflow Metrics

Use the shared metrics registry and atomic handles used by DiskDB. Do not add a
new lock to request or allocation hot paths.

Every ChunkDB RPC method records:

```text
request.{method}.lh
request.{method}.inflight.g
request.{method}.errors.c
```

Methods include `allocate_chunk`, `append_chunk`, `seal_chunk`,
`delete_chunk`, `delete_chunk_range`, `query_chunk`, `list_chunks`, and
`update_chunk_strip`. Histogram count/rate is the completed request count and load
type; the histogram supplies latency, the gauge exposes current load, and the
counter separates failed outcomes.

Allocation adds these boundaries and magnitudes:

- `allocate.placement.lh` and `allocate.placement_errors.c`;
- `allocate.diskdb_round.lh`, `allocate.diskdb_calls.c`,
  `allocate.diskdb_blocks.c`, and `allocate.diskdb_retries.c`;
- `allocate.commit.lh`, `allocate.commit_blocks.c`, and
  `allocate.commit_errors.c`;
- `allocate.record_build.lh`;
- `allocate.kv_persist.lh` for the single Active chunk write;
- `allocate.commit.lh`, `allocate.commit_blocks.c`, and
  `allocate.commit_errors.c` for asynchronous DiskDB commit;
- `allocate.response_build.lh`;
- `allocate.rollback.lh`, `allocate.rollback_blocks.c`, and
  `allocate.rollback_errors.c`;
- `allocate.inflight.g`, `allocate.strips.c`, and `allocate.blocks.c`.

Existing crowdb-rpc, DiskDB, KV, Paxos, WAL, storage, CPU, memory, and network
metrics provide the downstream decomposition. The analysis reports the
unattributed remainder between parent and child latency rather than assigning
it to an unmeasured stage.

## 4. Server and Client Wiring

Extend `ChunkdbArgs` with client transport controls and construct
`ChunkdbRpcTransport` with a configurable per-endpoint connection pool.
ChunkDB configuration gains separate KV and DiskDB client pool sizes and RPC
worker counts. Local combined deployment passes ChunkDB server workers and all
downstream settings to the actual process constructors.

Change the combined fixture from six KV-only topology nodes to three
co-located logical nodes. Preserve three racks and place one node in each.
Create three data groups, each replicated across the same three KV servers,
then assign one DiskDB server to each group. Start one ChunkDB server on each
node.

## 5. Regression and Log Collection

`tools/bench-chunkdb-regression.sh` follows the DiskDB harness: one timestamped
root, one directory per case, resolved configuration, `results.tsv`, command
output, and a manifest of collected logs. Each case must retain three files of
each server metrics family plus their crowdb-rpc logs and one CLI metrics/RPC
pair. Validate that samples overlap the workload interval.

Use logical capacity large enough that all five 20-second cases stop at the
deadline. A sample is invalid if it exhausts space, returns anything other
than twelve unique EC segments, reports an error, or has a busy-space delta
different from `successful_chunks * 12 * unit_bytes`.

After the first matrix, compare stage latency and rates with the current
DiskDB ceiling. Tune the dominant leg, rerun the winning case three times for
20 seconds, and retain only valid results in the script comment and permanent
flow analysis.

## 6. Failures and Fallback

A partial DiskDB response retries only missing blocks under the existing
bounded policy. Any terminal allocation, commit, or metadata failure rolls
back every allocated segment. Metrics never alter request behavior. Failure
in log validation invalidates the benchmark artifact but does not change data
path semantics.

## 7. Scope

- `app/crowdb-cli/src/commands/bench/{verb.rs,chunkdb.rs,metrics.rs}`: client
  controls, measurements, and result fields.
- `lib/crowdb-chunkdb-client/src/rpc_transport.rs`: endpoint connection pool.
- `app/crowdb-chunkdb/src/{chunkdb_config.rs,main.rs,metrics.rs}`: downstream
  controls and workflow metrics.
- `app/crowdb-chunkdb/src/{allocator.rs,allocator/pool.rs,lifecycle/handler.rs}`:
  stage instrumentation without changing strip sequencing.
- `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/*`: uniform request
  metrics.
- `lib/crowdb-console-shared/src/{lifecycle.rs,ops/cluster.rs}` and
  `app/crowdb-cli/src/commands/cluster.rs`: three-node co-located fixture and
  tunable wiring.
- `tools/bench-chunkdb-regression.sh`: focused matrix and retained artifacts.
- `doc/design/chunkdb/chunkdb-allocate-flow-analysis.md` and
  `doc/doc_index.md`: permanent measured analysis.

Complexity: High. The allocation behavior already matches the requested
batch-within-strip policy, but the deployment topology, five transport legs,
uniform instrumentation, correctness accounting, and log correlation cross
multiple processes.

## 8. Test Design

- Unit: configure each new pool/worker control, build a three-group EC plan,
  and assert one grouped request per data group with twelve total blocks.
- Integration: one EC 8+4 allocation returns twelve unique segments, publishes
  Active metadata, and increments request/stage counters with zero inflight at
  completion.
- Integration: inject partial allocation and persistence/commit failures;
  assert retries or full rollback and exact error/rollback metrics.
- E2E: deploy three co-located nodes and assert three KV, DiskDB, ChunkDB, rack,
  and data-group identities with one DiskDB owner per group.
- E2E: run every matrix row for 20 seconds and assert deadline stop, zero
  errors, exact segment shape, and exact busy-space accounting.
- E2E: validate the results schema and non-empty CLI/ChunkDB/DiskDB/KV metrics
  and RPC logs for the workload window.
- Regression: repeat the selected row three times and assert throughput spread
  within 10% of the median.

## 9. Module Structure

```text
app/crowdb-chunkdb/src/
  metrics.rs                         request and allocation-stage handles
  allocator.rs                       per-strip allocation attribution
  allocator/pool.rs                  DiskDB call magnitude and latency
  lifecycle/handler.rs               persistence and commit stages
  service/chunkdb_rpc_service/       per-method request wrappers
app/crowdb-cli/src/commands/bench/
  chunkdb.rs                         workload, verification, output
  verb.rs                            transport flags
lib/crowdb-console-shared/src/
  ops/cluster.rs                     three-node combined topology
  lifecycle.rs                       process arguments and configuration
tools/
  bench-chunkdb-regression.sh        matrix, artifacts, results
```

## 10. Open Questions

None. Multi-strip batching is explicitly deferred; the first implementation
keeps allocation strip by strip.
