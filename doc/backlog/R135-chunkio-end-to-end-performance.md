<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R135: chunkio — Write Flow Review and End-to-End Performance

## Problem

The chunk IO data path has unit and single-stack E2E coverage for the
large-object writer, but the simple E2E write currently stalls and there has
not been a full design-to-code review of the implemented flow. The public API
still requires callers to assemble a ChunkDB allocator, DiskIO client, RPC
server, connection, EC policy, and internal writer. Before treating the path
as a performance target, its design, implementation, API boundary, completion
ordering, and basic full-stack behavior need to be made coherent.

The project also has no production-shaped performance benchmark.
The current `crowdb-cli bench chunkdb` workload stops at chunk lifecycle and
block allocation: it does not call `LargeAsyncObjectWriter`, encode EC parity,
send block payloads through DiskIO, fsync the selected disks, or seal the
written chunks. Its combined deployment starts three KV servers, three DiskDB
instances, and three ChunkDB instances, but no DiskIO instances.

The existing large-object E2E test is not a performance fixture. It starts one
KV server with groups 0 and 1, one DiskDB, one DiskIO, and one ChunkDB. Its
`DiskioBlockWriter` also owns one fixed DiskIO connection. A production-shaped
EC strip can contain segments from disk groups on all three nodes, so routing
every segment through one DiskIO server is invalid for a three-node benchmark.

Without a complete benchmark, optimizations can improve ChunkDB allocation
while regressing EC compute, client buffering, DiskIO transport, fsync, or
chunk sealing. The project also lacks a retained baseline for logical write
bandwidth, physical EC write bandwidth, object latency, and full-flow errors.

The current scheduling does not yet make allocation a fully independent
preparation path. `write_stream` starts chunk prefetch concurrently with the
fetch loop, but the drive loop waits for the first chunk before consuming its
first buffered block. Strip allocation is bounded and asynchronous, yet
`open_next_strip` waits when prefetch falls behind. At chunk rotation,
`seal_current` joins parity and fsync work and persists the seal before the
next prepared chunk is opened. The data path itself also awaits each data-block
DiskIO RPC before accepting the next block, and EC input is processed only
after that RPC completes. These waits can put metadata or one-block latency on
the source-to-strip critical path.

Local execution has no physical NIC line-rate ceiling: all traffic crosses the
kernel TCP loopback path and competes for CPU, cache, and DRAM bandwidth. Raw
dual-channel memory bandwidth is only a hardware envelope, not expected chunk
throughput. For example, dual-channel DDR4-3200 has a theoretical 51.2 GB/s
transfer rate, before protocol processing and copies. EC 4+1 already expands
one logical byte to 1.25 physical bytes, and the current path adds source
buffering, `Bytes` to `Vec` conversion in `DiskioBlockWriter`, RPC buffers, and
kernel socket copies. Results therefore describe local full-stack software
throughput. When the host exposes memory-controller counters, collect memory
bandwidth during the workload; do not run a separate physical-memory stress
test or present theoretical bandwidth as a measured result.

Root designs:

- `doc/design/chunkio/design-crowdb-chunkio.md` §3 defines the large-write
  fetch, allocation, EC, DiskIO, fsync, and seal flow.
- `doc/design/diskio/design-crowdb-diskio.md` §5.4 defines `NullDisk` as the
  benchmark backend that exercises the DiskIO engine without real storage.
- `doc/design/chunkdb/design-crowdb-chunkdb.md` defines strip placement and
  lifecycle persistence.

Operator and engineering scenarios:

- An engineer runs one command on a host without spare block devices and gets
  a three-node full-path large-write baseline using `NullDisk`.
- An engineer changes chunk allocation, EC encoding, RPC, or DiskIO and can
  tell whether end-to-end logical bandwidth, physical bandwidth, or p99 object
  latency changed.
- A segment is placed on any of the three disk groups and the writer routes it
  to the DiskIO instance that owns that disk instead of a fixed endpoint.
- A later small-write or read requirement adds a workload to the same cluster,
  lifecycle, logging, and result schema without duplicating the deployment.

## Solution

Review and refine the existing large-write design and implementation, prove it
with a small full-stack E2E fixture, then build a reusable three-node benchmark
and use it to remove measured critical-path and memory-copy costs.

1. **Design and code review** — trace the implemented flow across
   `lib/crowdb-chunk-client/`, `lib/crowdb-diskio-client/`,
   `app/crowdb-diskio/`, ChunkDB allocation, and the existing large-object E2E
   test. Reconcile code with `design-crowdb-chunkio.md` for source buffering,
   chunk/strip preparation, data and parity writes, EC ownership, fsync
   ordering, chunk rotation, seal, error cleanup, backpressure, and memory
   bounds. Record the first divergence behind the current `write_stream` stall.
   Refine the permanent write-flow design before changing behavior.

2. **Chunk-client API design** — provide a clear application-facing API in
   `crowdb-chunk-client` that owns service discovery, topology refresh, DiskIO
   routing, bounded preparation, writer construction, stream execution, abort,
   and per-object results. A caller supplies management seeds, write policy,
   object size, and an async byte source; it does not construct internal
   allocators, RPC servers, connections, chunks, strips, or parity workers.
   Keep low-level seams available for focused tests. The API remains independent
   of CLI types so a future S3-like server can use it directly.

3. **Simple full-flow E2E** — first make one small local stack reliable: one KV
   server with group 0 and one data group, one DiskDB, one ChunkDB, and one
   DiskIO using `NullDisk`. Use the public chunk-client API to stream a
   multi-strip object and an object that rotates chunks. Verify allocation,
   writes, parity completion, write-before-fsync ordering, sealing, locations,
   and exact metadata/space accounting. Diagnose and fix the current stall
   rather than increasing its timeout. This is a correctness and API gate, not
   a performance result; `NullDisk` cannot verify read-after-write content.

4. **Preparation path separation** — begin bounded chunk and strip preparation
   as soon as object size and EC policy are known. Predict the required window,
   keep allocation ahead of the write cursor, and let the stream operation
   consume prepared state. Allocation, placement, and metadata persistence run
   as a second path and do not block steady-state source-read to data-strip
   writes while the window is maintained. If preparation falls behind, wait
   safely and record the stall. Do not eagerly allocate an unbounded object.

5. **Write critical-path refinement** — after the simple E2E flow is correct,
   measure and evaluate bounded concurrent data-block writes, feeding EC before
   the corresponding write response when ownership permits, and overlapping
   old-chunk finalization with an already prepared next chunk. Sequence each
   disk's fsync after all corresponding data and parity writes. Preserve bounded
   backpressure, object ordering, cleanup, and durability semantics.

6. **Memory-copy inventory and reduction** — account for every logical and
   EC-expanded byte from the async source through fetch buffers, `Bytes`, EC
   shards, `DiskioBlockWriter`, `DiskioClient`, crowdb-rpc buffers, TCP
   loopback, the DiskIO request handler, and `NullDisk`. Record unavoidable
   reads/writes separately from removable copies. In particular, evaluate
   removing the current `Bytes` to `Vec` conversion and preserving owned RPC
   buffers through completion. Accept a zero-copy or reduced-copy change only
   when lifetime safety is explicit and the full-flow benchmark improves.

7. **Distributed DiskIO routing** — extend the simple writer to three DiskIO
   endpoints. Resolve each segment's disk ID through hardware and DiskIO
   service-registry state, select the unique owner, and keep bounded reusable
   connections per endpoint. Refresh stale topology only where retry is safe.
   Missing or ambiguous ownership fails explicitly; never fall back to an
   arbitrary server. Preserve the narrow `DiskWriter` test seam.

8. **Three-node benchmark deployment** — extend
   `lib/crowdb-console-shared/src/ops/cluster.rs` and CLI deployment to start
   three racks/nodes, three KV servers, three DiskDB, three ChunkDB, and three
   DiskIO using `NullDisk`. Create group 0, three replicated KV data groups,
   and one DiskIO-owned disk group per node. Wait for leaders, registrations,
   bindings, topology, and disk discovery before measurement.

9. **Library-owned benchmark workload** — put deterministic bounded-source
   generation, concurrent large writes, workload execution, and per-object
   results in reusable `crowdb-chunk-client` benchmark support. Add a thin CLI
   verb that maps arguments to this API and formats its result. Support duration
   or object count, object size, concurrency, EC policy, block/chunk sizes,
   preparation/buffering controls, and RPC controls. Do not implement small
   writes or reads, but keep deployment and result boundaries reusable for them.

10. **Runtime host metrics and full-flow measurement** — probe Linux perf PMUs
    for memory-controller/Data Fabric counters and, when available, collect
    average and peak DRAM read/write bandwidth during the exact workload
    window. This is passive observation of chunk IO, not a separate memory
    benchmark. Record the counter names, units, scope, multiplexing ratio, and
    CPU/NUMA placement needed to interpret the sample. If the required PMU is
    unavailable or permission is denied, record memory bandwidth as
    `unsupported`; do not infer it from cache misses, theoretical DIMM rate, or
    application byte counts. Independently report software-visible payload
    bytes, known copy bytes, copy amplification per logical byte, logical and
    EC-expanded physical bytes/s, object rate, p50/p99, preparation stalls,
    data-write inflight/latency, EC time, parity, fsync, finalization, CPU, and
    peak RSS. Also collect TCP-loopback byte counters for the workload window.
    Metrics added on hot paths remain lock-free.

11. **Regression sentinel and retained artifacts** — add
    `tools/bench-chunkio-write-regression.sh`, following R98/R131. Build the
    five binaries, deploy a fresh topology per case, run a bounded object-size
    and concurrency matrix, and retain parameters, results, and CLI plus all
    twelve service logs. Continue after individual failures, preserve partial
    data on timeout, destroy the cluster, and exit non-zero after the matrix if
    any correctness or execution gate failed.

```text
Preparation path: object request -> bounded chunk/strip allocation
                                      |  ChunkDB -> DiskDB -> KV/Paxos/WAL
                                      v
Critical path:    AsyncRead -> buffered blocks -> EC + routed writes
                                                   /       |       \
                                              DiskIO 1  DiskIO 2  DiskIO 3
                                              NullDisk  NullDisk  NullDisk
                                      |
Completion path: parity writes -> ordered fsync -> chunk seal -> Locations
```

Edge-case outcomes:

- A DiskIO registration or disk-owner mapping is incomplete at readiness
  deadline: deployment fails before measurement and retains diagnostics.
- A segment has no unique DiskIO owner: the object fails; no random endpoint
  fallback is allowed.
- Capacity is exhausted before the configured deadline or object count: the
  sample is invalid and reports `NoSpace` rather than truncated throughput.
- A write, parity task, fsync, allocation, or seal fails: the object is counted
  once as failed, the case exits non-zero, and already recorded logs remain.
- The process receives the sentinel timeout: request graceful stop, bound
  teardown time, retain partial metrics, and report `timeout`, not `deadline`.
- A future read workload is requested while unsupported: CLI parsing rejects
  it rather than silently running the write workload.

## Dependencies

- Uses the completed R94 large-object writer and the current ChunkDB, DiskDB,
  DiskIO, KV, and service-registry implementations.
- Reuses R98's three-node combined deployment, ChunkDB/DiskDB metrics, RPC
  controls, retained-log convention, and exact allocation accounting.
- The `write_stream` stall recorded in
  `doc/working/plan-module-boundaries.md` must be diagnosed and fixed before a
  performance baseline is accepted. The benchmark may expose that defect but
  must not mask it with skipped objects or relaxed timeouts.
- R113 may later use this sentinel to validate batch strip allocation, but R135
  does not depend on R113 and must measure the current allocation behavior.
- R106 and R107 may add small-write and read workloads later. They reuse this
  fixture but are outside R135 acceptance.

## Acceptance

**Review and simple flow**:

- Trace design and code from object request through completion -> record every
  synchronous wait, task/channel handoff, allocation dependency, buffer owner,
  payload copy, write, parity operation, fsync, seal, and cleanup edge -> update
  the permanent chunk IO design to match the reviewed intended behavior before
  implementation. Invariant: design and code describe one auditable write
  flow. Integration test.
- Start one KV server with group 0 and one data group, one DiskDB, one ChunkDB,
  and one `NullDisk` DiskIO -> use only the high-level chunk-client API to write
  a multi-strip object -> assert the operation completes, seals its chunk, and
  passes exact location, strip, and busy-space checks. Invariant: the simplest
  real-service write flow works before performance scaling. E2E test.
- Use the same simple fixture to write across a chunk boundary -> assert
  preparation, rotation, ordered fsync, sealing, and returned locations finish
  without timeout. Invariant: the previous `write_stream` stall is fixed rather
  than hidden by the benchmark harness. E2E test.

**Deployment and routing**:

- Start a clean local fixture -> inspect configuration and service registry ->
  assert exactly three racks, three nodes, three KV servers, three DiskDB,
  three ChunkDB, three DiskIO, group 0, three replicated data groups, and one
  DiskIO-owned disk group per node. Invariant: benchmark topology matches the
  production-shaped three-node layout. E2E test.
- Deploy without real block-device paths -> wait for disk discovery -> assert
  every configured logical disk is served by `NullDisk` and accepts write and
  fsync requests. Invariant: the benchmark requires no real storage while
  preserving the complete RPC/engine path. E2E test.
- Place one EC strip across all three disk groups -> write every segment ->
  assert each request reaches the unique DiskIO owner and no server returns
  `DiskNotExist`. Invariant: disk ID determines the endpoint. Integration test.
- Remove or duplicate one disk ownership mapping -> attempt a write -> assert
  an explicit topology error and no arbitrary endpoint fallback. Invariant:
  ambiguous routing cannot misdirect data. Integration test.

**Large-write correctness and measurement**:

- Construct the high-level chunk client from management seeds and write policy
  -> prepare and stream an object without manually constructing allocators,
  DiskIO connections, or internal writers -> assert the same result as the
  lower-level E2E path. Invariant: applications use one clear library-owned
  API and do not reproduce pipeline wiring. Integration test.
- Start a known-size write with delayed source data -> observe preparation ->
  assert the bounded initial chunk/strip window is ready before the first data
  block and allocation proceeds independently as the cursor advances.
  Invariant: predictable metadata work is prepared off the data critical path.
  Integration test.
- Slow allocation below the data consumption rate -> continue the write ->
  assert the writer waits without losing or reordering data and reports
  non-zero preparation-stall count and duration. Invariant: a missed prefetch
  is explicit backpressure, not silent corruption. Integration test.
- Stream one multi-strip object through the three-node fixture -> await completion ->
  assert `write_stream` returns, every parity/fsync task finishes, the chunk is
  sealed, and returned locations cover the exact logical length. Invariant:
  acknowledged objects completed the full durability workflow. E2E test.
- Write an object that crosses a chunk boundary -> inspect locations and chunk
  metadata -> assert ordered contiguous logical ranges, expected chunk/strip
  counts, and exact DiskDB busy-space growth. Invariant: benchmark traffic is
  structurally correct, not merely acknowledged. E2E test.
- Run deterministic input larger than the configured memory budget -> sample
  process memory and completion -> assert input generation remains bounded by
  writer/concurrency configuration rather than object size. Invariant: the
  benchmark does not become its own memory bottleneck. E2E test.
- Complete a strip with delayed parity writes -> observe fsync submission ->
  assert each disk's fsync starts only after all corresponding data and parity
  writes complete. Invariant: optimization cannot acknowledge durability with
  an fsync/write race. Integration test.
- Compare the serial data-block baseline with each accepted concurrency,
  EC-overlap, finalization-overlap, or zero-copy change -> assert bounded
  memory/backpressure and identical metadata while retaining before/after
  logical bandwidth and p99 results. Invariant: only measured full-flow wins
  are promoted. E2E test.
- Run a successful bounded case -> inspect its result row -> assert logical
  bytes/s, EC-expanded physical bytes/s, objects/s, p50, p99, object counts,
  chunk/strip counts, DiskIO write/fsync counts, duration, errors, stop reason,
  and accounting status are present and internally consistent. Invariant: one
  row describes the complete write flow. Integration test.
- Run a chunk write while supported memory-controller PMUs are available ->
  scope counters to the workload window -> assert measured DRAM read/write
  GB/s, counter identity, units, scope, multiplexing, and CPU/NUMA placement
  are recorded with logical/physical throughput and copy amplification.
  Invariant: memory bandwidth is passively measured during real chunk IO.
  E2E test.
- Run the same workload without the required PMU or permission -> assert the
  result says memory bandwidth is `unsupported` while still reporting known
  software copy bytes, RSS, CPU, and loopback counters. Invariant: unavailable
  physical bandwidth is never estimated or mislabeled as measured. E2E test.

**Regression lifecycle and extension boundary**:

- Run the sentinel's selected concurrency and object-size matrix -> assert
  every case uses a fresh full-stack deployment, reaches its configured stop
  condition with zero errors, passes metadata/accounting checks, and records a
  baseline without imposing an unmeasured throughput target. Invariant:
  initial performance claims come from valid samples. E2E test.
- Force one middle case to fail -> let the sentinel finish -> assert later
  cases run, all artifacts remain, and the final exit status is non-zero.
  Invariant: one failure cannot erase the comparison matrix. E2E test.
- Complete one case -> inspect its timestamped root -> assert non-empty CLI,
  KV, DiskDB, ChunkDB, DiskIO, and RPC logs exist for all twelve service
  processes and cover the workload window. Invariant: results remain
  attributable after the run. E2E test.
- Request a future small-write or read workload -> assert the CLI rejects it;
  add a test-only workload implementation through the shared runner -> assert
  it reuses deployment, result, and teardown plumbing. Invariant: future flows
  extend the benchmark without pretending to be implemented. Unit test.
- Invoke the large-write benchmark through the CLI -> inspect dependencies and
  execution -> assert the CLI only maps arguments to the chunk-client API and
  formats its result, with no chunk/strip scheduling logic. Invariant: the
  reusable data-path implementation remains in `crowdb-chunk-client`.
  Integration test.

Run `pixi run -- cargo test -p crowdb-chunk-client --tests`,
`pixi run -- cargo test -p crowdb-cli --test chunkio_bench_test`, and
`pixi run -- env CHUNKIO_BENCH_DURATION=20 bash tools/bench-chunkio-write-regression.sh`.
Run `pixi run -- cargo fmt --all -- --check` and
`pixi run -- cargo clippy --all-targets -- -D warnings` before completion.
