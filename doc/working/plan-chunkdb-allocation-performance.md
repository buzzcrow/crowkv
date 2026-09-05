<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkDB Allocation Performance Plan

Upstream: [R98](../backlog/R98-chunkdb-performance-optimization.md) and
[working design](design-chunkdb-allocation-performance.md).

Goal: measure and tune one-strip EC 8+4 allocation on the three-node
co-located production-shaped fixture without cross-strip batching.

## Phase 1: Review Checkpoint

- [x] **Review measurement design**: agree on topology, one-strip EC 8+4
  semantics, transport controls, counters, matrix, and result schema before
  code changes. Files: `doc/working/design-chunkdb-allocation-performance.md`.

## Phase 2: Fixture and Tunables

- [x] **Build three-node combined topology**: co-locate KV, DiskDB, and
  ChunkDB; create three racks and three data groups. Files:
  `lib/crowdb-console-shared/src/ops/cluster.rs`, related tests.
- [x] **Expose client and server transport controls**: plumb connection pools
  and worker counts through CLI, process configs, and constructors. Files:
  `app/crowdb-cli/src/commands/{cluster.rs,bench/verb.rs,bench/chunkdb.rs}`,
  `lib/crowdb-chunkdb-client/src/rpc_transport.rs`,
  `app/crowdb-chunkdb/src/{chunkdb_config.rs,main.rs}`,
  `lib/crowdb-console-shared/src/lifecycle.rs`.

## Phase 3: Workflow Metrics

- [x] **Instrument ChunkDB requests**: add uniform latency/count, inflight,
  and errors for every RPC method. Files:
  `app/crowdb-chunkdb/src/{metrics.rs,service/chunkdb_rpc_service/*}`.
- [x] **Instrument allocation stages**: measure placement, grouped DiskDB
  calls, commit, record construction, Init/Active KV persistence, response,
  retry, and rollback. Files: `app/crowdb-chunkdb/src/{allocator.rs,allocator/pool.rs,lifecycle/handler.rs,metrics.rs}`.
- [x] **Instrument benchmark client**: record scheduling, e2e, operation, and
  physical-block rates. Files: `app/crowdb-cli/src/commands/bench/{chunkdb.rs,metrics.rs}`.

## Phase 4: Regression and Analysis

- [x] **Refine regression harness**: EC 8+4 matrix at 1/16/128/256/512 tasks,
  20-second capacity, complete parameter columns, manifest, and all RPC/metrics
  logs. Files: `tools/bench-chunkdb-regression.sh`.
- [x] **Run discovery matrix**: execute all valid rows with exact accounting
  and identify the dominant stage. Files: generated `bench-log/` artifacts.
- [x] **Tune measured bottleneck**: change one control or implementation stage
  at a time and retain an A/B comparison. Files: selected after measurement.
- [x] **Run confirmation matrix**: repeat the selected 20-second case three
  times and verify 10% stability. Files: generated `bench-log/` artifacts.
- [x] **Record flow analysis**: document commands, hardware, results, log root,
  metrics attribution, and bottleneck. Files:
  `doc/design/chunkdb/chunkdb-allocate-flow-analysis.md`, `doc/doc_index.md`.

## Phase 5: Verification and Cleanup

- [~] **Run gates**: formatting, clippy, affected tests, full suite, and
  ChunkDB regression through `pixi run`.
- [ ] **Review changes**: apply `/review`, fix correctness and hot-path issues,
  and rerun affected gates.
- [ ] **Fold and clean up**: merge stable intent into permanent design, remove
  the backlog entry and working documents, and commit cleanup separately.

## Tests

- Unit: tunables, pooling, EC grouping, and metric snapshots.
- Integration: success, partial response, retry, commit/persistence failure,
  rollback, and exact counter behavior.
- E2E: three-node topology, full matrix, exact space/shape, retained logs, and
  three-run stability.
- Gates: `pixi run rs-fmt`, `pixi run rs-lint`, affected test tasks,
  `pixi run test-suite`, and `pixi run -- bash tools/bench-chunkdb-regression.sh`.
