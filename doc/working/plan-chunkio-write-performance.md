<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk IO Write Flow and Performance Plan

Upstream: [R135](../backlog/R135-chunkio-end-to-end-performance.md) and
[working design](design-chunkio-write-performance.md).

Goal: make the large-write API and simple E2E flow correct, then measure and
refine it through a three-node `NullDisk` benchmark.

## Review and Correctness

- [x] **Diagnose simple write**: reproduce the isolated E2E test and locate the
  first pending operation. Files: `lib/crowdb-chunk-client/tests/`,
  `lib/crowdb-chunk-client/src/`, `lib/crowdb-test-harness/`.
- [x] **Correct completion ordering**: sequence fsync after data/parity writes
  and add focused tests. Files: `lib/crowdb-chunk-client/src/chunk/`,
  `lib/crowdb-chunk-client/tests/`.
- [x] **Add public client API**: own discovery, routing, preparation, execution,
  and results in the library. Files: `lib/crowdb-chunk-client/src/client.rs`,
  `lib/crowdb-chunk-client/src/client/`, `lib/crowdb-chunk-client/src/lib.rs`.
- [x] **Pass simple E2E**: use the public API for multi-strip and chunk-rotation
  writes on one real stack. Files: `lib/crowdb-chunk-client/tests/`,
  `lib/crowdb-test-harness/`.

## Distributed Flow

- [x] **Add DiskIO routing**: publish immutable disk-owner snapshots and bounded
  endpoint pools. Files: `lib/crowdb-chunk-client/src/disk_io/`,
  `lib/crowdb-kv-client/src/`.
- [ ] **Deploy three DiskIO services**: extend combined local deployment,
  readiness, persistence, destroy, and logs. Files:
  `lib/crowdb-console-shared/src/`, `app/crowdb-cli/src/commands/cluster.rs`.
- [ ] **Pass distributed E2E**: write an EC strip across three disk groups and
  verify routing and accounting. Files: `lib/crowdb-chunk-client/tests/`.

## Performance

- [x] **Remove payload copy**: preserve owned payload through DiskIO RPC
  completion. Files: `lib/crowdb-diskio-client/src/`,
  `lib/crowdb-chunk-client/src/disk_io/`.
- [ ] **Refine critical path**: measure and apply bounded write/EC/finalization
  overlap while preserving memory and ordering. Files:
  `lib/crowdb-chunk-client/src/`, `lib/crowdb-chunk-client/tests/`.
- [ ] **Add library benchmark runner**: implement bounded deterministic large
  writes and aggregate results. Files: `lib/crowdb-chunk-client/src/benchmark.rs`.
- [ ] **Add thin CLI verb**: map arguments and format the library result. Files:
  `app/crowdb-cli/src/commands/bench/`, `app/crowdb-cli/Cargo.toml`.
- [ ] **Add regression sentinel**: run and retain the three-node matrix and
  `bw_mib` samples. Files: `tools/bench-chunkio-write-regression.sh`.

## Documentation and Gates

- [ ] **Run affected tests**: unit, simple E2E, distributed E2E, CLI integration,
  and sentinel separately through Pixi.
- [ ] **Fold design**: update permanent chunk IO and DiskIO designs and remove
  temporary artifacts and R135.
- [ ] **Run final gates**: format, lint, and full ordered local CI through Pixi.

## Files

- `lib/crowdb-chunk-client/`
- `lib/crowdb-diskio-client/`
- `lib/crowdb-test-harness/`
- `lib/crowdb-console-shared/`
- `app/crowdb-cli/`
- `tools/bench-chunkio-write-regression.sh`
- `doc/design/chunkio/design-crowdb-chunkio.md`
- `doc/design/diskio/design-crowdb-diskio.md`

## Tests

- Unit: completion ordering, preparation stalls, result aggregation.
- Integration: routing ownership/errors, CLI adapter.
- E2E: simple stack, chunk rotation, distributed EC write.
- Regression: `tools/bench-chunkio-write-regression.sh`.
