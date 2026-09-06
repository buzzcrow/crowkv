<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskDB and ChunkDB Module Boundaries Plan

The module-boundary implementation and design folding are complete. The active
follow-up is to diagnose and fix the chunk-client write stall, then finish the
local CI sweep.

## Completed

- [x] Split DiskDB keep-alive into scheduler, heartbeat, observation, reconciliation, and loading modules.
- [x] Split DiskDB RPC registration, handler groups, and FlatBuffer framing.
- [x] Split ChunkDB lifecycle orchestration from the lock/cache primitive.
- [x] Split ChunkDB RPC registration, handler groups, and FlatBuffer framing.
- [x] Run the affected DiskDB, DiskDB client, ChunkDB, and ChunkDB client suites.
- [x] Pass workspace formatting and Clippy with warnings denied.
- [x] Fold the module structure into the permanent DiskDB and ChunkDB designs.
- [x] Remove the completed backlog item.
- [x] Run every unaffected `test-*` Pixi task separately.

## Chunk-Client Write Follow-Up

- [~] **Diagnose write stall**: trace `write_stream` from the first pending
  operation through chunk allocation and DiskIO completion, identify the first
  divergence, and fix the underlying error. Files:
  `lib/crowdb-chunk-client/`, `lib/crowdb-diskio-client/`,
  `lib/crowdb-common/cpp/`, and the relevant service path.
- [ ] **Verify large-object writes**: pass
  `e2e_case1_single_chunk_multi_strip` and `e2e_case2_chunk_rotation`. Files:
  `lib/crowdb-chunk-client/tests/large_object_writer_e2e.rs`.
- [ ] **Finish local CI**: pass `pixi run test-chunk-client`, then run every
  remaining `test-*` Pixi task separately.

## Starting Evidence

`test-chunk-client` does not complete because
`e2e_case1_single_chunk_multi_strip` and `e2e_case2_chunk_rotation` stall after
their full stacks become ready and `write_stream` starts.

Reproduce the isolated first case:

```text
pixi run clean-env
pixi run -- cargo test -p crowdb-chunk-client --test large_object_writer_e2e e2e_case1_single_chunk_multi_strip -- --nocapture
```

Observed on each attempt:

```text
crowdb-diskdb ready
crowdb-diskio ready (disks discovered)
crowdb-chunkdb ready
chunkdb client endpoints refreshed
chunkdb topology settled
=== writing 12 MB ===
test e2e_case1_single_chunk_multi_strip has been running for over 60 seconds
```

Attempts:

- Full `pixi run test-chunk-client` after the initial local-CI cleanup stalled with both large-object E2E cases running.
- A clean isolated retry stalled at `write_stream`.
- The same clean isolated test on pre-change commit `084b206` stalled at the identical step, proving the failure predates the module refactor.
- Two further clean isolated retries on the task branch stalled at the identical step.

Logs show healthy topology refresh and one DiskIO warning immediately after writing starts:

```text
DiskIOUring::submit_write: fd not registered, routing to pipeline 0
```

No panic or service error was logged in the recorded attempts. Start at the
first pending `write_stream` operation and treat the DiskIO routing warning as
evidence to verify, not as the assumed root cause.
