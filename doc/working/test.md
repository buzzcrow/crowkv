<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Test Task Backlog

<!-- DO NOT DELETE THIS FILE — it is a persistent backlog, not a per-task draft. -->

**Override:** This file is **persistent** — it is not deleted after the
requirement (R9) is complete. Only completed tasks are removed; the file
itself remains as the ongoing test task backlog. This overrides the
`/implement-requirement` workflow's cleanup step which would normally delete
`plan-<topic>.md`.

Unfinished test tasks, grouped by layer. Each task has a checkbox for tracking.
For test strategy, layer scope, and coverage details, see [`design/kv/design-crowdb-kv-test.md`](../design/kv/design-crowdb-kv-test.md).

## Current CI Test Design

CI uses six parallel jobs. The jobs are grouped by execution environment and
process isolation, not one job per Rust package. Each job pays a fixed setup
overhead for checkout, system packages, Pixi, and the Cargo cache, so grouping
compatible tests keeps wall-clock time low without mixing incompatible runtime
requirements.

The component task is the source of truth for running a package's tests. The
group task is the source of truth for assigning component tasks to a CI job.
GitHub Actions runs only the group tasks; developers can run either level
locally.

| Job | Group task | Component tasks | Environment |
| --- | --- | --- | --- |
| **Lint** | `test-task-coverage` | `cargo fmt`, `cargo clippy` | Formatting, linting, and package-to-task coverage validation |
| **CppTests** | `test-cpp` | `test-tree-ct`, `test-common-ct`, `test-rpc-ct`, `test-diskio-ct`, `test-tree-ffi`, `test-rpc-ffi` | CMake-built C++ tests and Rust FFI tests |
| **UnitTests** | `test-unit` | `test-common`, `test-protocol`, `test-kv-core`, `test-kv-client`, `test-chunkdb-client` | Pure Rust tests without subprocess dependencies |
| **ServerTests** | `test-server` | `test-kv-server`, `test-diskdb`, `test-diskdb-client`, `test-chunkdb`, `test-chunk-client`, `test-diskio-client` | Tests that spawn KV, DiskDB, or DiskIO processes |
| **ConsoleTests** | `test-console` | `test-console-shared`, `test-console-cli`, `test-console-server` | Console and lifecycle tests that spawn KV servers |
| **UITests** | `test-console-ui` | Frontend Vitest and Playwright E2E | Real backend subprocesses and system browser |

All test tasks live in the `# ── Test ──` section of `pixi.toml`. Group
tasks invoke the component tasks in a fixed order and use `set -e`, so a
component failure stops the group. Subprocess groups clean the environment
before execution, and the ServerTests, ConsoleTests, and UITests jobs perform
an `always()` cleanup after their test and artifact steps.

### Coverage guard

`pixi run test-task-coverage` runs
`tools/check-test-task-coverage.py`. It reads Cargo workspace metadata and
requires every workspace package to be assigned to a component test task. The
Lint job runs this guard before the other test jobs, preventing a new Rust
package from silently disappearing from CI.

The only allowlisted support packages are:

- `crowdb-test-harness`: support library covered through dependent package tests.
- `crowdb-port-alloc`: E2E support binary exercised by `test-console-ui`.

A new test folder inside an assigned package needs no CI mapping change because
the component task uses `cargo test -p <package> --all-targets`. A new workspace
package must be added to `TASK_PACKAGES` in the coverage script and assigned to
the appropriate component task.

### Adding tests

1. Add or update the component task in `pixi.toml`.
2. Choose the group by runtime requirements:
   - CMake-built C++ tests → `test-cpp`.
   - Rust tests without subprocesses → `test-unit`.
   - Server or storage subprocesses → `test-server`.
   - Console lifecycle or CLI subprocesses → `test-console`.
   - Browser E2E → `test-console-ui`.
3. Add a new workspace package to `TASK_PACKAGES` when applicable.
4. Run `pixi run test-task-coverage` and the affected group task locally.
5. Update `.github/workflows/ci.yml` only when adding a new CI job or changing
   the group-to-job mapping.

## Suite Timing

The historical measurements below were taken on 2026-08-28 on Linux
(post-task-fb pull `81d56124` + KV client dead-connection fix) and macOS.
The latest Linux run was performed on 2026-09-05 by running each component
Pixi task independently, in table order. Latest times are wall-clock task
times including incremental build and subprocess startup/shutdown. C++ ctest
suites report their test count from ctest; Rust and UI suites report the
runner's test results. A timeout is recorded when the task exceeded the
300-second per-suite limit; it is not counted as an assertion failure.

Status icons: ✅ = PASS (0 failures), ⚠️ = PASS with ignored tests, ❌ = TIMEOUT or failures.

| Suite               | Tests | macOS  | Linux (08-28) | Linux (09-06) | Status          |
| ------------------- | ----- | ------ | ------------- | ------------- | --------------- |
| `test-tree-ct`      | 416   | 20.1 s | 15.78 s       | 39.41 s       | ✅              |
| `test-common-ct`    | 21    | —      | 17.78 s       | 0.56 s        | ✅              |
| `test-tree-ffi`     | 30    | 13.5 s | 0.54 s        | 0.45 s        | ✅              |
| `test-rpc-ct`       | 57    | —      | 2.59 s        | 3.72 s        | ✅              |
| `test-rpc-ffi`      | 13    | —      | 0.68 s        | 0.35 s        | ✅              |
| `test-diskio-ct`    | 95    | —      | 4.46 s        | 6.56 s        | ✅              |
| `test-common`       | 65    | 21.9 s | 9.78 s        | 9.63 s        | ✅              |
| `test-protocol`     | 121   | 12.2 s | 0.70 s        | 0.06 s        | ✅              |
| `test-kv-core`      | 558   | 43.2 s | 63.20 s       | 51.42 s       | ✅              |
| `test-kv-client`    | 49    | 23.4 s | 4.70 s        | 2.91 s        | ✅              |
| `test-chunkdb-client` | 10  | 13.8 s | 2.00 s        | 0.00 s        | ✅              |
| `test-kv-server`    | 81    | 53.0 s | 43.52 s       | 43.81 s       | ✅              |
| `test-diskdb`       | 127   | 42.8 s | 25.76 s       | 56.35 s       | ✅              |
| `test-diskdb-client` | 7    | 13.9 s | 16.81 s       | 4.72 s        | ✅              |
| `test-chunkdb`      | 76    | 27.8 s | 19.75 s       | 7.45 s        | ✅              |
| `test-chunk-client` | 49    | —      | 12.11 s       | 14.50 s       | ✅              |
| `test-diskio-client` | 4    | —      | 43.22 s       | 14.69 s       | ✅              |
| `test-console-shared` | 64  | 39.2 s | 9.36 s        | 179.72 s      | ✅              |
| `test-console-cli`  | 17    | 69.4 s | 44.09 s       | 19.15 s       | ⚠️ (5 ignored)  |
| `test-console-server` | 74  | 50.7 s | 42.72 s       | 90.31 s       | ✅              |
| `test-console-ui`   | 138   | 165.7 s | 270.0 s      | 284.5 s       | ✅              |

---

## WAL Subsystem

Source: `lib/crowdb-kv/src/wal/`. Tests: 12 files, ~92 tests.

- [ ] **WAL disk-loss recovery (full fail-out)**: the full fail-out procedure
  (step-out RPC + reconfiguration, `design-crowdb-kv-wal.md` §8.1) is not yet
  implemented. The test should verify the node fails out of the group and
  rejoins via snapshot install after the disk is replaced. **Blocked** on the
  fail-out feature landing.

## Store

Source: `lib/crowdb-kv/src/store/`. Tests: 8 files, 26 tests.

- [ ] **Per-group WAL disk isolation**: `WalConfig.wal_disks` is per-`WalEngine`,
  not per-group within a store — the server startup path
  (`create_group_with_wal`) derives `wal_disks` from the store-level config, so
  groups cannot be assigned different physical disks. **Blocked** on a
  store-level config change to support per-group `wal_disks` override.

## Deployment

Source: `app/crowdb-kv-server/`. Tests: 9 files.

- [ ] **Network partition between processes**: verify cluster behavior when
  network connectivity between processes is severed and restored. **Blocked**:
  no network partition simulation infrastructure exists in the testkit.
  Needs a partition/drop mechanism (e.g. a proxy layer or toxiproxy-style
  interceptor) before the test can be written.

## Console UI E2E (`test-console-ui`) — 2026-09-06

Source: `app/crowdb-web/ui/e2e/`. 138 tests (84 Vitest + 54 Playwright),
~4.7 min (single worker, real backend + real `crowdb-kv-server` subprocess,
system Chrome). All 54 Playwright tests pass; 0 failures.

Vitest: 7 files, 84 tests, 2.75 s.
Playwright: 54 tests, 4.7 min (284.5 s).

### Fixes applied (2026-09-06)

- `http_add_rack_node` in `app/crowdb-web/src/lifecycle.rs` now guards its
  best-effort sysdata sync with `has_fully_running_group0`, matching
  `http_add_node` and `http_add_rack`. Without the guard, node creation via
  `POST /api/racks/:id/nodes` attempted a full retry budget against a
  non-existent group-0 endpoint before bootstrap, stalling the web server
  and causing cascading E2E failures (shifting failure locations, server
  disappearance, `ECONNREFUSED 127.0.0.1:4193`).
- `handleRefresh` in `app/crowdb-web/ui/src/App.tsx` now always calls
  `refreshAllServers()` regardless of the active domain. Previously
  `refreshAllServers` was conditional on `physicalActive || capacityActive`,
  which created a race when the domain was switched and Refresh was clicked
  before React re-rendered. The race caused `allServers` (which drives
  `diskdbNodeIds` and DDB tree items) to remain stale, so DDB-xxx items
  were absent from the cluster tree even though the API confirmed DiskDB
  registration.

### Slowest Playwright tests (per-test wall-clock, 2026-09-06)

| Duration | Spec:line | Test |
| -------- | --------- | ---- |
| 7.7 s    | `31-kv-ops-advanced:98` | prefix/selected/inline delete + copy, load more, all-groups |
| 7.7 s    | `13-todo-ui-behavior:28` | creates three fully-enabled nodes with disjoint DiskDB listeners |
| 6.5 s    | `51-chunk-capacity-disk:100` | disk maintenance operations, set-status, and health badges |
| 5.1 s    | `50-chunk-capacity-disk-group:565` | full deploy flow: deploy diskdb via UI, restart, stop, delete |
| 4.1 s    | `53-chunk-capacity-canvas:212` | datacenter root in Capacity sidebar; inspector shows cluster totals |
| 4.0 s    | `90-flow-full-chain:18` | rack → node → server → store → group → replica → kv |
| 3.8 s    | `22-kv-topology:354` | two groups on overlapping 3-node subsets operate independently |
| 3.7 s    | `10-cluster-rack-node:244` | confirm-gates store, node, and rack deletion |
| 2.8 s    | `50-chunk-capacity-disk-group:428` | assign disk-group to diskdb via UI (owner + bind) |
| 2.0 s    | `11-cluster-server-lifecycle:231` | deleting a node cascades service shutdown |

### Notable slow steps (>= 3 s, from `stepTimer` instrumentation)

| Duration  | Spec | Step label |
| --------- | ---- | ---------- |
| 3.6 s     | `50-chunk-capacity-disk-group` | `disk-group: DG + disk CRUD UI` |

### Runtime improvements completed

- **`10-cluster-rack-node:244`: 54.8 s → 3.7 s.** Server shutdown is
  reaped in the background and deletion skips dead group-0 retry paths.
- **`11-cluster-server-lifecycle:231`: 24.6 s → 2.0 s.** The same
  non-blocking shutdown and bounded leader refresh remove the lifecycle
  retry stall.
- **`90-flow-full-chain:18`: 11.9 s → 4.0 s.** The spec now runs one
  complete single-node chain; dedicated reconfiguration specs retain the
  multi-node Add Replica coverage.
- **`50-chunk-capacity-disk-group:428`: 10.8 s → 2.8 s.** Test-mode
  DiskDB deployments use one-second heartbeat and group-0 sync intervals;
  production defaults remain unchanged.

### UI E2E optimization and diagnosis lessons

- Remeasure the exact test before editing. The 54.8 s and 24.6 s records were
  stale after lifecycle changes already in the worktree; current baselines
  were 3.7 s and 2.2 s before further work.
- Use `stepTimer` around the mutation response, service-state poll, DOM
  refresh, and teardown. Command wall time includes the frontend build and web
  server startup and is not the per-test regression signal.
- Compare the exact test, complete spec, and original ordered selection. An
  exact pass followed by a parallel suite failure points to isolation; the
  deployer failure was confirmed as shared topology/port interference by a
  passing serial run.
- Preserve behavior while removing duplicate setup. The full-chain test keeps
  one complete UI-created path; dedicated KV specs retain multi-node Add
  Replica coverage.
- Keep timing controls at the process boundary. DiskDB's ten-second cadence
  became one second only under web-server test mode, while production defaults
  stayed unchanged.
- Poll lifecycle APIs and assert the resulting DOM instead of sleeping. Reuse
  one API request context within a poll phase.
- Treat strict-locator failures as missing scope: target `main`, the named
  sidebar, or a named dialog instead of choosing a page-level first match.

### Non-UI slow tests (2026-09-05)

Slowest individual tests from the Rust and C++ suites measured after the
optimizations below. C++ ctest reports per-test wall-clock; Rust reports
per-test-binary wall-clock (individual test functions are not timed by the
default harness). Already-fixed items (test-tree-ct GC, test-kv-core
group_test, test-diskio-client) are omitted.

| Duration | Suite | Test / binary |
| -------- | ----- | ------------- |
| 62.7 s   | `test-console-shared` | `ops_hardware_test.rs` (hardware ops tests) |
| 62.7 s   | `test-console-shared` | `lib.rs` (console-shared unit tests) |
| 31.4 s   | `test-console-shared` | `ops_kv_server_test.rs` (KV server ops) |
| 24.0 s   | `test-diskdb` | `diskdb_e2e_test.rs` (DiskDB E2E) |
| 20.3 s   | `test-console-server` | `lifecycle_routes_test.rs` (deploy/restart/stop) |
| 16.1 s   | `test-console-cli` | `lifecycle_cli_test.rs` (CLI lifecycle commands) |
| 15.2 s   | `test-console-server` | `ops_migration_test.rs` (ops migration routes) |
| 14.1 s   | `test-console-server` | `cluster_restart_incremental_test.rs` (restart cycles) |
| 11.5 s   | `test-kv-server` | `recovery_failure_test.rs` (failure recovery) |
| 11.4 s   | `test-diskio-client` | `disk_io_full_test.rs` (full disk IO E2E) |
| 10.1 s   | `test-kv-server` | `cluster_e2e_test.rs` (cluster E2E) |
| 7.6 s    | `test-console-server` | `replica_leader_removal_test.rs` (leader removal) |
| 7.4 s    | `test-kv-server` | `recovery_test.rs` (WAL recovery) |
| 6.3 s    | `test-common` | `store_test.rs` (store lifecycle) |
| 1.2 s    | `test-tree-ct` | `CompactSparseBlocksFailureInjectionTest.ReopenFromPriorAnchorIsClean/(1,true)` |

Hot spots (root-cause analysis, 2026-09-05):

- **`test-console-shared` ops_hardware + lib.rs (63 + 63 s)** — the two
  binaries dominate `test-console-shared` (180 s total). `ops_hardware_test.rs`
  has 16 `#[tokio::test]` functions exercising hardware CRUD, disk-group
  operations, and zone loading against real subprocesses. `lib.rs` has 56 unit
  tests with no sleeps/spawns — its 63 s is **build/link time + tokio runtime
  startup** (24 `#[tokio::test]` functions each start a runtime). Not a pure
  runtime bottleneck; the ops_hardware binary is the actionable target.
- **`test-console-shared` ops_kv_server_test.rs (31 s)** — KV server ops
  tests with real `crowdb-kv-server` subprocess spawns and leader election
  waits. Sharing servers across tests would help.
- **`test-diskdb` diskdb_e2e_test.rs (24 s)** — 5 tests, 15 kv-server
  spawns (3 per test). `allocate_all_free_all` does 1536 individual
  `allocate_block` RPC calls against a real cluster. Sharing clusters
  across tests or reducing allocation count would help.
- **`test-console-server` lifecycle/ops-migration (20 + 15 + 14 s)** —
  all dominated by real `crowdb-kv-server` subprocess spawns and
  `stop_pid_with_timeout(5s)` cleanup. `lifecycle_routes_test.rs` spawns
  ~6 processes with 4× 5 s stop timeouts. `ops_migration_test.rs` spawns
  1 server per test (5 tests) with 5 s stop timeout each.
  `cluster_restart_incremental_test.rs` does ~40 process spawns across 5
  tests. Sharing servers across tests or shortening stop timeout in test
  mode would help.
- **`test-console-cli` lifecycle_cli_test.rs (16 s)** — 1 test, 10 CLI
  subprocess invocations + 1 kv-server. Server bootstrap waits 30 s
  readiness + 5 s leader election. `kv server restart` and `stop` each
  pay 15 s stop timeout + 30 s readiness. Reducing CLI invocations or
  batching assertions would cut ~5 s.
- **`test-kv-server` recovery_failure_test.rs (12 s)** — 1 test spawns a
  full 3-node cluster just to assert `ZoneLoader` rejects a malformed
  disk group. Over-provisioned: an in-process single-node or mock would
  cut most of the 12 s. (Note: this file is in `crowdb-diskdb/tests/`,
  not `crowdb-kv-server/tests/`.)
- **`test-kv-server` cluster_e2e_test.rs (10 s)** — 6 tests, 20 kv-server
  spawns. `--election-profile e2e` uses 300–600 ms election timeouts.
  `wait_for_stable_leader` called 3× in the 5-node test, each with 800 ms
  `stable_for` delay. Sharing clusters across tests where topology allows
  would reduce spawn overhead.

### Applied optimizations (2026-09-05)

- **`test-tree-ct` GC/compact tests (was 53 + 31 s → now 4.5 + 4.7 s)** —
  set `SyncMode::kSkip` (already documented as "tests/CI only" in
  `page_store.h:31–35`) after `open_blocks()` in
  `lib/crowdb-tree/tests/unit/gc_test.cpp:259,313`. Also applied to
  `NormalSnapshotDoesNotRelocateSparseBlocks`. The anchor-protection test
  retains `kFull` because it reopens the store and verifies on-disk
  durability. `test-tree-ct` dropped from 71.7 s to 41.0 s.
- **`test-kv-core` group_test.rs (was 45.4 s → now 36.0 s)** — lowered
  timeout ceilings from 5/10/15 s to 3/5/5 s across all `group_test/*.rs`
  files, and reduced `r65_replication` heartbeat test from 50 puts to 10
  puts. All 98 tests still pass. `test-kv-core` dropped from 56.9 s to
  45.8 s.
- **`test-diskio-client` disk_io_full_test.rs (was 30.0 s → now 11.2 s)**
  — reduced `BENCH_CYCLES` from 100 to 25 in
  `lib/crowdb-test-harness/src/diskio.rs:560` (still 100 write+read RPCs
  per backend, sufficient for concurrency smoke), and shortened group-0
  sync interval from 2000 ms to 200 ms (`diskio.rs:201`).
  `test-diskio-client` dropped from 44.5 s to 14.6 s.
