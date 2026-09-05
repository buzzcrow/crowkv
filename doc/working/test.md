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

| Suite               | Tests | macOS  | Linux (08-28) | Linux (09-05) | Status          |
| ------------------- | ----- | ------ | ------------- | ------------- | --------------- |
| `test-tree-ct`      | 416   | 20.1 s | 15.78 s       | 71.68 s       | ✅              |
| `test-common-ct`    | 21    | —      | 17.78 s       | 24.99 s       | ✅              |
| `test-tree-ffi`     | 30    | 13.5 s | 0.54 s        | 1.04 s        | ✅              |
| `test-rpc-ct`       | 57    | —      | 2.59 s        | 27.88 s       | ✅              |
| `test-rpc-ffi`      | 13    | —      | 0.68 s        | 0.83 s        | ✅              |
| `test-diskio-ct`    | 95    | —      | 4.46 s        | 30.53 s       | ✅              |
| `test-common`       | 65    | 21.9 s | 9.78 s        | 10.01 s       | ✅              |
| `test-protocol`     | 121   | 12.2 s | 0.70 s        | 0.40 s        | ✅              |
| `test-kv-core`      | 558   | 43.2 s | 63.20 s       | 56.92 s       | ✅              |
| `test-kv-client`    | 49    | 23.4 s | 4.70 s        | 7.39 s        | ✅              |
| `test-chunkdb-client` | 10  | 13.8 s | 2.00 s        | 3.77 s        | ✅              |
| `test-kv-server`    | 81    | 53.0 s | 43.52 s       | 44.59 s       | ✅              |
| `test-diskdb`       | 127   | 42.8 s | 25.76 s       | 45.83 s       | ✅              |
| `test-diskdb-client` | 7    | 13.9 s | 16.81 s       | 13.22 s       | ✅              |
| `test-chunkdb`      | 76    | 27.8 s | 19.75 s       | 13.62 s       | ✅              |
| `test-chunk-client` | 49    | —      | 12.11 s       | 22.84 s       | ✅              |
| `test-diskio-client` | 4    | —      | 43.22 s       | 44.48 s       | ✅              |
| `test-console-shared` | 64  | 39.2 s | 9.36 s        | 188.91 s      | ✅              |
| `test-console-cli`  | 17    | 69.4 s | 44.09 s       | 27.72 s       | ⚠️ (5 ignored)  |
| `test-console-server` | 74  | 50.7 s | 42.72 s       | 107.02 s      | ✅              |
| `test-console-ui`   | 138   | 165.7 s | 270.0 s      | 270.0 s       | ✅              |

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

## Console UI E2E (`test-console-ui`) — 2026-09-05

Source: `app/crowdb-web/ui/e2e/`. 138 tests (84 Vitest + 54 Playwright),
~4.5 min (single worker, real backend + real `crowdb-kv-server` subprocess,
system Chrome). All 54 Playwright tests pass; 0 failures.

### Slowest Playwright tests (per-test wall-clock, 2026-09-05)

| Duration | Spec:line | Test |
| -------- | --------- | ---- |
| 41.4 s   | `01-shell-ui-behaviors:31` | dialog defaults, cancel, and tree interactions |
| 20.1 s   | `13-todo-ui-behavior:28` | creates three fully-enabled nodes and keeps derived DiskDB listeners disjoint |
| 12.4 s   | `90-flow-full-chain:20` | rack → node → server → store → group → replica → kv, both views |
| 10.9 s   | `50-chunk-capacity-disk-group:427` | assign disk-group to diskdb via UI (owner + bind) |
| 9.2 s    | `21-kv-reconfig:254` | stopping a non-leader keeps quorum, stopping the leader triggers reelection |

### Slow steps (>= 5 s, from `stepTimer` instrumentation)

| Duration  | Spec | Step label |
| --------- | ---- | ---------- |
| 31.4 s    | `01-shell-ui-behaviors` | `shell: create nodes` |
| 9.1 s     | `13-todo-ui-behavior` | `todo-ui: create logical and physical test data` |
| 5.3 s     | `01-shell-ui-behaviors` | `shell: Add Group dialog` |

### E2E runtime review (2026-09-05)

The tables above reflect the latest full-suite run after the initial E2E
runtime changes. A follow-up isolated run reduced `01-shell-ui-behaviors:31`
from 41.4 seconds to 10.1 seconds; that result still needs confirmation in the
next full-suite run. Each change preserves the original assertions and
coverage; setup/teardown ordering, readiness checks, and click dispatch were
adjusted.

Changes are confined to six E2E specs:

- `01-shell-ui-behaviors`: keep the dedicated racks/nodes 211–213, but create
  them before stopping the metadata servers. Wait for the real group-0 leader
  before submitting the Add Group dialog and validate its response. This
  avoids sysdata retry stalls while preserving dialog defaults, candidate
  lists, cancellation, multi-node group creation, and tree interactions.
- `11-cluster-server-lifecycle`: initialize group 0 before the
  ping/restart/stop flow. The preceding deployment test still covers
  stopping a server before cluster initialization. Removes two 15-second
  waits from keepalive registration against an unavailable group-0 endpoint.
- `20-kv-store-group`: give each test local setup and cleanup. The creation
  test deploys nodes 5, 171, 172; the deletion test deploys nodes 7, 8.
  Each initializes group 0 and keeps its metadata node alive through all
  scenarios. Previously, later metadata writes retried the endpoint of a
  server stopped by an earlier scenario for about 32 seconds.
- `31-kv-ops-advanced`: check that Scan and inline Delete are enabled,
  then dispatch their DOM clicks through transient toast overlays. Preserve
  real handlers, requests, dialogs, and table assertions. Add scan/delete
  response body checks and verify the inline-deleted key disappears.
- `40-inspector-activity`: reuse initialized node 32 for ping/restart/stop
  after checking KV mutation logging and Clear. Remove the second
  deployment, intermediate reset, and page reload. Keep the success-toast and
  persistent activity assertions, and add successful API response-body checks.
  Check Clear is enabled and dispatch its click through overlays.
- `10-cluster-rack-node`: after the API confirms DiskDB registration,
  refresh the UI before asserting the DiskDB service appears in the cluster
  tree. Removes a race between asynchronous registration and the node
  dialog's earlier refresh without changing coverage.

No production code, fsync behavior, timeout, retry, worker count, or test
count was changed.

| Test | Before | After |
| --- | --- | --- |
| Shell dialog/tree (`01:31`) | 41.4 s | 10.1 s (isolated) |
| Server ping/restart/stop (`11:160`) | 31.6 s | 2.7 s |
| Store/group/replica creation (`20:18`) | 5.1 s | 6.6 s |
| Replica/group deletion (`20:147`) | 34.5 s | 5.3 s |
| Advanced KV operations (`31:98`) | 14.2 s | 7.7 s |
| Inspector mutation/lifecycle (`40:43`) | 33.3 s | 3.7 s |
| Full suite | 6.1 min | 4.5 min |

### Remaining runtime investigations

- **`13-todo-ui-behavior:28` (20.1 s)** — three sequential `add node` UI
  dialogs (~3 s each) + `create logical and physical test data` (9.1 s).
  Each node add deploys both KV and DiskDB services. The sequential nature
  is inherent (each node build depends on the prior tree state). Retain
  all three nodes for disjoint-listener coverage.
- **`90-flow-full-chain:20` (12.4 s)** — `full-chain: add store 188 UI`
  (3.9 s). The UI add-store step pays the cluster-init + leader-election
  cost. Replacing with API seeding would remove the full-chain coverage
  this spec is intended to provide.
- **`50-chunk-capacity-disk-group:427` (10.9 s)** — `assign disk-group to
  diskdb via UI` deploys diskdb via API, polls for instance registration,
  then creates a DG + disk + owner + bind through the UI. The diskdb
  deploy + registration poll dominates (~5 s). Setup already uses the API;
  preserve the UI owner/bind operations and real capacity observation.
- **`21-kv-reconfig:254` (9.2 s)** — `quorum: openKvPanel` and
  `reelect: openKvPanel` each pay the full page reload cost. Replacing
  with the header Refresh action can race the stores request and hang
  until the test timeout; keep `page.goto('/')` for now.

### Non-UI slow tests (2026-09-05)

Slowest individual tests from the Rust and C++ suites measured in the same
run as the table above. C++ ctest reports per-test wall-clock; Rust reports
per-test-binary wall-clock (individual test functions are not timed by the
default harness).

| Duration | Suite | Test / binary |
| -------- | ----- | ------------- |
| 53.1 s   | `test-tree-ct` | `Gc.CompactSparseBlocksRespectsByteBudget` |
| 45.4 s   | `test-kv-core` | `group_test.rs` (Paxos group consensus tests) |
| 31.2 s   | `test-tree-ct` | `Gc.CompactSparseBlocksMaintainsDataIntegrity` |
| 30.0 s   | `test-diskio-client` | `disk_io_full_test.rs` (full disk IO E2E) |
| 20.4 s   | `test-console-server` | `lifecycle_routes_test.rs` (deploy/restart/stop) |
| 18.0 s   | `test-console-shared` | `lib.rs` (console-shared unit tests) |
| 16.3 s   | `test-console-cli` | `lifecycle_cli_test.rs` (CLI lifecycle commands) |
| 15.3 s   | `test-console-server` | `rolling_upgrade_test.rs` (mixed-version cluster) |
| 15.2 s   | `test-console-server` | `ops_migration_test.rs` (ops migration routes) |
| 13.8 s   | `test-tree-ct` | `CompactSparseBlocksFailureInjectionTest.ReopenFromPriorAnchorIsClean/(0,true)` |
| 12.7 s   | `test-console-server` | `cluster_restart_incremental_test.rs` (restart cycles) |
| 11.8 s   | `test-kv-server` | `recovery_failure_test.rs` (failure recovery) |
| 11.7 s   | `test-console-server` | `replica_leader_removal_test.rs` (leader removal) |
| 11.1 s   | `test-tree-ct` | `Persist.BlockCompactionGapFiltering` |
| 10.7 s   | `test-diskdb` | `diskdb_e2e_test.rs` (DiskDB E2E) |
| 10.4 s   | `test-kv-server` | `cluster_e2e_test.rs` (cluster E2E) |

Hot spots (root-cause analysis, 2026-09-05):

- **`test-tree-ct` GC/compact tests (53 + 31 s)** — both tests write only
  200 keys (25 KiB data) but run multiple full disk-durable `snapshot()`
  + `compact_sparse_blocks()` passes. Each `snapshot()` fsyncs every dirty
  block file twice (`BlockPageStore::sync`, `persist.cpp:974–1081`). The
  bottleneck is `SyncMode::kFull` (default), not the GC logic itself.
  `SyncMode::kSkip` is already documented as "tests/CI only" in
  `page_store.h:31–35` but not used in these tests. Setting it after
  `open_blocks()` would eliminate the fsync cost while exercising the
  same compaction code paths. One-line fix per test in
  `lib/crowdb-tree/tests/unit/gc_test.cpp:253,306`.
- **`test-kv-core` group_test.rs (45 s)** — 98 in-process Paxos tests
  over loopback RPC. No subprocess spawning. The time is in timeout-based
  polling loops: `wait_for_leader` (5 s ceiling, 5 ms poll), `poll_for_value`
  (5 s ceiling, 10 ms poll), and convergence checks (5–15 s ceilings).
  Actual events resolve in milliseconds. Some tests use relaxed election
  config (`election_min_ms: 500`, `election_max_ms: 1000`). The heaviest
  tests are `full_restart_delete` (60 keys, 15 s timeouts), `r65_replication`
  (50 puts + 50× convergence, 1 MB value), and `t1_early_ack_crash` (multiple
  10 s leader waits). Lowering timeout ceilings from 5–15 s to 2–3 s and
  reducing `r65` data volume (50→10 puts) would cut significantly.
- **`test-diskio-client` disk_io_full_test.rs (30 s)** — 1 test, 3
  subprocesses (1 kv-server + 2 diskio). Two backends (`NullDisk`,
  `MemDisk`) run sequentially. Each pays a 2000 ms group-0 sync wait
  (test harness override, `diskio.rs:200`) before disk discovery. The
  concurrent benchmark runs 4 threads × 100 cycles × 4096 B write+read
  per backend = 800 RPCs per backend. `BENCH_CYCLES=100` is excessive for
  E2E smoke; 25 would suffice. Shortening sync interval to 200 ms and
  reducing bench cycles would cut ~15 s.
- **`test-console-server` lifecycle/rolling-upgrade (20 + 15 + 15 s)** —
  all dominated by real `crowdb-kv-server` subprocess spawns and
  `stop_pid_with_timeout(5s)` cleanup. `lifecycle_routes_test.rs` spawns
  ~6 processes with 4× 5 s stop timeouts. `rolling_upgrade_test.rs` does
  6 spawns + 3 kills with 10–15 s leader election polls × 3.
  `ops_migration_test.rs` spawns 1 server per test (5 tests) with 5 s
  stop timeout each. Sharing servers across tests or shortening stop
  timeout in test mode would help.
- **`test-console-shared` lib.rs (18 s)** — 56 unit tests, no sleeps,
  no spawns, no timeouts. The 18 s is **build/link time + tokio runtime
  startup** (24 `#[tokio::test]` functions each start a runtime). Not a
  runtime bottleneck; no action needed.
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
- **`test-diskdb` diskdb_e2e_test.rs (11 s)** — 5 tests, 15 kv-server
  spawns (3 per test). `allocate_all_free_all` does 1536 individual
  `allocate_block` RPC calls against a real cluster. Sharing clusters
  across tests or reducing allocation count would help.
- **`test-kv-server` cluster_e2e_test.rs (10 s)** — 6 tests, 20 kv-server
  spawns. `--election-profile e2e` uses 300–600 ms election timeouts.
  `wait_for_stable_leader` called 3× in the 5-node test, each with 800 ms
  `stable_for` delay. Sharing clusters across tests where topology allows
  would reduce spawn overhead.
