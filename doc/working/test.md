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
| `test-console-ui`   | 138   | 165.7 s | 270.0 s      | 366.0 s       | ✅              |

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
~6.1 min (single worker, real backend + real `crowdb-kv-server` subprocess,
system Chrome). All 54 Playwright tests pass; 0 failures.

### Top 10 slowest Playwright tests (per-test wall-clock, 2026-09-05)

| #  | Duration | Spec:line | Test |
| -- | -------- | --------- | ---- |
| 1  | 41.3 s   | `01-shell-ui-behaviors:31` | dialog defaults, cancel, and tree interactions |
| 2  | 34.5 s   | `20-kv-store-group:147` | deletes a replica and a group through the UI and verifies the real backend |
| 3  | 33.3 s   | `40-inspector-activity:43` | records mutations and async operations, and clear empties the log |
| 4  | 31.6 s   | `11-cluster-server-lifecycle:160` | ping, restart, and stop server via context menu |
| 5  | 19.1 s   | `21-kv-reconfig:254` | stopping a non-leader keeps quorum, stopping the leader triggers reelection |
| 6  | 17.1 s   | `13-todo-ui-behavior:28` | creates three fully-enabled nodes and keeps derived DiskDB listeners disjoint |
| 7  | 14.2 s   | `31-kv-ops-advanced:93` | prefix/selected/inline delete + copy, load more, all-groups mode, auto-scan toggle |
| 8  | 12.3 s   | `90-flow-full-chain:20` | rack → node → server → store → group → replica → kv, both views |
| 9  | 10.9 s   | `50-chunk-capacity-disk-group:427` | assign disk-group to diskdb via UI (owner + bind) |
| 10 | 8.0 s    | `13-todo-ui-behavior:267` | closes the node dialog and preserves KV when DiskDB deployment fails |

### Slow steps (>= 5 s, from `stepTimer` instrumentation)

| Duration  | Spec | Step label |
| --------- | ---- | ---------- |
| 31.4 s    | `01-shell-ui-behaviors` | `shell: create nodes` |
| 31.9 s    | `20-kv-store-group` | `del-replica-group: setup group` |
| 15.3 s    | `11-cluster-server-lifecycle` | `ping-restart-stop: restart` |
| 15.0 s    | `11-cluster-server-lifecycle` | `ping-restart-stop: teardown` |
| 15.2 s    | `40-inspector-activity` | `inspector: restart` |
| 14.7 s    | `40-inspector-activity` | `inspector: stop server 47` |
| 6.2 s     | `21-kv-reconfig` | `reelect: openKvPanel` |
| 5.9 s     | `21-kv-reconfig` | `quorum: openKvPanel` |
| 5.2 s     | `01-shell-ui-behaviors` | `shell: Add Group dialog` |

### Current issues to investigate (2026-09-05)

Prioritized list of slow / flaky items for improvement:

1. **`01-shell-ui-behaviors:31` (41.3 s)** — `shell: create nodes` (31.4 s)
   dominates: the test creates 17 rack/node pairs via UI dialogs to test
   dialog defaults and cancel behavior. Each node creation involves a
   dialog fill + API call + tree re-render. Consider splitting into
   smaller tests or using API seeding for the bulk nodes and testing
   dialog behavior on just 1–2 nodes.

2. **`20-kv-store-group:147` (34.5 s)** — `del-replica-group: setup group`
   (31.9 s) sets up a 3-replica group with leader election wait. The
   `setup group` step includes `createStore` + `addGroup` + `addReplica`
   × 2 + `waitForLeader`, each paying network + consensus round-trip
   costs. Consider batching or pre-initializing the cluster.

3. **`40-inspector-activity:43` (33.3 s)** — `inspector: restart` (15.2 s)
   + `inspector: stop server 47` (14.7 s). Each server restart/stop
   involves SIGTERM → graceful WAL flush → engine close → process exit
   (~700 ms) plus the inspector activity log polling for the async
   operation to complete. The WAL flush on shutdown is forced even with
   `--no-fsync` — skipping it in test mode would cut ~700 ms per stop.

4. **`11-cluster-server-lifecycle:160` (31.6 s)** — `ping-restart-stop:
   restart` (15.3 s) + `teardown` (15.0 s). Same root cause as #3: server
   restart/stop pays the full graceful-shutdown cost. The teardown stops
   all deployed servers in parallel but each still takes ~700 ms.

5. **`21-kv-reconfig:254` (19.1 s)** — `reelect: openKvPanel` (6.2 s) +
   `quorum: openKvPanel` (5.9 s). `page.goto('/')` + KV panel init after
   node stop/restart. The full page reload is required because
   `selectOption` hangs on stale options after node deletions. Consider
   partial DOM refresh instead of full `page.goto('/')`.

6. **`13-todo-ui-behavior:28` (17.1 s)** — three sequential `add node`
   UI dialogs (3 s each) + `create logical and physical test data`
   (4.6 s). Each node add involves a dialog with diskdb deploy. The
   sequential nature is inherent (each node build depends on the prior
   tree state).

7. **`31-kv-ops-advanced:93` (14.2 s)** — `kv: scan` (3.2 s) +
   `kv: inline delete` (3.8 s) + 6 sub-assertions. Scan/delete API calls
   + table re-renders compound; both are O(n) in key count.

8. **`90-flow-full-chain:20` (12.3 s)** — `full-chain: add store 188 UI`
   (4.0 s). The UI add-store step pays the cluster-init + leader-election
   cost. The test exercises the full rack→node→server→store→group→replica
   chain in one test.

9. **`50-chunk-capacity-disk-group:427` (10.9 s)** — `assign disk-group
   to diskdb via UI` deploys diskdb via API, polls for instance
   registration, then creates a DG + disk + owner + bind through the UI.
   The diskdb deploy + registration poll dominates (~5 s).

10. **`stopNodeServer` teardown across all tests** — each server shutdown
    takes ~700 ms (SIGTERM → graceful WAL flush → engine close → exit).
    Tests in `21`/`22` stop 3–5 servers in parallel (~2.2 s per test).
    ~15 tests × 2.2 s = ~33 s of total teardown. Options: skip WAL flush
    on test-mode shutdown, or use `resetAll` in `finally` blocks (one
    API call vs N).
