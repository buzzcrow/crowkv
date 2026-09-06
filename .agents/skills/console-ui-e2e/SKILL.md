---
name: console-ui-e2e
description: Build, diagnose, and optimize CROWDB console UI and Playwright E2E tests.
subagent: true
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console UI E2E

Apply to `app/crowdb-web/ui/src/**` and `app/crowdb-web/ui/e2e/**`, including
browser-test failures and runtime regressions. Run the affected spec for every
visible change; add a regression assertion for every UI bug fix.

## Structure and cost

- Specs: `e2e/flows/NN-<area>-<function>.spec.ts`. Prefixes order the
  single-worker suite: `0x` shell, `1x` physical, `2x` KV cluster, `3x` KV
  data, `4x` inspector, `5x` capacity/DiskDB, `9x` cross-function.
- Extend the existing page-function spec; add one only for a new function.
- Share cluster setup with `beforeAll`, clean up in `afterAll`, and run
  mutating cases last. Use unique IDs and `freePort()` per file.
- Use `resetAll` only when an empty backend is required.
- Record `// Baseline: Xs (date)`; investigate runtime above 2x.
- Measure the current exact test before changing it; saved timing reports may
  describe an earlier worktree. Compare Playwright's per-test time, not the
  command wall time that includes UI build and web-server startup.
- Instrument setup, mutation response, lifecycle poll, DOM refresh, and
  teardown separately with `stepTimer`. Optimize the first slow boundary.
- Before removing repeated setup from a cross-function flow, locate the
  dedicated specs that retain each behavior and assertion.

## Assertions

- Trace handler -> JSON -> hook -> props -> DOM before asserting unfamiliar
  state. Probe raw API data when its shape is uncertain.
- Prefer role, label, test-id, and scoped locators. Add `data-testid` for
  ambiguity; never resolve it with page-level `.first()` or `.last()`.
- Assertions time out at 3 seconds; leader election at 10 seconds.
  `expect.poll` uses `intervals: [100]`.
- Poll lifecycle state; never sleep.
- Do not assert on toast alerts. If one intercepts a click, click via `evaluate`.
- Never swallow errors, weaken assertions, add retries, or inflate waits.
  Use `waitForResponse` only when the response is an assertion target, not as
  a substitute for observing lifecycle or DOM state.

## Diagnose

- Reproduce the exact test first, then the complete affected spec. If it passes
  alone, run the original ordered selection and inspect shared backend state,
  reused IDs, port collisions, and leaked processes.
- Use the retained trace, `error-context.md`, browser console errors, raw API
  responses, and `stepTimer` output to locate the first divergence. Distinguish
  backend mutation time from the UI's post-mutation refresh.
- A strict-locator failure is an ambiguity bug. Scope to `main`, the named
  sidebar, or a named dialog; do not use page-level `.first()` or `.last()`.
- Reuse one `APIRequestContext` inside a poll or test phase. Creating and
  disposing a context on every 100 ms poll iteration adds noise and overhead.
- For asynchronous service cadence, poll the resulting API state. If the real
  production interval dominates E2E time, add an explicit test-mode interval
  at process configuration and keep production defaults unchanged.
- Verify process lifecycle with both the API record and the OS process list.
  A test that passes alone but fails under default concurrency usually has an
  isolation problem, not a timeout problem; confirm by running its test file
  serially before changing code.

Useful console/KV-client signals:

- `new standalone instance created`: transport was not shared.
- `no mgmt seeds configured`: invalid empty seed set.
- `no KV servers deployed`: cluster is not initialized.
- `no rpc endpoint resolved` or `no group-0 endpoint found`: no usable
  group-0 endpoint.
- `group-0 query failed`: RPC failed and config fallback was used.
- `topology refresh failed` or `topology discovery failed`: inspect whether
  the seed is a store listen address or node RPC address.

## Run

```sh
pixi run cargo build -p crowdb-kv-server -p crowdb-diskdb -p crowdb-port-alloc
pixi run bash -c 'export CROWDB_KV_SERVER_BINARY=$(pwd)/target/debug/crowdb-kv-server \
  && cd app/crowdb-web/ui \
  && npx playwright test --config=e2e/realBackend.config.ts e2e/flows/NN-<area>-<fn>.spec.ts'
```

Use `pixi run test-console-ui` for the full suite. If blocked, keep the
regression assertion and report why. Apply `/debug-test` for its generic
first-divergence method; this skill owns Playwright-specific diagnosis.
