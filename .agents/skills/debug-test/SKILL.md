---
name: debug-test
description: Diagnose a failing test from first divergence to root cause.
subagent: true
triggers:
  - user
  - model
---

<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Debug Test

Do not increase timeouts, suppress errors, weaken assertions, add caller-side
retries, or otherwise make only the symptom pass.

1. Check relevant environment variables, stale processes, logs, persisted
   state, and build artifacts.
2. Reproduce the exact test. If it passes alone, compare the affected suite
   under its default concurrency and serial execution; inspect shared IDs,
   ports, temp directories, and leaked processes.
3. List setup, action, and assertion steps. Find the first divergence using
   logs, APIs, persisted data, and timing.
4. Classify it as code/design, timing/order, or environment.
5. For an unexplained process exit, analyze its crash report before changing code.
6. Add focused temporary instrumentation only when evidence is insufficient;
   remove it afterward.
7. Fix the earliest upstream cause. Rerun the test, affected suite, and quality
   gate. Add a regression test for a code bug.

For browser or console UI tests, apply `/console-ui-e2e`; it owns Playwright
traces, locators, browser/API divergence, shared real-backend state, and UI
runtime analysis. During requirement work, follow `/implement-requirement`
retry and blocking rules.
