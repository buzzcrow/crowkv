// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 4.0s (2026-09-06)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, clusterInit, stopNodeServerAndReleasePorts, resetAll, waitForLeader, freePort } from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

/**
 * Cross-function end-to-end flow: rack → node → deploy server → store →
 * group → replica → KV put/get, driven entirely through the SPA against a
 * live crowdb-web + crowdb-kv-server, in both Cluster and KV Cluster views.
 *
 * One single-node chain covers every layer without duplicating the
 * dedicated multi-node replica-reconfiguration flows.
 */
test.describe('flow · full chain', () => {
  test('rack → node → server → store → group → replica → kv, both views', async ({ page, baseURL }) => {
    // --- smoke chain: single node, store 7 / group 70, KV put + get ---
    const consoleErrors: string[] = [];
    page.on('console', (m) => {
      if (m.type() === 'error') consoleErrors.push(m.text());
    });

    await step('full-chain: resetAll', () => resetAll(baseURL!));
    await step('full-chain: goto', () => page.goto('/'));

    // --- Shell renders ---
    await expect(page.getByTestId('domain-cluster')).toBeVisible({ timeout: 3_000 });
    await expect(page.getByRole('button', { name: 'KV', exact: true })).toBeVisible();
    await expect(page.getByPlaceholder('Filter...')).toBeVisible();

    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

    try {
    // --- Cluster: add rack ---
    await step('full-chain: add rack UI', async () => {
      await page.getByTestId('domain-cluster').click();
      await page.getByRole('button', { name: 'Add Rack' }).click();
      await expect(page.getByRole('dialog')).toBeVisible();
      await page.getByLabel('Rack ID').fill('77');
      await page.getByLabel('Name (optional)').fill('Rack Smoke');
      await page.getByRole('button', { name: /create rack/i }).click();
      await expect(aside.getByText('R-77 (Rack Smoke)')).toBeVisible({ timeout: 3_000 });
    });

    // --- Cluster: add node via context menu ---
    await step('full-chain: add node UI', async () => {
      await aside.getByText('R-77 (Rack Smoke)').click({ button: 'right' });
      await page.getByRole('menuitem', { name: 'Add Node' }).click();
      await expect(page.getByRole('dialog')).toBeVisible();
      await page.getByLabel('Node ID').fill('77');
      await page.getByLabel('Host').fill('127.0.0.1');
      await page.getByLabel('Enable CrowDB Storage on this node').uncheck();
      await page.getByLabel('Enable DiskDB on this node').uncheck();
      await page.getByRole('button', { name: /create node/i }).click();
      await expect(aside.getByText('N-77', { exact: true })).toBeVisible({ timeout: 3_000 });
    });

    // --- Cluster: deploy CrowDB Storage Server via context menu ---
    const restPort = freePort('kv-mgmt');
    const rpcPort = freePort('kv-listen');
    await step('full-chain: deploy server UI', async () => {
      await aside.getByText('N-77', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /Deploy CrowDB Storage/i }).click();
      await expect(page.getByRole('dialog', { name: /Deploy CrowDB Storage on 77/ })).toBeVisible();
      await page.getByLabel('REST Port').fill(String(restPort));
      await page.getByLabel('RPC Port').fill(String(rpcPort));
      await page.getByRole('button', { name: /^Deploy$/ }).click();
    });

    // Backend confirms the server is running.
    await step('full-chain: poll server', () => expect.poll(async () => {
      const api = await apiContext(baseURL!);
      try {
        const r = await api.get('/api/nodes/77/server');
        if (!r.ok()) return 0;
        const body = await r.json();
        return body.pid ?? 0;
      } finally {
        await api.dispose();
      }
    }, { timeout: 3_000, intervals: [100] }).toBeGreaterThan(0));
    expect(consoleErrors.filter((e) => !/Failed to load resource/i.test(e)), 'console errors after deploy').toEqual([]);

    // --- KV: add empty KV store on n1 ---
    await step('full-chain: clusterInit', () => clusterInit(baseURL!, [77]));
    await step('full-chain: add store UI', async () => {
      await page.getByTestId('domain-kv').click();
      await page.getByRole('button', { name: 'Add Store' }).click();
      await expect(page.getByRole('dialog', { name: 'Add KV Store' })).toBeVisible();
      await page.getByLabel('KV Store ID (numeric)').fill('7');
      await page.getByLabel(/^77\b/).check();
      await page.getByRole('button', { name: /create kv store/i }).click();
      await expect(aside.getByText('S-7').first()).toBeVisible({ timeout: 3_000 });
    });

    // --- KV: create first group in store 7 ---
    await step('full-chain: add group UI', async () => {
      await aside.getByText('S-7').first().click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add group/i }).click();
      await expect(page.getByRole('dialog', { name: 'Add Group' })).toBeVisible();
      await page.getByLabel('Group ID (numeric)').fill('70');
      await page.getByLabel('Starting Replica ID (numeric)').fill('700');
      await page.getByLabel(/^77\b/).check();
      await page.getByRole('button', { name: /create group/i }).click();

      // --- KV: expand store, see group + replica ---
      const store7 = page.getByRole('treeitem').filter({ hasText: 'S-7' });
      const expandStore7 = store7.getByRole('button', { name: 'Expand' });
      if (await expandStore7.count()) await expandStore7.click();
      await expect(aside.getByText('G-70').first()).toBeVisible({ timeout: 3_000 });
    });
    expect(consoleErrors.filter((e) => !/Failed to load resource/i.test(e)), 'console errors after group creation').toEqual([]);

    await step('full-chain: waitForLeader group 70', () =>
      waitForLeader(baseURL!, 7, 70, 10_000));

    // --- KV via KV Operator panel ---
    await step('full-chain: KV put UI', async () => {
      await page.getByTestId('domain-kv').click();

      // Wait for the KV operator panel to load stores, then select the
      // test store and group so the Put inputs are rendered.
      await expect(page.getByTestId('kv-store-select')).toBeVisible({ timeout: 5_000 });
      await page.getByTestId('kv-store-select').selectOption('7');
      await expect(page.getByTestId('kv-group-select')).toBeVisible({ timeout: 5_000 });
      await page.getByTestId('kv-group-select').selectOption('70');

      // Put
      await page.getByLabel('Put key').fill('smoke-key');
      await page.getByLabel('Put value').fill('smoke-value');
      const putResponsePromise = page.waitForResponse((r) => r.url().includes('/kv/put'));
      await page.getByRole('button', { name: /^Put$/ }).click();
      await putResponsePromise;
    });

    await step('full-chain: KV get UI', async () => {
      // Get
      await page.getByLabel('Get key').fill('smoke-key');
      await page.getByRole('button', { name: /^Get$/ }).click();
      await expect(page.getByTestId('kv-get-result')).toBeVisible({ timeout: 3_000 });
    });
    expect(consoleErrors.filter((e) => !/Failed to load resource/i.test(e)), 'console errors after KV ops').toEqual([]);

    // --- Backend verifies the full chain ---
    const api = await apiContext(baseURL!);
    try {
      await step('full-chain: verify replicas API', async () => {
        const replicas = await api.get('/api/stores/7/groups/70/replicas');
        expect(replicas.ok(), await replicas.text()).toBeTruthy();
        const list = await replicas.json();
        expect(Array.isArray(list) ? list.length : 0).toBeGreaterThanOrEqual(1);
      });
    } finally {
      await api.dispose();
    }

    // Ignore transient network 404s (e.g. a KV poll racing store
    // creation); fail only on real JS/runtime errors.
    const jsErrors = consoleErrors.filter((e) => !/Failed to load resource/i.test(e));
    expect(jsErrors, jsErrors.join('\n')).toEqual([]);
    } finally {
      // Stop the smoke server so it does not pollute the rest of the flow
      // (its bootstrap store 1 would otherwise aggregate into later views).
      await step('full-chain: teardown smoke', () => stopNodeServerAndReleasePorts(baseURL!, 77));
    }

  });
});
