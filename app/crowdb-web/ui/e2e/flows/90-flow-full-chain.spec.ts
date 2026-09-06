// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 4.7s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, clusterInit, stopNodeServerAndReleasePorts, resetAll, waitForLeader, freePort } from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

/**
 * Cross-function end-to-end flow: rack → node → deploy server → store →
 * group → replica → KV put/get, driven entirely through the SPA against a
 * live crowdb-web + crowdb-kv-server, in both Cluster and KV Cluster views.
 *
 * The two source flows cannot share one setup: the second half resets the
 * backend to an empty registry before rebuilding the whole chain through
 * the UI. They therefore run as one long test, smoke chain first, reset +
 * multi-node chain second.
 */
test.describe('flow · full chain', () => {
  test('rack → node → server → store → group → replica → kv, both views', async ({ page, baseURL }) => {
    // --- smoke chain: single node, store 7 / group 70, KV put + get ---
    const consoleErrors: string[] = [];
    page.on('console', (m) => {
      if (m.type() === 'error') consoleErrors.push(m.text());
    });

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
    await step('full-chain: deploy server UI', async () => {
      await aside.getByText('N-77', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /Deploy CrowDB Storage/i }).click();
      await expect(page.getByRole('dialog', { name: /Deploy CrowDB Storage on 77/ })).toBeVisible();
      await page.getByLabel('REST Port').fill('9901');
      await page.getByLabel('RPC Port').fill('9902');
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

    // Wait for a leader to be elected before KV operations. GroupView has
    // no top-level leader field — the leader is the replica self-reporting
    // role "leader" (snake_case on the wire).
    await step('full-chain: poll leader', () => expect.poll(async () => {
      const api = await apiContext(baseURL!);
      try {
        const r = await api.get('/api/stores/7/groups/70');
        if (!r.ok()) return false;
        const body = await r.json();
        const replicas: any[] = Array.isArray(body.replicas) ? body.replicas : [];
        return replicas.some((rep) => String(rep.role).toLowerCase() === 'leader');
      } finally {
        await api.dispose();
      }
    }, { timeout: 10_000, intervals: [100] }).toBe(true));

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

    // --- full chain: fresh backend, two nodes, store 188 / group 1880 + added replica ---
    const api = await apiContext(baseURL!);
    try {
      await step('full-chain: resetAll', () => resetAll(baseURL!));
      await step('full-chain: goto (full)', async () => {
        await page.goto('/');
        await expect(page.getByTestId('domain-cluster')).toBeVisible({ timeout: 3_000 });
        await page.getByTestId('domain-cluster').click();
        await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 3_000 });
      });

      // 1. Add rack r18.
      await step('full-chain: add rack 18 UI', async () => {
        await page.locator('aside').getByRole('button', { name: 'Add Rack' }).click();
        await expect(page.getByRole('dialog', { name: 'Add Rack' })).toBeVisible();
        await page.getByLabel('Rack ID').fill('18');
        await page.getByLabel('Name (optional)').fill('Rack Eighteen');
        await page.getByRole('button', { name: /create rack/i }).click();
      });

      // 2. Add node n18a to r18 via rack context menu.
      await step('full-chain: add node 181 UI', async () => {
        await page.getByRole('treeitem').filter({ hasText: 'Rack Eighteen' }).click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add node/i }).click();
        await expect(page.getByRole('dialog', { name: 'Add Node' })).toBeVisible();
        await page.getByLabel('Rack', { exact: true }).selectOption('18');
        await page.getByLabel('Node ID').fill('181');
        await page.getByLabel('Host').fill('127.0.0.1');
        await page.getByLabel('Enable CrowDB Storage on this node').uncheck();
        await page.getByLabel('Enable DiskDB on this node').uncheck();
        await page.getByRole('button', { name: /create node/i }).click();
      });

      // 3. Add node n18b to r18 via rack context menu.
      await step('full-chain: add node 182 UI', async () => {
        await page.getByRole('treeitem').filter({ hasText: 'Rack Eighteen' }).click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add node/i }).click();
        await expect(page.getByRole('dialog', { name: 'Add Node' })).toBeVisible();
        await page.getByLabel('Rack', { exact: true }).selectOption('18');
        await page.getByLabel('Node ID').fill('182');
        await page.getByLabel('Host').fill('127.0.0.1');
        await page.getByLabel('Enable CrowDB Storage on this node').uncheck();
        await page.getByLabel('Enable DiskDB on this node').uncheck();
        await page.getByRole('button', { name: /create node/i }).click();
      });

      // Ensure rack r18 is expanded so its nodes are visible. The tree may
      // have mounted with racks from earlier specs (shared test-mode backend),
      // leaving the freshly-added r18 collapsed.
      const rack18 = page.getByRole('treeitem', { name: /R-18 \(Rack Eighteen\)/ });
      const expandRack18 = rack18.getByRole('button', { name: 'Expand' });
      if (await expandRack18.count()) await expandRack18.click();

      // 4. Deploy CrowDB Storage Server on n18a.
      const restPort181 = freePort('kv-mgmt');
      const rpcPort181 = freePort('kv-listen');
      await step('full-chain: deploy 181 UI', async () => {
        await page.getByRole('treeitem').filter({ hasText: 'N-181' }).click({ button: 'right' });
        await page.getByRole('menuitem', { name: /deploy CrowDB Storage/i }).click();
        await expect(page.getByRole('dialog', { name: /deploy CrowDB Storage on 181/i })).toBeVisible();
        await page.getByLabel('REST Port').fill(String(restPort181));
        await page.getByLabel('RPC Port').fill(String(rpcPort181));
        await page.getByRole('button', { name: 'Deploy' }).click();
      });

      // 5. Deploy CrowDB Storage Server on n18b.
      const restPort182 = freePort('kv-mgmt');
      const rpcPort182 = freePort('kv-listen');
      await step('full-chain: deploy 182 UI', async () => {
        await page.getByRole('treeitem').filter({ hasText: 'N-182' }).click({ button: 'right' });
        await page.getByRole('menuitem', { name: /deploy CrowDB Storage/i }).click();
        await expect(page.getByRole('dialog', { name: /deploy CrowDB Storage on 182/i })).toBeVisible();
        await page.getByLabel('REST Port').fill(String(restPort182));
        await page.getByLabel('RPC Port').fill(String(rpcPort182));
        await page.getByRole('button', { name: 'Deploy' }).click();
      });

      // Verify both servers are running via API before proceeding.
      await step('full-chain: poll servers', async () => {
        await expect.poll(async () => {
          const r = await api.get('/api/nodes/181/server');
          if (!r.ok()) return 0;
          return (await r.json()).pid ?? 0;
        }, { timeout: 3_000, intervals: [100] }).toBeGreaterThan(0);
        await expect.poll(async () => {
          const r = await api.get('/api/nodes/182/server');
          if (!r.ok()) return 0;
          return (await r.json()).pid ?? 0;
        }, { timeout: 3_000, intervals: [100] }).toBeGreaterThan(0);
      });

      // Switch to KV view.
      await page.getByTestId('domain-kv').click();
      await expect(page.getByRole('heading', { name: 'KV' })).toBeVisible({ timeout: 3_000 });

      // 6. Create empty store 188 on both nodes.
      await step('full-chain: clusterInit (full)', () => clusterInit(baseURL!, [181, 182]));
      // Wait for group-0 leader election to settle before creating stores.
      // The election may take a few seconds to converge after init.
      await step('full-chain: waitForLeader group 0', () => waitForLeader(baseURL!, 0, 0, 15_000));
      await step('full-chain: add store 188 UI', async () => {
        await page.locator('aside').getByRole('button', { name: 'Add Store' }).click();
        await expect(page.getByRole('dialog', { name: 'Add KV Store' })).toBeVisible();
        await page.getByLabel('KV Store ID (numeric)').fill('188');
        await page.getByLabel(/^181\b/).check();
        await step('full-chain: submit store 188', async () => {
          const storeResponse = page.waitForResponse((response) =>
            response.request().method() === 'POST' && response.url().endsWith('/api/stores'));
          await page.getByRole('button', { name: /create kv store/i }).click();
          const response = await storeResponse;
          expect(response.ok(), await response.text()).toBeTruthy();
          expect(await response.json()).toMatchObject({ store_id: 188, nodes: [181, 182] });
        });
        // Wait for the dialog to close AND S-188 to appear — the tree
        // poll may surface S-188 before the addStore API call returns
        // and the dialog closes, so checking only S-188 is racy.
        await expect(page.getByRole('dialog', { name: 'Add KV Store' })).toBeHidden({ timeout: 10_000 });
        await expect(aside.getByText('S-188').first()).toBeVisible({ timeout: 10_000 });
      });

      await step('full-chain: add group 1880 UI', async () => {
        await aside.getByText('S-188').first().click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add group/i }).click();
        await expect(page.getByRole('dialog', { name: 'Add Group' })).toBeVisible();
        await page.getByLabel('Group ID (numeric)').fill('1880');
        await page.getByLabel('Starting Replica ID (numeric)').fill('18800');
        await page.getByLabel(/^182\b/).uncheck();
        await page.getByLabel(/^181\b/).check();
        await page.getByRole('button', { name: /create group/i }).click();

        // Store created after tree mount -> expand it to reveal its group.
        const store188 = page.getByRole('treeitem').filter({ hasText: 'S-188' });
        const expandStore188 = store188.getByRole('button', { name: 'Expand' });
        if (await expandStore188.count()) await expandStore188.click();
      });

      // 7. Add replica to group 1880 on n18b via UI.
      await step('full-chain: add replica UI', async () => {
        await expect(aside.getByText('G-1880').first()).toBeVisible({ timeout: 10_000 });
        const group1880 = page.getByRole('treeitem').filter({ hasText: 'G-1880' });
        const expandGroup1880 = group1880.getByRole('button', { name: 'Expand' });
        if (await expandGroup1880.count()) await expandGroup1880.click();
        await aside.getByText('G-1880').first().click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add replica/i }).click();
        await expect(page.getByRole('dialog', { name: 'Add Replica' })).toBeVisible();
        await page.getByLabel('Node', { exact: true }).selectOption('182');
        await page.getByRole('button', { name: /add replica/i }).click();

        // Verify both replicas exist in the tree.
        await expect(aside.getByText('LR-18800')).toBeVisible({ timeout: 10_000 });
        await expect(aside.getByText('LR-18801')).toBeVisible({ timeout: 10_000 });
      });

      await step('full-chain: verify replicas API', async () => {
        const response = await api.get('/api/stores/188/groups/1880/replicas');
        expect(response.ok(), await response.text()).toBeTruthy();
        const replicas = await response.json();
        expect(replicas).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ replica_id: 18800, node_id: 181 }),
            expect.objectContaining({ replica_id: 18801, node_id: 182 }),
          ]),
        );
      });
    } finally {
      await api.dispose();
      await step('full-chain: teardown full', () => Promise.all([
        stopNodeServerAndReleasePorts(baseURL!, 181),
        stopNodeServerAndReleasePorts(baseURL!, 182),
      ]));
    }
  });
});
