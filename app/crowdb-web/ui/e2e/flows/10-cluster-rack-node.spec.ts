// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 10s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createNode, createRack, createStore, deployNodeServer, freePort, freePortRange, removeDiskdb, resetAll, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

test.describe('cluster · rack + node CRUD', () => {
  test('renders the SPA shell against a real empty backend', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (message) => {
      if (message.type() === 'error') {
        consoleErrors.push(message.text());
      }
    });

    await step('shell: goto', () => page.goto('/'));

    await expect(page.getByTestId('domain-cluster')).toBeVisible({ timeout: 3_000 });
    await expect(page.getByTestId('domain-kv')).toBeVisible({ timeout: 3_000 });
    await expect(page.getByPlaceholder('Filter...')).toBeVisible();

    const healthText = page.locator('header').getByText(/healthy|degraded|failed|unknown/i);
    await expect(healthText).toBeVisible({ timeout: 3_000 });

    // Ignore transient network 404s; fail only on real JS/runtime errors.
    const jsErrors = consoleErrors.filter((e) => !/Failed to load resource/i.test(e));
    expect(jsErrors, jsErrors.join('\n')).toEqual([]);
  });

  test('creates racks and nodes through the UI and verifies the real backend', async ({ page, baseURL }) => {
    // --- Add a rack through the UI ---
    await step('rack-CRUD: resetAll', () => resetAll(baseURL!));
    {
      await step('rack-CRUD: goto', () => page.goto('/'));

      await step('rack-CRUD: add rack UI', async () => {
        await page.getByTestId('domain-cluster').click();
        await page.getByRole('button', { name: 'Add Rack' }).click();

        await expect(page.getByRole('dialog', { name: 'Add Rack' })).toBeVisible();
        await page.getByLabel('Rack ID').fill('1');
        await page.getByLabel('Name (optional)').fill('Rack One');
        await page.getByRole('button', { name: /create rack/i }).click();

        await expect(page.locator('aside').getByText('Rack One')).toBeVisible({ timeout: 3_000 });
      });

      const api = await apiContext(baseURL!);
      try {
        await step('rack-CRUD: verify rack API', async () => {
          const response = await api.get('/api/racks');
          expect(response.ok(), await response.text()).toBeTruthy();
          const racks = await response.json();
          expect(racks).toEqual(
            expect.arrayContaining([
              expect.objectContaining({ id: 1, name: 'Rack One' }),
            ]),
          );
        });
      } finally {
        await api.dispose();
      }
    }

    // --- Add a node via the rack context menu (services disabled) ---
    {
      await step('rack-CRUD: createRack 3', () => createRack(baseURL!, { id: 3, name: 'Rack Three' }));

      await step('rack-CRUD: add node UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText('R-3 (Rack Three)')).toBeVisible({ timeout: 3_000 });

        // Right-click the rack row: the context menu pre-selects the rack in the
        // Add Node dialog (defaultRackId), so no manual rack selection is needed.
        await aside.getByText('R-3 (Rack Three)').click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add node/i }).click();

        await expect(page.getByRole('dialog', { name: 'Add Node' })).toBeVisible();
        await page.getByLabel('Node ID').fill('3');
        await page.getByLabel('Host').fill('127.0.0.1');
        await page.getByLabel('Enable CrowDB Storage on this node').uncheck();
        await page.getByLabel('Enable DiskDB on this node').uncheck();
        await expect(page.getByRole('button', { name: /create node/i })).toBeEnabled();
        await page.getByRole('button', { name: /create node/i }).click();

        await expect(aside.getByText('N-3', { exact: true })).toBeVisible({ timeout: 3_000 });
      });

      const api = await apiContext(baseURL!);
      try {
        await step('rack-CRUD: verify node API', async () => {
          const response = await api.get('/api/nodes');
          expect(response.ok(), await response.text()).toBeTruthy();
          const nodes = await response.json();
          expect(nodes).toEqual(
            expect.arrayContaining([
              expect.objectContaining({ id: 3, rack_id: 3, host: '127.0.0.1' }),
            ]),
          );
        });
      } finally {
        await api.dispose();
      }
    }

    // --- Add a node with CrowDB Storage and DiskDB services enabled ---
    {
      const rackId = 31;
      const nodeId = 310;
      const restPort = freePort();
      const rpcPort = freePort();
      const diskdbRpcPort = freePortRange(3);
      await step('rack-CRUD: createRack 31', () => createRack(baseURL!, { id: rackId, name: 'Rack Thirty-One' }));

      await step('rack-CRUD: add node+svc UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText(`R-${rackId} (Rack Thirty-One)`)).toBeVisible({ timeout: 3_000 });

        // Right-click the rack → Add Node. Both "Enable CrowDB Storage" and
        // "Enable DiskDB" checkboxes default to checked.
        await aside.getByText(`R-${rackId} (Rack Thirty-One)`).click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add node/i }).click();

        await expect(page.getByRole('dialog', { name: 'Add Node' })).toBeVisible();
        await page.getByLabel('Node ID').fill(String(nodeId));
        await page.getByLabel('Host').fill('127.0.0.1');

        // Both service checkboxes should be checked by default.
        await expect(page.getByLabel('Enable CrowDB Storage on this node')).toBeChecked();
        await expect(page.getByLabel('Enable DiskDB on this node')).toBeChecked();

        // Fill in unique ports for KV (REST + RPC) and DiskDB (RPC).
        // The DiskDB RPC Port field should be pre-filled with an
        // auto-incremented value (not the hardcoded 29920 base) —
        // regression: previously always 29920, causing port collisions
        // when creating multiple nodes with DiskDB.
        const diskdbPortInput = page.getByTestId('diskdb-rpc-port');
        const preFilledDiskdbPort = await diskdbPortInput.inputValue();
        expect(preFilledDiskdbPort).toMatch(/^\d+$/);
        expect(preFilledDiskdbPort).not.toBe('29920');

        await page.getByLabel('REST Port').fill(String(restPort));
        await page.getByTestId('kv-rpc-port').fill(String(rpcPort));
        await page.getByTestId('diskdb-rpc-port').fill(String(diskdbRpcPort));

        await expect(page.getByRole('button', { name: /create node/i })).toBeEnabled();
        await page.getByRole('button', { name: /create node/i }).click();

        // The node should appear in the sidebar.
        await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 3_000 });
      });

      const api = await apiContext(baseURL!);
      try {
        await step('rack-CRUD: verify node+svc API', async () => {
          // Verify the node exists via API.
          const nodesResp = await api.get('/api/nodes');
          expect(nodesResp.ok(), await nodesResp.text()).toBeTruthy();
          const nodes = await nodesResp.json();
          expect(nodes).toEqual(
            expect.arrayContaining([
              expect.objectContaining({ id: nodeId, rack_id: rackId, host: '127.0.0.1' }),
            ]),
          );

          // Verify the CrowDB Storage server was deployed (has a live pid).
          await expect.poll(async () => {
            const r = await api.get(`/api/nodes/${nodeId}/server`);
            if (!r.ok()) return 0;
            return (await r.json()).pid ?? 0;
          }, { timeout: 10_000, intervals: [100] }).toBeGreaterThan(0);

          // Verify the DiskDB instance was deployed — check /api/servers for
          // a diskdb entry with this node_id.
          await expect.poll(async () => {
            const r = await api.get('/api/servers');
            if (!r.ok()) return false;
            const servers = await r.json();
            return servers.some((s: { node_id?: number; service_type: string }) =>
              s.node_id === nodeId && s.service_type === 'diskdb');
          }, { timeout: 10_000, intervals: [100] }).toBe(true);
        });

        await step('rack-CRUD: DDB inspect UI', async () => {
          // DDB-xxx tree items live in the Cluster domain alongside KV
          // servers. The Capacity view only shows the physical disk
          // hierarchy (DG > Disk), not service items.
          await page.getByTestId('domain-cluster').click();
          // Node creation refreshes before DiskDB registration necessarily
          // completes. The API poll above establishes that registration has
          // finished; refresh once now so the tree observes that state.
          await page.getByRole('button', { name: 'Refresh' }).click();
          const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
          const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
          if (await expandNode.count() > 0) await expandNode.first().click();
          await expect(aside.getByText(`DDB-${nodeId}`, { exact: true })).toBeVisible({ timeout: 10_000 });

          // The DDB item should have a health badge (regression: previously
          // no health icon). The badge renders with a title attribute set
          // to the health status (Healthy/Degraded|Failed|Unknown).
          const ddbItem = aside.getByRole('treeitem').filter({ hasText: `DDB-${nodeId}` });
          await expect(ddbItem.getByTitle(/Healthy|Degraded|Failed|Unknown/)).toBeVisible({ timeout: 10_000 });

          // Selecting the DDB item opens the inspector. The Type field must
          // read "DiskDB" (not "CrowDB Storage"), and service_type must NOT
          // leak in as a "Parent: service_type" row — it is not a parent.
          await aside.getByText(`DDB-${nodeId}`, { exact: true }).click();
          const inspector = page.locator('aside[aria-label="Entity inspector"]');
          await expect(inspector).toBeVisible({ timeout: 3_000 });
          const ddbTypeDd = inspector.locator('dl > div').filter({ has: page.locator('dt', { hasText: 'Type' }) }).locator('dd');
          await expect(ddbTypeDd).toHaveText('DiskDB', { timeout: 3_000 });
          await expect(inspector.getByText(/service_type/i)).toHaveCount(0);

          // Selecting the KV server shows Type = "KV". KV servers live
          // beneath their physical node in the Cluster domain.
          const clusterNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` });
          if (await clusterNode.getByRole('button', { name: 'Expand' }).count()) await clusterNode.getByRole('button', { name: 'Expand' }).first().click();
          await aside.getByText(`KV-${nodeId}`, { exact: true }).click();
          const kvTypeDd = inspector.locator('dl > div').filter({ has: page.locator('dt', { hasText: 'Type' }) }).locator('dd');
          await expect(kvTypeDd).toHaveText('KV', { timeout: 3_000 });
        });
      } finally {
        await api.dispose();
        await step('rack-CRUD: teardown', () => stopNodeServer(baseURL!, nodeId));
        await step('rack-CRUD: removeDiskdb', () => removeDiskdb(baseURL!, nodeId));
      }
    }
  });

  /**
   * Destructive confirms for store / node / rack (Req §3.2, §6).
   *
   * Replica/group deletes are covered by the KV cluster specs; this closes
   * the cluster and KV *root* deletes. Each delete is confirm-gated:
   * we cancel once to prove the guard, then confirm and verify removal in
   * the DOM and via the backend.
   */
  test('confirm-gates store, node, and rack deletion', async ({ page, baseURL }) => {
    await step('del-gate: resetAll', () => resetAll(baseURL!));
    await step('del-gate: seedRackAndNode', () => seedRackAndNode(baseURL!, 25, 25));
    await step('del-gate: deployNodeServer', () => deployNodeServer(baseURL!, 25, freePort(), freePort()));
    await step('del-gate: createStore', () => createStore(baseURL!, 255, [25]));
    // A serverless node (clean to delete) and an empty rack (clean to delete).
    await step('del-gate: createNode', () => createNode(baseURL!, { id: 274, rack_id: 25 }));
    await step('del-gate: createRack', () => createRack(baseURL!, { id: 255, name: 'Rack TwentyFive Empty' }));

    const api = await apiContext(baseURL!);
    try {
      await step('del-gate: goto', () => page.goto('/'));
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      // ── Store (KV) ──────────────────────────────────────────
      await step('del-gate: delete store UI', async () => {
        await page.getByTestId('domain-kv').click();
        await expect(aside.getByText('S-255', { exact: true }).first()).toBeVisible({ timeout: 3_000 });

        // Cancel first.
        await aside.getByText('S-255', { exact: true }).first().click({ button: 'right' });
        await page.getByRole('menuitem', { name: /delete store/i }).click();
        await page.getByRole('dialog', { name: 'Delete Store' }).getByRole('button', { name: 'Cancel' }).click();
        await expect(aside.getByText('S-255', { exact: true }).first()).toBeVisible();

        // Confirm.
        await aside.getByText('S-255', { exact: true }).first().click({ button: 'right' });
        await page.getByRole('menuitem', { name: /delete store/i }).click();
        await page.getByRole('dialog', { name: 'Delete Store' }).getByRole('button', { name: /delete store/i }).click();
        await expect(aside.getByText('S-255', { exact: true })).toHaveCount(0, { timeout: 3_000 });
      });

      await step('del-gate: verify store API', async () => {
        const storesResp = await api.get('/api/stores');
        expect(storesResp.ok(), await storesResp.text()).toBeTruthy();
        expect(await storesResp.json()).not.toEqual(
          expect.arrayContaining([expect.objectContaining({ store_id: 255 })]),
        );
      });

      // ── Node (cluster, serverless n25x) ─────────────────────────
      await step('del-gate: delete node UI', async () => {
        await page.getByTestId('domain-cluster').click();
        const node25x = page.getByRole('treeitem').filter({ hasText: 'N-274' });
        await expect(node25x).toBeVisible({ timeout: 3_000 });

        await node25x.click({ button: 'right' });
        await page.getByRole('menuitem', { name: /delete node/i }).click();
        await page.getByRole('dialog', { name: 'Delete Node' }).getByRole('button', { name: 'Cancel' }).click();
        await expect(node25x).toBeVisible();

        await node25x.click({ button: 'right' });
        await page.getByRole('menuitem', { name: /delete node/i }).click();
        await page.getByRole('dialog', { name: 'Delete Node' }).getByRole('button', { name: /delete node/i }).click();
        await expect(page.getByRole('treeitem').filter({ hasText: 'N-274' })).toHaveCount(0, { timeout: 3_000 });
      });

      const nodeResp = await api.get('/api/nodes/274');
      expect(nodeResp.status()).toBe(404);

      // ── Rack (cluster, empty r25e) ──────────────────────────────
      await step('del-gate: delete rack UI', async () => {
        const rack25e = page.getByRole('treeitem').filter({ hasText: 'R-255' });
        await expect(rack25e).toBeVisible({ timeout: 3_000 });
        await rack25e.click({ button: 'right' });
        await page.getByRole('menuitem', { name: /delete rack/i }).click();
        await page.getByRole('dialog', { name: 'Delete Rack' }).getByRole('button', { name: /delete rack/i }).click();
        await expect(page.getByRole('treeitem').filter({ hasText: 'R-255' })).toHaveCount(0, { timeout: 3_000 });
      });

      await step('del-gate: verify rack API', async () => {
        const racksResp = await api.get('/api/racks');
        expect(racksResp.ok(), await racksResp.text()).toBeTruthy();
        expect(await racksResp.json()).not.toEqual(
          expect.arrayContaining([expect.objectContaining({ id: 255 })]),
        );
      });
    } finally {
      await api.dispose();
      await step('del-gate: teardown', () => stopNodeServer(baseURL!, 25));
    }
  });

  /**
   * Datacenter root (plan-datacenter-root): a fixed UI-only `datacenter`
   * node sits above racks in the Cluster sidebar and topology canvas.
   * Right-clicking it offers only Add Rack (the default DC is immutable).
   */
  test('datacenter root wraps racks; Add Rack from datacenter context menu', async ({ page, baseURL }) => {
    await step('dc-root: createRack 60', () => createRack(baseURL!, { id: 60, name: 'Rack Sixty' }));

    await step('dc-root: goto', () => page.goto('/'));
    await step('dc-root: add rack from DC UI', async () => {
      await page.getByTestId('domain-cluster').click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      // The datacenter root is the top treeitem, above the rack.
      const dcItem = aside.getByRole('treeitem').filter({ hasText: /^datacenter$/ });
      await expect(dcItem).toBeVisible({ timeout: 3_000 });
      await expect(aside.getByRole('treeitem').first()).toHaveText(/datacenter/);

      // The rack appears as the datacenter's child.
      await expect(aside.getByText('R-60 (Rack Sixty)')).toBeVisible({ timeout: 3_000 });

      // The topology canvas also renders the datacenter node at layer 0.
      await expect(page.locator('.react-flow__node').filter({ hasText: 'datacenter' })).toBeVisible({ timeout: 5_000 });

      // Right-click the datacenter → Add Rack (the only menu item).
      await aside.getByText('datacenter', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add rack/i }).click();

      await expect(page.getByRole('dialog', { name: 'Add Rack' })).toBeVisible();
      await page.getByLabel('Rack ID').fill('61');
      await page.getByLabel('Name (optional)').fill('Rack Sixty-One');
      await page.getByRole('button', { name: /create rack/i }).click();

      // The new rack appears as another child of the datacenter.
      await expect(aside.getByText('R-61 (Rack Sixty-One)')).toBeVisible({ timeout: 3_000 });
    });

    const api = await apiContext(baseURL!);
    try {
      await step('dc-root: verify rack API', async () => {
        const response = await api.get('/api/racks');
        expect(response.ok(), await response.text()).toBeTruthy();
        const racks = await response.json();
        expect(racks).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ id: 61, name: 'Rack Sixty-One' }),
          ]),
        );
      });
    } finally {
      await api.dispose();
    }
  });

  // Runs last: needs an empty backend, so it resets all registry state.
  test('rejects duplicate rack and node IDs from the add dialogs', async ({ page, baseURL }) => {
    await step('dup-id: resetAll', () => resetAll(baseURL!));

    // --- Adding a rack with an existing ID shows an error toast ---
    {
      await step('dup-id: createRack 37', () => createRack(baseURL!, { id: 37, name: 'r37' }));

      await step('dup-id: dup rack UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

        // Click Add Rack
        await aside.getByRole('button', { name: 'Add Rack' }).click();
        const dialog = page.getByRole('dialog', { name: 'Add Rack' });
        await expect(dialog).toBeVisible();
        await dialog.getByLabel('Rack ID').fill('37');
        await dialog.getByLabel('Name (optional)').fill('duplicate');

        // Submit and expect error toast
        const responsePromise = page.waitForResponse((r: any) => r.url().includes('/api/racks'));
        await dialog.getByRole('button', { name: /create rack/i }).click();
        const response = await responsePromise;
        expect(response.status()).toBe(409);
      });
    }

    // --- Adding a node with an existing ID shows an error toast ---
    {
      await step('dup-id: setup dup node', async () => {
        await createRack(baseURL!, { id: 372, name: 'r37b' });
        await createNode(baseURL!, { id: 372, rack_id: 372 });
      });

      await step('dup-id: dup node UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();

        // Right-click rack to get Add Node
        const rackItem = page.getByRole('treeitem').filter({ hasText: 'R-372' });
        await expect(rackItem).toBeVisible({ timeout: 3_000 });
        await rackItem.click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add node/i }).click();

        const dialog = page.getByRole('dialog', { name: 'Add Node' });
        await expect(dialog).toBeVisible();
        await dialog.getByLabel('Node ID').fill('372');
        await dialog.getByLabel('Host').fill('127.0.0.1');

        const responsePromise = page.waitForResponse((r: any) => r.url().includes('/api/nodes'));
        await dialog.getByRole('button', { name: /create node/i }).click();
        const response = await responsePromise;
        expect(response.status()).toBe(409);
      });
    }
  });
});
