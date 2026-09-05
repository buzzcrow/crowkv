// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 9s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, clusterInit, createNode, createRack, deployNodeServer, freePort, resetAll, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

test.describe('cluster · server lifecycle', () => {
  test('context menu items differ for node without server, node with server, and server', async ({ page, baseURL }) => {
    // --- Node without server: Deploy CrowDB Storage + Delete Node, no restart/stop ---
    await step('ctx-menu: resetAll', () => resetAll(baseURL!));
    {
      await step('ctx-menu: setup no-server', async () => {
        await createRack(baseURL!, { id: 490, name: 'Rack 490' });
        await createNode(baseURL!, { id: 490, rack_id: 490 });
      });

      await step('ctx-menu: no-server UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();

        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        const expandRack = aside.getByRole('treeitem').filter({ hasText: 'R-490' }).locator('button[aria-label="Expand"]');
        if (await expandRack.count() > 0) await expandRack.click();
        await expect(aside.getByText('N-490', { exact: true })).toBeVisible({ timeout: 5_000 });

        // Right-click the node in the tree.
        await aside.getByText('N-490', { exact: true }).click({ button: 'right' });

        // Should have Deploy CrowDB Storage and Delete Node.
        await expect(page.getByRole('menuitem', { name: /deploy crowdb storage/i })).toBeVisible();
        await expect(page.getByRole('menuitem', { name: /delete node/i })).toBeVisible();

        // Should NOT have restart/stop (no server deployed).
        await expect(page.getByRole('menuitem', { name: /restart crowdb storage/i })).toHaveCount(0);
        await expect(page.getByRole('menuitem', { name: /stop crowdb storage/i })).toHaveCount(0);

        await page.keyboard.press('Escape');
      });
    }

    // --- Node with server: Deploy DiskDB + Ping + Delete Node, no restart/stop on node ---
    {
      await step('ctx-menu: setup with-server', async () => {
        await createRack(baseURL!, { id: 491, name: 'Rack 491' });
        await createNode(baseURL!, { id: 491, rack_id: 491 });
        await deployNodeServer(baseURL!, 491, freePort(), freePort());
      });

      await step('ctx-menu: with-server UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();

        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        // Wait for the server to be deployed via API (KV-xxx tree items
        // are in the KV domain, not the Cluster domain).
        const waitApi = await apiContext(baseURL!);
        try {
          await expect.poll(async () => {
            const r = await waitApi.get('/api/nodes/491/server');
            return r.ok() ? await r.json() : null;
          }, { timeout: 10_000, intervals: [100] }).toBeTruthy();
        } finally {
          await waitApi.dispose();
        }

        // Right-click the node (not the server).
        await aside.getByText('N-491', { exact: true }).click({ button: 'right' });

        // Server is deployed, so no "Deploy CrowDB Storage" but "Deploy DiskDB" appears.
        await expect(page.getByRole('menuitem', { name: /deploy diskdb/i })).toBeVisible();
        await expect(page.getByRole('menuitem', { name: /ping/i })).toBeVisible();
        await expect(page.getByRole('menuitem', { name: /delete node/i })).toBeVisible();

        // Should NOT have restart/stop on the node — those are on the service.
        await expect(page.getByRole('menuitem', { name: /restart crowdb storage/i })).toHaveCount(0);
        await expect(page.getByRole('menuitem', { name: /stop crowdb storage/i })).toHaveCount(0);

        await page.keyboard.press('Escape');
      });
      await step('ctx-menu: teardown 491', () => stopNodeServer(baseURL!, 491));
    }

    // --- Server node: Restart, Stop, Delete CrowDB Storage ---
    {
      await step('ctx-menu: setup server-node', async () => {
        await createRack(baseURL!, { id: 492, name: 'Rack 492' });
        await createNode(baseURL!, { id: 492, rack_id: 492 });
        await deployNodeServer(baseURL!, 492, freePort(), freePort());
      });

      await step('ctx-menu: server-node UI', async () => {
        await page.goto('/');
        // KV-xxx tree items are in the Cluster domain under their physical node.
        await page.getByTestId('domain-cluster').click();

        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        // Wait for the server node to appear.
        await expect(aside.getByText('KV-492')).toBeVisible({ timeout: 10_000 });

        // Right-click the server (KV) node.
        await aside.getByText('KV-492', { exact: true }).click({ button: 'right' });

        await expect(page.getByRole('menuitem', { name: /restart crowdb storage/i })).toBeVisible();
        await expect(page.getByRole('menuitem', { name: /stop crowdb storage/i })).toBeVisible();
        await expect(page.getByRole('menuitem', { name: /delete crowdb storage/i })).toBeVisible();

        // Should NOT have "Deploy" or "Delete Node" on the service.
        await expect(page.getByRole('menuitem', { name: /deploy/i })).toHaveCount(0);
        await expect(page.getByRole('menuitem', { name: /delete node/i })).toHaveCount(0);

        await page.keyboard.press('Escape');
      });
      await step('ctx-menu: teardown 492', () => stopNodeServer(baseURL!, 492));
    }
  });

  test('deploys and stops a real crowdb-kv-server through the UI', async ({ page, baseURL }) => {
    await step('deploy-ui: seedRackAndNode', () => seedRackAndNode(baseURL!, 4, 4));

    const restPort = freePort();
    const rpcPort = freePort();
    const api = await apiContext(baseURL!);
    try {
      await step('deploy-ui: deploy dialog', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText('N-4', { exact: true })).toBeVisible({ timeout: 3_000 });

        await aside.getByText('N-4', { exact: true }).click({ button: 'right' });
        await page.getByRole('menuitem', { name: /deploy CrowDB Storage/i }).click();

        await expect(page.getByRole('dialog', { name: /deploy CrowDB Storage on 4/i })).toBeVisible();
        await page.getByLabel('REST Port').fill(String(restPort));
        await page.getByLabel('RPC Port').fill(String(rpcPort));
        await page.getByRole('button', { name: 'Deploy' }).click();
      });

      await step('deploy-ui: poll server', () => expect.poll(async () => {
        const server = await api.get('/api/nodes/4/server');
        if (!server.ok()) return null;
        return await server.json();
      }, { timeout: 5_000, intervals: [100] }).toEqual(
        expect.objectContaining({
          node_id: 4,
          url: `http://127.0.0.1:${restPort}`,
          rpc_url: `http://127.0.0.1:${rpcPort}`,
          pid: expect.any(Number),
        }),
      ));
    } finally {
      await step('deploy-ui: teardown', () => stopNodeServer(baseURL!, 4));
      await api.dispose();
    }
  });

  // Kept separate: needs an empty backend so the tree holds a single node.
  test('ping, restart, and stop server via context menu', async ({ page, baseURL }) => {
    await step('ping-restart-stop: resetAll', () => resetAll(baseURL!));
    await step('ping-restart-stop: setup', async () => {
      await createRack(baseURL!, { id: 27, name: 'r27' });
      await createNode(baseURL!, { id: 27, rack_id: 27 });
      await deployNodeServer(baseURL!, 27, freePort(), freePort());
      await clusterInit(baseURL!, [27]);
    });

    try {
      await step('ping-restart-stop: ping', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
        const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-27' });
        await expect(nodeItem).toBeVisible({ timeout: 3_000 });

        // Ping — on the node context menu. Verify it actually succeeds.
        await nodeItem.click({ button: 'right' });
        const pingPromise = page.waitForResponse((r: any) => r.url().includes('/ping'));
        await page.getByRole('menuitem', { name: /ping/i }).click();
        const pingResp = await pingPromise;
        expect((await pingResp.json()).ok).toBe(true);
      });

      // Restart and Stop are on the server (KV) context menu, not the
      // node. KV-xxx tree items are in the Cluster domain under the node.
      await page.getByTestId('domain-cluster').click();
      const serverItem = page.getByRole('treeitem').filter({ hasText: 'KV-27' });
      await expect(serverItem).toBeVisible({ timeout: 5_000 });

      // Restart
      await step('ping-restart-stop: restart', async () => {
        await serverItem.click({ button: 'right' });
        const restartResponse = page.waitForResponse((r: any) => r.url().includes('/server/restart'));
        await page.getByRole('menuitem', { name: /restart CrowDB Storage/i }).click();
        await restartResponse;
      });

      // Stop
      await step('ping-restart-stop: stop', async () => {
        await serverItem.click({ button: 'right' });
        const stopResponse = page.waitForResponse((r: any) => r.url().includes('/server/stop'));
        await page.getByRole('menuitem', { name: /stop CrowDB Storage/i }).click();
        await stopResponse;
      });

      // Health pill: the server badge should drop from Healthy after stop
      // (useClusterTree polls every 1s; monitor_cache is dropped on stop).
      await step('ping-restart-stop: health badge', async () => {
        const healthBadge = serverItem.locator('[title]').filter({ hasText: /^(Healthy|Failed|Unknown|Degraded)$/ });
        await expect(healthBadge.filter({ hasText: 'Healthy' })).toHaveCount(0, { timeout: 10_000 });
      });

      // After stop, verify server is no longer running via API
      const api = await apiContext(baseURL!);
      try {
        await step('ping-restart-stop: verify stopped API', async () => {
          const resp = await api.get('/api/nodes/27');
          expect(resp.ok()).toBeTruthy();
          const node = await resp.json();
          const serverState = node.server?.state ?? node.server?.status ?? 'unknown';
          expect(serverState).not.toBe('running');
        });
      } finally {
        await api.dispose();
      }
    } finally {
      await step('ping-restart-stop: teardown', () => stopNodeServer(baseURL!, 27));
    }
  });

  test('deleting a node cascades service shutdown; deleting the service keeps the node', async ({ page, baseURL }) => {
    // --- Delete node with deployed server cascades service shutdown ---
    await step('cascade: resetAll', () => resetAll(baseURL!));
    {
      await step('cascade: setup delete-node', async () => {
        await createRack(baseURL!, { id: 493, name: 'Rack 493' });
        await createNode(baseURL!, { id: 493, rack_id: 493 });
        await deployNodeServer(baseURL!, 493, freePort(), freePort());
      });

      const api = await apiContext(baseURL!);
      try {
        await step('cascade: delete node UI', async () => {
          await page.goto('/');
          await page.getByTestId('domain-cluster').click();

          const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
          // Wait for the server to be deployed via API (KV-xxx tree
          // items are in the KV domain, not the Cluster domain).
          await expect.poll(async () => {
            const r = await api.get('/api/nodes/493/server');
            return r.ok();
          }, { timeout: 10_000, intervals: [100] }).toBe(true);

          // Server is deployed before the cascade delete.
          expect((await api.get('/api/nodes/493/server')).status()).toBe(200);

          // Right-click node → Delete Node.
          await aside.getByText('N-493', { exact: true }).click({ button: 'right' });
          await page.getByRole('menuitem', { name: /delete node/i }).click();

          // Confirm delete dialog.
          const deleteDialog = page.getByRole('dialog', { name: /delete node/i });
          await expect(deleteDialog).toBeVisible();
          const confirmBtn = deleteDialog.getByRole('button', { name: /delete node/i });
          await confirmBtn.click();

          // Node should disappear from the tree.
          await expect(aside.getByText('N-493', { exact: true })).toHaveCount(0, { timeout: 10_000 });

          // Server record removed by the cascade (not orphaned), node gone.
          expect((await api.get('/api/nodes/493/server')).status()).toBe(404);
          expect((await api.get('/api/nodes/493')).status()).toBe(404);
        });
      } finally {
        await api.dispose();
      }
    }

    // --- Delete crowdb storage service removes server but keeps node ---
    {
      await step('cascade: setup delete-svc', async () => {
        await createRack(baseURL!, { id: 494, name: 'Rack 494' });
        await createNode(baseURL!, { id: 494, rack_id: 494 });
        await deployNodeServer(baseURL!, 494, freePort(), freePort());
      });

      await step('cascade: delete svc UI', async () => {
        await page.goto('/');
        // KV-xxx tree items are in the Cluster domain under their physical node.
        await page.getByTestId('domain-cluster').click();

        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText('KV-494')).toBeVisible({ timeout: 10_000 });

        // Right-click server → Delete CrowDB Storage.
        await aside.getByText('KV-494', { exact: true }).click({ button: 'right' });
        await page.getByRole('menuitem', { name: /delete crowdb storage/i }).click();

        // Confirm.
        const deleteDialog = page.getByRole('dialog', { name: /delete crowdb storage/i });
        await expect(deleteDialog).toBeVisible();
        const confirmBtn = deleteDialog.getByRole('button', { name: /delete crowdb storage/i });
        const deleteResp = page.waitForResponse((r: any) =>
          r.request().method() === 'DELETE' && r.url().includes('/api/nodes/494/server'));
        await confirmBtn.click();
        await deleteResp;

        // Server disappears from tree, node remains.
        await expect(aside.getByText('KV-494', { exact: true })).toHaveCount(0, { timeout: 10_000 });
        // Switch to Cluster domain to verify node remains.
        await page.getByTestId('domain-cluster').click();
        await expect(aside.getByText('N-494', { exact: true })).toBeVisible();
      });

      // Verify via API: node still exists, server is gone.
      const api = await apiContext(baseURL!);
      try {
        await step('cascade: verify delete-svc API', async () => {
          const nodeResp = await api.get('/api/nodes/494');
          expect(nodeResp.ok()).toBeTruthy();
          const serverResp = await api.get('/api/nodes/494/server');
          expect(serverResp.status()).toBe(404);
        });
      } finally {
        await api.dispose();
      }
    }
  });
});
