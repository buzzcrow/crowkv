// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { createServer } from 'node:net';
import { test, expect } from '../fixtures/realBackend';
import {
  addDiskGroup,
  addDisksBatch,
  addGroup,
  apiContext,
  createRack,
  createStore,
  freePort,
  freePortRange,
  resetAll,
} from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

const RACK_ID = 701;
const NODE_IDS = [701, 702, 703];
const STORE_ID = 770;
const GROUP_ID = 7700;
const REPLICA_ID = 77000;
const DISK_GROUP_ID = 7710;
const DISK_ID = '0123456789abcdef-0123456789abcdef';

test.describe('todo-ui behavior · service deployment and view ownership', () => {
  test('creates three fully-enabled nodes and keeps derived DiskDB listeners disjoint', async ({ page, baseURL }) => {
    test.setTimeout(120_000);
    await step('todo-ui: reset', () => resetAll(baseURL!));
    await step('todo-ui: create rack', () => createRack(baseURL!, { id: RACK_ID, name: 'Todo UI Rack' }));

    const deploymentPorts = new Map<number, { rest: number; kvRpc: number; diskdbRpc: number }>();
    const failedResponses: string[] = [];
    page.on('response', (response) => {
      if (response.status() >= 500 && /\/api\/nodes\/\d+\/(?:server|diskdb)\/deploy/.test(response.url())) {
        failedResponses.push(`${response.status()} ${response.url()}`);
      }
    });

    try {
      for (const nodeId of NODE_IDS) {
        const rest = freePort('kv-mgmt');
        const kvRpc = freePort('kv-listen');
        // DiskDB derives its HTTP and public crowdb-rpc listeners from this
        // one UI port. Reserve three consecutive ports for every node.
        const diskdbRpc = freePortRange(3, 'diskdb-listen');
        deploymentPorts.set(nodeId, { rest, kvRpc, diskdbRpc });

        await step(`todo-ui: add node ${nodeId}`, async () => {
          if (nodeId === NODE_IDS[0]) {
            await page.goto('/');
            await page.getByTestId('domain-cluster').click();
          }
          const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
          await aside.getByText(`R-${RACK_ID} (Todo UI Rack)`).click({ button: 'right' });
          await page.getByRole('menuitem', { name: /add node/i }).click();

          const dialog = page.getByRole('dialog', { name: 'Add Node' });
          await expect(dialog).toBeVisible();
          await dialog.getByLabel('Node ID').fill(String(nodeId));
          await dialog.getByLabel('Host').fill('127.0.0.1');
          await expect(dialog.getByLabel('Enable CrowDB Storage on this node')).toBeChecked();
          await expect(dialog.getByLabel('Enable DiskDB on this node')).toBeChecked();
          await dialog.getByLabel('REST Port').fill(String(rest));
          await dialog.getByTestId('kv-rpc-port').fill(String(kvRpc));
          await dialog.getByTestId('diskdb-rpc-port').fill(String(diskdbRpc));
          const diskdbDeployResponse = page.waitForResponse(
            (response) => response.url().includes(`/api/nodes/${nodeId}/diskdb/deploy`),
            { timeout: 30_000 },
          );
          await dialog.getByRole('button', { name: /create node/i }).click();

          // A successful node creation must complete the dialog, not leave it
          // open while the two service deployments finish or report results.
          await expect(dialog).toHaveCount(0, { timeout: 20_000 });
          const diskdbResponse = await diskdbDeployResponse;
          expect(diskdbResponse.status(), await diskdbResponse.text()).toBe(201);
          await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 10_000 });
        });
      }

      const api = await apiContext(baseURL!);
      try {
        await step('todo-ui: verify service registrations and listeners', async () => {
          await expect.poll(async () => {
            const response = await api.get('/api/servers');
            if (!response.ok()) return [];
            const servers = await response.json();
            return NODE_IDS.every((nodeId) =>
              servers.some((s: any) => s.node_id === nodeId && String(s.service_type ?? 'kv').toLowerCase() === 'kv') &&
              servers.some((s: any) => s.node_id === nodeId && String(s.service_type).toLowerCase() === 'diskdb'),
            ) ? servers : [];
          }, { timeout: 30_000, intervals: [250] }).not.toEqual([]);

          const servers = await (await api.get('/api/servers')).json();
          const listeners = new Set<number>();
          for (const nodeId of NODE_IDS) {
            const ports = deploymentPorts.get(nodeId)!;
            const diskdb = servers.find((s: any) => s.node_id === nodeId && String(s.service_type).toLowerCase() === 'diskdb');
            expect(diskdb, `missing DiskDB registration for node ${nodeId}`).toBeTruthy();
            if (diskdb.pid !== undefined && diskdb.pid !== null) expect(Number(diskdb.pid)).toBeGreaterThan(0);
            const publicEndpoint = String(diskdb.endpoint ?? diskdb.rpc_url ?? '');
            expect(publicEndpoint).toMatch(new RegExp(`:${ports.diskdbRpc + 2}(?:/|$)`));
            for (const listener of [ports.diskdbRpc, ports.diskdbRpc + 1, ports.diskdbRpc + 2]) {
              expect(listeners.has(listener), `DiskDB listener port ${listener} is reused`).toBe(false);
              listeners.add(listener);
            }
          }
        });
        expect(failedResponses, `service deployment responses failed: ${failedResponses.join(', ')}`).toEqual([]);
      } finally {
        await api.dispose();
      }

      await step('todo-ui: create logical and physical test data', async () => {
        await createStore(baseURL!, STORE_ID, NODE_IDS);
        await addGroup(baseURL!, STORE_ID, GROUP_ID, REPLICA_ID, [NODE_IDS[0]]);
        // Wait for the diskdb instance to register with group-0 before
        // addDiskGroup (auto-assign owner requires a live diskdb instance).
        const diskdbPort = deploymentPorts.get(NODE_IDS[0])!.diskdbRpc + 2;
        {
          const api = await apiContext(baseURL!);
          try {
            await expect.poll(async () => {
              const response = await api.get('/api/diskdb/instances');
              if (!response.ok()) return false;
              const instances = await response.json();
              return instances.some((entry: any) => String(entry.rpc_endpoint).includes(`:${diskdbPort}`));
            }, { timeout: 15_000, intervals: [200] }).toBe(true);
          } finally {
            await api.dispose();
          }
        }
        await addDiskGroup(baseURL!, NODE_IDS[0], DISK_GROUP_ID, 'Physical Group');
        await addDisksBatch(baseURL!, NODE_IDS[0], DISK_GROUP_ID, [{ disk_id: DISK_ID }]);
        const api = await apiContext(baseURL!);
        try {
          const diskdbPort = deploymentPorts.get(NODE_IDS[0])!.diskdbRpc + 2;
          let instanceId = '';
          await expect.poll(async () => {
            const response = await api.get('/api/diskdb/instances');
            if (!response.ok()) return false;
            const instances = await response.json();
            const instance = instances.find((entry: any) => String(entry.rpc_endpoint).includes(`:${diskdbPort}`));
            instanceId = String(instance?.instance_id ?? '');
            return instanceId.length > 0;
          }, { timeout: 10_000, intervals: [100] }).toBe(true);
          const ownerResponse = await api.put(`/api/disk-groups/${RACK_ID}/${NODE_IDS[0]}/${DISK_GROUP_ID}/owner`, {
            data: { instance_id: instanceId, lease_expiry_ms: Date.now() + 3_600_000 },
          });
          expect(ownerResponse.ok(), await ownerResponse.text()).toBeTruthy();
          const bindResponse = await api.put(`/api/disk-groups/${RACK_ID}/${NODE_IDS[0]}/${DISK_GROUP_ID}/bind`, {
            data: { store_id: STORE_ID, group_id: GROUP_ID },
          });
          expect(bindResponse.ok(), await bindResponse.text()).toBeTruthy();
          await expect.poll(async () => {
            const response = await api.get('/api/diskdb/instances');
            if (!response.ok()) return false;
            const instances = await response.json();
            return instances.some((entry: any) =>
              String(entry.instance_id) === instanceId && entry.owned_dg_ids.includes(DISK_GROUP_ID),
            );
          }, { timeout: 10_000, intervals: [100] }).toBe(true);
        } finally {
          await api.dispose();
        }
      });

      await step('todo-ui: cluster owns physical children and KV server', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        const rack = aside.getByRole('treeitem').filter({ hasText: `R-${RACK_ID}` });
        if (await rack.getByRole('button', { name: 'Expand' }).count()) await rack.getByRole('button', { name: 'Expand' }).click();
        const node = aside.getByRole('treeitem').filter({ hasText: `N-${NODE_IDS[0]}` });
        await expect(node).toBeVisible({ timeout: 10_000 });
        if (await node.getByRole('button', { name: 'Expand' }).count()) await node.getByRole('button', { name: 'Expand' }).click();
        await expect(aside.getByText(`KV-${NODE_IDS[0]}`, { exact: true })).toBeVisible();
        const diskdbSubtree = aside.getByTestId(`tree-node-DDB-${NODE_IDS[0]}`);
        await expect(diskdbSubtree).toBeVisible({ timeout: 10_000 });
        await expect(diskdbSubtree.getByText(/Physical Group.*DG-7710/)).toBeVisible({ timeout: 10_000 });
        const diskGroup = diskdbSubtree.getByRole('treeitem').filter({ hasText: /DG-7710/ });
        if (await diskGroup.getByRole('button', { name: 'Expand' }).count()) await diskGroup.getByRole('button', { name: 'Expand' }).click();
        await expect(diskdbSubtree.getByText(DISK_ID.slice(0, 12), { exact: false })).toBeVisible();

        await node.click({ button: 'right' });
        await expect(page.getByRole('menuitem', { name: /ping/i })).toBeVisible();
        await expect(page.getByRole('menuitem', { name: /add disk group/i })).toHaveCount(0);
        await page.keyboard.press('Escape');

        const canvasDiskGroup = page.locator('.react-flow__node').filter({ hasText: /Physical Group.*DG-7710/ });
        await expect(canvasDiskGroup).toBeVisible({ timeout: 10_000 });
        await expect(canvasDiskGroup.getByTestId('compact-disk-stack')).toContainText(DISK_ID.slice(0, 12));
      });

      await step('todo-ui: cluster shows Store/Group/Replica under KV server', async () => {
        // The KV server in the Cluster domain must show its hosted
        // Store > Group > Replica hierarchy so users can see which
        // logical entities each server owns.
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        const kvItem = aside.getByRole('treeitem').filter({ hasText: `KV-${NODE_IDS[0]}` });
        const kvExpand = kvItem.locator('button[aria-label="Expand"]');
        if (await kvExpand.count() > 0) await kvExpand.click();
        // Store 770 should appear under the KV server.
        const storeItem = aside.getByTestId(`tree-node-S-${NODE_IDS[0]}-${STORE_ID}`);
        await expect(storeItem).toBeVisible({ timeout: 10_000 });
        const storeExpand = storeItem.locator('button[aria-label="Expand"]');
        if (await storeExpand.count() > 0) await storeExpand.click();
        // Group 7700 should appear under the store.
        const groupItem = aside.getByTestId(`tree-node-G-${NODE_IDS[0]}-${STORE_ID}-${GROUP_ID}`);
        await expect(groupItem).toBeVisible({ timeout: 5_000 });
        const groupExpand = groupItem.locator('button[aria-label="Expand"]');
        if (await groupExpand.count() > 0) await groupExpand.click();
        // Replica 77000 should appear under the group.
        await expect(aside.getByText(`LR-${REPLICA_ID}`, { exact: true })).toBeVisible({ timeout: 5_000 });

        // The canvas should also show the Store node under the KV server.
        const canvasStore = page.locator('.react-flow__node').filter({ hasText: `S-${STORE_ID}` });
        await expect(canvasStore).toBeVisible({ timeout: 10_000 });
      });

      await step('todo-ui: KV logical tree, operations center, and inspector', async () => {
        await page.getByTestId('domain-kv').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        const datacenter = aside.getByRole('treeitem').filter({ hasText: /^datacenter$/ });
        await expect(datacenter).toBeVisible();
        const store = aside.getByRole('treeitem').filter({ hasText: `S-${STORE_ID}` });
        await expect(store).toBeVisible({ timeout: 10_000 });
        if (await store.getByRole('button', { name: 'Expand' }).count()) await store.getByRole('button', { name: 'Expand' }).click();
        const group = aside.getByRole('treeitem').filter({ hasText: `G-${GROUP_ID}` });
        await expect(group).toBeVisible();
        if (await group.getByRole('button', { name: 'Expand' }).count()) await group.getByRole('button', { name: 'Expand' }).click();
        await expect(aside.getByText(`LR-${REPLICA_ID}`, { exact: true })).toBeVisible();
        // KV has one logical tree: no KV server or physical node parent exists.
        await expect(aside.getByText(`KV-${NODE_IDS[0]}`, { exact: true })).toHaveCount(0);

        await expect(page.getByLabel('Put key')).toBeVisible();
        await expect(page.getByLabel('Put value')).toBeVisible();
        await group.click();
        const inspector = page.locator('aside[aria-label="Entity inspector"]');
        await expect(inspector).toBeVisible();
        await expect(inspector.locator('div').filter({ hasText: /^Group$/ }).first()).toBeVisible();
        await expect(inspector.locator('div.tw-font-semibold').filter({ hasText: `G-${GROUP_ID}` })).toBeVisible();
      });

      await step('todo-ui: chunk hides DiskDB server', async () => {
        // Capacity view must NOT show the DDB server — it's a service
        // item that belongs in the Cluster domain only. The physical
        // disk hierarchy (DG > Disk) remains.
        await page.getByTestId('domain-chunk').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        const rack = aside.getByRole('treeitem').filter({ hasText: `R-${RACK_ID}` });
        if (await rack.getByRole('button', { name: 'Expand' }).count()) await rack.getByRole('button', { name: 'Expand' }).click();
        const node = aside.getByRole('treeitem').filter({ hasText: `N-${NODE_IDS[0]}` });
        await expect(node).toBeVisible({ timeout: 10_000 });
        if (await node.getByRole('button', { name: 'Expand' }).count()) await node.getByRole('button', { name: 'Expand' }).click();
        await expect(aside.getByText(/Physical Group.*DG-7710/)).toBeVisible();
        // DDB server must NOT appear in the Capacity view.
        await expect(aside.getByText(`DDB-${NODE_IDS[0]}`, { exact: true })).toHaveCount(0, { timeout: 5_000 });
      });
    } finally {
      await resetAll(baseURL!);
    }
  });

  test('closes the node dialog and preserves KV when DiskDB deployment fails', async ({ page, baseURL }) => {
    test.setTimeout(120_000);
    await resetAll(baseURL!);
    await createRack(baseURL!, { id: 704, name: 'Failure Rack' });

    const diskdbBase = freePortRange(3, 'diskdb-listen');
    const blocker = createServer();
    const blockerSockets = new Set<import('node:net').Socket>();
    blocker.on('connection', (sock) => {
      blockerSockets.add(sock);
      sock.on('close', () => blockerSockets.delete(sock));
    });
    const destroyAndClose = (srv: typeof blocker) => {
      for (const s of blockerSockets) s.destroy();
      blockerSockets.clear();
      return new Promise<void>((resolve) => srv.close(() => resolve()));
    };
    await new Promise<void>((resolve, reject) => {
      blocker.once('error', reject);
      blocker.listen(diskdbBase + 1, '127.0.0.1', () => resolve());
    });

    try {
      await page.goto('/');
      await page.getByTestId('domain-cluster').click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      await aside.getByText('R-704 (Failure Rack)').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add node/i }).click();

      const dialog = page.getByRole('dialog', { name: 'Add Node' });
      await dialog.getByLabel('Node ID').fill('704');
      await dialog.getByLabel('Host').fill('127.0.0.1');
      await dialog.getByLabel('REST Port').fill(String(freePort('kv-mgmt')));
      await dialog.getByTestId('kv-rpc-port').fill(String(freePort('kv-listen')));
      await dialog.getByTestId('diskdb-rpc-port').fill(String(diskdbBase));

      const diskdbResponse = page.waitForResponse((response) => response.url().includes('/api/nodes/704/diskdb/deploy'));
      await dialog.getByRole('button', { name: /create node/i }).click();
      await expect(dialog).toHaveCount(0, { timeout: 10_000 });
      expect((await diskdbResponse).status()).toBe(502);
      await expect(aside.getByText('N-704', { exact: true })).toBeVisible({ timeout: 10_000 });
      await expect.poll(async () => (await page.request.get(`${baseURL}/api/nodes/704/server`)).ok(), { timeout: 10_000 }).toBe(true);

      await aside.getByText('N-704', { exact: true }).click({ button: 'right' });
      await expect(page.getByRole('menuitem', { name: /deploy diskdb/i })).toBeVisible();

      // Release the conflict, then retry DiskDB from the node context menu.
      // The retry must only deploy DiskDB — it must not redeploy KV or
      // attempt to recreate the node.
      await destroyAndClose(blocker);

      await page.getByRole('menuitem', { name: /deploy diskdb/i }).click();
      const deployDialog = page.getByRole('dialog', { name: 'Deploy DiskDB' });
      await expect(deployDialog).toBeVisible();
      await deployDialog.getByLabel('RPC Port (crowdb-rpc)').fill(String(diskdbBase));
      const retryResponse = page.waitForResponse(
        (response) => response.url().includes('/api/nodes/704/diskdb/deploy'),
        { timeout: 30_000 },
      );
      await deployDialog.getByRole('button', { name: /deploy/i }).click();
      expect((await retryResponse).status()).toBe(201);
      await expect(deployDialog).toHaveCount(0, { timeout: 10_000 });

      // Verify the DiskDB server is registered and the node shows it.
      const api = await apiContext(baseURL!);
      try {
        await expect.poll(async () => {
          const response = await api.get('/api/servers');
          if (!response.ok()) return false;
          const servers = await response.json();
          return servers.some((s: any) => s.node_id === 704 && String(s.service_type).toLowerCase() === 'diskdb');
        }, { timeout: 30_000, intervals: [250] }).toBe(true);
      } finally {
        await api.dispose();
      }
      // Force a UI refresh — the dialog's onSuccess callback may have
      // raced with the server registration. Clicking Refresh guarantees
      // allServers is updated so DDB-704 appears in the sidebar.
      await page.getByRole('button', { name: /refresh/i }).click();
      await expect(aside.getByText('DDB-704', { exact: true })).toBeVisible({ timeout: 30_000 });
    } finally {
      await destroyAndClose(blocker);
      await resetAll(baseURL!);
    }
  });
});
