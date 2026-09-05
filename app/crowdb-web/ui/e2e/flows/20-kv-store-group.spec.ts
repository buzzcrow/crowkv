// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 3.1s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, addGroup, clusterInit, createStore, deployNodeServer, freePort, resetAll, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

test.describe('kv cluster · store + group CRUD', () => {
  test.beforeEach(async ({ baseURL }) => {
    await resetAll(baseURL!);
  });

  test.afterEach(async ({ baseURL }) => {
    await Promise.all([5, 171, 172, 7, 8].map((id) => stopNodeServer(baseURL!, id)));
  });

  test('creates stores, groups and replicas through the UI against a real deployed server', async ({ page, baseURL }) => {
    await step('store-group: setup servers', () => Promise.all([5, 171, 172].map(async (id) => {
      await seedRackAndNode(baseURL!, id, id);
      await deployNodeServer(baseURL!, id, freePort(), freePort());
    })));
    await clusterInit(baseURL!, [5]);

    // --- store + group creation chain (store 57, groups 570 / 580) ---
    const chainApi = await apiContext(baseURL!);
    try {
      await step('store-group: create store+group UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-kv').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

        await aside.getByRole('button', { name: 'Add Store' }).click();
        await expect(page.getByRole('dialog', { name: 'Add KV Store' })).toBeVisible();
        await page.getByLabel('KV Store ID (numeric)').fill('57');
        await page.getByLabel(/^5\b/).check();
        await page.getByRole('button', { name: /create kv store/i }).click();

        await expect(aside.getByText('S-57').first()).toBeVisible({ timeout: 3_000 });

        // The fixed datacenter root sits above stores in the KV domain.
        await expect(aside.getByRole('treeitem').filter({ hasText: /^datacenter$/ })).toBeVisible({ timeout: 3_000 });
        await expect(aside.getByRole('treeitem').first()).toHaveText(/datacenter/);

        // Add the first group via the store row context menu.
        await aside.getByText('S-57').first().click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add group/i }).click();
        await expect(page.getByRole('dialog', { name: 'Add Group' })).toBeVisible();
        await page.getByLabel('Group ID (numeric)').fill('570');
        await page.getByLabel('Starting Replica ID (numeric)').fill('5700');
        await page.getByLabel(/^5\b/).check();
        await page.getByRole('button', { name: /create group/i }).click();

        // Expand the freshly-created store row (created after tree mount, so it
        // is collapsed by default) to reveal its groups.
        const store57 = page.getByRole('treeitem').filter({ hasText: 'S-57' });
        const expandStore57 = store57.getByRole('button', { name: 'Expand' });
        if (await expandStore57.count()) await expandStore57.click();
        await expect(aside.getByText('G-570').first()).toBeVisible({ timeout: 3_000 });

        // Verify parent-child: S-57 is expanded and G-570 is visible in the tree
        const store57Item = page.getByRole('treeitem').filter({ hasText: 'S-57' });
        await expect(store57Item).toHaveAttribute('aria-expanded', 'true');
        await expect(aside.getByText('G-570').first()).toBeVisible({ timeout: 3_000 });

        // Add a second group via the store row context menu.
        await aside.getByText('S-57').first().click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add group/i }).click();
        await expect(page.getByRole('dialog', { name: 'Add Group' })).toBeVisible();
        await page.getByLabel('Group ID (numeric)').fill('580');
        await page.getByLabel('Starting Replica ID (numeric)').fill('5800');
        await page.getByLabel(/^5\b/).check();
        await page.getByRole('button', { name: /create group/i }).click();

        await expect(aside.getByText('G-580').first()).toBeVisible({ timeout: 3_000 });
      });

      await step('store-group: verify API', async () => {
        const stores = await chainApi.get('/api/stores');
        expect(stores.ok(), await stores.text()).toBeTruthy();
        expect(await stores.json()).toEqual(expect.arrayContaining([expect.objectContaining({ store_id: 57 })]));

        const groups = await chainApi.get('/api/stores/57/groups');
        expect(groups.ok(), await groups.text()).toBeTruthy();
        expect(await groups.json()).toEqual(expect.arrayContaining([expect.objectContaining({ group_id: 570 }), expect.objectContaining({ group_id: 580 })]));
      });
    } finally {
      await chainApi.dispose();
    }

    // --- add a replica to an existing group (store 177, group 1770) ---
    // Reuse deployed servers while the metadata node stays available.
    await step('store-group: setup replica', async () => {
      // Seed a store with an initial group on n17a.
      await createStore(baseURL!, 177, [171]);
      await addGroup(baseURL!, 177, 1770, 17700, [171]);
    });

    const addReplicaApi = await apiContext(baseURL!);
    try {
      await step('store-group: add replica UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-kv').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText('G-1770').first()).toBeVisible({ timeout: 3_000 });

        // Right-click selects + targets the group (without toggling its expand,
        // so the existing replica row stays visible).
        await aside.getByText('G-1770').first().click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add replica/i }).click();

        await expect(page.getByRole('dialog', { name: 'Add Replica' })).toBeVisible();
        await page.getByLabel('Node', { exact: true }).selectOption('172');
        await page.getByRole('button', { name: /add replica/i }).click();

        // Verify the new replica appears in the logical tree.
        await expect(aside.getByText('LR-17701')).toBeVisible({ timeout: 3_000 });
      });

      await step('store-group: verify replica API', async () => {
        // Verify backend: two replicas in the group.
        const response = await addReplicaApi.get('/api/stores/177/groups/1770/replicas');
        expect(response.ok(), await response.text()).toBeTruthy();
        const replicas = await response.json();
        expect(replicas).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ replica_id: 17700, node_id: 171 }),
            expect.objectContaining({ replica_id: 17701, node_id: 172 }),
          ]),
        );
      });
    } finally {
      await addReplicaApi.dispose();
    }
  });

  test('deletes a replica and a group through the UI and verifies the real backend', async ({ page, baseURL }) => {
    // Keep node 7, which bootstraps group 0, alive through both deletion scenarios.
    await step('del-replica-group: setup servers', () => Promise.all([7, 8].map(async (id) => {
      await seedRackAndNode(baseURL!, id, id);
      await deployNodeServer(baseURL!, id, freePort(), freePort());
    })));
    await clusterInit(baseURL!, [7]);

    // --- delete a replica (store 77, group 770, replica 7700) ---
    await step('del-replica-group: setup replica', async () => {
      await createStore(baseURL!, 77, [7]);
      await addGroup(baseURL!, 77, 770, 7700, [7]);
    });

    const replicaApi = await apiContext(baseURL!);
    try {
      await step('del-replica-group: delete replica UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-kv').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText('LR-7700')).toBeVisible({ timeout: 3_000 });

        await aside.getByText('LR-7700').click({ button: 'right' });
        await page.getByRole('menuitem', { name: /delete replica/i }).click();
        await expect(page.getByRole('dialog', { name: 'Delete Replica' })).toBeVisible();
        await page.getByRole('button', { name: /delete replica/i }).click();

        await expect(aside.getByText('LR-7700')).toHaveCount(0, { timeout: 3_000 });
      });

      await step('del-replica-group: verify replica API', async () => {
        const response = await replicaApi.get('/api/stores/77/groups/770/replicas');
        if (response.status() === 404) {
          expect(await response.text()).toContain('group 770 in store 77 not found');
        } else {
          expect(response.ok(), await response.text()).toBeTruthy();
          expect(await response.json()).not.toEqual(expect.arrayContaining([expect.objectContaining({ replica_id: 7700 })]));
        }
      });
    } finally {
      await replicaApi.dispose();
    }

    // --- delete a group (store 88, group 880) ---
    await step('del-replica-group: setup group', async () => {
      await createStore(baseURL!, 88, [8]);
      await addGroup(baseURL!, 88, 880, 8800, [8]);
    });

    const groupApi = await apiContext(baseURL!);
    try {
      await step('del-replica-group: delete group UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-kv').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText('G-880').first()).toBeVisible({ timeout: 3_000 });

        await aside.getByText('G-880').first().click({ button: 'right' });
        await page.getByRole('menuitem', { name: /delete group/i }).click();
        await expect(page.getByRole('dialog', { name: 'Delete Group' })).toBeVisible();
        await page.getByRole('button', { name: /delete group/i }).click();

        await expect(aside.getByText('G-880')).toHaveCount(0, { timeout: 3_000 });
      });

      await step('del-replica-group: verify group API', async () => {
        const response = await groupApi.get('/api/stores/88/groups');
        expect(response.ok(), await response.text()).toBeTruthy();
        expect(await response.json()).not.toEqual(expect.arrayContaining([expect.objectContaining({ group_id: 880 })]));
      });
    } finally {
      await groupApi.dispose();
    }
  });
});
