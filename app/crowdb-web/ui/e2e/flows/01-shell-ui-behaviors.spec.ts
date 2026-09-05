// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 2.5s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import {
  addGroup,
  apiContext,
  createNode,
  createRack,
  createStore,
  deployNodeServer,
  freePort,
  resetAll,
  seedRackAndNode,
  stopNodeServer,
  waitForLeader,
} from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

function nextNumericId(values: Array<string | number>): string {
  const max = values.reduce<number>((acc, value) => {
    const raw = String(value).trim();
    if (!/^\d+$/.test(raw)) return acc;
    return Math.max(acc, Number(raw));
  }, 0);
  return String(max + 1);
}

test.describe('shell · UI behaviors', () => {
  test('dialog defaults, cancel, and tree interactions', async ({ page, baseURL }) => {
    // --- create dialog defaults and eligible candidate lists ---
    await step('shell: resetAll', () => resetAll(baseURL!));
    // Batch independent API calls to reduce total round-trip time under load.
    await step('shell: create racks', () => Promise.all([
      createRack(baseURL!, { id: 201, name: 'Rack Twenty A' }),
      createRack(baseURL!, { id: 202, name: 'Rack Twenty B' }),
      createRack(baseURL!, { id: 203, name: 'Rack Twenty C' }),
      createRack(baseURL!, { id: 204, name: 'Rack Twenty D' }),
    ]));
    await step('shell: create nodes', () => Promise.all([
      createNode(baseURL!, { id: 201, rack_id: 201 }),
      createNode(baseURL!, { id: 202, rack_id: 202 }),
      createNode(baseURL!, { id: 203, rack_id: 203 }),
      createNode(baseURL!, { id: 204, rack_id: 204 }),
    ]));
    await step('shell: deploy servers', () => Promise.all([
      deployNodeServer(baseURL!, 201, freePort(), freePort()),
      deployNodeServer(baseURL!, 202, freePort(), freePort()),
      deployNodeServer(baseURL!, 203, freePort(), freePort()),
    ]));
    await step('shell: create store', () => createStore(baseURL!, 207, [201, 202]));
    await step('shell: wait for group-0 leader', () => waitForLeader(baseURL!, 0, 0, 15_000));

    const api = await apiContext(baseURL!);
    try {
      const storesResponse = await api.get('/api/stores');
      expect(storesResponse.ok(), await storesResponse.text()).toBeTruthy();
      const stores = await storesResponse.json();
      const expectedStoreId = nextNumericId((Array.isArray(stores) ? stores : []).map((store: any) => store.store_id));
      const groupsResponse = await api.get('/api/stores/207/groups');
      expect(groupsResponse.ok(), await groupsResponse.text()).toBeTruthy();
      const groups = await groupsResponse.json();
      const expectedGroupId = nextNumericId((Array.isArray(groups) ? groups : []).map((group: any) => group.group_id));
      const expectedReplicaId = '1';

      await step('shell: goto', () => page.goto('/'));
      await page.getByTestId('domain-kv').click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      await step('shell: Add Store dialog', async () => {
        await aside.getByRole('button', { name: 'Add Store' }).click();
        const addStoreDialog = page.getByRole('dialog', { name: 'Add KV Store' });
        await expect(addStoreDialog).toBeVisible();
        await expect(addStoreDialog.getByLabel('KV Store ID (numeric)')).toHaveValue(expectedStoreId);
        await expect(addStoreDialog.getByLabel(/^201\b/)).toBeVisible();
        await expect(addStoreDialog.getByLabel(/^202\b/)).toBeVisible();
        await expect(addStoreDialog.getByLabel(/^203\b/)).toBeVisible();
        await expect(addStoreDialog.getByLabel(/^204\b/)).toHaveCount(0);
        await addStoreDialog.getByRole('button', { name: 'Cancel' }).click();
      });

      await expect(aside.getByText('S-207').first()).toBeVisible({ timeout: 10_000 });
      await step('shell: Add Group dialog', async () => {
        await aside.getByText('S-207').first().click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add group/i }).click();
        const addGroupDialog = page.getByRole('dialog', { name: 'Add Group' });
        await expect(addGroupDialog).toBeVisible();
        await expect(addGroupDialog.getByLabel('KV Store')).toHaveValue('207');
        await expect(addGroupDialog.getByLabel('Group ID (numeric)')).toHaveValue(expectedGroupId);
        await expect(addGroupDialog.getByLabel('Starting Replica ID (numeric)')).toHaveValue(expectedReplicaId);
        await expect(addGroupDialog.getByLabel(/^201\b/)).toBeVisible();
        await expect(addGroupDialog.getByLabel(/^202\b/)).toBeVisible();
        await expect(addGroupDialog.getByLabel(/^203\b/)).toBeVisible();
        await expect(addGroupDialog.getByLabel(/^204\b/)).toHaveCount(0);
        await addGroupDialog.getByLabel(/^201\b/).check();
        await addGroupDialog.getByLabel(/^202\b/).check();
        const n20cInput = addGroupDialog.getByLabel(/^203\b/);
        if (await n20cInput.isChecked()) await n20cInput.uncheck();
        const createGroupResp = page.waitForResponse((r: any) =>
          r.request().method() === 'POST' && r.url().includes('/api/stores/207/groups'));
        await addGroupDialog.getByRole('button', { name: /create group/i }).click();
        const response = await createGroupResp;
        expect(response.ok(), await response.text()).toBeTruthy();
      });

      const expectedReplicaAfterGroup = String(Number(expectedReplicaId) + 2);
      await expect(aside.getByText(`G-${expectedGroupId}`).first()).toBeVisible({ timeout: 10_000 });
      await step('shell: Add Replica dialog', async () => {
        await aside.getByText(`G-${expectedGroupId}`).first().click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add replica/i }).click();
        const addReplicaDialog = page.getByRole('dialog', { name: 'Add Replica' });
        await expect(addReplicaDialog).toBeVisible();
        await expect(addReplicaDialog.getByLabel('Replica ID (optional)')).toHaveValue(expectedReplicaAfterGroup);
        const nodeOptions = await addReplicaDialog.getByLabel('Node', { exact: true }).locator('option').evaluateAll((options) =>
          options.map((option) => ({ value: (option as HTMLOptionElement).value, disabled: (option as HTMLOptionElement).disabled })),
        );
        const optionValues = nodeOptions.filter((option) => !option.disabled).map((option) => option.value);
        expect(optionValues).toEqual(expect.arrayContaining(['203', '204']));
        expect(optionValues).not.toEqual(expect.arrayContaining(['201', '202']));
        await addReplicaDialog.getByLabel('Node', { exact: true }).selectOption('203');
        await addReplicaDialog.getByRole('button', { name: /add replica/i }).click();
      });

      await step('shell: Add Replica cancel', async () => {
        await aside.getByText(`G-${expectedGroupId}`).first().click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add replica/i }).click();
        const remainingReplicaDialog = page.getByRole('dialog', { name: 'Add Replica' });
        await expect(remainingReplicaDialog.getByLabel('Replica ID (optional)')).toHaveValue(String(Number(expectedReplicaAfterGroup) + 1));
        const remainingOptions = await remainingReplicaDialog.getByLabel('Node', { exact: true }).locator('option').evaluateAll((options) =>
          options.map((option) => ({ value: (option as HTMLOptionElement).value, disabled: (option as HTMLOptionElement).disabled })),
        );
        const remainingValues = remainingOptions.filter((option) => !option.disabled).map((option) => option.value);
        expect(remainingValues).toEqual(expect.arrayContaining(['204']));
        expect(remainingValues).not.toEqual(expect.arrayContaining(['201', '202', '203']));
        await remainingReplicaDialog.getByRole('button', { name: 'Cancel' }).click();
      });

      // Create the dedicated tree-interaction fixtures while group 0 is still
      // available. The assertions remain below, after the dialog-cancel checks.
      await step('shell: create tree racks', () => Promise.all([
        createRack(baseURL!, { id: 211, name: 'Rack Twenty One A' }),
        createRack(baseURL!, { id: 212, name: 'Rack Twenty One B' }),
        createRack(baseURL!, { id: 213, name: 'Rack Twenty One C' }),
      ]));
      await step('shell: create tree nodes', () => Promise.all([
        createNode(baseURL!, { id: 211, rack_id: 211 }),
        createNode(baseURL!, { id: 212, rack_id: 212 }),
        createNode(baseURL!, { id: 213, rack_id: 213 }),
      ]));
    } finally {
      await api.dispose();
      await step('shell: stop servers', () => Promise.all([
        stopNodeServer(baseURL!, 201),
        stopNodeServer(baseURL!, 202),
        stopNodeServer(baseURL!, 203),
      ]));
    }

    // --- dialog cancel does not create entity ---
    await step('shell: goto', () => page.goto('/'));
    await page.getByTestId('domain-cluster').click();

    await step('shell: Add Rack cancel', async () => {
      await page.getByRole('button', { name: 'Add Rack' }).click();
      const d = page.getByRole('dialog', { name: 'Add Rack' });
      await expect(d).toBeVisible();
      await d.getByLabel('Rack ID').fill('r20cancel');
      await d.getByLabel('Name (optional)').fill('Should Not Exist');
      await d.getByRole('button', { name: 'Cancel' }).click();
      await expect(d).toHaveCount(0);
    });

    const cancelAside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    await expect(cancelAside.getByText('r20cancel')).toHaveCount(0);

    const cancelApi = await apiContext(baseURL!);
    try {
      const resp = await cancelApi.get('/api/racks');
      expect(resp.ok()).toBeTruthy();
      const racks = await resp.json();
      expect(racks).not.toEqual(expect.arrayContaining([expect.objectContaining({ id: 'r20cancel' })]));
    } finally {
      await cancelApi.dispose();
    }

    // --- tree chevron vs text click behavior ---
    await step('shell: goto', () => page.goto('/'));
    await page.getByTestId('domain-cluster').click();
    const rack21a = page.getByRole('treeitem').filter({ hasText: 'R-211 (Rack Twenty One A)' });
    const node21c = page.getByRole('treeitem').filter({ hasText: 'N-213' });
    await expect(rack21a).toBeVisible({ timeout: 3_000 });
    await expect(node21c).toBeVisible({ timeout: 3_000 });

    // Chevron click collapses/expands without selecting
    await rack21a.getByRole('button', { name: 'Collapse' }).click();
    await expect(rack21a).toHaveAttribute('aria-expanded', 'false');
    await rack21a.getByRole('button', { name: 'Expand' }).click();
    await expect(rack21a).toHaveAttribute('aria-expanded', 'true');

    // Text click selects the node
    await node21c.getByRole('button', { name: 'N-213' }).click();
    await expect(node21c).toHaveAttribute('aria-selected', 'true');
  });

  test('sidebar filter, header refresh, and health pill states', async ({ page, baseURL }) => {
    // --- sidebar filter narrows tree and clearing restores all items ---
    await step('shell: resetAll', () => resetAll(baseURL!));
    await step('shell: create racks', () => Promise.all([
      createRack(baseURL!, { id: 341, name: 'Alpha' }),
      createRack(baseURL!, { id: 342, name: 'Beta' }),
      createRack(baseURL!, { id: 343, name: 'Gamma' }),
    ]));
    await step('shell: create nodes', () => Promise.all([
      createNode(baseURL!, { id: 341, rack_id: 341 }),
      createNode(baseURL!, { id: 342, rack_id: 342 }),
      createNode(baseURL!, { id: 343, rack_id: 343 }),
    ]));

    await step('shell: goto', () => page.goto('/'));
    await page.getByTestId('domain-cluster').click();

    const filterAside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const rackA = page.getByRole('treeitem').filter({ hasText: 'R-341' });
    const rackB = page.getByRole('treeitem').filter({ hasText: 'R-342' });
    const rackC = page.getByRole('treeitem').filter({ hasText: 'R-343' });

    // All visible initially
    await expect(rackA).toBeVisible({ timeout: 3_000 });
    await expect(rackB).toBeVisible();
    await expect(rackC).toBeVisible();

    // Type filter "alpha"
    await filterAside.getByPlaceholder('Filter...').fill('alpha');
    await expect(rackA).toBeVisible({ timeout: 3_000 });
    await expect(rackB).toHaveCount(0);
    await expect(rackC).toHaveCount(0);

    // Clear filter
    await filterAside.getByPlaceholder('Filter...').fill('');
    await expect(rackA).toBeVisible({ timeout: 3_000 });
    await expect(rackB).toBeVisible();
    await expect(rackC).toBeVisible();

    // --- header refresh picks up backend changes without page reload ---
    await step('shell: resetAll', () => resetAll(baseURL!));
    await step('shell: create rack', () => createRack(baseURL!, { id: 351, name: 'r35a' }));

    await step('shell: goto', () => page.goto('/'));
    await page.getByTestId('domain-cluster').click();
    const refreshAside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

    // Verify initial rack
    await expect(refreshAside.getByText('R-351')).toBeVisible({ timeout: 3_000 });

    // Add a new rack via API (backend change)
    await step('shell: create rack', () => createRack(baseURL!, { id: 352, name: 'r35b' }));

    // Click Refresh button
    await page.getByRole('button', { name: 'Refresh' }).click();

    // New rack should appear without page reload
    await expect(refreshAside.getByText('R-352')).toBeVisible({ timeout: 3_000 });
    await expect(refreshAside.getByText('R-351')).toBeVisible();

    // --- health pill shows Unknown initially and Healthy after group creation ---
    await step('shell: resetAll', () => resetAll(baseURL!));
    await step('shell: seed rack/node', () => seedRackAndNode(baseURL!, 36, 36));
    await step('shell: deploy server', () => deployNodeServer(baseURL!, 36, freePort(), freePort()));

    try {
      await step('shell: goto', () => page.goto('/'));

      // With no stores/groups, health should be Unknown
      const healthPill = page.locator('header').getByText(/Unknown|Healthy|Degraded|Failed/);
      await expect(healthPill).toContainText('Unknown', { timeout: 3_000 });

      // Create store + group with leader
      await step('shell: create store', () => createStore(baseURL!, 360, [36]));
      await step('shell: add group', () => addGroup(baseURL!, 360, 3600, 36000, [36]));
      await step('shell: wait for leader', () => waitForLeader(baseURL!, 360, 3600));

      // Click refresh to pick up the new state
      await page.getByRole('button', { name: 'Refresh' }).click();

      // Health should now be Healthy
      await expect(healthPill).toContainText('Healthy', { timeout: 10_000 });
    } finally {
      await step('shell: stop server', () => stopNodeServer(baseURL!, 36));
    }
  });
});
