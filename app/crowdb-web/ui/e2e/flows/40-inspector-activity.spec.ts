// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 5.5s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import {
  addGroup,
  createStore,
  deployNodeServer,
  freePort,
  resetAll,
  seedRackAndNode,
  stopNodeServer,
  waitForLeader,
} from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

async function openKvPanel(page: any, storeId: string, groupId: string) {
  await step('inspector: goto', () => page.goto('/'));
  await page.getByTestId('domain-kv').click();
  await page.getByTestId('kv-store-select').selectOption(storeId);
  await page.getByTestId('kv-group-select').selectOption(groupId);
}

async function putKey(page: any, key: string, value: string) {
  await step('inspector: kv put', async () => {
    await page.getByLabel('Put key').fill(key);
    await page.getByLabel('Put value').fill(value);
    const responsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
    await page.getByRole('button', { name: /^Put$/ }).click();
    const response = await responsePromise;
    expect(response.ok(), await response.text()).toBeTruthy();
  });
}

test.describe('inspector · activity log', () => {
  test.beforeEach(async ({ baseURL }) => {
    await step('inspector: resetAll', () => resetAll(baseURL!));
  });

  test('records mutations and async operations, and clear empties the log', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    // --- KV mutation appears in the activity log and clear works ---
    await step('inspector: seed rack/node', () => seedRackAndNode(baseURL!, 32, 32));
    await step('inspector: deploy server', () => deployNodeServer(baseURL!, 32, freePort(), freePort()));
    await step('inspector: create store', () => createStore(baseURL!, 320, [32]));
    await step('inspector: add group', () => addGroup(baseURL!, 320, 3200, 32000, [32]));
    await step('inspector: wait for leader', () => waitForLeader(baseURL!, 320, 3200));

    try {
      await openKvPanel(page, '320', '3200');
      await putKey(page, 'activity-key', 'activity-val');

      // Select a node in the tree to make the inspector visible
      await page.getByTestId('domain-cluster').click();

      // Try to find and click the node — rack may already be expanded
      const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-32' });
      // If rack is collapsed, expand it first
      const expandBtn = page.getByRole('treeitem').filter({ hasText: 'R-32' }).locator('button[aria-label="Expand"]');
      if (await expandBtn.count() > 0) {
        await expandBtn.click();
      }
      await nodeItem.getByRole('button', { name: 'N-32' }).click();

      // Open inspector activity tab
      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector).toBeVisible({ timeout: 10_000 });
      await inspector.getByRole('tab', { name: 'Activity' }).click();

      // Verify an entry appears (the KV Put should be logged)
      await expect(inspector.getByText(/KV Put/i)).toBeVisible({ timeout: 10_000 });

      // Click through transient toast overlays after checking the control is enabled.
      const clearBtn = inspector.getByRole('button', { name: /clear log/i });
      await expect(clearBtn).toBeEnabled({ timeout: 10_000 });
      await clearBtn.evaluate((button: HTMLButtonElement) => button.click());

      // Verify entries are removed
      await expect(inspector.getByText('No activity yet.')).toBeVisible({ timeout: 10_000 });

      // Reuse the initialized server for lifecycle activity; stop it after all assertions.
      await page.getByTestId('domain-cluster').click();
      await expect(nodeItem).toBeVisible({ timeout: 3_000 });

      // Ping — on the node context menu.
      await step('inspector: ping', async () => {
        await nodeItem.click({ button: 'right' });
        const pingResponse = page.waitForResponse((response) => response.url().includes('/nodes/32/ping'));
        await page.getByRole('menuitem', { name: /ping/i }).click();
        const response = await pingResponse;
        expect(response.ok(), await response.text()).toBeTruthy();
        expect(await response.json()).toMatchObject({ ok: true });
      });

      await expect(page.getByRole('alert').filter({ hasText: /ping/i })).toBeVisible({ timeout: 10_000 });
      await expect(inspector.getByText(/ping node/i)).toBeVisible({ timeout: 3_000 });

      // Restart and Stop are on the KV server context menu. KV-xxx
      // tree items are in the Cluster domain under their physical node.
      await page.getByTestId('domain-cluster').click();
      const serverItem = page.getByRole('treeitem').filter({ hasText: 'KV-32' });
      await expect(serverItem).toBeVisible({ timeout: 5_000 });

      // Restart — verify the deployed process returned by the real backend.
      await step('inspector: restart', async () => {
        await serverItem.click({ button: 'right' });
        const restartResponse = page.waitForResponse((r: any) => r.url().includes('/server/restart'));
        await page.getByRole('menuitem', { name: /restart CrowDB Storage/i }).click();
        const response = await restartResponse;
        expect(response.ok(), await response.text()).toBeTruthy();
        const result = await response.json();
        expect(result.node_id).toBe(32);
        expect(result.pid).toBeGreaterThan(0);
      });
      await expect(page.getByRole('alert').filter({ hasText: /restart/i })).toBeVisible({ timeout: 10_000 });

      // Stop — verify the backend accepted the shutdown signal.
      await step('inspector: stop', async () => {
        await serverItem.click({ button: 'right' });
        const stopResponse = page.waitForResponse((r: any) => r.url().includes('/server/stop'));
        await page.getByRole('menuitem', { name: /stop CrowDB Storage/i }).click();
        const response = await stopResponse;
        expect(response.ok(), await response.text()).toBeTruthy();
        const result = await response.json();
        expect(result.sent).toBe(true);
      });
      await expect(page.getByRole('alert').filter({ hasText: /stop/i })).toBeVisible({ timeout: 10_000 });

      // Verify all three operations appear in the activity log.
      // Switch back to Cluster domain to select the node.
      await page.getByTestId('domain-cluster').click();
      await nodeItem.getByRole('button', { name: 'N-32' }).click();
      await expect(inspector).toBeVisible({ timeout: 3_000 });
      await inspector.getByRole('tab', { name: 'Activity' }).click();

      await expect(inspector.getByText(/ping node/i)).toBeVisible({ timeout: 10_000 });
      await expect(inspector.getByText(/restart CrowDB Storage/i)).toBeVisible({ timeout: 10_000 });
      await expect(inspector.getByText(/stop CrowDB Storage/i)).toBeVisible({ timeout: 10_000 });
    } finally {
      await step('inspector: stop server 32', () => stopNodeServer(baseURL!, 32));
    }
  });

  test('clears stale properties when the domain changes or the cluster resets', async ({ page, baseURL }) => {
    await step('inspector: seed reset node', () => seedRackAndNode(baseURL!, 48, 48));
    await step('inspector: open selected node', async () => {
      await page.goto('/');
      const node = page.getByRole('treeitem').filter({ hasText: 'N-48' });
      await expect(node).toBeVisible();
      await node.getByRole('button', { name: 'N-48' }).click();
      await expect(page.getByRole('complementary', { name: 'Entity inspector' })).toBeVisible();

      await page.getByTestId('domain-kv').click();
      await expect(page.getByRole('complementary', { name: 'Entity inspector' })).toHaveCount(0);

      await page.getByTestId('domain-cluster').click();
      await node.getByRole('button', { name: 'N-48' }).click();
      await expect(page.getByRole('complementary', { name: 'Entity inspector' })).toBeVisible();
    });

    await step('inspector: reset clears selection', async () => {
      await page.getByRole('button', { name: /^Reset$/ }).click();
      const dialog = page.getByRole('dialog', { name: 'Delete Cluster' });
      await expect(dialog).toBeVisible();
      const responsePromise = page.waitForResponse((response) => response.url().includes('/api/cluster/destroy'));
      await dialog.getByRole('button', { name: 'Delete Cluster' }).click();
      expect((await responsePromise).ok()).toBeTruthy();
      await expect(page.getByRole('complementary', { name: 'Entity inspector' })).toHaveCount(0);
    });
  });
});
