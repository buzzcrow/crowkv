// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 3.0s (2026-08-16)

import { test, expect, consoleBaseURL } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, freePort, resetAll, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

// One rack/node/server shared by every test in this file (IDs reused from
// the former 26-kv-demo spec so they stay unique):
//   store 261 · groups 2610 + 2611 — advanced ops, all-groups mode,
//     auto-scan toggle and the demo inject/delete-all flows.
//   store 262 · group 2620 — the 120-key load-more corpus, kept in its own
//     store so it never pollutes store 261's unfiltered/all-groups scans.
const apiBase = consoleBaseURL();

async function openKvPanel(page: any, storeId: string, groupId?: string) {
  await step('kv: goto', () => page.goto('/'));
  await page.getByTestId('domain-kv').click();
  await page.getByTestId('kv-store-select').selectOption(storeId);
  if (groupId !== undefined) {
    await page.getByTestId('kv-group-select').selectOption(groupId);
  }
}

async function putKey(page: any, key: string, value: string) {
  await step('kv: put', async () => {
    await page.getByLabel('Put key').fill(key);
    await page.getByLabel('Put value').fill(value);
    const responsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
    await page.getByRole('button', { name: /^Put$/ }).click();
    const response = await responsePromise;
    expect(response.ok(), await response.text()).toBeTruthy();
  });
}

async function scanAndRefresh(page: any) {
  await step('kv: scan', async () => {
    const scanResponse = page.waitForResponse((r: any) => r.url().includes('/kv/scan'));
    // Success toasts can cover this control; dispatch its click after checking it is enabled.
    const scan = page.getByRole('button', { name: /^Scan$/ });
    await expect(scan).toBeEnabled();
    await scan.evaluate((button: HTMLButtonElement) => button.click());
    const response = await scanResponse;
    expect(response.ok(), await response.text()).toBeTruthy();
    expect(await response.json()).toMatchObject({ items: expect.any(Array) });
    await expect(page.getByTestId('kv-scan-table')).toBeVisible({ timeout: 3_000 });
  });
}

async function scanAllDemoKeys(baseURL: string, storeId: number, groupId: number): Promise<string[]> {
  return step('kv: scan demo keys', async () => {
    const keys: string[] = [];
    let startAfter = '';
    for (;;) {
      const url = `/api/stores/${storeId}/groups/${groupId}/kv/scan?prefix=demo_&limit=500${startAfter ? `&start_after=${encodeURIComponent(startAfter)}` : ''}`;
      const resp = await fetch(`${baseURL}${url}`);
      const body = await resp.json();
      keys.push(...body.items.map((i: any) => i.key_utf8));
      if (!body.truncated || body.items.length === 0) break;
      startAfter = body.items[body.items.length - 1].key_utf8;
    }
    return keys;
  });
}

test.describe('kv ops · advanced deletes, load-more, all-groups, demo', () => {
  test.beforeAll(async () => {
    // The demo/advanced flows assume a backend with no leftover topology.
    await step('kv: resetAll', () => resetAll(apiBase));
    try {
      await step('kv: seed rack/node', () => seedRackAndNode(apiBase, 262, 262));
      await step('kv: deploy server', () => deployNodeServer(apiBase, 262, freePort(), freePort()));
      await step('kv: create store 261 + groups', async () => {
        await createStore(apiBase, 261, [262]);
        await addGroup(apiBase, 261, 2610, 26100, [262]);
        await addGroup(apiBase, 261, 2611, 26110, [262]);
      });
      await step('kv: wait for leaders 261', () => Promise.all([
        waitForLeader(apiBase, 261, 2610),
        waitForLeader(apiBase, 261, 2611),
      ]));
      await step('kv: create store 262 + group', async () => {
        await createStore(apiBase, 262, [262]);
        await addGroup(apiBase, 262, 2620, 26200, [262]);
      });
      await step('kv: wait for leader 262', () => waitForLeader(apiBase, 262, 2620));
    } catch (err) {
      await stopNodeServer(apiBase, 262);
      throw err;
    }
  });

  test.afterAll(async () => {
    await step('kv: stop server', () => stopNodeServer(apiBase, 262));
  });

  test('prefix/selected/inline delete + copy, load more, all-groups mode, auto-scan toggle', async ({ page }) => {
    // --- delete prefix, delete selected, inline delete, copy (former 28-kv-advanced-ops) ---
    await openKvPanel(page, '261', '2610');

    // Put keys with a common prefix
    await putKey(page, 'adv-a-1', 'val1');
    await putKey(page, 'adv-a-2', 'val2');
    await putKey(page, 'adv-b-1', 'val3');

    // Scan to see all keys
    await scanAndRefresh(page);
    await expect(page.getByTestId('kv-scan-table').getByText('adv-a-1')).toBeVisible({ timeout: 3_000 });

    // Delete Prefix: delete all keys starting with "adv-a-"
    await step('kv: delete prefix', async () => {
      await page.getByLabel('Delete key').fill('adv-a-');
      await page.getByRole('button', { name: /delete prefix/i }).click();
      // Confirm dialog appears
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible({ timeout: 3_000 });
      const deletePrefixResponse = page.waitForResponse((r: any) => r.url().includes('/kv/delete'));
      await dialog.getByRole('button', { name: 'Delete' }).click();
      await deletePrefixResponse;
      // Wait for the component's automatic re-scan (setTimeout 100ms) to
      // complete before triggering a manual scan, so the two /kv/scan
      // responses don't race and overwrite each other.
      await page.waitForResponse((r: any) => r.url().includes('/kv/scan'));
    });

    // Scan again — adv-a-* keys should be gone
    await scanAndRefresh(page);
    await expect(page.getByTestId('kv-scan-table').getByText('adv-b-1')).toBeVisible({ timeout: 3_000 });
    await expect(page.getByTestId('kv-scan-table').getByText('adv-a-1')).toHaveCount(0);
    await expect(page.getByTestId('kv-scan-table').getByText('adv-a-2')).toHaveCount(0);

    // Delete Selected: check the checkbox for adv-b-1, then delete
    await step('kv: delete selected', async () => {
      const row = page.getByTestId('kv-scan-table').locator('tr').filter({ hasText: 'adv-b-1' });
      await row.locator('input[type="checkbox"]').check();
      await expect(page.getByRole('button', { name: /delete selected/i })).toBeEnabled({ timeout: 3_000 });
      await page.getByRole('button', { name: /delete selected/i }).click();
      const confirmDialog = page.getByRole('dialog');
      await expect(confirmDialog).toBeVisible({ timeout: 3_000 });
      const deleteSelectedResponse = page.waitForResponse((r: any) => r.url().includes('/kv/delete'));
      await confirmDialog.getByRole('button', { name: 'Delete' }).click();
      await deleteSelectedResponse;
    });

    // Scan — adv-b-1 should be gone
    await scanAndRefresh(page);
    await expect(page.getByTestId('kv-scan-table').getByText('adv-b-1')).toHaveCount(0);

    // Inline delete: put a new key, click inline delete, confirm dialog
    await putKey(page, 'adv-inline', 'val-inline');
    await scanAndRefresh(page);
    await expect(page.getByTestId('kv-scan-table').getByText('adv-inline')).toBeVisible({ timeout: 3_000 });
    await step('kv: inline delete', async () => {
      // The preceding put toast can cover the row action.
      const inlineDelete = page.getByTestId('inline-delete-adv-inline');
      await expect(inlineDelete).toBeEnabled();
      await inlineDelete.evaluate((button: HTMLButtonElement) => button.click());
      const inlineDialog = page.getByRole('dialog');
      await expect(inlineDialog).toBeVisible({ timeout: 3_000 });
      const inlineDeleteResponse = page.waitForResponse((r: any) => r.url().includes('/kv/delete'));
      await inlineDialog.getByRole('button', { name: 'Delete' }).click();
      const response = await inlineDeleteResponse;
      expect(response.ok(), await response.text()).toBeTruthy();
      expect(await response.json()).toMatchObject({ ok: true });
    });

    await expect(page.getByTestId('kv-scan-table').getByText('adv-inline', { exact: true })).toHaveCount(0);

    // Copy: put a key, get it, verify copy button exists
    await putKey(page, 'adv-copy', 'copy-val');
    await step('kv: get copy', async () => {
      await page.getByLabel('Get key').fill('adv-copy');
      const getResponse = page.waitForResponse((r: any) => r.url().includes('/kv/get'));
      await page.getByRole('button', { name: /^Get$/ }).click();
      await getResponse;
      await expect(page.getByTestId('kv-get-result')).toBeVisible({ timeout: 3_000 });
      await expect(page.getByTestId('kv-copy-value')).toBeVisible();
    });

    // --- >100 keys: truncated indicator + Load More (former 29-kv-load-more) ---
    // Bulk-insert 120 keys via API (much faster than UI one-by-one)
    await step('kv: bulk insert 120 keys', async () => {
      for (let i = 0; i < 120; i++) {
        const key = `load-key-${String(i).padStart(3, '0')}`;
        const resp = await fetch(`${apiBase}/api/stores/262/groups/2620/kv/put`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ key, value: `val-${i}` }),
        });
        if (!resp.ok) {
          const text = await resp.text();
          throw new Error(`KV put failed for key ${key}: ${resp.status} ${text}`);
        }
      }
    });

    await openKvPanel(page, '262', '2620');

    // Scan
    await step('kv: scan load-more', async () => {
      const scanResponse = page.waitForResponse((r: any) => r.url().includes('/kv/scan'));
      await page.getByRole('button', { name: /scan/i }).click();
      await scanResponse;
      await expect(page.getByTestId('kv-scan-table')).toBeVisible({ timeout: 3_000 });
    });

    // Verify truncated indicator
    await expect(page.getByText(/truncated/i)).toBeVisible({ timeout: 3_000 });

    // Verify Load More button is visible
    await expect(page.getByRole('button', { name: /load more/i })).toBeVisible();

    // Count rows in table (should be 100)
    const initialRowCount = await page.getByTestId('kv-scan-table').locator('tbody tr').count();
    expect(initialRowCount).toBe(100);

    // Click Load More
    await step('kv: load more', async () => {
      const loadMoreResponse = page.waitForResponse((r: any) => r.url().includes('/kv/scan'));
      await page.getByRole('button', { name: /load more/i }).click();
      await loadMoreResponse;
    });

    // Verify additional rows appear
    await expect(page.getByTestId('kv-scan-table').locator('tbody tr')).toHaveCount(120, { timeout: 3_000 });

    // --- All Groups mode aggregates scan and disables get (former 30-kv-all-groups-mode) ---
    await openKvPanel(page, '261');

    // Put keys in each group
    await page.getByTestId('kv-group-select').selectOption('2610');
    await putKey(page, 'all-groups-key-0', 'val-0');

    await page.getByTestId('kv-group-select').selectOption('2611');
    await putKey(page, 'all-groups-key-1', 'val-1');

    // Switch to All Groups
    await page.getByTestId('kv-group-select').selectOption('All Groups');

    // Get should be disabled in All Groups mode
    await expect(page.getByRole('button', { name: /^Get$/ })).toBeDisabled();

    // Scan should aggregate keys from both groups
    await step('kv: all-groups scan', async () => {
      const allGroupsScanResponse = page.waitForResponse((r: any) => r.url().includes('/kv/scan'));
      await page.getByRole('button', { name: /scan/i }).click();
      await allGroupsScanResponse;
      await expect(page.getByTestId('kv-scan-table')).toBeVisible({ timeout: 3_000 });
      await expect(page.getByTestId('kv-scan-table').getByText('all-groups-key-0')).toBeVisible({ timeout: 10_000 });
      await expect(page.getByTestId('kv-scan-table').getByText('all-groups-key-1')).toBeVisible({ timeout: 10_000 });
    });

    // Group column should be visible in All Groups mode
    await expect(page.getByTestId('kv-scan-table').locator('th').filter({ hasText: 'Group' })).toBeVisible();

    // --- auto-scan off does not refresh, on does (former 31-kv-auto-scan-toggle) ---
    await openKvPanel(page, '261', '2610');

    // Put an initial key and scan
    await putKey(page, 'auto-key-1', 'val-1');
    await step('kv: auto-scan initial', async () => {
      const scanResp = page.waitForResponse((r: any) => r.url().includes('/kv/scan'));
      await page.getByRole('button', { name: /scan/i }).click();
      await scanResp;
      await expect(page.getByTestId('kv-scan-table').getByText('auto-key-1')).toBeVisible({ timeout: 3_000 });
    });

    // Turn auto-scan off
    await page.getByLabel('auto-scan').uncheck();

    // Put another key — scan table should NOT auto-refresh
    await putKey(page, 'auto-key-2', 'val-2');
    // auto-scan is off, so auto-key-2 should never appear in the table.
    await expect(page.getByTestId('kv-scan-table').getByText('auto-key-2')).toHaveCount(0, { timeout: 1_000 });

    // Turn auto-scan back on
    await page.getByLabel('auto-scan').check();

    // Put another key — scan table should auto-refresh
    await putKey(page, 'auto-key-3', 'val-3');
    await expect(page.getByTestId('kv-scan-table').getByText('auto-key-3')).toBeVisible({ timeout: 3_000 });
  });

  test('demo inject and delete-all: single group, all groups, specific group', async ({ page }) => {
    // --- inject into single group then delete all (former 26-kv-demo #1) ---
    await openKvPanel(page, '261', '2610');

    // Inject 5 demo keys (default is 20, we use a smaller count for speed)
    await page.getByLabel('Demo key count').fill('5');
    await step('kv: inject 5', async () => {
      const injectResponsePromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
      await page.getByRole('button', { name: /Inject/ }).click();
      await injectResponsePromise;
    });

    // Wait for scan to auto-trigger and show demo keys
    await expect(page.getByTestId('kv-scan-table').getByText(/demo_key_/).first()).toBeVisible({ timeout: 3_000 });
    const keys = await scanAllDemoKeys(apiBase, 261, 2610);
    expect(keys.length).toBe(5);
    expect(keys.every((k) => k.startsWith('demo_key_'))).toBe(true);

    // Delete all demo keys — wait for all delete responses to settle
    await step('kv: delete-all demo', async () => {
      await page.getByRole('button', { name: /Delete all demo/ }).click();
      const dialog = page.getByRole('dialog');
      await expect(dialog).toBeVisible();
      await dialog.getByRole('button', { name: 'Delete' }).click();
      // Poll until no demo keys remain (delete-all sends multiple requests)
      await expect.poll(async () => {
        const remaining = await scanAllDemoKeys(apiBase, 261, 2610);
        return remaining.length;
      }, { timeout: 5_000, intervals: [100] }).toBe(0);
    });

    // Verify scan table no longer shows demo keys
    await page.getByRole('button', { name: /scan/i }).click();
    await expect(page.getByTestId('kv-scan-table').getByText(/demo_key_/)).toHaveCount(0, { timeout: 3_000 });

    // --- inject in All Groups mode distributes across groups (former 26-kv-demo #2) ---
    await openKvPanel(page, '261', 'All Groups');

    // Inject 20 demo keys in All Groups mode — should randomly distribute
    await page.getByLabel('Demo key count').fill('20');
    await step('kv: inject 20 all-groups', async () => {
      const allGroupsInjectPromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
      await page.getByRole('button', { name: /Inject/ }).click();
      await allGroupsInjectPromise;
    });

    // Wait for scan to show demo keys
    await expect(page.getByTestId('kv-scan-table').getByText(/demo_key_/).first()).toBeVisible({ timeout: 3_000 });
    const keys0 = await scanAllDemoKeys(apiBase, 261, 2610);
    const keys1 = await scanAllDemoKeys(apiBase, 261, 2611);
    expect(keys0.length + keys1.length).toBe(20);

    // Both groups should have at least some keys (probabilistic, but
    // with 20 keys across 2 groups the chance of all-20-in-one is
    // 2 * (1/2)^20 ≈ 0.0002, safe to assert)
    expect(keys0.length).toBeGreaterThan(0);
    expect(keys1.length).toBeGreaterThan(0);

    // Delete all demo keys in All Groups mode — poll until clean
    await step('kv: delete-all all-groups', async () => {
      await page.getByRole('button', { name: /Delete all demo/ }).click();
      const allGroupsDialog = page.getByRole('dialog');
      await expect(allGroupsDialog).toBeVisible();
      await allGroupsDialog.getByRole('button', { name: 'Delete' }).click();
      // Poll until no demo keys remain in either group
      await expect.poll(async () => {
        const r0 = await scanAllDemoKeys(apiBase, 261, 2610);
        const r1 = await scanAllDemoKeys(apiBase, 261, 2611);
        return r0.length + r1.length;
      }, { timeout: 5_000, intervals: [100] }).toBe(0);
    });

    // --- inject into a specific second group only targets that group (former 26-kv-demo #3) ---
    // Select the second group specifically
    await openKvPanel(page, '261', '2611');

    // Inject 10 demo keys into group 2611 only
    await page.getByLabel('Demo key count').fill('10');
    await step('kv: inject 10 group 2611', async () => {
      const secondGroupInjectPromise = page.waitForResponse((r: any) => r.url().includes('/kv/put'));
      await page.getByRole('button', { name: /Inject/ }).click();
      await secondGroupInjectPromise;
    });

    await expect(page.getByTestId('kv-scan-table').getByText(/demo_key_/).first()).toBeVisible({ timeout: 3_000 });

    // All 10 keys should be in group 2611, none in 2610
    const secondKeys0 = await scanAllDemoKeys(apiBase, 261, 2610);
    const secondKeys1 = await scanAllDemoKeys(apiBase, 261, 2611);
    expect(secondKeys0.length).toBe(0);
    expect(secondKeys1.length).toBe(10);

    // Delete all demo keys (still in group 2611 context) — poll until clean
    await step('kv: delete-all group 2611', async () => {
      await page.getByRole('button', { name: /Delete all demo/ }).click();
      const secondGroupDialog = page.getByRole('dialog');
      await expect(secondGroupDialog).toBeVisible();
      await secondGroupDialog.getByRole('button', { name: 'Delete' }).click();
      await expect.poll(async () => {
        const remaining = await scanAllDemoKeys(apiBase, 261, 2611);
        return remaining.length;
      }, { timeout: 5_000, intervals: [100] }).toBe(0);
    });
  });
});
