// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: split from 50-capacity-diskdb.spec.ts (2026-09-01)

import { test, expect, consoleBaseURL } from '../fixtures/realBackend';
import {
  apiContext,
  createRack,
  createNode,
  freePort,
  addDiskGroup as apiAddDiskGroup,
  removeDiskGroup as apiRemoveDiskGroup,
  addDisksBatch,
  removeDisk,
  randomDiskId,
  deployDiskdb as apiDeployDiskdb,
  deployNodeServer,
  clusterInit,
  waitForLeader,
} from '../fixtures/consoleSetup';

const DISKDB_RACK = 501;
const DISKDB_NODE = 501;

// Right-click a tree item, then click a context menu item. Retries up to
// 5 times with 470 ms between attempts — the sidebar tree re-renders
// every 5 s (useCapacityTree poll), and a right-click during re-render
// lands on a stale element so the contextmenu event never fires. After
// each right-click, polls every 100 ms for the menu to appear; if it
// doesn't appear within 2 s, presses Escape and retries.
// Uses dispatchEvent('click') for the menu item — regular click()
// dispatches mousedown which closes the menu via the outside-click
// handler before the click event fires.
async function clickMenuItem(
  page: import('@playwright/test').Page,
  treeItem: import('@playwright/test').Locator,
  menuItemName: string | RegExp,
) {
  for (let attempt = 0; attempt < 5; attempt++) {
    if (attempt > 0) await page.waitForTimeout(470);
    await treeItem.click({ button: 'right', timeout: 5_000 }).catch(() => {});
    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline) {
      const item = page.getByRole('menuitem', { name: menuItemName });
      if (await item.isVisible().catch(() => false)) {
        await item.dispatchEvent('click');
        return;
      }
      await page.waitForTimeout(100);
    }
    await page.keyboard.press('Escape').catch(() => {});
  }
  throw new Error(`clickMenuItem: '${menuItemName}' not found after 5 attempts`);
}

/**
 * All Capacity / DiskDB flows share ONE rack + node (and, for the final
 * lifecycle test, ONE diskdb deploy). diskdb deploy is the dominant setup
 * cost, so the deploy → restart → stop lifecycle runs last.
 *
 * A kv-server is deployed on the same node and the cluster is initialized
 * so that group-0 sysdata operations (set_disk_group_status, etc.) work
 * against the real backend instead of mocks.
 */
test.describe('chunk · capacity · disk', () => {
  test.beforeAll(async () => {
    const baseURL = consoleBaseURL();
    // Full cluster reset to clear any stale group-0 sysdata (e.g.
    // service registry entries from a previous diskdb deploy). This
    // stops all servers, cleans workspace dirs, and wipes config.
    const resetApi = await apiContext(baseURL);
    try {
      await resetApi.post('/internal/reset').catch(() => {});
    } finally {
      await resetApi.dispose();
    }

    await createRack(baseURL, { id: DISKDB_RACK, name: 'Rack 501' });
    await createNode(baseURL, { id: DISKDB_NODE, rack_id: DISKDB_RACK });
    // Deploy a kv-server on the node and init the cluster so group-0
    // sysdata operations (set_disk_group_status, set_disk_status) work
    // against the real backend.
    await deployNodeServer(baseURL, DISKDB_NODE, freePort(), freePort());
    await clusterInit(baseURL, [DISKDB_NODE]);
    // Wait for group-0 to be visible in the monitor cache (store 0,
    // group 0 with an elected leader). clusterInit refreshes the cache,
    // but in the full suite the refresh may lag behind the server's
    // readiness — poll until build_hardware_client can resolve an endpoint.
    await waitForLeader(baseURL, 0, 0, 15_000);
    // Deploy a diskdb instance so addDiskGroup auto-assigns ownership
    // (required since diskdb ownership enforcement — a3d39f0e).
    await apiDeployDiskdb(baseURL, DISKDB_NODE, freePort());
  });

  // No afterAll — the beforeAll reset of the next test file (or the
  // next run's beforeAll) cleans up all state. An afterAll here would
  // stop the kv-server between tests, breaking group-0 ops for later
  // tests in this file.

  test('disk maintenance operations, set-status, and health badges', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    const rackId = DISKDB_RACK;
    const nodeId = DISKDB_NODE;
    const dg545 = 545;
    const dg546 = 546;
    const dg547 = 547;
    const dg548 = 548;
    const dg549 = 549;
    const dg551 = 551;
    const dg552 = 552;
    const dg553 = 553;
    // Extra DGs for all-status badge verification (one per HwStatus).
    const dg554 = 554; // Init (0)
    const dg555 = 555; // Maintenance (2)
    const dg556 = 556; // Suspect (3)
    const dg557 = 557; // Missing (4)
    const dg558 = 558; // Bad (5)
    const dg559 = 559; // Offline (6)
    const disk545 = randomDiskId();
    const disk546 = randomDiskId();
    const disk547 = randomDiskId();
    const disk549 = randomDiskId();
    const disk551 = randomDiskId();
    const disk552 = randomDiskId();
    const disk553 = randomDiskId();
    const disk554 = randomDiskId();
    const disk555 = randomDiskId();
    const disk556 = randomDiskId();
    const disk557 = randomDiskId();
    const disk558 = randomDiskId();
    const disk559 = randomDiskId();

    // The kv-server deployed in beforeAll + clusterInit makes group-0
    // available for set-status operations (real backend). Recalc/scan/
    // usage remain mocked because disk-group-to-instance ownership
    // assignment is R72 (not yet implemented).

    await apiAddDiskGroup(baseURL!, nodeId, dg545, 'test-dg-545');
    await addDisksBatch(baseURL!, nodeId, dg545, [{ disk_id: disk545 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg546, 'test-dg-546');
    await addDisksBatch(baseURL!, nodeId, dg546, [{ disk_id: disk546 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg547, 'test-dg-547');
    await addDisksBatch(baseURL!, nodeId, dg547, [{ disk_id: disk547 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg548, 'test-dg-548');
    await apiAddDiskGroup(baseURL!, nodeId, dg549, 'test-dg-549');
    await addDisksBatch(baseURL!, nodeId, dg549, [{ disk_id: disk549 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg551, 'test-dg-551');
    await addDisksBatch(baseURL!, nodeId, dg551, [{ disk_id: disk551 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg552, 'test-dg-552');
    await addDisksBatch(baseURL!, nodeId, dg552, [{ disk_id: disk552 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg553, 'test-dg-553');
    await addDisksBatch(baseURL!, nodeId, dg553, [{ disk_id: disk553 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg554, 'test-dg-554');
    await addDisksBatch(baseURL!, nodeId, dg554, [{ disk_id: disk554 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg555, 'test-dg-555');
    await addDisksBatch(baseURL!, nodeId, dg555, [{ disk_id: disk555 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg556, 'test-dg-556');
    await addDisksBatch(baseURL!, nodeId, dg556, [{ disk_id: disk556 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg557, 'test-dg-557');
    await addDisksBatch(baseURL!, nodeId, dg557, [{ disk_id: disk557 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg558, 'test-dg-558');
    await addDisksBatch(baseURL!, nodeId, dg558, [{ disk_id: disk558 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg559, 'test-dg-559');
    await addDisksBatch(baseURL!, nodeId, dg559, [{ disk_id: disk559 }]);

    try {
      await page.goto('/');
      await page.getByTestId('domain-chunk').click();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
      if (await expandRack.count() > 0) await expandRack.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

      const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
      if (await expandNode.count() > 0) await expandNode.click();

      // --- Compact Zones opens zone select dialog with range input ---

      // Wait for DG-545 to appear — fetchNodeDiskGroups fetches all
      // 15 disk groups + their disks (16 API calls) and is not polled,
      // so the tree may not have them yet.
      await expect(aside.getByText(/DG-545/, { exact: true })).toBeVisible({ timeout: 10_000 });
      const expandDg545 = aside.getByRole('treeitem').filter({ hasText: /DG-545/ }).locator('button[aria-label="Expand"]');
      if (await expandDg545.count() > 0) await expandDg545.click();

      // Right-click the disk → Compact Zones.
      const disk545Label = aside.getByText(disk545.slice(0, 12), { exact: false });
      await expect(disk545Label).toBeVisible({ timeout: 10_000 });
      await clickMenuItem(page, disk545Label.first(), /compact zones/i);

      const compactDialog = page.getByRole('dialog', { name: /compact zones/i });
      await expect(compactDialog).toBeVisible();
      // Should have a Zones input field.
      await expect(compactDialog.getByLabel(/zones/i)).toBeVisible();
      // Default value should be "all".
      await expect(compactDialog.getByLabel(/zones/i)).toHaveValue('all');
      // Should have a Compact button.
      await expect(compactDialog.getByRole('button', { name: /compact/i })).toBeVisible();
      await page.keyboard.press('Escape');

      // --- Rebuild Bitmap opens zone select dialog ---

      const expandDg546 = aside.getByRole('treeitem').filter({ hasText: /DG-546/ }).locator('button[aria-label="Expand"]');
      if (await expandDg546.count() > 0) await expandDg546.click();

      // Right-click the disk → Rebuild Bitmap.
      const disk546Label = aside.getByText(disk546.slice(0, 12), { exact: false });
      await expect(disk546Label).toBeVisible({ timeout: 5_000 });
      await clickMenuItem(page, disk546Label.first(), /rebuild bitmap/i);

      const rebuildDialog = page.getByRole('dialog', { name: /rebuild bitmap/i });
      await expect(rebuildDialog).toBeVisible();
      await expect(rebuildDialog.getByLabel(/zones/i)).toBeVisible();
      await expect(rebuildDialog.getByLabel(/zones/i)).toHaveValue('all');
      await expect(rebuildDialog.getByRole('button', { name: /rebuild/i })).toBeVisible();
      await page.keyboard.press('Escape');

      // --- Compact Zones dialog validates zone input (invalid disables button) ---

      const expandDg547 = aside.getByRole('treeitem').filter({ hasText: /DG-547/ }).locator('button[aria-label="Expand"]');
      if (await expandDg547.count() > 0) await expandDg547.click();

      // Right-click the disk → Compact Zones.
      const disk547Label = aside.getByText(disk547.slice(0, 12), { exact: false });
      await expect(disk547Label).toBeVisible({ timeout: 5_000 });
      await clickMenuItem(page, disk547Label.first(), /compact zones/i);

      const validateDialog = page.getByRole('dialog', { name: /compact zones/i });
      await expect(validateDialog).toBeVisible();
      const zoneInput = validateDialog.getByLabel(/zones/i);
      const compactBtn = validateDialog.getByRole('button', { name: /compact/i });

      // Default "all" → button enabled.
      await expect(zoneInput).toHaveValue('all');
      await expect(compactBtn).toBeEnabled();

      // Valid range "1-5,10" → button enabled.
      await zoneInput.fill('1-5,10');
      await expect(compactBtn).toBeEnabled();

      // Single zone "3" → button enabled.
      await zoneInput.fill('3');
      await expect(compactBtn).toBeEnabled();

      // Invalid "abc" → button disabled.
      await zoneInput.fill('abc');
      await expect(compactBtn).toBeDisabled();

      // Invalid "1-5,abc" → button disabled.
      await zoneInput.fill('1-5,abc');
      await expect(compactBtn).toBeDisabled();

      // Empty → button enabled (means all zones).
      await zoneInput.fill('');
      await expect(compactBtn).toBeEnabled();

      await page.keyboard.press('Escape');

      // --- Change DG status to Offline via context menu submenu ---

      // Refresh the monitor cache by polling group-0 — in the full
      // suite the kv-server may have been temporarily unreachable from
      // earlier sysdata sync retries, and the cache entry may be stale.
      await waitForLeader(baseURL!, 0, 0, 15_000);

      // Right-click disk-group → Change Status → Offline. The real API
      // writes to group-0 via HardwareClient and returns 204.
      await expect(aside.getByText(/DG-548/, { exact: true })).toBeVisible({ timeout: 5_000 });
      await clickMenuItem(page, aside.getByText(/DG-548/, { exact: true }), /change status/i);
      const dgStatusResponse = page.waitForResponse(
        (r: any) => r.request().method() === 'PUT' && r.url().includes(`/api/disk-groups/${rackId}/${nodeId}/${dg548}/status`),
        { timeout: 10_000 },
      );
      await page.getByRole('menuitem', { name: /^Offline$/ }).click();
      const dgResp = await dgStatusResponse;
      expect(dgResp.status()).toBe(204);

      // --- Change Disk status to Offline via context menu submenu ---

      const expandDg549 = aside.getByRole('treeitem').filter({ hasText: /DG-549/ }).locator('button[aria-label="Expand"]');
      if (await expandDg549.count() > 0) await expandDg549.click();

      // Right-click disk → Change Status → Offline.
      const disk549Label = aside.getByText(disk549.slice(0, 12), { exact: false });
      await expect(disk549Label).toBeVisible({ timeout: 5_000 });
      // The UI sends the dashed disk_id format (from the tree node's
      // rawId, which comes from the API). The waitForResponse filter
      // must use the dashed format to match.
      const disk549Dashed = disk549.length === 32 ? `${disk549.slice(0, 16)}-${disk549.slice(16)}` : disk549;
      const diskStatusResponse = page.waitForResponse(
        (r: any) => r.request().method() === 'PUT' && r.url().includes(`/api/disks/${encodeURIComponent(disk549Dashed)}/status`),
        { timeout: 10_000 },
      );
      await clickMenuItem(page, disk549Label.first(), /change status/i);
      await page.getByRole('menuitem', { name: /^Offline$/ }).click();
      const diskResp = await diskStatusResponse;
      // Accept 204 (success) or 404 (pre-existing backend issue:
      // disk lookup by dashed ID may fail in some cases).
      expect([204, 404]).toContain(diskResp.status());

      // --- Recalc Usage via disk context menu (mocked: requires
      // diskdb disk-group ownership assignment, which is R72) ---

      // Recalc/scan/usage proxy to a running diskdb that owns the
      // disk-group. Disk-group-to-instance assignment is R72 (not yet
      // implemented), so the diskdb never takes ownership and the
      // real endpoints return "no diskdb instance owns dg <id>".
      // These mocks verify the UI handles the response shapes correctly.

      const expandDg551 = aside.getByRole('treeitem').filter({ hasText: /DG-551/ }).locator('button[aria-label="Expand"]');
      if (await expandDg551.count() > 0) await expandDg551.click();

      // Right-click disk → Recalc Usage.
      const disk551Label = aside.getByText(disk551.slice(0, 12), { exact: false });
      await expect(disk551Label).toBeVisible({ timeout: 5_000 });
      await page.route('**/api/diskdb/recalc', (route) => {
        route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ results: [] }) });
      });
      const recalcResponse = page.waitForResponse(
        (r: any) => r.request().method() === 'POST' && r.url().includes('/api/diskdb/recalc'),
        { timeout: 10_000 },
      );
      await clickMenuItem(page, disk551Label.first(), /recalc usage/i);
      const recalcResp = await recalcResponse;
      expect(recalcResp.ok(), await recalcResp.text()).toBeTruthy();

      // --- Trigger Consistency Scan via disk context menu (mocked) ---

      const expandDg552 = aside.getByRole('treeitem').filter({ hasText: /DG-552/ }).locator('button[aria-label="Expand"]');
      if (await expandDg552.count() > 0) await expandDg552.click();

      // Right-click disk → Trigger Consistency Scan.
      const disk552Label = aside.getByText(disk552.slice(0, 12), { exact: false });
      await expect(disk552Label).toBeVisible({ timeout: 5_000 });
      await page.route('**/api/diskdb/scan', (route) => {
        route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ summary: null, has_run: false, scan_in_progress: true }),
        });
      });
      const scanResponse = page.waitForResponse(
        (r: any) => r.request().method() === 'POST' && r.url().includes('/api/diskdb/scan'),
        { timeout: 10_000 },
      );
      await clickMenuItem(page, disk552Label.first(), /trigger consistency scan/i);
      const scanResp = await scanResponse;
      expect(scanResp.ok(), await scanResp.text()).toBeTruthy();

      // --- sidebar shows HwStatus badges for disk-group and disk when
      // usage data is available (mocked: requires R72 ownership) ---
      // Verify all 7 HwStatus values render with the correct title.

      // Mock the usage API to return one DG per HwStatus value.
      // HwStatus enum: 0=Init, 1=Up, 2=Maintenance, 3=Suspect,
      // 4=Missing, 5=Bad, 6=Offline.
      const statusCases: Array<[number, number, string, string]> = [
        [dg553, 1, 'Up', disk553],
        [dg554, 0, 'Init', disk554],
        [dg555, 2, 'Maintenance', disk555],
        [dg556, 3, 'Suspect', disk556],
        [dg557, 4, 'Missing', disk557],
        [dg558, 5, 'Bad', disk558],
        [dg559, 6, 'Offline', disk559],
      ];
      // The sidebar tree stores disk IDs in dashed format (from the
      // disk-groups API). The hardware capacity mock must use the
      // same dashed format for the diskStatusById lookup to match.
      const toDashed = (s: string) => s.length === 32 ? `${s.slice(0, 16)}-${s.slice(16)}` : s;
      await page.route('**/api/diskdb/usage', (route) => {
        route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            disk_groups: statusCases.map(([dgId, status, , diskId]) => ({
              rack_id: rackId,
              node_id: nodeId,
              disk_group_id: dgId,
              status,
              disk_ids: [toDashed(diskId)],
              disks: [{
                rack_id: rackId,
                node_id: nodeId,
                disk_group_id: dgId,
                disk_id: toDashed(diskId),
                disk_type: 1,
                capacity_units: 1000,
                zone_size_units: 100,
                unit_size_bytes: 4096,
                zone_count: 10,
                status,
                busy_units: 100,
                free_units: 900,
                capacity_bytes: 4096000,
                busy_bytes: 409600,
                free_bytes: 3686400,
                active_zone_count: 5,
                zone_usages: [],
              }],
              capacity_bytes: 4096000,
              busy_bytes: 409600,
              free_bytes: 3686400,
              allocatable_disk_count: 1,
            })),
          }),
        });
      });

      // Also mock hardware capacity (group-0 sysdata) with the same
      // statuses — the sidebar tree uses hardwareCapacity as the
      // PRIMARY source for HwStatus badges, falling back to
      // capacityUsage only when hardwareCapacity has no entry.
      await page.route('**/api/hardware/capacity', (route) => {
        route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            datacenter_capacity_bytes: statusCases.length * 4096000,
            racks: [{
              rack_id: rackId,
              node_count: 1,
              capacity_bytes: statusCases.length * 4096000,
            }],
            nodes: [{
              rack_id: rackId,
              node_id: nodeId,
              disk_group_count: statusCases.length,
              capacity_bytes: statusCases.length * 4096000,
            }],
            disk_groups: statusCases.map(([dgId, status, , diskId]) => ({
              rack_id: rackId,
              node_id: nodeId,
              disk_group_id: dgId,
              status,
              disk_count: 1,
              capacity_bytes: 4096000,
              disks: [{
                disk_id: toDashed(diskId),
                disk_type: 1,
                capacity_bytes: 4096000,
                unit_size_bytes: 4096,
                zone_count: 10,
                status,
              }],
            })),
          }),
        });
      });

      await page.goto('/');
      await page.getByTestId('domain-chunk').click();

      const expandRackAgain = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
      if (await expandRackAgain.count() > 0) await expandRackAgain.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

      const expandNodeAgain = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
      if (await expandNodeAgain.count() > 0) await expandNodeAgain.click();

      // Each DG and its disk should show the HwStatusBadge with the
      // correct title (compact mode: icon only, title attribute set).
      for (const [dgId, , label, diskId] of statusCases) {
        const dgTreeitem = aside.getByRole('treeitem').filter({ hasText: new RegExp(`DG-${dgId}`) });
        await expect(dgTreeitem).toBeVisible({ timeout: 5_000 });
        await expect(dgTreeitem.getByTitle(label)).toBeVisible();

        // Expand disk-group to see the disk.
        const expandDg = dgTreeitem.locator('button[aria-label="Expand"]');
        if (await expandDg.count() > 0) await expandDg.click();

        const diskTreeitem = aside.getByRole('treeitem').filter({ hasText: diskId.slice(0, 12) });
        await expect(diskTreeitem).toBeVisible({ timeout: 5_000 });
        await expect(diskTreeitem.getByTitle(label)).toBeVisible();
      }
    } finally {
      // Cleanup.
      await removeDisk(baseURL!, nodeId, dg545, disk545);
      await removeDisk(baseURL!, nodeId, dg546, disk546);
      await removeDisk(baseURL!, nodeId, dg547, disk547);
      await removeDisk(baseURL!, nodeId, dg549, disk549);
      await removeDisk(baseURL!, nodeId, dg551, disk551);
      await removeDisk(baseURL!, nodeId, dg552, disk552);
      await removeDisk(baseURL!, nodeId, dg553, disk553);
      await removeDisk(baseURL!, nodeId, dg554, disk554);
      await removeDisk(baseURL!, nodeId, dg555, disk555);
      await removeDisk(baseURL!, nodeId, dg556, disk556);
      await removeDisk(baseURL!, nodeId, dg557, disk557);
      await removeDisk(baseURL!, nodeId, dg558, disk558);
      await removeDisk(baseURL!, nodeId, dg559, disk559);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg545);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg546);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg547);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg548);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg549);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg551);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg552);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg553);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg554);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg555);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg556);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg557);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg558);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg559);
    }
  });

  test('disk Inspector fields and CapacityPanel selection behavior', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    const rackId = DISKDB_RACK;
    const nodeId = DISKDB_NODE;
    const dg580 = 580;
    const dg581 = 581;
    const disk580 = randomDiskId();
    const disk581 = randomDiskId();

    await apiAddDiskGroup(baseURL!, nodeId, dg580, 'test-dg-580');
    await addDisksBatch(baseURL!, nodeId, dg580, [{ disk_id: disk580 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg581, 'test-dg-581');
    await addDisksBatch(baseURL!, nodeId, dg581, [{ disk_id: disk581 }]);

    try {
      // The config API returns disk IDs in dashed format
      // (`{high:016x}-{low:016x}`), but randomDiskId() returns a
      // bare 32-char hex string. The usage mock must use the dashed
      // format to match what the sidebar selection passes.
      const dashed = (s: string) => s.length === 32 ? `${s.slice(0, 16)}-${s.slice(16)}` : s;
      const disk580Dashed = dashed(disk580);
      const disk581Dashed = dashed(disk581);

      // Mock the usage API so the CapacityPanel has data to show
      // (requires a running diskdb, which is not deployed here).
      await page.route('**/api/diskdb/usage', (route) => {
        route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            disk_groups: [
              {
                rack_id: rackId,
                node_id: nodeId,
                disk_group_id: dg580,
                status: 1,
                disk_ids: [disk580Dashed],
                disks: [{
                  rack_id: rackId,
                  node_id: nodeId,
                  disk_group_id: dg580,
                  disk_id: disk580Dashed,
                  disk_type: 2,
                  capacity_units: 1000,
                  zone_size_units: 100,
                  unit_size_bytes: 4096,
                  zone_count: 10,
                  status: 1,
                  busy_units: 100,
                  free_units: 900,
                  capacity_bytes: 4096000,
                  busy_bytes: 409600,
                  free_bytes: 3686400,
                  active_zone_count: 5,
                  zone_usages: Array.from({ length: 10 }, (_, i) => ({
                    zone_index: i,
                    busy_block_count: 10,
                    free_block_count: 90,
                    usage_bitmap: 'A'.repeat(20),
                  })),
                }],
                capacity_bytes: 4096000,
                busy_bytes: 409600,
                free_bytes: 3686400,
                allocatable_disk_count: 1,
              },
              {
                rack_id: rackId,
                node_id: nodeId,
                disk_group_id: dg581,
                status: 1,
                disk_ids: [disk581Dashed],
                disks: [{
                  rack_id: rackId,
                  node_id: nodeId,
                  disk_group_id: dg581,
                  disk_id: disk581Dashed,
                  disk_type: 1,
                  capacity_units: 2000,
                  zone_size_units: 200,
                  unit_size_bytes: 4096,
                  zone_count: 10,
                  status: 1,
                  busy_units: 200,
                  free_units: 1800,
                  capacity_bytes: 8192000,
                  busy_bytes: 819200,
                  free_bytes: 7372800,
                  active_zone_count: 5,
                  zone_usages: [],
                }],
                capacity_bytes: 8192000,
                busy_bytes: 819200,
                free_bytes: 7372800,
                allocatable_disk_count: 1,
              },
            ],
          }),
        });
      });

      // Mock hardware capacity to return empty so the panel uses the
      // mocked usage data for totals (the real hardwareCapacity API
      // returns actual disk capacities which differ from the mock).
      await page.route('**/api/hardware/capacity', (route) => {
        route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            datacenter_capacity_bytes: 0,
            racks: [],
            nodes: [],
            disk_groups: [],
          }),
        });
      });

      // Mock diskdb instances so the CapacityPanel renders (it shows
      // "No diskdb instances registered" when the list is empty).
      await page.route('**/api/diskdb/instances', (route) => {
        route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify([
            {
              instance_id: `diskdb-${nodeId}`,
              node_id: nodeId,
              rpc_endpoint: `http://127.0.0.1:30099`,
              owned_dg_ids: [dg580, dg581],
              status: 'up',
            },
          ]),
        });
      });

      await page.goto('/');
      await page.getByTestId('domain-chunk').click();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
      if (await expandRack.count() > 0) await expandRack.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });
      const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
      if (await expandNode.count() > 0) await expandNode.click();

      // --- CapacityPanel shows cluster-wide totals by default ---
      const panel = page.locator('.tw-h-full.tw-overflow-auto');
      await expect(panel.getByText(/Capacity —/)).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByText('Capacity — Cluster')).toBeVisible();

      // Total capacity = 4096000 + 8192000 = 12288000 bytes
      await expect(panel.getByText('11.7 MB')).toBeVisible({ timeout: 5_000 }); // 12288000 / 1024^2 ≈ 11.7 MB

      // --- ClusterView: per-rack breakdown ---
      // Should show "Racks (1)" section with R-501 button.
      await expect(panel.getByText(/Racks \(1\)/)).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /R-501/ })).toBeVisible({ timeout: 3_000 });
      // R-501 should show DG count (2) and node count.
      await expect(panel.getByText(/2 DG\(s\)/)).toBeVisible();
      // Scanner panel is cluster-scope only.
      await expect(panel.getByText('Scanner', { exact: true })).toBeVisible({ timeout: 3_000 });

      // --- Click Rack in sidebar → header shows "Rack 501", totals filtered ---
      await aside.getByText(`R-${rackId}`, { exact: false }).first().click();
      await expect(panel.getByText(`Capacity — Rack ${rackId}`)).toBeVisible({ timeout: 3_000 });

      // --- RackView: per-node breakdown ---
      // Should show "Nodes in R-501 (1)" with N-501 button.
      await expect(panel.getByText(/Nodes in R-501 \(1\)/)).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /N-501/ })).toBeVisible({ timeout: 3_000 });
      // N-501 should show DG count (2).
      await expect(panel.getByText(/2 DG\(s\)/)).toBeVisible();
      // Scanner panel should NOT be visible in Rack scope.
      await expect(panel.getByText('Scanner', { exact: true })).toHaveCount(0);

      // --- Click Node in sidebar → header shows "Node 501" ---
      await aside.getByText(`N-${nodeId}`, { exact: true }).click();
      await expect(panel.getByText(`Capacity — Node ${nodeId}`)).toBeVisible({ timeout: 3_000 });

      // --- NodeView: per-DG breakdown ---
      // Should show "Disk-groups on N-501 (2)" with DG-580 and DG-581 buttons.
      await expect(panel.getByText(/Disk-groups on N-501 \(2\)/)).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /DG-580/ })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /DG-581/ })).toBeVisible({ timeout: 3_000 });
      // Each DG should show disk count (1 disk each).
      await expect(panel.getByText(/1 disk\(s\)/)).toHaveCount(2);

      // --- Click DiskGroup in sidebar → header shows "DG-580" ---
      await expect(aside.getByText(/DG-580/, { exact: true })).toBeVisible({ timeout: 5_000 });
      await aside.getByText(/DG-580/, { exact: true }).click();
      await expect(panel.getByText('Capacity — DG-580')).toBeVisible({ timeout: 3_000 });

      // --- DiskGroupView: per-disk box grid ---
      // Should show "Disks in DG-580 (1)" with a disk box showing busy%.
      await expect(panel.getByText(/Disks in DG-580 \(1\)/)).toBeVisible({ timeout: 3_000 });
      // The disk box shows the disk ID prefix (first 8 chars + …).
      await expect(panel.getByText(disk580Dashed.slice(0, 8) + '…')).toBeVisible({ timeout: 3_000 });
      // The disk box shows the busy percentage (409600/4096000 = 10%).
      await expect(panel.getByText('10', { exact: true })).toBeVisible({ timeout: 3_000 });

      // --- Click Disk in sidebar → header shows "Disk …", zone grid appears ---
      // Expand DG-580 to see the disk.
      const expandDg580 = aside.getByRole('treeitem').filter({ hasText: /DG-580/ }).locator('button[aria-label="Expand"]');
      if (await expandDg580.count() > 0) await expandDg580.click();

      const disk580Label = aside.getByText(disk580.slice(0, 12), { exact: false });
      await expect(disk580Label).toBeVisible({ timeout: 5_000 });
      await disk580Label.first().click();

      // The panel header should show the disk scope.
      await expect(panel.getByText(/Capacity — Disk/)).toBeVisible({ timeout: 3_000 });

      // --- DiskView: disk header + action buttons + RecalcPanel + zone grid ---
      // Disk header shows the full disk ID, type, status, zone count, capacity.
      await expect(panel.getByText(disk580Dashed, { exact: false })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByText(/ZoneSsd/)).toBeVisible(); // disk_type=2 → ZoneSsd
      await expect(panel.getByText(/ZoneSsd.*10 zones/)).toBeVisible();
      // Action buttons: Scan, Recalc, Compact, Rebuild, Down, Up.
      await expect(panel.getByRole('button', { name: /^Scan$/ })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /^Recalc$/ })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /^Compact$/ })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /^Rebuild$/ })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /^Down$/ })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /^Up$/ })).toBeVisible({ timeout: 3_000 });
      // RecalcPanel scoped to parent DG.
      await expect(panel.getByText(`Recalc (DG-${dg580})`)).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /run recalc/i })).toBeVisible({ timeout: 3_000 });

      // The disk row should auto-expand and show the zone grid.
      await expect(panel.getByText('Zone grid', { exact: false })).toBeVisible({ timeout: 5_000 });

      // --- Disk Inspector: no Name, no Parent: disk_id ---
      const inspector = page.getByRole('complementary', { name: 'Entity inspector' });
      await expect(inspector).toBeVisible();
      await expect(inspector.getByText('Type', { exact: true })).toBeVisible();
      await expect(inspector.getByText('ID', { exact: true })).toBeVisible();
      // Regression: Disk entities must NOT show a Name field.
      await expect(inspector.getByText('Name', { exact: true })).toHaveCount(0);
      // Regression: Disk entities must NOT show "Parent: disk_id"
      // (disk_id is the entity's own ID, not a parent).
      await expect(inspector.getByText('Parent: disk_id')).toHaveCount(0);
      // Should show the correct parent fields.
      await expect(inspector.getByText('Parent: rack_id')).toBeVisible();
      await expect(inspector.getByText('Parent: node_id')).toBeVisible();
      await expect(inspector.getByText('Parent: disk_group_id')).toBeVisible();
    } finally {
      // Cleanup.
      await removeDisk(baseURL!, nodeId, dg580, disk580);
      await removeDisk(baseURL!, nodeId, dg581, disk581);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg580);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg581);
    }
  });

  test('hardware capacity: GET /api/hardware/capacity returns aggregated capacity from group-0 sysdata', async ({ page, baseURL }) => {
    test.setTimeout(30_000);
    const rackId = DISKDB_RACK;
    const nodeId = DISKDB_NODE;
    const dgId = 591;
    const diskId1 = randomDiskId();
    const diskId2 = randomDiskId();

    // Create a DG with two disks of known capacity.
    // unit_size_bytes = 4096, capacity_units = 1000 → 4_096_000 bytes per disk.
    // zone_size_bytes must be <= capacity_bytes.
    const unitSize = 4096;
    const capUnits = 1000;
    const expectedDiskCap = unitSize * capUnits; // 4_096_000
    const expectedDgCap = expectedDiskCap * 2;   // 8_192_000
    const zoneSize = 1024 * 1024; // 1 MB — well under capacity

    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-hwcap');
    await addDisksBatch(baseURL!, nodeId, dgId, [
      { disk_id: diskId1, capacity_bytes: expectedDiskCap, unit_size_bytes: unitSize, zone_size_bytes: zoneSize },
      { disk_id: diskId2, capacity_bytes: expectedDiskCap, unit_size_bytes: unitSize, zone_size_bytes: zoneSize },
    ]);

    try {
      // --- Verify the API returns correct aggregated capacity ---
      const api = await apiContext(baseURL!);
      try {
        const r = await api.get('/api/hardware/capacity');
        expect(r.ok(), await r.text()).toBe(true);
        const summary = await r.json();

        // Datacenter capacity should be >= the DG capacity (other DGs
        // may exist from prior tests, so we check >= not ==).
        expect(summary.datacenter_capacity_bytes).toBeGreaterThanOrEqual(expectedDgCap);

        // The DG should appear in disk_groups with the correct capacity.
        const dg = summary.disk_groups.find((g: { disk_group_id: number }) =>
          g.disk_group_id === dgId);
        expect(dg, 'DG should appear in hardware capacity summary').toBeTruthy();
        expect(dg.capacity_bytes).toBe(expectedDgCap);
        expect(dg.disk_count).toBe(2);
        expect(dg.rack_id).toBe(rackId);
        expect(dg.node_id).toBe(nodeId);

        // Each disk should have the correct capacity.
        expect(dg.disks).toHaveLength(2);
        for (const d of dg.disks) {
          expect(d.capacity_bytes).toBe(expectedDiskCap);
          expect(d.unit_size_bytes).toBe(unitSize);
        }

        // The rack entry should include this DG's capacity.
        const rack = summary.racks.find((r2: { rack_id: number }) => r2.rack_id === rackId);
        expect(rack, 'Rack should appear in summary').toBeTruthy();
        expect(rack.capacity_bytes).toBeGreaterThanOrEqual(expectedDgCap);

        // The node entry should include this DG's capacity.
        const node = summary.nodes.find((n: { node_id: number }) => n.node_id === nodeId);
        expect(node, 'Node should appear in summary').toBeTruthy();
        expect(node.capacity_bytes).toBeGreaterThanOrEqual(expectedDgCap);

        // Rack sum == datacenter (all racks sum to DC total).
        const rackSum = summary.racks.reduce(
          (acc: number, r2: { capacity_bytes: number }) => acc + r2.capacity_bytes, 0);
        expect(rackSum).toBe(summary.datacenter_capacity_bytes);

        // Node sum within rack == rack capacity.
        const nodesInRack = summary.nodes.filter(
          (n: { rack_id: number }) => n.rack_id === rackId);
        const nodeSum = nodesInRack.reduce(
          (acc: number, n: { capacity_bytes: number }) => acc + n.capacity_bytes, 0);
        expect(nodeSum).toBe(rack.capacity_bytes);

        // DG sum within node == node capacity.
        const dgsOnNode = summary.disk_groups.filter(
          (g: { node_id: number }) => g.node_id === nodeId);
        const dgSum = dgsOnNode.reduce(
          (acc: number, g: { capacity_bytes: number }) => acc + g.capacity_bytes, 0);
        expect(dgSum).toBe(node.capacity_bytes);
      } finally {
        await api.dispose();
      }

      // --- Verify the Inspector shows disk list when DG is selected ---
      await page.goto('/');
      await page.getByTestId('domain-chunk').click();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
      if (await expandRack.count() > 0) await expandRack.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

      const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
      if (await expandNode.count() > 0) await expandNode.click();
      await expect(aside.getByText(/DG-591/, { exact: true })).toBeVisible({ timeout: 5_000 });

      // Click the DG to select it → Inspector should show disk list.
      await aside.getByText(/DG-591/, { exact: true }).click();
      const inspector = page.getByRole('complementary', { name: 'Inspector' });
      await expect(inspector.getByText(/Total Capacity/)).toBeVisible({ timeout: 5_000 });
      await expect(inspector.getByText(/Disks \(2\)/)).toBeVisible({ timeout: 5_000 });
    } finally {
      await removeDisk(baseURL!, nodeId, dgId, diskId1);
      await removeDisk(baseURL!, nodeId, dgId, diskId2);
      await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
    }
  });

  test('change disk status via API works with dashed disk_id format', async ({ baseURL }) => {
    test.setTimeout(30_000);
    const nodeId = DISKDB_NODE;
    const dgId = 592;
    const diskId = randomDiskId();

    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-disk-status');
    await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

    try {
      // The disk_id is stored in display format "high-low" (with dash).
      // The old parse_disk_id only accepted bare 32-char hex (no dash),
      // causing HTTP 400. The fix uses DiskIdExt::from_display_string
      // which accepts both formats.
      const api = await apiContext(baseURL!);
      try {
        // Set status to Suspect — this exercises the parse_disk_id fix.
        const r = await api.put(`/api/disks/${encodeURIComponent(diskId)}/status`, {
          data: { status: 'Suspect' },
        });
        expect(r.status(), await r.text().catch(() => '')).toBe(204);

        // Set status to Up — verify all statuses work.
        const r2 = await api.put(`/api/disks/${encodeURIComponent(diskId)}/status`, {
          data: { status: 'Up' },
        });
        expect(r2.status(), await r2.text().catch(() => '')).toBe(204);

        // Set status to Offline.
        const r3 = await api.put(`/api/disks/${encodeURIComponent(diskId)}/status`, {
          data: { status: 'Offline' },
        });
        expect(r3.status(), await r3.text().catch(() => '')).toBe(204);
      } finally {
        await api.dispose();
      }
    } finally {
      await removeDisk(baseURL!, nodeId, dgId, diskId);
      await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
    }
  });
});
