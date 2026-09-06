// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { expect, request } from '@playwright/test';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execSync } from 'node:child_process';
import { createConnection } from 'node:net';

const __dirname = dirname(fileURLToPath(import.meta.url));

export const DEFAULT_SERVER_BINARY =
  process.env.CROWDB_KV_SERVER_BINARY ?? resolve(__dirname, '../../../../../target/debug/crowdb-kv-server');

// Path to the crowdb-port-alloc CLI binary. Used for flock-coordinated
// port allocation with bind probes — no port 0, no TOCTOU.
const PORT_ALLOC_BIN =
  process.env.CROWDB_PORT_ALLOC_BIN ?? resolve(__dirname, '../../../../../target/debug/crowdb-port-alloc');

// Per-process claim file root for E2E port allocation. Uses a temp
// directory keyed by PID so parallel test runs don't collide.
const PORT_ALLOC_ROOT = resolve(`/tmp/crowdb-port-alloc-e2e-${process.pid}`);

// Allocate a single port for the given service via the crowdb-port-alloc
// CLI. Services: kv-mgmt, kv-listen, diskdb-listen, diskdb-http,
// diskdb-rpc, chunkdb-http, chunkdb-rpc, diskio-rpc, web.
export function freePort(service = 'kv-mgmt'): number {
  const out = execSync(
    `${PORT_ALLOC_BIN} --root "${PORT_ALLOC_ROOT}" --service ${service}`,
    { encoding: 'utf-8' },
  ).trim();
  return parseInt(out, 10);
}

// Allocate `count` consecutive ports for the given service. Returns the
// base port (first in the range).
export function freePortRange(count: number, service = 'kv-mgmt'): number {
  if (count < 1) throw new Error('freePortRange: count must be >= 1');
  const out = execSync(
    `${PORT_ALLOC_BIN} --root "${PORT_ALLOC_ROOT}" --service ${service} --count ${count}`,
    { encoding: 'utf-8' },
  ).trim();
  const ports = out.split('\n').map((p) => parseInt(p.trim(), 10));
  return ports[0];
}

// ── Timing instrumentation ──────────────────────────────────────────

export { step as stepTime } from './stepTimer';

const SLOW_THRESHOLD_MS = 2000;
const VERY_SLOW_THRESHOLD_MS = 5000;

function logPhaseTime(phase: string, startMs: number): number {
  const elapsed = Date.now() - startMs;
  if (elapsed >= VERY_SLOW_THRESHOLD_MS) {
    console.error(`[DEPLOYER] phase '${phase}' took ${elapsed}ms (very slow, expected <${VERY_SLOW_THRESHOLD_MS}ms)`);
  } else if (elapsed >= SLOW_THRESHOLD_MS) {
    console.warn(`[DEPLOYER] phase '${phase}' took ${elapsed}ms (slow, expected <${SLOW_THRESHOLD_MS}ms)`);
  }
  return elapsed;
}

// ── Types ────────────────────────────────────────────────────────────

// Retry a POST that writes to group-0 sysdata (createStore, addGroup,
// addReplica). Under heavy CI load the group-0 leader's RPC endpoint
// can be temporarily unresponsive, causing a transient "retries
// exhausted" 500. The server-side retry_sysmd + KV client already
// retry internally, but the total window can be insufficient on a
// loaded CI machine. This gives up to 3 attempts with 1s backoff.
async function postWithSysdataRetry(
  baseURL: string,
  path: string,
  data: Record<string, unknown>,
  label: string,
): Promise<void> {
  const maxAttempts = 3;
  for (let attempt = 1; ; attempt++) {
    const api = await apiContext(baseURL);
    try {
      const response = await api.post(path, { data });
      if (response.status() === 201) return;
      if (response.status() === 500 && attempt < maxAttempts) {
        const text = await response.text();
        console.log(`${label} attempt ${attempt} got 500: ${text}, retrying...`);
        await new Promise((res) => setTimeout(res, 1000));
        continue;
      }
      expect(response.status(), await response.text()).toBe(201);
    } finally {
      await api.dispose();
    }
  }
}

export interface TestRack {
  id: number;
  name?: string;
}

export interface TestNode {
  id: number;
  rack_id: number;
  host?: string;
}

export interface TopologyDescriptor {
  nodeCount: number;
  storeCount: number;
  groupsPerStore: number;
  replicasPerGroup: number;
  rackBase: number;
  nodeBase: number;
  storeBase: number;
  groupBase: number;
  /** Deploy a diskdb instance on each node. */
  deployDiskdb?: boolean;
  /** Disk-groups per node (0 = none). */
  diskGroupsPerNode?: number;
  /** Disks per disk-group (0 = none). */
  disksPerGroup?: number;
}

export interface NodeInfo {
  id: number;
  rackId: number;
  pid: number;
  mgmtUrl: string;
  rpcUrl: string;
  restPort: number;
  rpcPort: number;
}

export interface GroupInfo {
  groupId: number;
  leaderNodeId: number | null;
  leaderEndpoint: string | null;
}

export interface StoreInfo {
  storeId: number;
  nodes: number[];
  groups: GroupInfo[];
}

export interface DiskdbInfo {
  nodeId: number;
  pid: number;
  rpcPort: number;
}

export interface ClusterInfo {
  racks: number[];
  nodes: NodeInfo[];
  stores: StoreInfo[];
  diskdbInstances: DiskdbInfo[];
}

export const SIMPLE: TopologyDescriptor = {
  nodeCount: 3,
  storeCount: 1,
  groupsPerStore: 1,
  replicasPerGroup: 3,
  rackBase: 100,
  nodeBase: 100,
  storeBase: 800,
  groupBase: 8000,
};

export const COMPLEX: TopologyDescriptor = {
  nodeCount: 8,
  storeCount: 2,
  groupsPerStore: 2,
  replicasPerGroup: 3,
  rackBase: 200,
  nodeBase: 200,
  storeBase: 900,
  groupBase: 9000,
};

// ── Low-level API helpers (kept for backward compat) ─────────────────

export async function apiContext(baseURL: string) {
  return request.newContext({ baseURL });
}

// Debug helper: dump API state to console.log.
export async function dumpApiState(baseURL: string, nodeId?: number) {
  const api = await apiContext(baseURL);
  try {
    const racks = await (await api.get('/api/racks?recursive=3')).json();
    for (const rack of racks.items || []) {
      for (const n of rack.nodes || []) {
        if (nodeId == null || n.id === nodeId) {
          console.log(`DEBUG racks: node ${n.id} has_server=${n.has_server} server=${JSON.stringify(n.server)}`);
        }
      }
    }
    const servers = await (await api.get('/api/servers')).json();
    for (const s of servers) {
      if (nodeId == null || s.node_id === nodeId) {
        console.log(`DEBUG servers: node ${s.node_id} service_type=${s.service_type} health=${s.health} pid=${s.pid}`);
      }
    }
  } finally {
    await api.dispose();
  }
}

export async function createRack(baseURL: string, rack: TestRack) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/racks', {
      data: { id: rack.id, name: rack.name ?? rack.id },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function createNode(baseURL: string, node: TestNode) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/racks/${encodeURIComponent(node.rack_id)}/nodes`, {
      data: { id: node.id, rack_id: node.rack_id, host: node.host ?? '127.0.0.1' },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function seedRackAndNode(baseURL: string, rackId = 1, nodeId = 1) {
  await createRack(baseURL, { id: rackId, name: `rack-${rackId}` });
  await createNode(baseURL, { id: nodeId, rack_id: rackId });
}

export async function deployNodeServer(baseURL: string, nodeId: number, restPort: number, rpcPort: number) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/server/deploy`, {
      data: {
        rest_port: restPort,
        rpc_port: rpcPort,
        binary: DEFAULT_SERVER_BINARY,
        election_profile: 'e2e',
        // Skip fsync on every write + shutdown. Data is still in the OS
        // page cache (visible to a restart on the same host), just not
        // forced to durable storage. Avoids slow macOS fsync and reduces
        // SSD wear during long E2E sessions.
        no_fsync: true,
      },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function stopNodeServer(baseURL: string, nodeId: number) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/server/stop`);
    if (!response.ok() && response.status() !== 400 && response.status() !== 404 && response.status() !== 409) {
      console.warn(`stopNodeServer(${nodeId}) returned ${response.status()}:`, await response.text());
    }
  } catch (err) {
    console.warn(`stopNodeServer(${nodeId}) failed:`, err);
  } finally {
    await api.dispose();
  }
}

async function endpointAcceptsConnections(endpoint: string): Promise<boolean> {
  const url = new URL(endpoint);
  return new Promise((resolveConnection) => {
    const socket = createConnection({ host: url.hostname, port: Number(url.port) });
    let settled = false;
    const finish = (reachable: boolean) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolveConnection(reachable);
    };
    socket.once('connect', () => finish(true));
    socket.once('error', () => finish(false));
    socket.setTimeout(250, () => finish(false));
  });
}

/** Stop and promptly terminate a disposable local E2E server before port reuse. */
export async function stopNodeServerAndReleasePorts(baseURL: string, nodeId: number) {
  const api = await apiContext(baseURL);
  let pid = 0;
  let mgmtUrl = '';
  try {
    const response = await api.get(`/api/nodes/${encodeURIComponent(nodeId)}/server`);
    if (response.ok()) {
      const server = await response.json();
      pid = Number(server.pid ?? 0);
      mgmtUrl = String(server.mgmt_url ?? '');
    }
  } finally {
    await api.dispose();
  }

  await stopNodeServer(baseURL, nodeId);
  if (pid <= 0) return;

  try {
    process.kill(pid, 'SIGKILL');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
  }
  if (mgmtUrl) {
    await expect.poll(() => endpointAcceptsConnections(mgmtUrl), {
      timeout: 3_000,
      intervals: [100],
    }).toBe(false);
  }
}

export async function addGroup(baseURL: string, storeId: number, groupId: number, replicaId: number, nodeIds: number[]) {
  await postWithSysdataRetry(
    baseURL,
    `/api/stores/${storeId}/groups`,
    { group_id: groupId, replica_id: replicaId, nodes: nodeIds },
    `addGroup(s${storeId}/g${groupId})`,
  );
}

export async function addReplica(baseURL: string, storeId: number, groupId: number, nodeId: number, replicaId?: number) {
  const body: Record<string, unknown> = { node_id: nodeId };
  if (replicaId !== undefined) body.replica_id = replicaId;
  await postWithSysdataRetry(
    baseURL,
    `/api/stores/${storeId}/groups/${groupId}/replicas`,
    body,
    `addReplica(s${storeId}/g${groupId}/n${nodeId})`,
  );
}

export async function waitForLeader(baseURL: string, storeId: number, groupId: number, timeoutMs = 3_000) {
  const api = await apiContext(baseURL);
  try {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const r = await api.get(`/api/stores/${storeId}/groups/${groupId}`);
      if (r.ok()) {
        const v = await r.json();
        const hasLeader =
          (Array.isArray(v.replicas) && v.replicas.some((x: any) => String(x.role).toLowerCase() === 'leader')) ||
          (typeof v.leader_id === 'number' && v.leader_id > 0);
        if (hasLeader) return;
      }
      await new Promise((res) => setTimeout(res, 50));
    }
    throw new Error(`leader not elected for store ${storeId} group ${groupId} within ${timeoutMs}ms`);
  } finally {
    await api.dispose();
  }
}

export async function clusterInit(baseURL: string, nodeIds: number[]) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/cluster/init', { data: { nodes: nodeIds } });
    if (response.status() !== 201 && response.status() !== 409) {
      throw new Error(`cluster_init failed: ${response.status()} ${await response.text()}`);
    }
  } finally {
    await api.dispose();
  }
}

export async function createStore(baseURL: string, storeId: number, nodeIds: number[]) {
  if (storeId !== 0) {
    await clusterInit(baseURL, nodeIds);
  }
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/stores', {
      data: { store_id: storeId, nodes: nodeIds },
    });
    expect(response.status(), await response.text()).toBe(201);
    await expect.poll(async () => {
      const verify = await api.get(`/api/nodes/${nodeIds[0]}/stores`);
      if (!verify.ok()) return false;
      const stores = await verify.json();
      return Array.isArray(stores) && stores.some((store: any) => Number(store.store_id) === storeId);
    }, { timeout: 15_000, intervals: [200] }).toBe(true);
  } finally {
    await api.dispose();
  }
}

// Like createStore but skips clusterInit — use after group-0 has been
// bootstrapped once via clusterInit. add_store works on any deployed
// node; it does not require the node to be in group-0's membership.
export async function createStoreNoInit(baseURL: string, storeId: number, nodeIds: number[]) {
  await postWithSysdataRetry(
    baseURL,
    '/api/stores',
    { store_id: storeId, nodes: nodeIds },
    `createStoreNoInit(s${storeId})`,
  );
}

export async function resetAll(baseURL: string) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/internal/reset');
    expect(response.status(), await response.text()).toBe(200);
  } finally {
    await api.dispose();
  }
}

export async function clusterClean(baseURL: string) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/cluster/clean');
    expect(response.status(), await response.text()).toBe(200);
  } finally {
    await api.dispose();
  }
}

// ── DiskDB helpers ───────────────────────────────────────────────────

export const DEFAULT_DISKDB_BINARY =
  process.env.CROWDB_DISKDB_BIN ?? resolve(__dirname, '../../../../../target/debug/crowdb-diskdb');

export async function deployDiskdb(
  baseURL: string,
  nodeId: number,
  rpcPort: number,
  listenPort?: number,
  httpPort?: number,
) {
  const api = await apiContext(baseURL);
  try {
    const body: Record<string, number> = { rpc_port: rpcPort };
    if (listenPort !== undefined) body.listen_port = listenPort;
    if (httpPort !== undefined) body.http_port = httpPort;
    const response = await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/diskdb/deploy`, {
      data: body,
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function stopDiskdb(baseURL: string, nodeId: number) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/diskdb/stop`);
    if (!response.ok() && response.status() !== 400 && response.status() !== 404) {
      console.warn(`stopDiskdb(${nodeId}) returned ${response.status()}:`, await response.text());
    }
  } catch (err) {
    console.warn(`stopDiskdb(${nodeId}) failed:`, err);
  } finally {
    await api.dispose();
  }
}

export async function removeDiskdb(baseURL: string, nodeId: number) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.delete(`/api/nodes/${encodeURIComponent(nodeId)}/diskdb`);
    if (!response.ok() && response.status() !== 404) {
      console.warn(`removeDiskdb(${nodeId}) returned ${response.status()}:`, await response.text());
    }
  } catch (err) {
    console.warn(`removeDiskdb(${nodeId}) failed:`, err);
  } finally {
    await api.dispose();
  }
}

export async function addDiskGroup(baseURL: string, nodeId: number, dgId: number, name?: string) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/disk-groups`, {
      data: { id: dgId, name: name ?? '' },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function removeDiskGroup(baseURL: string, nodeId: number, dgId: number) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.delete(`/api/nodes/${encodeURIComponent(nodeId)}/disk-groups/${encodeURIComponent(dgId)}`);
    if (!response.ok() && response.status() !== 404) {
      console.warn(`removeDiskGroup(${nodeId}, ${dgId}) returned ${response.status()}:`, await response.text());
    }
  } catch (err) {
    console.warn(`removeDiskGroup(${nodeId}, ${dgId}) failed:`, err);
  } finally {
    await api.dispose();
  }
}

export async function addDisksBatch(
  baseURL: string,
  nodeId: number,
  dgId: number,
  disks: { disk_id: string; disk_type?: string; capacity_bytes?: number; zone_size_bytes?: number; unit_size_bytes?: number }[],
) {
  const api = await apiContext(baseURL);
  try {
    const payload = disks.map((d) => ({
      disk_id: d.disk_id,
      disk_type: d.disk_type ?? 'Hdd',
      capacity_bytes: d.capacity_bytes ?? 4 * 1024 * 1024 * 1024 * 1024,
      zone_size_bytes: d.zone_size_bytes ?? 32 * 1024 * 1024 * 1024,
      unit_size_bytes: d.unit_size_bytes ?? 1024 * 1024,
    }));
    const response = await api.post(
      `/api/nodes/${encodeURIComponent(nodeId)}/disk-groups/${encodeURIComponent(dgId)}/disks/batch`,
      { data: { disks: payload } },
    );
    expect(response.status(), await response.text()).toBe(201);
    return await response.json();
  } finally {
    await api.dispose();
  }
}

export async function removeDisk(baseURL: string, nodeId: number, dgId: number, diskId: string) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.delete(
      `/api/nodes/${encodeURIComponent(nodeId)}/disk-groups/${encodeURIComponent(dgId)}/disks/${encodeURIComponent(diskId)}`,
    );
    if (!response.ok() && response.status() !== 404) {
      console.warn(`removeDisk(${nodeId}, ${dgId}, ${diskId}) returned ${response.status()}:`, await response.text());
    }
  } catch (err) {
    console.warn(`removeDisk(${nodeId}, ${dgId}, ${diskId}) failed:`, err);
  } finally {
    await api.dispose();
  }
}

export function randomDiskId(): string {
  return Array.from({ length: 32 }, () => '0123456789abcdef'[Math.floor(Math.random() * 16)]).join('');
}

export async function assignDiskGroup(
  baseURL: string,
  rackId: number,
  nodeId: number,
  dgId: number,
  instanceId: string,
  storeId: number,
  groupId: number,
) {
  const api = await apiContext(baseURL);
  try {
    const leaseMs = Date.now() + 3_600_000;
    const ownerResp = await api.put(
      `/api/disk-groups/${rackId}/${nodeId}/${dgId}/owner`,
      { data: { instance_id: instanceId, lease_expiry_ms: leaseMs } },
    );
    expect(ownerResp.ok(), await ownerResp.text()).toBeTruthy();
    const bindResp = await api.put(
      `/api/disk-groups/${rackId}/${nodeId}/${dgId}/bind`,
      { data: { store_id: storeId, group_id: groupId } },
    );
    expect(bindResp.ok(), await bindResp.text()).toBeTruthy();
  } finally {
    await api.dispose();
  }
}

// ── CrowdbClusterDeployer ──────────────────────────────────────────────

/**
 * Reusable cluster lifecycle manager — TS mirror of the Rust
 * `CrowdbClusterDeployer`. Encapsulates the full cluster deploy/wait/
 * cleanup cycle so all E2E tests share one implementation.
 *
 * Tracks deployed node IDs and diskdb instances for cleanup. Call
 * `start()` to deploy a full cluster, `stop()` to stop all servers,
 * `teardown()` to stop + reset, and `reset()` for a clean slate.
 */
export class CrowdbClusterDeployer {
  readonly baseURL: string;
  private deployedNodeIds: number[] = [];
  private diskdbNodeIds: number[] = [];
  private _info: ClusterInfo | null = null;

  constructor(baseURL: string) {
    this.baseURL = baseURL;
  }

  get info(): ClusterInfo | null {
    return this._info;
  }

  /** POST /internal/reset — full cluster reset. */
  async reset(): Promise<void> {
    const t = Date.now();
    await resetAll(this.baseURL);
    logPhaseTime('reset', t);
  }

  /**
   * Full cluster start: reset, provision racks/nodes, deploy KV
   * servers, cluster_init, stores/groups, (optional) diskdb,
   * wait healthy, collect info.
   */
  async start(topo: TopologyDescriptor, timeoutMs = 30_000): Promise<ClusterInfo> {
    const totalStart = Date.now();
    await this.reset();

    const t = Date.now();
    const { racks, nodes } = await this.provisionRacksAndNodes(topo);
    logPhaseTime('provision_racks_and_nodes', t);

    const t2 = Date.now();
    const nodeInfos = await this.deployKvServers(nodes, topo);
    logPhaseTime('deploy_kv_servers', t2);

    const t3 = Date.now();
    await clusterInit(this.baseURL, nodes);
    logPhaseTime('cluster_init', t3);

    const t4 = Date.now();
    const stores = await this.createStoresAndGroups(nodes, topo);
    logPhaseTime('create_stores_and_groups', t4);

    const t5 = Date.now();
    const diskdbInstances = await this.deployDiskdbInstances(nodes, topo);
    logPhaseTime('deploy_diskdb_instances', t5);

    const t6 = Date.now();
    await this.createDiskGroupsAndDisks(nodes, topo);
    logPhaseTime('create_disk_groups_and_disks', t6);

    const t7 = Date.now();
    await this.waitHealthy(stores, timeoutMs);
    logPhaseTime('wait_healthy', t7);

    const t8 = Date.now();
    const storesWithLeaders = await this.collectLeaderInfo(stores);
    logPhaseTime('collect_leader_info', t8);

    this._info = { racks, nodes: nodeInfos, stores: storesWithLeaders, diskdbInstances };
    const totalMs = Date.now() - totalStart;
    if (totalMs >= VERY_SLOW_THRESHOLD_MS) {
      console.error(`[DEPLOYER] start() took ${totalMs}ms total (very slow)`);
    } else if (totalMs >= SLOW_THRESHOLD_MS) {
      console.warn(`[DEPLOYER] start() took ${totalMs}ms total (slow)`);
    } else {
      console.log(`[DEPLOYER] start() took ${totalMs}ms total`);
    }
    return this._info;
  }

  /** Stop all deployed KV servers + diskdb instances in parallel. */
  async stop(): Promise<void> {
    const totalStart = Date.now();
    const t = Date.now();
    const diskdbCount = this.diskdbNodeIds.length;
    const kvCount = this.deployedNodeIds.length;

    const stopPromises: Promise<void>[] = [];
    for (const nodeId of this.diskdbNodeIds) {
      stopPromises.push(stopDiskdb(this.baseURL, nodeId));
    }
    for (const nodeId of this.deployedNodeIds) {
      stopPromises.push(stopNodeServer(this.baseURL, nodeId));
    }
    await Promise.all(stopPromises);
    logPhaseTime('stop_all_servers', t);

    this.diskdbNodeIds = [];
    this.deployedNodeIds = [];
    this._info = null;

    const totalMs = Date.now() - totalStart;
    if (totalMs >= VERY_SLOW_THRESHOLD_MS) {
      console.error(`[DEPLOYER] stop() took ${totalMs}ms total (very slow, kv=${kvCount}, diskdb=${diskdbCount})`);
    } else if (totalMs >= SLOW_THRESHOLD_MS) {
      console.warn(`[DEPLOYER] stop() took ${totalMs}ms total (slow, kv=${kvCount}, diskdb=${diskdbCount})`);
    } else {
      console.log(`[DEPLOYER] stop() took ${totalMs}ms total (kv=${kvCount}, diskdb=${diskdbCount})`);
    }
  }

  /** Full teardown: stop all servers + reset config. */
  async teardown(): Promise<void> {
    const totalStart = Date.now();
    await this.stop();
    await this.reset();
    const totalMs = Date.now() - totalStart;
    if (totalMs >= VERY_SLOW_THRESHOLD_MS) {
      console.error(`[DEPLOYER] teardown() took ${totalMs}ms total (very slow)`);
    } else if (totalMs >= SLOW_THRESHOLD_MS) {
      console.warn(`[DEPLOYER] teardown() took ${totalMs}ms total (slow)`);
    } else {
      console.log(`[DEPLOYER] teardown() took ${totalMs}ms total`);
    }
  }

  /** Register a node as deployed (for cleanup tracking). */
  registerNode(nodeId: number): void {
    if (!this.deployedNodeIds.includes(nodeId)) {
      this.deployedNodeIds.push(nodeId);
    }
  }

  /** Register a diskdb instance as deployed (for cleanup tracking). */
  registerDiskdb(nodeId: number): void {
    if (!this.diskdbNodeIds.includes(nodeId)) {
      this.diskdbNodeIds.push(nodeId);
    }
  }

  // ── Private helpers ──────────────────────────────────────────────

  private async provisionRacksAndNodes(topo: TopologyDescriptor): Promise<{ racks: number[]; nodes: number[] }> {
    const racks: number[] = [];
    const nodes: number[] = [];
    // Create all rack+node pairs in parallel — each pair is independent.
    await Promise.all(
      Array.from({ length: topo.nodeCount }, (_, i) => {
        const rackId = topo.rackBase + i;
        const nodeId = topo.nodeBase + i;
        racks.push(rackId);
        nodes.push(nodeId);
        return createRack(this.baseURL, { id: rackId, name: `rack-${rackId}` }).then(() =>
          createNode(this.baseURL, { id: nodeId, rack_id: rackId }),
        );
      }),
    );
    return { racks, nodes };
  }

  private async deployKvServers(nodes: number[], topo: TopologyDescriptor): Promise<NodeInfo[]> {
    const deployPromises = nodes.map((nodeId) => {
      const restPort = freePort('kv-mgmt');
      const rpcPort = freePort('kv-listen');
      return deployNodeServer(this.baseURL, nodeId, restPort, rpcPort).then(async () => {
        const api = await apiContext(this.baseURL);
        try {
          const r = await api.get(`/api/nodes/${nodeId}/server`);
          const v = await r.json();
          return {
            id: nodeId,
            rackId: topo.rackBase + (nodeId - topo.nodeBase),
            pid: v.pid ?? 0,
            mgmtUrl: v.url ?? '',
            rpcUrl: v.rpc_url ?? '',
            restPort,
            rpcPort,
          } as NodeInfo;
        } finally {
          await api.dispose();
        }
      });
    });
    const nodeInfos = await Promise.all(deployPromises);
    this.deployedNodeIds = [...nodes];
    return nodeInfos;
  }

  private async createStoresAndGroups(nodes: number[], topo: TopologyDescriptor): Promise<StoreInfo[]> {
    const storeNodes = nodes.slice(0, Math.min(topo.replicasPerGroup, nodes.length));

    // Build the full list of (storeId, groupId) pairs, then create
    // stores and groups concurrently. Stores are independent; groups
    // within a store are independent Paxos groups.
    const storeSpecs: { storeId: number; groups: { groupId: number; groupNodes: number[] }[] }[] = [];
    for (let s = 0; s < topo.storeCount; s++) {
      const storeId = topo.storeBase + s;
      const groups: { groupId: number; groupNodes: number[] }[] = [];
      for (let g = 0; g < topo.groupsPerStore; g++) {
        const groupId = topo.groupBase + s * topo.groupsPerStore + g;
        const groupNodes = nodes.slice(0, topo.replicasPerGroup);
        groups.push({ groupId, groupNodes });
      }
      storeSpecs.push({ storeId, groups });
    }

    // Create all stores in parallel first (addGroup requires the store
    // to exist on each node).
    await Promise.all(storeSpecs.map((spec) => createStore(this.baseURL, spec.storeId, storeNodes)));

    // Create all groups in parallel — each is an independent Paxos group.
    const groupPromises: Promise<{ storeId: number; groupId: number }>[] = [];
    for (const spec of storeSpecs) {
      for (const g of spec.groups) {
        groupPromises.push(
          addGroup(this.baseURL, spec.storeId, g.groupId, 1, g.groupNodes).then(() => ({
            storeId: spec.storeId,
            groupId: g.groupId,
          })),
        );
      }
    }
    await Promise.all(groupPromises);

    // Assemble StoreInfo from the specs.
    return storeSpecs.map((spec) => ({
      storeId: spec.storeId,
      nodes: storeNodes,
      groups: spec.groups.map((g) => ({ groupId: g.groupId, leaderNodeId: null, leaderEndpoint: null })),
    }));
  }

  private async deployDiskdbInstances(nodes: number[], topo: TopologyDescriptor): Promise<DiskdbInfo[]> {
    if (!topo.deployDiskdb) return [];
    // Deploy all diskdb instances in parallel — each is independent.
    // Allocate independent ports for listen, http, and rpc.
    const instances = await Promise.all(
      nodes.map((nodeId) => {
        const listenPort = freePort('diskdb-listen');
        const httpPort = freePort('diskdb-http');
        const rpcPort = freePort('diskdb-rpc');
        return deployDiskdb(this.baseURL, nodeId, rpcPort, listenPort, httpPort).then(() => ({
          nodeId,
          pid: 0,
          rpcPort,
        }));
      }),
    );
    this.diskdbNodeIds = [...nodes];
    return instances;
  }

  private async createDiskGroupsAndDisks(nodes: number[], topo: TopologyDescriptor): Promise<void> {
    if (!topo.diskGroupsPerNode || topo.diskGroupsPerNode === 0) return;
    // Create disk-groups + disks on all nodes in parallel — each node's
    // disk-groups are independent.
    await Promise.all(
      nodes.map((nodeId) =>
        (async () => {
          for (let dg = 0; dg < topo.diskGroupsPerNode!; dg++) {
            const dgId = 1 + dg;
            await addDiskGroup(this.baseURL, nodeId, dgId, `dg-${dgId}`);
            if (topo.disksPerGroup && topo.disksPerGroup! > 0) {
              const disks = [];
              for (let d = 0; d < topo.disksPerGroup!; d++) {
                disks.push({ disk_id: randomDiskId() });
              }
              await addDisksBatch(this.baseURL, nodeId, dgId, disks);
            }
          }
        })(),
      ),
    );
  }

  private async waitHealthy(stores: StoreInfo[], timeoutMs: number): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    // Poll all groups in parallel — leader elections are independent.
    const groupSpecs: { storeId: number; groupId: number }[] = [];
    for (const store of stores) {
      for (const group of store.groups) {
        groupSpecs.push({ storeId: store.storeId, groupId: group.groupId });
      }
    }
    await Promise.all(
      groupSpecs.map(({ storeId, groupId }) =>
        (async () => {
          while (true) {
            const api = await apiContext(this.baseURL);
            try {
              const r = await api.get(`/api/stores/${storeId}/groups/${groupId}`);
              if (r.ok()) {
                const v = await r.json();
                if (Array.isArray(v.replicas) && v.replicas.some((x: any) => String(x.role).toLowerCase() === 'leader')) {
                  break;
                }
              }
            } finally {
              await api.dispose();
            }
            if (Date.now() >= deadline) {
              throw new Error(`no leader for store ${storeId} group ${groupId} within ${timeoutMs}ms`);
            }
            await new Promise((res) => setTimeout(res, 100));
          }
        })(),
      ),
    );
  }

  private async collectLeaderInfo(stores: StoreInfo[]): Promise<StoreInfo[]> {
    // Fetch leader info for all groups in parallel — each is an
    // independent API call.
    const promises: Promise<void>[] = [];
    for (const store of stores) {
      for (const group of store.groups) {
        promises.push(
          (async () => {
            const api = await apiContext(this.baseURL);
            try {
              const r = await api.get(`/api/stores/${store.storeId}/groups/${group.groupId}`);
              if (r.ok()) {
                const v = await r.json();
                const leader = v.replicas?.find((x: any) => String(x.role).toLowerCase() === 'leader');
                group.leaderNodeId = leader?.node_id ?? null;
              }
              const epResp = await api.get(`/api/stores/${store.storeId}/groups/${group.groupId}/endpoint`);
              if (epResp.ok()) {
                const ep = await epResp.json();
                if (ep.rpc_url) group.leaderEndpoint = ep.rpc_url;
              }
            } finally {
              await api.dispose();
            }
          })(),
        );
      }
    }
    await Promise.all(promises);
    return stores;
  }
}

// ── Legacy setupCluster / teardownCluster (backward compat) ──────────

export interface SetupResult {
  racks: number[];
  nodes: number[];
  stores: number[];
  groups: { storeId: number; groupId: number }[];
  apiBase: string;
}

/**
 * Create a full cluster topology via API calls: racks, nodes, deploy,
 * stores, groups, wait for leaders. Returns the created entity IDs.
 * Each node gets its own rack (1:1 mapping) for simplicity.
 */
export async function setupCluster(baseURL: string, topo: TopologyDescriptor): Promise<SetupResult> {
  const racks: number[] = [];
  const nodes: number[] = [];

  // Create all rack+node pairs in parallel — each pair is independent.
  await Promise.all(
    Array.from({ length: topo.nodeCount }, (_, i) => {
      const rackId = topo.rackBase + i;
      const nodeId = topo.nodeBase + i;
      racks.push(rackId);
      nodes.push(nodeId);
      return createRack(baseURL, { id: rackId, name: `rack-${rackId}` }).then(() =>
        createNode(baseURL, { id: nodeId, rack_id: rackId }),
      );
    }),
  );

  await Promise.all(nodes.map((nodeId) => deployNodeServer(baseURL, nodeId, freePort('kv-mgmt'), freePort('kv-listen'))));

  const stores: number[] = [];
  const groups: { storeId: number; groupId: number }[] = [];

  // Create all stores in parallel — stores are independent.
  const storeNodes = nodes.slice(0, Math.min(topo.replicasPerGroup, nodes.length));
  for (let s = 0; s < topo.storeCount; s++) {
    stores.push(topo.storeBase + s);
  }
  await Promise.all(stores.map((storeId) => createStore(baseURL, storeId, storeNodes)));

  // Create all groups in parallel — each addGroup creates an independent
  // Paxos group. The backend handles per-node fan-out concurrently.
  const groupPromises: Promise<void>[] = [];
  for (let s = 0; s < topo.storeCount; s++) {
    const storeId = topo.storeBase + s;
    for (let g = 0; g < topo.groupsPerStore; g++) {
      const groupId = topo.groupBase + s * topo.groupsPerStore + g;
      const groupNodes = nodes.slice(0, topo.replicasPerGroup);
      groups.push({ storeId, groupId });
      groupPromises.push(
        addGroup(baseURL, storeId, groupId, 1, groupNodes).then(() =>
          waitForLeader(baseURL, storeId, groupId),
        ),
      );
    }
  }

  await Promise.all(groupPromises);

  return { racks, nodes, stores, groups, apiBase: baseURL };
}

/**
 * Stop all deployed servers from a setupCluster call.
 */
export async function teardownCluster(baseURL: string, result: SetupResult) {
  await Promise.all(result.nodes.map((nodeId) => stopNodeServer(baseURL, nodeId)));
}
