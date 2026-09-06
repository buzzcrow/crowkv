// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { Suspense, useState, useCallback, useMemo, lazy, useEffect, useRef, type ReactNode } from 'react';
import { Server, Database, Plus, Trash2, Activity, RotateCw, Square, HardDrive, Boxes, CheckCircle2, XCircle, PowerOff, Wrench, AlertTriangle, EyeOff, HelpCircle } from 'lucide-react';
import type { CenterPanelMode } from './shell/Header';
import { DomainProvider, useDomain } from './contexts/DomainContext';
import { SelectionProvider, useSelection, type SelectedEntity } from './contexts/SelectionContext';
import { ToastProvider, useToast } from './contexts/ToastContext';
import { ActivityProvider, useActivity } from './contexts/ActivityContext';
import { useClusterTree } from './data/useClusterTree';
import { useLogicalTree } from './data/useLogicalTree';
import { useCapacityTree } from './data/useCapacityTree';
import { Header, ClusterHealth } from './shell/Header';
import { Sidebar } from './shell/Sidebar';
import { ToastContainer } from './components/ToastContainer';
import { TreeNode } from './components/Tree';
import { ContextMenu, useContextMenu, MenuItemOrSeparator } from './components/ContextMenu';
import type { MenuTarget } from './topology/TopologyCanvas';
import {
  AddRackDialog,
  AddNodeDialog,
  AddStoreDialog,
  AddGroupDialog,
  AddReplicaDialog,
  AddDiskGroupDialog,
  AddDiskDialog,
  AssignDiskGroupDialog,
  DeployServerDialog,
  DeployDiskdbDialog,
  ConfirmDeleteDialog,
  InitClusterDialog,
  ZoneSelectDialog,
} from './components/dialogs';
import { Domain } from './types';
import {
  removeRack,
  removeNode,
  removeStore,
  removeGroup,
  removeReplica,
  stopServer,
  restartServer,
  pingNode,
  setApiBase,
  resetCluster,
  triggerDiskdbScan,
  recalcDiskdbUsage,
  compactDiskdbZones,
  rebuildDiskdbZoneBitmap,
  setDiskStatus,
  setDiskGroupStatus,
  restartDiskdb,
  stopDiskdb,
  removeServer,
  removeDiskdb,
  removeDiskGroup,
  removeDisk,
  listServers,
} from './api';
import { deployPortDefaultsForNode, diskdbPortDefaultsForNode, nextIdFromSuffix, nextNumericId } from './components/dialogs/defaults';
import { buildCrowdbKVServers, crowdbKvServerNodeIds, extractPort } from './data/crowdbKvServers';
import { isCrowdbKVServerAvailable } from './data/crowdbKvServers';
import { toUiHealth, HW_STATUS_NAMES } from './utils/entityDisplay';
import { ClusterView } from './views/ClusterView';
import { KvView } from './views/KvView';
import { ChunkView } from './views/ChunkView';

const Inspector = lazy(() => import('./shell/Inspector').then((m) => ({ default: m.Inspector })));

export interface CrowdbConsoleProps {
  /** API prefix for all backend calls (default "/api"). */
  apiPrefix?: string;
  /** Mount hint for host routers (default "/"). Not used for navigation in v1. */
  basePath?: string;
  /** Hide all mutating controls. */
  readonly?: boolean;
  /** Opt feature areas in/out. */
  modules?: Partial<Record<'racks' | 'nodes' | 'stores' | 'groups' | 'replicas' | 'kv' | 'activity', boolean>>;
  /** Initial domain (default Cluster). */
  initialDomain?: Domain;
  /** Structured event callback for host integration. */
  onEvent?: (event: { type: string; payload?: unknown }) => void;
}

function AppContent({ apiPrefix = '/api', readonly = false, modules, onEvent }: CrowdbConsoleProps) {
  const { domain } = useDomain();
  const { selectedEntity, selectEntity, clearSelection } = useSelection();
  const { success, error } = useToast();
  const { log } = useActivity();

  // Re-root data-plane traffic onto the host-provided apiPrefix. The
  // standalone mount also sets this pre-render in `main.tsx`; this keeps
  // an embedding host's prop authoritative.
  useEffect(() => {
    setApiBase(apiPrefix);
  }, [apiPrefix]);

  const [lastUsedRackId, setLastUsedRackId] = useState<number>(0);
  const [rememberedDeployPorts, setRememberedDeployPorts] = useState<{ mgmt: number[]; rpc: number[]; diskdbRpc: number[] }>({ mgmt: [], rpc: [], diskdbRpc: [] });
  const [lastRefreshTime, setLastRefreshTime] = useState<Date>(new Date());
  const [refreshing, setRefreshing] = useState(false);
  const [centerPanel, setCenterPanel] = useState<CenterPanelMode>('topology');
  const [sidebarWidth, setSidebarWidth] = useState(280);
  const [inspectorWidth, setInspectorWidth] = useState(320);
  const [resizing, setResizing] = useState<'left' | 'right' | null>(null);
  const [canvasFocusRequest, setCanvasFocusRequest] = useState<{ targetId: string; subtree: boolean; nonce: number } | null>(null);
  // When a cross-jump drives the domain change, it sets a pending
  // selection that must survive the domain-change effect (which
  // otherwise clears the selection). The ref is checked once, then
  // reset, so subsequent manual domain switches still clear.
  const pendingSelectionRef = useRef<SelectedEntity | null>(null);

  useEffect(() => {
    if (pendingSelectionRef.current) {
      const pending = pendingSelectionRef.current;
      pendingSelectionRef.current = null;
      selectEntity(pending);
    } else {
      clearSelection();
    }
    setCanvasFocusRequest(null);
  }, [domain, clearSelection, selectEntity]);

  const [dialog, setDialog] = useState<{
    addRack?: boolean;
    addNode?: { rackId: number };
    addStore?: boolean;
    addGroup?: { storeId: string };
    addReplica?: { storeId: string; groupId: string };
    addDiskGroup?: { nodeId: number };
    addDisk?: { nodeId: number; dgId: number };
    assignDiskGroup?: { rackId: number; nodeId: number; dgId: number; dgName?: string };
    deployServer?: { nodeId: number };
    deployDiskdb?: { nodeId: number } | null;
    delete?: { type: string; id: string | number; onDelete: () => Promise<void>; cascadeWarning?: string };
    initCluster?: boolean;
    compactZones?: { diskId: string; zoneCount?: number };
    rebuildBitmap?: { diskId: string; zoneCount?: number };
  }>({});

  const { menuState, openMenu, closeMenu } = useContextMenu();

  const physicalActive = domain === Domain.Cluster;
  const capacityActive = domain === Domain.Chunk;
  const { racks, nodes, nodeStores, nodeHealthById, nodeDiskGroups: clusterDiskGroups, loading: physLoading, error: physError, refresh: refreshPhysical } = useClusterTree({
    enabled: true,
    recursive: 2,
    pollIntervalActive: 1000,
    pollIntervalInactive: 30000,
  });
  const { stores, groups, loading: logLoading, error: logError, refresh: refreshLogical } = useLogicalTree({
    enabled: true,
    recursive: 2,
    pollIntervalActive: 1000,
    pollIntervalInactive: 30000,
  });
  const { instances: diskdbInstances, usage: capacityUsage, hardwareCapacity, scanStatus: capacityScanStatus, loading: capLoading, error: capError, refresh: refreshCapacity, nodeDiskGroups: capNodeDiskGroups, fetchNodeDiskGroups } = useCapacityTree({
    enabled: domain === Domain.Chunk || domain === Domain.Cluster,
    pollIntervalActive: 5000,
    pollIntervalInactive: 30000,
  });

  // Merge disk-group maps: prefer cluster tree (fresh, all nodes), fall
  // back to capacity tree (on-demand fetch for the Capacity panel).
  const nodeDiskGroups = useMemo(() => {
    const merged: Record<number, import('./data/useClusterTree').NodeDiskGroups> = {};
    for (const [id, ndg] of Object.entries(capNodeDiskGroups)) {
      merged[Number(id)] = ndg;
    }
    for (const [id, ndg] of Object.entries(clusterDiskGroups)) {
      merged[Number(id)] = ndg;
    }
    return merged;
  }, [capNodeDiskGroups, clusterDiskGroups]);
  const existingDiskGroupIds = useMemo(
    () => Array.from(new Set([
      ...Object.values(nodeDiskGroups).flatMap((entry) => entry.diskGroups.map((dg) => dg.id)),
      ...(hardwareCapacity?.disk_groups || []).map((group) => group.disk_group_id),
    ])),
    [nodeDiskGroups, hardwareCapacity],
  );

  const loading = physLoading || logLoading || capLoading;
  const dataError = physError || logError || capError;
  const servers = useMemo(() => buildCrowdbKVServers(nodes, racks), [nodes, racks]);
  const serverNodeIds = useMemo(() => crowdbKvServerNodeIds(servers), [servers]);
  const [allServers, setAllServers] = useState<import('./api').ServerSummary[]>([]);
  const serverErrorShownRef = useRef(false);
  const refreshAllServers = useCallback(async () => {
    try {
      setAllServers(await listServers());
      serverErrorShownRef.current = false;
    } catch (err) {
      setAllServers([]);
      // Only show the toast once per failure streak — polling retries
      // every few seconds and would otherwise flood the UI with
      // identical "backend unreachable" toasts.
      if (!serverErrorShownRef.current) {
        serverErrorShownRef.current = true;
        error(`Failed to load server list: ${err instanceof Error ? err.message : 'backend unreachable'}`);
      }
    }
  }, [error]);
  useEffect(() => {
    if (physicalActive || capacityActive) {
      refreshAllServers();
    }
  }, [physicalActive, capacityActive, diskdbInstances, refreshAllServers]);
  const diskdbNodeIds = useMemo(
    () => new Set(allServers.filter((s) => s.service_type === 'diskdb' && s.node_id != null).map((s) => s.node_id!)),
    [allServers],
  );
  const diskdbHealthById = useMemo(() => {
    const m = new Map<number, string>();
    for (const s of allServers) {
      if (s.service_type === 'diskdb' && s.node_id != null) m.set(s.node_id, s.health);
    }
    return m;
  }, [allServers]);
  const diskdbInstanceIdByNodeId = useMemo(() => {
    const instanceByPort = new Map(
      diskdbInstances
        .map((instance) => [extractPort(instance.rpc_endpoint), instance.instance_id] as const)
        .filter((entry): entry is readonly [number, string] => entry[0] != null),
    );
    const result = new Map<number, string>();
    for (const server of allServers) {
      if (server.service_type !== 'diskdb' || server.node_id == null) continue;
      const port = extractPort(server.endpoint || server.rpc_url);
      const instanceId = port == null ? undefined : instanceByPort.get(port);
      if (instanceId != null) result.set(server.node_id, instanceId);
    }
    return result;
  }, [allServers, diskdbInstances]);
  // Cluster is initialized once the system store (store 0) exists.
  const clusterInitialized = useMemo(
    () => stores.some((s) => String(s.store_id) === '0'),
    [stores],
  );

  const clusterHealth: ClusterHealth = useMemo(() => {
    if (dataError) return 'Failed';
    if (groups.length === 0) return 'Unknown';
    const statuses = groups.map((g) => toUiHealth(String((g as any).state || (g as any).health || '')));
    if (statuses.some((status) => status === 'Failed')) return 'Failed';
    if (statuses.some((status) => status === 'Degraded')) return 'Degraded';
    if (statuses.every((status) => status === 'Healthy')) return 'Healthy';
    return 'Unknown';
  }, [groups, dataError]);

  const handleRefresh = useCallback(async () => {
    setRefreshing(true);
    try {
      const tasks: Promise<unknown>[] = [refreshPhysical(), refreshLogical(), refreshCapacity()];
      if (capacityActive || physicalActive) {
        tasks.push(fetchNodeDiskGroups(nodes.map((n) => n.id)));
      }
      tasks.push(refreshAllServers());
      await Promise.all(tasks);
      setLastRefreshTime(new Date());
    } finally {
      setRefreshing(false);
    }
  }, [refreshPhysical, refreshLogical, refreshCapacity, capacityActive, physicalActive, fetchNodeDiskGroups, nodes, refreshAllServers]);

  // Fetch node disk-groups when the Capacity or Physical view is active.
  useEffect(() => {
    if ((capacityActive || physicalActive) && nodes.length > 0) {
      fetchNodeDiskGroups(nodes.map((n) => n.id));
    }
  }, [capacityActive, physicalActive, nodes, fetchNodeDiskGroups]);

  // After cluster init succeeds, refresh the tree so the system group
  // appears. Init only bootstraps store 0 / group 0; store creation is
  // a separate step the user initiates via the "+" button.
  const handleInitSuccess = useCallback(async () => {
    await handleRefresh();
    setDialog((d) => ({ ...d, initCluster: false }));
  }, [handleRefresh]);

  useEffect(() => {
    if (!resizing) return;

    const onMouseMove = (event: MouseEvent) => {
      if (resizing === 'left') {
        setSidebarWidth(Math.min(420, Math.max(200, event.clientX)));
        return;
      }
      setInspectorWidth(Math.min(560, Math.max(280, window.innerWidth - event.clientX)));
    };

    const onMouseUp = () => setResizing(null);

    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
    return () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };
  }, [resizing]);

  /** Run a mutation, surface toast + activity, then refresh. */
  const runMutation = useCallback(
    async (action: string, target: string, fn: () => Promise<unknown>) => {
      try {
        await fn();
        log({ action, target, status: 'Success' });
        success(`${action}: ${target}`);
        onEvent?.({ type: 'mutation', payload: { action, target } });
        await handleRefresh();
      } catch (err) {
        const msg = err instanceof Error ? err.message : 'failed';
        log({ action, target, status: 'Failed', message: msg });
        error(`${action} failed: ${msg}`);
      }
    },
    [log, success, error, onEvent, handleRefresh],
  );

  const requestDelete = useCallback(
    (type: string, id: string | number, onDelete: () => Promise<void>, cascadeWarning?: string) => {
      setDialog((d) => ({ ...d, delete: { type, id, onDelete, cascadeWarning } }));
    },
    [],
  );

  const handleResetCluster = useCallback(() => {
    setDialog((d) => ({
      ...d,
      delete: {
        type: 'Cluster',
        id: 'all',
        onDelete: async () => {
          await runMutation('Reset Cluster', 'all', async () => {
            await resetCluster();
            clearSelection();
          });
        },
      },
    }));
  }, [runMutation, clearSelection]);

  // Status icons for the "Change Status" submenu.
  const statusIcons: Record<string, ReactNode> = {
    Init: <HelpCircle className="tw-h-4 tw-w-4" />,
    Up: <CheckCircle2 className="tw-h-4 tw-w-4" />,
    Maintenance: <Wrench className="tw-h-4 tw-w-4" />,
    Suspect: <AlertTriangle className="tw-h-4 tw-w-4" />,
    Missing: <EyeOff className="tw-h-4 tw-w-4" />,
    Bad: <XCircle className="tw-h-4 tw-w-4" />,
    Offline: <PowerOff className="tw-h-4 tw-w-4" />,
  };

  /** Build the "Change Status" submenu items for a DG or Disk. */
  const buildStatusSubmenu = useCallback(
    (onSet: (status: string) => Promise<void>): MenuItemOrSeparator[] => {
      return HW_STATUS_NAMES.map((name) => ({
        id: `status-${name.toLowerCase()}`,
        label: name,
        icon: statusIcons[name],
        onSelect: () => onSet(name),
      }));
    },
    [statusIcons],
  );

  /** Build per-layer context menu items for a normalized target. */
  const buildMenuItems = useCallback(
    (t: MenuTarget): MenuItemOrSeparator[] => {
      if (readonly) return [];
      const items: MenuItemOrSeparator[] = [];
      const p = t.parentIds || {};

      if (physicalActive) {
        if (t.type === 'Datacenter') {
          // The default DC is immutable — only Add Rack is offered.
          items.push({
            id: 'add-rack',
            label: 'Add Rack',
            icon: <Plus className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, addRack: true })),
          });
        } else if (t.type === 'Rack' && modules?.nodes !== false) {
          const rackId = Number(t.rawId);
          items.push({
            id: 'add-node',
            label: 'Add Node',
            icon: <Server className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, addNode: { rackId } })),
          });
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'del-rack',
            label: 'Delete Rack',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => requestDelete('Rack', rackId, async () => { await runMutation('Delete Rack', `Rack ${rackId}`, () => removeRack(rackId)); }),
          });
        } else if (t.type === 'Node') {
          const nodeId = Number(t.rawId);
          const hasServer = serverNodeIds.has(nodeId);
          const hasDiskdb = diskdbNodeIds.has(nodeId);
          // Add Services — deploy CrowDB Storage and/or DiskDB.
          if (!hasServer) {
            items.push({
              id: 'deploy',
              label: 'Deploy CrowDB Storage',
              icon: <Server className="tw-h-4 tw-w-4" />,
              onSelect: () => setDialog((d) => ({ ...d, deployServer: { nodeId } })),
            });
          }
          if (!hasDiskdb) {
            items.push({
              id: 'deploy-diskdb',
              label: 'Deploy DiskDB',
              icon: <HardDrive className="tw-h-4 tw-w-4" />,
              onSelect: () => setDialog((d) => ({ ...d, deployDiskdb: { nodeId } })),
            });
          }
          items.push({
            id: 'ping',
            label: 'Ping',
            icon: <Activity className="tw-h-4 tw-w-4" />,
            onSelect: () =>
              runMutation('Ping Node', t.label || t.id, async () => {
                const r = await pingNode(nodeId);
                if (!r.ok) throw new Error(r.error || 'unreachable');
              }),
          });
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'del-node',
            label: 'Delete Node',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            // Cascade: the backend's DELETE /api/nodes/:id handler
            // (http_remove_node) calls stop_and_remove_server_for_node
            // which stops the KV process, removes the server entry, and
            // purges topology — all before removing the node. Calling
            // removeServer separately here would hit check_require_empty
            // (which refuses if the node hosts group-0 replicas), blocking
            // the cascade. So we only remove diskdb explicitly (no
            // check_require_empty gate) and let removeNode handle the KV
            // cascade.
            onSelect: () => requestDelete('Node', nodeId, async () => {
              await runMutation('Delete Node', t.label || t.id, async () => {
                if (hasDiskdb) await removeDiskdb(nodeId);
                await removeNode(nodeId);
              });
            }),
          });
        } else if (t.type === 'DiskGroup') {
          const dgId = Number(t.rawId);
          const nodeId = Number(p.node_id);
          const rackId = Number(p.rack_id);
          items.push({ id: 'add-disk', label: 'Add Disk', icon: <HardDrive className="tw-h-4 tw-w-4" />, onSelect: () => setDialog((d) => ({ ...d, addDisk: { nodeId, dgId } })) });
          items.push({ id: 'dg-status', label: 'Change Status', icon: <Activity className="tw-h-4 tw-w-4" />, submenu: buildStatusSubmenu((status) => runMutation(`Set DG ${status}`, t.label || t.id, () => setDiskGroupStatus(rackId, nodeId, dgId, status))) });
          items.push({ id: 'assign-dg', label: 'Assign to DiskDB', icon: <Server className="tw-h-4 tw-w-4" />, onSelect: () => setDialog((d) => ({ ...d, assignDiskGroup: { rackId, nodeId, dgId, dgName: t.label } })) });
          items.push({ id: 'del-dg', label: 'Delete Disk Group', icon: <Trash2 className="tw-h-4 tw-w-4" />, destructive: true, onSelect: () => requestDelete('Disk Group', dgId, async () => { await runMutation('Delete Disk Group', t.label || t.id, () => removeDiskGroup(nodeId, dgId)); }, 'All disks in this disk group will also be removed.') });
        } else if (t.type === 'Disk') {
          const diskId = String(p.disk_id || t.rawId || t.id);
          const nodeId = Number(p.node_id);
          const dgId = Number(p.disk_group_id);
          items.push({ id: 'disk-status', label: 'Change Status', icon: <Activity className="tw-h-4 tw-w-4" />, submenu: buildStatusSubmenu((status) => runMutation(`Set Disk ${status}`, t.label || t.id, () => setDiskStatus(diskId, status))) });
          items.push({ id: 'del-disk', label: 'Delete Disk', icon: <Trash2 className="tw-h-4 tw-w-4" />, destructive: true, onSelect: () => requestDelete('Disk', diskId, async () => { await runMutation('Delete Disk', diskId, () => removeDisk(nodeId, dgId, diskId)); }) });
        } else if (t.type === 'Server') {
          // Server context menu: dispatch on serviceType (KV vs DiskDB).
          const nodeId = Number(p.node_id);
          if (t.serviceType === 'diskdb') {
            items.push({
              id: 'ddb-restart',
              label: 'Restart DiskDB',
              icon: <RotateCw className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Restart DiskDB', t.label || t.id, () => restartDiskdb(nodeId)),
            });
            items.push({
              id: 'ddb-stop',
              label: 'Stop DiskDB',
              icon: <Square className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Stop DiskDB', t.label || t.id, () => stopDiskdb(nodeId)),
            });
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-ddb',
              label: 'Delete DiskDB',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('DiskDB', t.label || t.id, async () => {
                await runMutation('Delete DiskDB', t.label || t.id, () => removeDiskdb(nodeId));
              }),
            });
          } else {
            // CrowdbKV service context menu: restart, stop, delete.
            items.push({
              id: 'restart',
              label: 'Restart CrowDB Storage',
              icon: <RotateCw className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Restart CrowDB Storage', t.label || t.id, () => restartServer(nodeId)),
            });
            items.push({
              id: 'stop',
              label: 'Stop CrowDB Storage',
              icon: <Square className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Stop CrowDB Storage', t.label || t.id, () => stopServer(nodeId)),
            });
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-service',
              label: 'Delete CrowDB Storage',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('CrowDB Storage', t.label || t.id, async () => {
                await runMutation('Delete CrowDB Storage', t.label || t.id, () => removeServer(nodeId));
              }),
            });
          }
        }
      } else {
        if (t.type === 'Server') {
          // KV-domain server context menu: restart, stop, delete.
          const nodeId = Number(p.node_id);
          if (t.serviceType === 'diskdb') {
            items.push({
              id: 'ddb-restart',
              label: 'Restart DiskDB',
              icon: <RotateCw className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Restart DiskDB', t.label || t.id, () => restartDiskdb(nodeId)),
            });
            items.push({
              id: 'ddb-stop',
              label: 'Stop DiskDB',
              icon: <Square className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Stop DiskDB', t.label || t.id, () => stopDiskdb(nodeId)),
            });
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-ddb',
              label: 'Delete DiskDB',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('DiskDB', t.label || t.id, async () => {
                await runMutation('Delete DiskDB', t.label || t.id, () => removeDiskdb(nodeId));
              }),
            });
          } else {
            items.push({
              id: 'restart',
              label: 'Restart CrowDB Storage',
              icon: <RotateCw className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Restart CrowDB Storage', t.label || t.id, () => restartServer(nodeId)),
            });
            items.push({
              id: 'stop',
              label: 'Stop CrowDB Storage',
              icon: <Square className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Stop CrowDB Storage', t.label || t.id, () => stopServer(nodeId)),
            });
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-service',
              label: 'Delete CrowDB Storage',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('CrowDB Storage', t.label || t.id, async () => {
                await runMutation('Delete CrowDB Storage', t.label || t.id, () => removeServer(nodeId));
              }),
            });
          }
        } else if (t.type === 'Store' && modules?.groups !== false) {
          items.push({
            id: 'add-group',
            label: 'Add Group',
            icon: <Database className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, addGroup: { storeId: t.id } })),
          });
          // System store (store 0) cannot be deleted individually.
          if (t.id !== '0') {
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-store',
              label: 'Delete Store',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('Store', t.id, async () => { await runMutation('Delete Store', t.id, () => removeStore(t.id)); }),
            });
          }
        } else if (t.type === 'Group') {
          const storeId = p.store_id;
          const isSystemGroup = storeId === '0' && t.id === '0';
          if (modules?.replicas !== false) {
            items.push({
              id: 'add-replica',
              label: 'Add Replica',
              icon: <Plus className="tw-h-4 tw-w-4" />,
              onSelect: () => {
                if (storeId) setDialog((d) => ({ ...d, addReplica: { storeId: String(storeId), groupId: t.id } }));
              },
            });
          }
          // System group (store 0, group 0) cannot be deleted individually.
          if (!isSystemGroup) {
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-group',
              label: 'Delete Group',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => {
                if (storeId)
                  requestDelete('Group', t.id, async () => {
                    await runMutation('Delete Group', `${storeId}/${t.id}`, () => removeGroup(String(storeId), t.id));
                  });
              },
            });
          }
        } else if (t.type === 'Replica') {
          const storeId = p.store_id;
          const groupId = p.group_id;
          items.push({
            id: 'del-replica',
            label: 'Delete Replica',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => {
              if (storeId && groupId)
                requestDelete('Replica', t.id, async () => {
                  await runMutation('Delete Replica', `${storeId}/${groupId}/${t.id}`, () => removeReplica(String(storeId), String(groupId), t.id));
                });
            },
          });
        }
      }
      return items;
    },
    [readonly, physicalActive, modules, requestDelete, runMutation, serverNodeIds, diskdbNodeIds, buildStatusSubmenu],
  );

  /** Capacity view has its own menu code path — rack/node management
   * belongs to the Physical view; here only disk-group/disk operations
   * and DiskDB deploy are exposed. */
  const buildCapacityMenuItems = useCallback(
    (t: MenuTarget): MenuItemOrSeparator[] => {
      if (readonly) return [];
      const items: MenuItemOrSeparator[] = [];
      const p = t.parentIds || {};

      if (t.type === 'Datacenter') {
        // The default DC is immutable — only Add Rack is offered.
        items.push({
          id: 'add-rack',
          label: 'Add Rack',
          icon: <Plus className="tw-h-4 tw-w-4" />,
          onSelect: () => setDialog((d) => ({ ...d, addRack: true })),
        });
      } else if (t.type === 'Node') {
        const nodeId = Number(t.rawId ?? t.id);
        const hasDiskdb = diskdbNodeIds.has(nodeId);
        items.push({
          id: 'add-dg',
          label: 'Add Disk Group',
          icon: <Boxes className="tw-h-4 tw-w-4" />,
          onSelect: () => setDialog((d) => ({ ...d, addDiskGroup: { nodeId } })),
        });
        if (!hasDiskdb) {
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'ddb-deploy',
            label: 'Deploy DiskDB',
            icon: <HardDrive className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, deployDiskdb: { nodeId } })),
          });
        }
      } else if (t.type === 'Server') {
        // Chunk-domain DDB server context menu: restart, stop, delete.
        const nodeId = Number(p.node_id);
        items.push({
          id: 'ddb-restart',
          label: 'Restart DiskDB',
          icon: <RotateCw className="tw-h-4 tw-w-4" />,
          onSelect: () => runMutation('Restart DiskDB', t.label || t.id, () => restartDiskdb(nodeId)),
        });
        items.push({
          id: 'ddb-stop',
          label: 'Stop DiskDB',
          icon: <Square className="tw-h-4 tw-w-4" />,
          onSelect: () => runMutation('Stop DiskDB', t.label || t.id, () => stopDiskdb(nodeId)),
        });
        items.push({ id: 's1', separator: true });
        items.push({
          id: 'del-ddb',
          label: 'Delete DiskDB',
          icon: <Trash2 className="tw-h-4 tw-w-4" />,
          destructive: true,
          onSelect: () => requestDelete('DiskDB', t.label || t.id, async () => {
            await runMutation('Delete DiskDB', t.label || t.id, () => removeDiskdb(nodeId));
          }),
        });
      } else if (t.type === 'DiskGroup') {
        const dgId = Number(t.rawId);
        const dgNodeId = Number(p.node_id);
        const dgRackId = Number(p.rack_id);
        items.push({
          id: 'add-disk',
          label: 'Add Disk',
          icon: <HardDrive className="tw-h-4 tw-w-4" />,
          onSelect: () => setDialog((d) => ({ ...d, addDisk: { nodeId: dgNodeId, dgId } })),
        });
        items.push({ id: 's1', separator: true });
        items.push({
          id: 'dg-change-status',
          label: 'Change Status',
          icon: <Activity className="tw-h-4 tw-w-4" />,
          submenu: buildStatusSubmenu((status) => runMutation(`Set DG ${status}`, t.label || t.id, () => setDiskGroupStatus(dgRackId, dgNodeId, dgId, status))),
        });
        items.push({ id: 's2', separator: true });
        items.push({
          id: 'assign-dg',
          label: 'Assign to DiskDB',
          icon: <Server className="tw-h-4 tw-w-4" />,
          onSelect: () => setDialog((d) => ({ ...d, assignDiskGroup: { rackId: dgRackId, nodeId: dgNodeId, dgId, dgName: t.label } })),
        });
        items.push({ id: 's3', separator: true });
        items.push({
          id: 'del-dg',
          label: 'Delete Disk Group',
          icon: <Trash2 className="tw-h-4 tw-w-4" />,
          destructive: true,
          onSelect: () => requestDelete('Disk Group', dgId, async () => {
            await runMutation('Delete Disk Group', t.label || t.id, () => removeDiskGroup(dgNodeId, dgId));
          }, 'All disks in this disk group will also be removed.'),
        });
      } else if (t.type === 'Disk') {
        const diskId = String(p.disk_id || t.rawId || t.id);
        const diskNodeId = Number(p.node_id);
        const diskDgId = Number(p.disk_group_id);
        let diskZoneCount: number | undefined;
        for (const dg of capacityUsage?.disk_groups || []) {
          const found = (dg.disks || []).find((d) => d.disk_id === diskId);
          if (found) { diskZoneCount = found.zone_count; break; }
        }
        items.push({
          id: 'ddb-compact',
          label: 'Compact Zones',
          icon: <Database className="tw-h-4 tw-w-4" />,
          onSelect: () => setDialog((d) => ({ ...d, compactZones: { diskId, zoneCount: diskZoneCount } })),
        });
        items.push({
          id: 'ddb-rebuild',
          label: 'Rebuild Bitmap',
          icon: <RotateCw className="tw-h-4 tw-w-4" />,
          onSelect: () => setDialog((d) => ({ ...d, rebuildBitmap: { diskId, zoneCount: diskZoneCount } })),
        });
        items.push({ id: 's1', separator: true });
        items.push({
          id: 'ddb-scan',
          label: 'Trigger Consistency Scan',
          icon: <Activity className="tw-h-4 tw-w-4" />,
          onSelect: () => runMutation('Trigger Consistency Scan', t.label || t.id, () => triggerDiskdbScan(diskDgId)),
        });
        items.push({
          id: 'ddb-recalc',
          label: 'Recalc Usage',
          icon: <RotateCw className="tw-h-4 tw-w-4" />,
          onSelect: () => runMutation('Recalc Usage', t.label || t.id, () => recalcDiskdbUsage(diskDgId)),
        });
        items.push({ id: 's2', separator: true });
        items.push({
          id: 'disk-change-status',
          label: 'Change Status',
          icon: <Activity className="tw-h-4 tw-w-4" />,
          submenu: buildStatusSubmenu((status) => runMutation(`Set Disk ${status}`, t.label || t.id, () => setDiskStatus(diskId, status))),
        });
        items.push({ id: 's3', separator: true });
        items.push({
          id: 'del-disk',
          label: 'Delete Disk',
          icon: <Trash2 className="tw-h-4 tw-w-4" />,
          destructive: true,
          onSelect: () => requestDelete('Disk', diskId, async () => {
            await runMutation('Delete Disk', t.label || t.id, () => removeDisk(diskNodeId, diskDgId, diskId));
          }, 'All zones on this disk will be lost.'),
        });
      }
      return items;
    },
    [readonly, diskdbNodeIds, capacityUsage, requestDelete, runMutation, setDialog, buildStatusSubmenu],
  );

  const onTreeContextMenu = useCallback(
    (node: TreeNode, event: React.MouseEvent) => {
      const target: MenuTarget = {
        type: node.type,
        id: node.rawId != null ? String(node.rawId) : node.id,
        rawId: node.rawId,
        parentIds: node.parentIds,
        label: node.label,
        serviceType: node.serviceType,
      };
      const items = capacityActive ? buildCapacityMenuItems(target) : buildMenuItems(target);
      if (items.length > 0) openMenu(event, items);
    },
    [buildMenuItems, buildCapacityMenuItems, capacityActive, openMenu],
  );

  const onCanvasContextMenu = useCallback(
    (target: MenuTarget, event: React.MouseEvent) => {
      const items = capacityActive ? buildCapacityMenuItems(target) : buildMenuItems(target);
      if (items.length > 0) openMenu(event, items);
    },
    [buildMenuItems, buildCapacityMenuItems, capacityActive, openMenu],
  );

  const onTreeNodeClick = useCallback((node: TreeNode) => {
    setCanvasFocusRequest({ targetId: node.id, subtree: true, nonce: Date.now() });
  }, []);

  const handleAdd = useCallback(() => {
    if (readonly) return;
    if (physicalActive || capacityActive) setDialog((d) => ({ ...d, addRack: true }));
    else if (!clusterInitialized) setDialog((d) => ({ ...d, initCluster: true }));
    else setDialog((d) => ({ ...d, addStore: true }));
  }, [readonly, physicalActive, capacityActive, clusterInitialized]);

  const closeDialogs = useCallback(() => setDialog({}), []);

  const kvEnabled = modules?.kv !== false;

  const rackIds = useMemo(() => racks.map((r) => r.id), [racks]);
  const nodeIds = useMemo(() => nodes.map((n) => n.id), [nodes]);

  const defaultAddNodeRackId = useMemo(() => {
    if (dialog.addNode?.rackId) return dialog.addNode.rackId;
    if (lastUsedRackId && rackIds.includes(lastUsedRackId)) return lastUsedRackId;
    return racks[0]?.id ?? 0;
  }, [dialog.addNode?.rackId, lastUsedRackId, rackIds, racks]);

  const deployDialogDefaults = useMemo(() => {
    if (!dialog.deployServer?.nodeId) {
      return { defaultRestPort: '19910', defaultRpcPort: '19920' };
    }
    return deployPortDefaultsForNode(
      servers,
      dialog.deployServer.nodeId,
      19910,
      19920,
      rememberedDeployPorts.mgmt,
      rememberedDeployPorts.rpc,
    );
  }, [dialog.deployServer?.nodeId, rememberedDeployPorts, servers]);

  const addNodeDeployDefaults = useMemo(
    () => {
      const nextNodeId = Number(nextIdFromSuffix(nodeIds, 1));
      return {
        ...deployPortDefaultsForNode(
          servers,
          nextNodeId,
          19910,
          19920,
          rememberedDeployPorts.mgmt,
          rememberedDeployPorts.rpc,
        ),
        defaultDiskdbRpcPort: diskdbPortDefaultsForNode(
          diskdbInstances,
          nextNodeId,
          undefined,
          rememberedDeployPorts.diskdbRpc,
        ),
      };
    },
    [nodeIds, rememberedDeployPorts, servers, diskdbInstances],
  );

  const deployDiskdbDefaults = useMemo(() => {
    if (!dialog.deployDiskdb?.nodeId) return '29920';
    return diskdbPortDefaultsForNode(
      diskdbInstances,
      dialog.deployDiskdb.nodeId,
      undefined,
      rememberedDeployPorts.diskdbRpc,
    );
  }, [dialog.deployDiskdb?.nodeId, diskdbInstances, rememberedDeployPorts.diskdbRpc]);

  const storeDialogDefaults = useMemo(() => {
    const availableNodeIds = servers.filter((server) => isCrowdbKVServerAvailable(server)).map((server) => server.node_id);
    const defaultNodeIds = availableNodeIds.length <= 7 ? availableNodeIds : availableNodeIds.slice(0, 3);
    return {
      storeId: nextNumericId(stores.map((s) => String(s.store_id)), 1),
      nodeIds: defaultNodeIds.length > 0 ? defaultNodeIds : (nodes[0] ? [nodes[0].id] : []),
    };
  }, [nodes, servers, stores]);

  const groupDialogDefaults = useMemo(() => {
    const defaults: Record<string, { groupId: string; replicaId: string; nodeIds: number[] }> = {};
    const activeNodeIds = servers
      .filter((server) => isCrowdbKVServerAvailable(server))
      .map((server) => server.node_id);
    for (const store of stores) {
      const storeId = String(store.store_id);
      const groupsInStore = groups.filter((g) => String(g.store_id) === storeId);
      const groupId = nextNumericId(groupsInStore.map((g) => String(g.group_id)), 1);

      const replicaIds: string[] = [];
      for (const group of groupsInStore) {
        for (const replica of group.replicas || []) {
          replicaIds.push(String(replica.replica_id));
        }
      }

      const replicaId = nextNumericId(replicaIds, 1);
      const storeNodeIds = store.nodes.filter((nodeId) => activeNodeIds.includes(nodeId));
      const nodeIds = storeNodeIds.length > 0 ? storeNodeIds : activeNodeIds.slice(0, 3);

      defaults[storeId] = { groupId, replicaId, nodeIds };
    }
    return defaults;
  }, [groups, nodes, servers, stores]);

  const replicaDialogDefaults = useMemo(() => {
    const defaults: Record<string, { nodeId: number; replicaId: string }> = {};

    for (const group of groups) {
      const key = `${group.store_id}:${group.group_id}`;
      const existingReplicaIds = (group.replicas || []).map((replica) => String(replica.replica_id));
      const usedNodeIds = new Set((group.replicas || []).map((replica) => replica.node_id || 0));
      const preferredNode =
        servers.find((server) => !usedNodeIds.has(server.node_id))?.node_id ||
        servers[0]?.node_id ||
        nodes[0];

      defaults[key] = {
        nodeId: typeof preferredNode === 'number' ? preferredNode : (preferredNode?.id ?? 0),
        replicaId: nextNumericId(existingReplicaIds, 1),
      };
    }

    return defaults;
  }, [groups, nodes, servers]);

  const replicaDialogNodeInfo = useMemo(() => {
    const info: Record<string, { allNodes: typeof nodes; usedNodeIds: Set<number> }> = {};

    for (const group of groups) {
      const key = `${group.store_id}:${group.group_id}`;
      const usedNodeIds = new Set((group.replicas || []).map((replica) => replica.node_id || 0));
      info[key] = { allNodes: nodes, usedNodeIds };
    }

    return info;
  }, [groups, nodes]);

  return (
    <div className="tw-min-h-screen tw-bg-bg tw-text-text crowdb-console">
      <Header
        clusterHealth={clusterHealth}
        onRefresh={handleRefresh}
        refreshing={refreshing}
        onShowTopology={() => setCenterPanel('topology')}
        onShowCapacity={() => setCenterPanel('capacity')}
        onResetCluster={readonly ? undefined : handleResetCluster}
      />

      {dataError && (
        <div
          role="alert"
          className="tw-fixed tw-top-16 tw-left-1/2 -tw-translate-x-1/2 tw-z-50 tw-bg-failed/10 tw-border tw-border-failed/30 tw-text-failed tw-px-4 tw-py-2 tw-rounded-md tw-text-sm tw-shadow-lg"
        >
          Backend unreachable — retrying
        </div>
      )}

      <Sidebar
        racks={racks}
        servers={servers}
        stores={stores}
        nodeStores={nodeStores}
        nodeHealthById={nodeHealthById}
        loading={loading}
        readonly={readonly}
        width={sidebarWidth}
        clusterInitialized={clusterInitialized}
        onNodeClick={onTreeNodeClick}
        onNodeContextMenu={onTreeContextMenu}
        onAdd={handleAdd}
        diskdbInstances={diskdbInstances}
        capacityUsage={capacityUsage}
        hardwareCapacity={hardwareCapacity}
        nodeDiskGroups={nodeDiskGroups}
        diskdbNodeIds={diskdbNodeIds}
        diskdbHealthById={diskdbHealthById}
        diskdbInstanceIdByNodeId={diskdbInstanceIdByNodeId}
      />

      <div
        className="tw-fixed tw-top-14 tw-bottom-0 tw-z-30 tw-w-2 tw-cursor-col-resize hover:tw-bg-accent/20"
        style={{ left: sidebarWidth - 1 }}
        onMouseDown={() => setResizing('left')}
        aria-hidden="true"
      />

      <main
        className="tw-mt-14 tw-h-[calc(100vh-3.5rem)] tw-transition-[margin]"
        style={{
          marginLeft: sidebarWidth,
          marginRight: selectedEntity ? inspectorWidth : 0,
        }}
      >
        {domain === Domain.Cluster && (
          <ClusterView
            racks={racks}
            nodes={nodes}
            servers={servers}
            stores={stores}
            nodeStores={nodeStores}
            nodeHealthById={nodeHealthById}
            diskdbNodeIds={diskdbNodeIds}
            diskdbInstances={diskdbInstances}
            diskdbInstanceIdByNodeId={diskdbInstanceIdByNodeId}
            nodeDiskGroups={nodeDiskGroups}
            refreshToken={lastRefreshTime.getTime()}
            focusRequest={canvasFocusRequest}
            onEntityContextMenu={onCanvasContextMenu}
          />
        )}
        {domain === Domain.KV && kvEnabled && (
          <KvView stores={stores} selectedEntity={selectedEntity} readonly={readonly} backendError={!!dataError} loading={loading} />
        )}
        {domain === Domain.Chunk && (
          <ChunkView
            centerPanel={centerPanel}
            onCenterPanelChange={setCenterPanel}
            instances={diskdbInstances}
            usage={capacityUsage}
            hardwareCapacity={hardwareCapacity}
            scanStatus={capacityScanStatus}
            loading={capLoading}
            readonly={readonly}
            onRefresh={refreshCapacity}
            selectedEntity={selectedEntity}
            racks={racks}
            nodes={nodes}
            servers={servers}
            stores={stores}
            nodeStores={nodeStores}
            nodeHealthById={nodeHealthById}
            diskdbNodeIds={diskdbNodeIds}
            nodeDiskGroups={nodeDiskGroups}
            refreshToken={lastRefreshTime.getTime()}
            focusRequest={canvasFocusRequest}
            onEntityContextMenu={onCanvasContextMenu}
          />
        )}
      </main>

      <Suspense fallback={null}>
        <Inspector readonly={readonly} modules={modules} nodes={nodes} racks={racks} servers={servers} stores={stores} capacityUsage={capacityUsage} hardwareCapacity={hardwareCapacity} diskdbInstances={diskdbInstances} width={inspectorWidth} pendingSelectionRef={pendingSelectionRef} />
      </Suspense>

      {selectedEntity && (
        <div
          className="tw-fixed tw-top-14 tw-bottom-0 tw-z-30 tw-w-2 tw-cursor-col-resize hover:tw-bg-accent/20"
          style={{ right: inspectorWidth - 1 }}
          onMouseDown={() => setResizing('right')}
          aria-hidden="true"
        />
      )}

      {menuState && <ContextMenu items={menuState.items} position={menuState.position} onClose={closeMenu} />}

      {/* Dialogs */}
      <AddRackDialog
        isOpen={!!dialog.addRack}
        onClose={closeDialogs}
        existingRackIds={rackIds.map(String)}
        onSuccess={handleRefresh}
      />
      {dialog.addNode && (
        <AddNodeDialog
          isOpen
          onClose={closeDialogs}
          racks={racks}
          defaultRackId={String(defaultAddNodeRackId)}
          existingNodeIds={nodeIds.map(String)}
          defaultRestPort={addNodeDeployDefaults.defaultRestPort}
          defaultRpcPort={addNodeDeployDefaults.defaultRpcPort}
          defaultDiskdbRpcPort={addNodeDeployDefaults.defaultDiskdbRpcPort}
          onCreatedRackId={(rackId) => setLastUsedRackId(Number(rackId))}
          onDiskdbPortReserved={(port) => setRememberedDeployPorts((prev) => ({
            ...prev,
            diskdbRpc: prev.diskdbRpc.includes(port) ? prev.diskdbRpc : [...prev.diskdbRpc, port],
          }))}
          onSuccess={handleRefresh}
        />
      )}
      <InitClusterDialog
        isOpen={!!dialog.initCluster}
        onClose={closeDialogs}
        nodes={nodes}
        servers={servers}
        defaultNodeIds={storeDialogDefaults.nodeIds}
        onSuccess={handleInitSuccess}
      />
      <AddStoreDialog
        isOpen={!!dialog.addStore}
        onClose={closeDialogs}
        nodes={nodes}
        servers={servers}
        defaultStoreId={storeDialogDefaults.storeId}
        defaultNodeIds={storeDialogDefaults.nodeIds}
        onSuccess={handleRefresh}
      />
      {dialog.addGroup && (
        <AddGroupDialog
          isOpen
          onClose={closeDialogs}
          storeId={dialog.addGroup.storeId}
          stores={stores}
          nodes={nodes}
          servers={servers}
          defaultGroupId={groupDialogDefaults[dialog.addGroup.storeId]?.groupId || '1'}
          defaultReplicaId={groupDialogDefaults[dialog.addGroup.storeId]?.replicaId || '1'}
          defaultNodeIds={groupDialogDefaults[dialog.addGroup.storeId]?.nodeIds || []}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.addReplica && (
        <AddReplicaDialog
          isOpen
          onClose={closeDialogs}
          storeId={dialog.addReplica.storeId}
          groupId={dialog.addReplica.groupId}
          nodes={replicaDialogNodeInfo[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.allNodes || []}
          usedNodeIds={replicaDialogNodeInfo[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.usedNodeIds || new Set()}
          defaultNodeId={replicaDialogDefaults[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.nodeId ?? 0}
          defaultReplicaId={replicaDialogDefaults[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.replicaId || ''}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.deployServer && (
        <DeployServerDialog
          isOpen
          onClose={closeDialogs}
          nodeId={dialog.deployServer.nodeId}
          defaultRestPort={deployDialogDefaults.defaultRestPort}
          defaultRpcPort={deployDialogDefaults.defaultRpcPort}
          onSuccess={async ({ restPort, rpcPort }) => {
            setRememberedDeployPorts((prev) => ({
              mgmt: prev.mgmt.includes(restPort) ? prev.mgmt : [...prev.mgmt, restPort],
              rpc: prev.rpc.includes(rpcPort) ? prev.rpc : [...prev.rpc, rpcPort],
              diskdbRpc: prev.diskdbRpc,
            }));
            await handleRefresh();
          }}
        />
      )}
      {dialog.delete && (
        <ConfirmDeleteDialog
          isOpen
          onClose={closeDialogs}
          resourceType={dialog.delete.type}
          resourceId={String(dialog.delete.id)}
          onDelete={dialog.delete.onDelete}
          cascadeWarning={dialog.delete.cascadeWarning}
        />
      )}
      {dialog.addDiskGroup && (
        <AddDiskGroupDialog
          isOpen
          onClose={closeDialogs}
          nodeId={dialog.addDiskGroup.nodeId}
          existingDgIds={existingDiskGroupIds}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.addDisk && (
        <AddDiskDialog
          isOpen
          onClose={closeDialogs}
          nodeId={dialog.addDisk.nodeId}
          dgId={dialog.addDisk.dgId}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.assignDiskGroup && (
        <AssignDiskGroupDialog
          isOpen
          onClose={closeDialogs}
          rackId={dialog.assignDiskGroup.rackId}
          nodeId={dialog.assignDiskGroup.nodeId}
          dgId={dialog.assignDiskGroup.dgId}
          dgName={dialog.assignDiskGroup.dgName}
          instances={diskdbInstances}
          stores={stores}
          onSuccess={handleRefresh}
        />
      )}
      <DeployDiskdbDialog
        isOpen={!!dialog.deployDiskdb}
        onClose={closeDialogs}
        nodes={nodes}
        defaultNodeId={dialog.deployDiskdb?.nodeId}
        defaultRpcPort={deployDiskdbDefaults}
        onSuccess={async () => {
          setRememberedDeployPorts((prev) => ({
            mgmt: prev.mgmt,
            rpc: prev.rpc,
            diskdbRpc: prev.diskdbRpc.includes(Number(deployDiskdbDefaults))
              ? prev.diskdbRpc
              : [...prev.diskdbRpc, Number(deployDiskdbDefaults)],
          }));
          await handleRefresh();
        }}
      />

      {dialog.compactZones && (
        <ZoneSelectDialog
          isOpen
          onClose={closeDialogs}
          title="Compact Zones"
          description={`Compact zones on disk ${dialog.compactZones.diskId.slice(0, 12)}…`}
          confirmLabel="Compact"
          diskId={dialog.compactZones.diskId}
          zoneCount={dialog.compactZones.zoneCount}
          onConfirm={async (diskId, zones) => {
            await compactDiskdbZones(diskId, zones ?? undefined);
            await handleRefresh();
          }}
        />
      )}
      {dialog.rebuildBitmap && (
        <ZoneSelectDialog
          isOpen
          onClose={closeDialogs}
          title="Rebuild Bitmap"
          description={`Rebuild zone bitmap on disk ${dialog.rebuildBitmap.diskId.slice(0, 12)}…`}
          confirmLabel="Rebuild"
          diskId={dialog.rebuildBitmap.diskId}
          zoneCount={dialog.rebuildBitmap.zoneCount}
          onConfirm={async (diskId, zones) => {
            await rebuildDiskdbZoneBitmap(diskId, zones ?? undefined);
            await handleRefresh();
          }}
        />
      )}

      <ToastContainer />
    </div>
  );
}

export default function App(props: CrowdbConsoleProps = {}) {
  return (
    <DomainProvider initialDomain={props.initialDomain}>
      <SelectionProvider>
        <ToastProvider>
          <ActivityProvider>
            <AppContent {...props} />
          </ActivityProvider>
        </ToastProvider>
      </SelectionProvider>
    </DomainProvider>
  );
}
