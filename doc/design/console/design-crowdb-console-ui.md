<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Console Web UI

Depends on: [`design-crowdb-console.md`](design-crowdb-console.md), [`../kv/design-crowdb-kv.md`](../kv/design-crowdb-kv.md) §15.4.6
Satisfies: [`../kv/design-crowdb-kv.md`](../kv/design-crowdb-kv.md) §15.4.6

This document covers the **frontend SPA design decisions only**:
what we chose and why. Requirements (the *what*) live in
`../kv/design-crowdb-kv.md`; backend API contracts live in `design-crowdb-console.md`.

## Table of Contents

- [1. Goals (recap)](#1-goals-recap)
- [2. Stack decisions](#2-stack-decisions)
- [3. Information Architecture](#3-information-architecture)
  - [3.1 Selection & cross-jump](#31-selection--cross-jump)
- [4. Visual Language](#4-visual-language)
- [5. Topology Canvas (React Flow, slim)](#5-topology-canvas-react-flow-slim)
  - [5.1 Physical layout](#51-physical-layout)
  - [5.2 Logical layout](#52-logical-layout)
  - [5.3 Interactions](#53-interactions)
- [6. Inspector Panel](#6-inspector-panel)
- [6.1 KV Operator Panel (center panel)](#61-kv-operator-panel-center-panel)
- [7. Embedding Contract](#7-embedding-contract)
- [8. Data & Polling Strategy](#8-data--polling-strategy)
- [9. Module Layout](#9-module-layout)
- [10. Accessibility](#10-accessibility)
- [11. Testing](#11-testing)
- [12. Domain Restructure: Cluster / KV / Chunk](#12-domain-restructure-cluster--kv--chunk)
  - [12.1 Three domains](#121-three-domains)
  - [12.2 Why three domains (not view-modes)](#122-why-three-domains-not-view-modes)
  - [12.3 Swagger UI removal](#123-swagger-ui-removal)
  - [12.4 Batch Add Disk](#124-batch-add-disk)
- [13. DiskDB Server Deploy / Restart / Stop](#13-diskdb-server-deploy--restart--stop)
- [14. REST Proxy for DiskDB Runtime](#14-rest-proxy-for-diskdb-runtime)
- [15. Capacity Panel (Canvas Visualization)](#15-capacity-panel-canvas-visualization)
  - [15.1 Rendering](#151-rendering)
  - [15.2 Color encoding](#152-color-encoding)
  - [15.3 Polling](#153-polling)
  - [15.4 Scope dispatch and module structure](#154-scope-dispatch-and-module-structure)
- [16. Console-Shared DiskDB Client + CLI](#16-console-shared-diskdb-client--cli)
  - [16.1 Console-shared client](#161-console-shared-client)
  - [16.2 CLI subcommands](#162-cli-subcommands)

## 1. Goals (recap)

- Single page, no full-page navigation.
- Three first-class **domains** (Cluster / KV / Chunk) that drive the
  sidebar tree, the topology canvas, and the inspector together. The
  domain split aligns the web UI with the CLI's four-domain structure
  (Cluster / KV / Chunk / Bench — Bench is CLI-only).
- Full operator surface: rack/node/server lifecycle, store/group/replica
  CRUD, KV data plane, disk-group/disk lifecycle, capacity
  visualization.
- Offline-capable: no third-party CDN at runtime.
- Lean: minimal dependencies, no feature the requirement does not mandate.

## 2. Stack decisions

- **React + TypeScript + Vite + TailwindCSS** — carried over from the
  existing codebase; no framework migration.
- **React Flow for topology** — slim usage only (custom nodes, pan, click
  select). Deliberately no minimap, zoom toolbar, layout selector, or edge
  labels. The canvas is a navigation aid, not an analytics surface.
- **React Context for state** — domain, selection, toasts, activity.
  No Redux; the state surface is small enough that Context + local hooks
  suffice.
- **No client-side routing** — the SPA mounts at the document root;
  intra-SPA navigation is selection state, not URL navigation. This keeps
  embedding trivial (no history API conflicts).
- **Removed dependencies**: `recharts`, `jspdf`, `jspdf-autotable`,
  `uuid`, `react-router-dom` — none are needed for the lean v1 surface.

## 3. Information Architecture

A fixed three-pane shell. A single root-level **domain** (Cluster / KV
/ Chunk) selects which hierarchy every pane renders. The domain
replaces the former view-mode (Physical / Capacity / KV) and aligns
the web UI with the CLI's domain structure. The per-domain sidebar +
center panel layouts are detailed in §12.1.

```
┌─ Header ───────────────────────────────────────────────────────────┐
│ brand · health pill · domain toggle (Cluster/KV/Capacity) · refresh│
├─ Sidebar ─────┬─ Center panel ─────────────┬─ Inspector ────────────┤
│ (per-domain   │ (per-domain layout,        │ Details (key/value)    │
│  tree, see    │  see §12.1)                │ Activity (recent ops)  │
│  §12.1)       │                            │                        │
│               │                            │ (unchanged — scoped    │
│               │                            │  to current selection) │
└───────────────┴────────────────────────────┴────────────────────────┘
```

- **Header** (~56px): brand label, cluster health pill, domain toggle
  (Cluster / KV / Chunk), last-refresh time, manual refresh button.
- **Sidebar** (~240px): a text filter plus the hierarchy tree for the
  active domain. Click selects; right-click opens the per-layer context
  menu. No favorites, no recent, no saved presets.
- **Center panel**: content depends on the active domain — Cluster
  renders a hardware topology canvas; KV renders a tabbed logical
  canvas + KV operator panel; Chunk renders a capacity visualization
  panel. See §12.1 for the per-domain layouts.
- **Inspector** (~320px, collapsible): tabs scoped to the selection:
  Details and Activity only. KV operations have moved to the center KV
  Operator panel (§6.1).

Selection is held in one `SelectionContext`. The shell is rendered
once; switching domain swaps the tree data and the center panel only.

### 3.1 Selection & cross-jump

Selection is `{ type, id, parentIds }` where `type ∈ { Rack, Node,
Server, Store, Group, Replica, DiskGroup, Disk }`. Clicking any tree
row or canvas node sets it.

Cross-jump (one click) is supported for the common case only:
- KV `Replica` → "Show on node": switch to Cluster, expand the owning
  `Node`, select the matching server entry.
- Cluster `Server`/`Group` → "Show in KV": switch to KV, expand the
  owning `Store → Group`, select the unified row.

No navigation stack / back button in v1.

## 4. Visual Language

Single dark theme via CSS variables under `.crowdb-console` (existing
tokens in `src/index.css`). Status colors: `--healthy`, `--degraded`,
`--failed`, `--unknown`, plus `--remote` for remote-replica accent.

Status is never color-only. Every status row also carries a glyph
(✓ / ! / ✕ / ?). Leader replicas carry a crown badge. Remote replicas use
a dashed border + `--remote` accent so peer-list mis-wirings are visible.

Animations are minimal (selection/hover transitions); honor
`prefers-reduced-motion`.

## 5. Topology Canvas (React Flow, slim)

One layout at a time, chosen by domain. Layout is computed by a small
deterministic tree-layout pass in `topology/layout.ts` (columns by depth,
rows by sibling index). No dagre, no force simulation, no user-selectable
layouts.

### 5.1 Physical layout

Renders `Rack → Node → Server → PxStore → PxGroup → {Local, Remote…}`
read from the physical tree. Node types: `Rack`, `Node`, `Server`,
`PxStore`, `PxGroup`, `LocalReplica`, `RemoteReplica`. Edges follow
parent→child containment. Each `RemoteReplica` draws a solid edge to its
peer `LocalReplica` (a missing edge is the bug this view surfaces). The
leader radiates accent edges to followers.

### 5.2 Logical layout

Renders `Cluster → Store → Group → Replica…`. Node types: `Cluster`,
`Store`, `Group`, `Replica` (with a `node_id` badge). The leader radiates
accent edges to followers; no local/remote distinction.

### 5.3 Interactions

- Drag pans, wheel zooms (React Flow built-ins), click selects.
- Selecting a node drives the inspector and highlights the sidebar row.
- Right-click a node opens the same per-layer context menu as the tree.
- Tooltips on hover surface one useful fact (host, leader id, reachable).
- No minimap, zoom toolbar, search box, focus mode, export, or edge
  labels.

## 6. Inspector Panel

Tabs re-render against the current selection:

1. **Details** — labelled key/value table from the selected entity
   (physical or logical shape). Long values support copy-to-clipboard. A
   footer row shows the cross-jump link (§3.1).
2. **Activity** — chronological client-side list of UI-issued operations
   (timestamp, action, target, outcome). No filter/export in v1.

The KV tab has been removed from the Inspector. All KV operations now
live in the center KV Operator panel (§6.1), which provides a full-width
surface with store/group selectors, scan results, and an action bar.

## 6.1 KV Operator Panel (center panel)

A full-width center panel for KV data-plane operations, toggled from the
header via a "KV" button (mutually exclusive with the topology canvas).
Replaces the former Inspector KV tab, which was too cramped at
320px for comfortable key browsing.

**Design choices:**

- **Flat single-page layout (no tabs)** — action bar on top, scan
  results below. The user can scan, see results, and act (put/get/delete)
  without switching tabs.
- **Store/group selector with "All Groups" option** — when selected,
  scan iterates over every group and merges results (labeled by group).
  Demo inject randomly distributes keys across groups. This avoids
  forcing the user to pick a group when they want a store-wide view.
- **Auto-scan on first load** — when store and group are both set, the
  panel triggers a scan automatically so the user sees data immediately.
- **Independent of domain** — KV operations are always logical
  (store/group), regardless of which domain's canvas is active.

**Scan pagination (`start_after` token):**

The scan API returns at most `limit` items with a `truncated` flag but
had no way to fetch the next page. Rather than adding a total count
(expensive on large keyspaces), we adopted an S3 ListObjectsV2-style
`start_after` cursor: the caller passes the last key from the previous
batch; the engine returns keys strictly greater than `start_after` that
still match the prefix. The UI shows a "Load more" button when
`truncated` is true; clicking it appends the next batch.

**Decision — `CrowdbTreeEngine` over-fetch + filter:** The C++ crowdb-tree
scan API takes only prefix + limit (no `start_after`). Rather than
modifying C++ immediately, `CrowdbTreeEngine` over-fetches with the
original prefix, then filters out keys ≤ `start_after` in Rust before
applying the limit. This is inefficient when `start_after` is deep into
a large prefix range. A follow-up can push `start_after` into the C++
engine. When `start_after` is empty, the fast path is identical to the
old behavior.

**Demo delete at scale:** "Delete all demo" scans for `demo_` prefix
with pagination (up to 1000 keys for the confirmation count), then
deletes with 16-way parallel `kvDelete`. If more than 1000 keys exist,
scan+delete continues in batches after confirmation. The confirmation
dialog shows "1000+" when the count may be higher.

## 7. Embedding Contract

The SPA is mountable as a sub-component with a minimal props interface
(`apiPrefix`, `basePath`, `readonly`, `modules` opt-out, `initialDomain`,
`onEvent` callback). Three isolation rules:

- **Style isolation** — everything wraps in `.crowdb-console`; Tailwind
  uses the `tw-` prefix and `important: '.crowdb-console'`.
- **API isolation** — every fetch resolves against `apiPrefix`.
- **Standalone** — `index.html` mounts at the document root with defaults;
  `embed.ts` exports the component for hosts.

The `initialDomain` prop (values: `Cluster | KV | Chunk`) replaces the
former `initialViewMode`. The `modules` opt-out keys are
`'racks' | 'nodes' | 'stores' | 'groups' | 'replicas' | 'kv' |
'activity'` — the former `'swagger'` key is removed (Swagger UI is no
longer embedded). The former `initialNodeId` prop is removed.

## 8. Data & Polling Strategy

- **Two-tree contract** — the SPA speaks physical (`/api/racks`,
  `/api/nodes`) and logical (`/api/stores`) trees per `design-crowdb-console.md`.
  No panel constructs raw `host:port` URLs; `api.ts` is the single URL
  builder.
- **Asymmetric polling** — only the active view polls fast (~5s); the
  inactive view polls slow (~30s) so toggling renders immediately.
  Polling pauses while the tab is hidden.
- **Optimistic-free mutations** — mutations call the backend, await
  success, then trigger a refresh of the affected view; they do not
  hand-edit cached data. This trades a round-trip for correctness
  simplicity.

## 9. Module Layout

The source tree follows the pane structure: `shell/` (Header, Sidebar,
Inspector), `topology/` (canvas + layout), `panels/` (KvOperatorPanel,
ActivityLog), `components/` (Dialog, ContextMenu, dialogs,
UI primitives), and `contexts/` (Domain, Selection, Toast, Activity).
`api.ts` and `types/index.ts` are the single URL-builder and data-model
modules respectively.

**Deleted from v1**: CommandPalette, favorites, fuzzy search, export
utils, bulk action dialog, metrics history, theme context. None are
needed for the lean surface.

## 10. Accessibility

- Keyboard reachable: Tab/Enter/Escape on tree rows, dialogs, and menus;
  context menus mirror to keyboard-activatable buttons where practical.
- Color is never the sole status channel (glyph + color).
- Strings go through a single `t(key)` helper (English only) so a future
  locale pack needs no source changes. (Optional for v1; may inline.)

## 11. Testing

- Existing Vitest unit tests for dialog request bodies and `listRacks`
  envelope handling are **retained** (they pin the backend contract).
- The Playwright real-backend E2E suite (`app/crowdb-web/ui/e2e/`)
  targets this lean SPA; selectors track the rewritten DOM. The full
  chain rack→node→deploy→store→group→replica→KV is the acceptance bar.
- The web server's test mode keeps spawned DiskDB heartbeat and group-0
  sync intervals at one second. Normal deployments retain DiskDB's
  production defaults.
- Before group-0-backed tree reads, test mode validates the locally managed
  group-0 process and refreshes its topology. Production uses monitor-cache
  availability because group-0 may be hosted remotely and therefore has no
  locally tracked process.

---

## 12. Domain Restructure: Cluster / KV / Chunk

The header toggle evolved from two view-modes (Physical | Logical) to
three view-modes (Physical | Capacity | KV) and then to three
**domains** (Cluster / KV / Chunk). The domain split aligns the web
UI with the CLI's four-domain structure (Cluster / KV / Chunk / Bench
— Bench is CLI-only). Each domain owns a single responsibility and a
self-contained sidebar + center panel.

### 12.1 Three domains

The shell has one shared frame: a top bar, a left tree panel, a center panel, and a right properties panel. The header's domain toggle switches between Cluster, KV, and Capacity. `Capacity` is the user-facing name of the internal Chunk domain; `bench` is CLI-only. Each domain owns the content and behavior of its three panels; shared shell components provide layout, selection, context menus, loading states, and the inspector. Changing domains clears the selection so the right properties panel never shows an item from an inactive domain.

```
┌─ Header ───────────────────────────────────────────────────────────┐
│ brand · health pill · domain toggle (Cluster/KV/Capacity) · refresh│
├─ Sidebar ─────┬─ Center panel ─────────────┬─ Inspector ────────────┤
│ (per-domain   │ (per-domain layout,        │ Details (key/value)    │
│  tree, see    │  see below)                │ Activity (recent ops)  │
│  below)       │                            │                        │
│               │                            │ (unchanged — scoped    │
│               │                            │  to current selection) │
└───────────────┴────────────────────────────┴────────────────────────┘
```

**Domain 1 — Cluster (physical infrastructure)**

```
┌─ Sidebar ─────┐┌─ Center: hierarchy chart ─────────────────────────┐
│ ▾ Rack 1      ││  Rack 1                                           │
│   ▾ Node 1    ││   ├─ Node 1                                       │
│     ▾ DG-0    ││   │   └─ ┌─────────────────────────────┐          │
│       • disk0 ││   │       │ DG-0                       │          │
│       • disk1 ││   │       │  ⬢ a3f1  ⬢ b7c2  ⬢ e9d4   │          │
│   ▸ Node 2    ││   │       └─────────────────────────────┘          │
│ ▾ Rack 2      ││   └─ Node 2 ...                                   │
│   ▸ Node 3    ││  Rack 2 ...                                       │
└───────────────┘└───────────────────────────────────────────────────┘
```

The Cluster tree is the physical source for node and service lifecycle.
Disk groups assigned to a DiskDB instance appear beneath that owning DDB
service rather than directly beneath the node; unassigned disk groups are
not projected in Cluster. The center mirrors this ownership hierarchy and
renders each disk group as one compact card with its disks stacked inside.
The properties panel displays the selected physical item and recent
activity.

Context menus: Rack (Add Node, Delete Rack) · Node (Deploy KV Server,
Deploy DiskDB, Ping, Delete Node) · KV Server and DiskDB (Restart, Stop,
Delete) · DiskGroup (Add Disk batch, Remove, Set Status) · Disk (Remove,
Set Status). Adding disk groups is available only in Capacity; shared
disk-management actions do not duplicate their business logic.
Cluster-level ops (init / reset / clean) are triggered from the header
or a toolbar above the canvas.

**Domain 2 — KV (logical data operations)**

```
┌─ Sidebar ─────┐┌─ Center: [Cluster] [KV] ──────────────────────────┐
│ ▾ Rack 1      ││  Store 1                                          │
│   ▾ Node 1    ││   ├─ Group 1                                      │
│     ▸ kv-srv  ││   │   ├─ Replica 0 (node 1)                       │
│   ▸ Node 2    ││   │   └─ Replica 1 (node 2)                       │
│ ▾ Rack 2      ││   └─ Group 2 ...                                  │
│   ▸ Node 3    ││  Store 2 ...                                      │
│               ││  (Cluster tab shown — click [KV] for operator)    │
└───────────────┘└───────────────────────────────────────────────────┘

KV tab active:
┌─ Sidebar ─────┐┌─ Center: [Cluster] [KV] ──────────────────────────┐
│ ▾ Rack 1      ││  KV Operator: [store ▾] [group ▾]                 │
│   ▾ Node 1    ││  key: [_______]  value: [_______]  [Put]          │
│     ▸ kv-srv  ││  key: [_______]  [Get]  [Delete]                  │
│   ▸ Node 2    ││  scan: [prefix___] [Scan]  results: ...           │
│ ▾ Rack 2      ││                                                   │
│   ▸ Node 3    ││                                                   │
└───────────────┘└───────────────────────────────────────────────────┘
```

The KV left tree is the single logical source: datacenter → store →
group → replica. Logical entities are not repeated under physical KV
servers. The center always renders the KV operation panel. It supports
store/group selection, get, put, delete, scan, pagination, and operation
feedback. The properties panel displays details and activity for the
selected tree item. Node placement is replica metadata and a cross-jump
target, not a KV-tree parent.

Context menus: Store (Add Group, Delete) · Group (Add Replica, Delete) ·
Replica (Delete). KV server lifecycle actions are owned by Cluster and
are not duplicated in the KV tree.

**Domain 3 — Capacity (internal Chunk domain; chunkdb / diskdb / diskio management)**

```
┌─ Sidebar ─────┐┌─ Center: [Capacity] [Chunk] ──────────────────────┐
│ ▾ Rack 1      ││  (Capacity sub-view shown by default)              │
│   ▾ Node 1    ││                                                   │
│     ▸ chunkdb ││  Rack 1 / Node 1 / DG-0                           │
│     ▸ diskdb  ││   ┌─────────────────────────────────────┐         │
│       ▸ DG-0  ││   │ ▓▓▓▓▓░░░░  ▓▓▓░░░░░░  ▓▓▓▓▓▓░░    │         │
│         • d0  ││   │ disk a3f1   disk b7c2   disk e9d4   │         │
│         • d1  ││   │ 78% used    42% used    88% used    │         │
│     ▸ diskio  ││   └─────────────────────────────────────┘         │
│   ▸ Node 2    ││                                                   │
│ ▾ Rack 2      ││  Rack 1 / Node 2 / DG-1 ...                       │
│   ▸ Node 3    ││                                                   │
└───────────────┘└───────────────────────────────────────────────────┘
```

The Capacity tree keeps the physical node → disk-group → disk hierarchy
and shows DiskDB as an additional node item. It is the only domain that
allows adding disk groups. Disk-group and disk dialogs are shared with
the Cluster ownership projection, while Capacity owns creation and full
physical disk management.

The center capacity panel shows usage, busy/free space, scanner and
recalculation controls for the selected resource. The properties panel
shows the selected node, DiskDB, disk-group, or disk details and recent
activity. Future chunk-management operations may extend the center
panel without changing the shared shell.

`DomainContext` holds the active domain. Selection is shared across all
three domains via `SelectionContext`, while each domain defines how the
selected item is resolved and displayed. Switching domains changes the
tree and center panel without creating duplicate logical entities. The
inspector remains the right properties panel for the active selection.

### 12.2 Why three domains (not view-modes)

The former view-mode split (Physical / Capacity / KV) mixed
infrastructure management (Physical) with disk lifecycle (Capacity)
and KV operations (KV), but the CLI had already moved to a cleaner
domain split (Cluster / KV / Chunk / Bench). The domain restructure
unifies the two frontends:

- **Cluster** merges the former Physical + Capacity views — hardware
  topology and disk lifecycle are both infrastructure concerns and
  belong together. The Capacity center panel moves under the Chunk
  domain (capacity is a property of the chunk/disk storage layer).
- **KV** keeps the logical KV layer and data-plane. The KV tree is
  independent of physical server placement; KV server lifecycle belongs
  to the Cluster domain.
- **Chunk** is new — it hosts the chunk/disk storage layer (diskdb,
  chunkdb, diskio) and the capacity visualization that belongs to
  that layer. The Chunk center panel is the capacity panel today.
  Future chunk-management features may extend this center panel
  (`ops::chunk` is currently stubs).

### 12.3 Swagger UI removal

The former Swagger API panel (embedded OpenAPI browser + per-node
openapi.json proxy) is removed. The OpenAPI document remains served
by `crowdb-kv-server` at `/openapi.json` for direct access; the
console no longer embeds or proxies it. The `'swagger'` module opt-out
key and `initialNodeId` prop are removed from the embedding contract.

### 12.4 Batch Add Disk

A batch endpoint for atomic all-or-nothing disk creation (unchanged
from the former Capacity view, now accessed from the Cluster domain):

- Validates all `disk_id` formats upfront; rejects the whole batch if
  any is malformed (atomic).
- Writes all disks to config + group-0 sysdata in one transaction;
  if any write fails, rolls back (no partial success).

## 13. DiskDB Server Deploy / Restart / Stop

The Cluster domain owns service lifecycle actions for both KV Server and
DiskDB Server. The Chunk domain displays the DiskDB item but does not
own a second lifecycle workflow. The deploy/restart/stop handlers enable
`AddNodeDialog` to auto-deploy DiskDB alongside KV, and the service
context menu works for both types.

Deployment mechanism: SSH or local fork, same as KV. No Docker. The
`crowdb-diskdb` binary is spawned via `ssh::deploy_via_ssh` or
`lifecycle::deploy_local_in_dir`. A DiskDB deployment receives one
user-facing service endpoint port; the internal HTTP health listener and
any other required listener ports are reserved by the lifecycle layer and
are not exposed as DiskDB management properties.

New handlers mirroring the KV handlers:

```rust
pub struct DeployDiskdbBody {
    endpoint_port: u16,
}

pub async fn http_deploy_node_diskdb(
    State(state), Path(node_id), Json(body),
) -> Result<(StatusCode, Json<DeployResult>), ...>

pub async fn http_restart_node_diskdb(
    State(state), Path(node_id),
) -> Result<Json<DeployResult>, ...>

pub async fn http_stop_node_diskdb(
    State(state), Path(node_id),
) -> Result<Json<StopResult>, ...>
```

- `http_deploy_node_diskdb` — checks no existing DiskDB on the node
  (409 if present), resolves the node, derives the internal listener
  ports from the single `endpoint_port`, spawns via SSH or local fork,
  persists a DiskDB service entry, and records the pid. Route:
  `POST /api/nodes/:id/diskdb/deploy`.
- `http_restart_node_diskdb` — stops the tracked pid and re-deploys on
  the persisted endpoint port. Route:
  `POST /api/nodes/:id/diskdb/restart`.
- `http_stop_node_diskdb` — stops the tracked pid, clears it, and keeps
  the entry. Route: `POST /api/nodes/:id/diskdb/stop`.
- KV and DiskDB use distinct service types and public endpoint models.
  KV includes its HTTP management URL; DiskDB includes its service
  endpoint, health, and process state but not its internal HTTP health
  URL. Runtime PID tracking is keyed by `(node_id, service_type)`.
- `AddNodeDialog` calls `deployServer` (KV) then `deployDiskdb` (new
  API function) after `addNode` succeeds. Both are gated by the
  existing `enableCrowDB`-style checkbox (add `enableDiskDB`, default
  true).

Edge cases:
- Node with KV deployed but DiskDB deploy fails → KV stays deployed;
  the dialog reports the DiskDB failure; the operator can retry via
  the Server context menu's Deploy.
- DiskDB binary not found on the remote host → SSH deploy returns an
  error; surfaced as 502.
- Port conflict (another process on 9941/9942) → spawn fails; surfaced
  as 502. The handler does not pre-check ports (best-effort, matches
  KV behavior).

## 14. REST Proxy for DiskDB Runtime

`crowdb-web` proxies diskdb runtime RPCs (`QueryCapacityStats` drill-down,
scan, recalc, compact, rebuild) via REST endpoints under
`/api/diskdb/`. The CLI and web UI route through `crowdb-web` (no direct
crowdb-rpc from the browser or CLI). `AppState` owns a `DiskdbClient` built
from the same `ServiceRegistryClient` the console already uses:

```rust
diskdb_client: tokio::sync::RwLock<Option<DiskdbClient>>,
```

The `DiskdbClient` is lazily initialized on first diskdb REST request
(the service registry may not be ready at console startup).

Handlers:

- `GET /api/diskdb/instances` — reads live instances from the service
  registry and merges `owned_dg_ids` from the authoritative group-0
  ownership map (no crowdb-rpc fan-out). Returns instance id, endpoint,
  `last_heartbeat_ms`, current ownership, and keepalive `group_usages`.
- `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>&zone=<zi>` —
  `QueryCapacityStats` drill-down (all params optional). When `dg` is
  omitted, iterate all registered instances and merge the responses
  for cluster-wide totals. `DiskdbClient.query_capacity_stats(0)`
  routes to one instance only, so the merge lives in this handler.
- `GET /api/diskdb/scan-status?dg=<id>` — `get_scan_status`.
- `POST /api/diskdb/scan` — `trigger_scan` (optional `dg` in body).
- `POST /api/diskdb/recalc` — `recalc_disk_usage` (optional `dg`).
- `POST /api/diskdb/compact` — `compact_zone` (disk_id + optional
  zone_indices; empty = all zones).
- `POST /api/diskdb/rebuild` — `rebuild_zone_bitmap` (disk_id +
  optional zone_index; absent = all zones — handler loops over the
  disk's zones if zone_index is absent).
- `PUT /api/disks/:disk_id/status` — set a disk's `HwStatus` via
  `HardwareClient.set_disk_status`. Needed by the Set-Status dialog;
  no such endpoint existed before (only add/remove/move).

`GET /api/diskdb/usage` with no `dg` iterates
`read_all_diskdb_instances`, calls `query_capacity_stats` per
instance, merges `DiskGroupInfo` entries by id (summing
capacity/busy/free). A dead instance yields a degraded indicator,
not a failed page. Its contribution is skipped with a warning.

`PUT /api/disks/:disk_id/status` resolves the disk's rack/node/dg
from config, then calls `hw.set_disk_status`. 404 if the disk is
not in config.

Edge cases:
- Cluster overview with a dead instance → merged response excludes
  it; the `/instances` endpoint still lists it (with stale heartbeat)
  so the UI can show the degraded card.
- Zone drill-down → bitmap is omitted at disk level (flatbuffer contract);
  the UI issues the zone-level query separately.
- Scan already running → `trigger_scan` returns `scan_in_progress:
  true`; handler passes it through (no error).

## 15. Capacity Panel (Canvas Visualization)

The Chunk domain's Capacity sub-view renders capacity visualization
that scales to thousands of zones per disk and tens of thousands of
blocks per zone. Canvas with offscreen double-buffering handles 84×84
zone grids and 181×181 bitmap grids without flicker. DOM/SVG
rendering at that scale causes layout thrash and jank.

`CapacityPanel.tsx` renders when `domain === Chunk` and the Capacity
sub-view is active. The panel
content depends on the selected entity (from `SelectionContext`):

- **Cluster (Datacenter or no selection)** — per-rack breakdown. One
  row per rack with DG count, node count, and a capacity/busy/free
  bar. The cluster-wide scan status summary + trigger
  (`ScannerPanel`) renders here only. Data from
  `GET /api/diskdb/usage` (cluster merge).
- **Rack selected** — per-node breakdown within the rack. One row per
  node with DG count and a capacity/busy/free bar. Data from
  `GET /api/diskdb/usage` (cluster merge, client-filtered).
- **Node selected** — per-DG breakdown. One row per DG on the node
  with disk count (array icon + count, not per-disk boxes) and a
  capacity/busy/free bar. Data from `GET /api/diskdb/usage` (cluster
  merge, client-filtered).
- **DiskGroup selected** — per-disk boxes. Each disk is a box with a
  busy% gradient fill (green → amber → red, red = busy) + inline `%`
  label + tooltip (disk id + busy%). Data from
  `GET /api/diskdb/usage?dg=<id>`.
- **Disk selected** — zone grid + per-disk actions. Each zone is a
  box in a square grid (side = ceil(sqrt(zone_count))) with a
  green→amber→red gradient based on busy%. Hover shows a tooltip
  with zone id + usage %. A "jump to zone #" input handles direct
  navigation (7000 zones cannot be a dropdown). All disk-scoped
  actions are inline in the disk header: Scan and Recalc target the
  disk's parent DG (`triggerDiskdbScan` / `recalcDiskdbUsage` with
  the DG id); Compact, Rebuild, Up, and Down target the disk itself
  (`compactDiskdbZones` / `rebuildDiskdbZoneBitmap` /
  `setDiskStatus`). The per-DG recalc result (`RecalcPanel`) renders
  here, scoped to the parent DG. Data from
  `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>` (brief per-zone
  entries, no bitmap).
- **Zone selected (in-panel, within the Disk view)** — zone bitmap.
  Canvas grid of the zone's `usage_bitmap`
  (side = ceil(sqrt(unit_count))). Busy block = red filled cell, free
  block = green filled cell. Zone is not a sidebar entity; it is an
  in-panel click state inside the Disk view. Data from
  `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>&zone=<zi>` (full
  bitmap, on-demand only).

### 15.1 Rendering

Canvas, not SVG/DOM, for all levels:
- Offscreen canvas double-buffering: draw to an offscreen canvas,
  then `drawImage` blit to the visible canvas in one call. The
  visible canvas is never cleared-then-slowly-drawn (that flickers).
- Single `requestAnimationFrame` sync redraw for grids up to 181×181
  (32K cells) — fast enough to not flicker.
- On data refresh (3 s poll), retain the previous frame until the
  new one is fully drawn, then swap. No blank intermediate state.
- No DOM reflow. The canvas is a single element; only its bitmap
  content changes.

### 15.2 Color encoding

Green (free) → amber → red (busy):
- Zone/disk boxes: gradient fill based on `busy_blocks /
  unit_capacity` ratio. 0% = green, ~50% = amber, 100% = red.
- Bitmap cells: binary — busy = red filled, free = green filled.
- Redundant encoding: each zone/disk box shows a `%` text label on
  hover (zone id + usage %) or inline so the information is not
  color-only (color-blind friendly).

### 15.3 Polling

3 s refresh of the currently focused visualization:
- The poll refetches only the data for the selected entity level
  (rack/node → cluster merge; disk-group → dg query; disk → disk
  query; zone → zone query).
- On refetch, the canvas redraws via double-buffer (no flicker).
- If the selection changes, the poll target switches immediately;
  the old canvas is cleared on the next draw.

Zone count math (for layout):
- 200 TB disk / 32 GB zone = 6400 zones → 80×80 grid.
- 32 GB zone / 1 MB unit = 32K units → 181×181 grid.

Edge cases:
- Disk with 0 zones (freshly added, zone load in progress) → empty
  grid placeholder with "loading" text.
- Zone with `used_count == unit_capacity` → all cells red; reported
  as-is.
- `usage_bitmap` shorter than `unit_capacity` (last zone rounded) →
  pad with free (green) cells.
- Poll response slower than 3 s → keep previous frame; next poll
  catches up. No spinner overlay (would flicker).

### 15.4 Scope dispatch and module structure

`CapacityPanel` derives a `CapacityScope` (`Cluster | Rack | Node |
DiskGroup | Disk`) from the selected entity and renders one branch per
scope. The header (title + totals cards) is common to all scopes; only
the body branches. Each scope has a dedicated subview:

- `ClusterView` — per-rack breakdown + `ScannerPanel` (cluster-wide
  scan status + trigger).
- `RackView` — per-node breakdown.
- `NodeView` — per-DG breakdown.
- `DiskGroupView` — per-disk box grid.
- `DiskView` — zone grid (`ZoneGrid`) + zone bitmap (`ZoneBitmap`) +
  jump-to-zone input + per-disk action buttons + `RecalcPanel`
  (scoped to the parent DG).

Shared color/format utilities live in `utils/capacity.ts`:
- `busyColor(pct)` — green → amber → red gradient (4-step thresholds
  30/60/85/100), shared by `DiskGroupView` disk boxes, `ZoneGrid`,
  and the per-rack/per-node bars.
- `busyPct`, `formatBytes` — formatting helpers.

`useZoneBitmap(dg, disk, zone)` fetches the zone bitmap on demand
when a zone is clicked and caches the last result; the 3 s poll
refetches the focused zone via its `refresh` callback.

## 16. Console-Shared DiskDB Client + CLI

### 16.1 Console-shared client

`ConsoleClient` in `crowdb-console-shared` is the typed REST client used
by both the web UI (via `api.ts` wrappers) and `crowdb-cli`. It has
diskdb runtime methods + serde model types so the CLI and UI share one
deserialization path.

```rust
impl ConsoleClient {
    pub async fn list_diskdb_instances(&self) -> Result<Vec<DiskdbInstanceInfo>>
    pub async fn query_diskdb_usage(&self, dg: Option<u64>, disk: Option<String>, zone: Option<u32>) -> Result<UsageResponse>
    pub async fn get_scan_status(&self, dg: Option<u64>) -> Result<ScanSummary>
    pub async fn trigger_scan(&self, dg: Option<u64>) -> Result<ScanSummary>
    pub async fn recalc(&self, dg: Option<u64>) -> Result<RecalcResult>
    pub async fn compact(&self, disk_id: &str, zones: Option<Vec<u32>>) -> Result<CompactionResult>
    pub async fn rebuild(&self, disk_id: &str, zone: Option<u32>) -> Result<RebuildResult>
    pub async fn set_disk_status(&self, disk_id: &str, status: HwStatus) -> Result<()>
}
```

Serde model types (mirrors of the flatbuffer responses):
`DiskdbInstanceInfo`, `DiskGroupUsageSummary`, `DiskGroupUsage`,
`DiskUsage`, `ZoneUsage`, `ScanSummary`, `RecalcResult`,
`CompactionResult`, `RebuildResult`, `UsageResponse`.

### 16.2 CLI subcommands

Runtime queries (usage/zones/scan/recalc/compact/rebuild) are
reachable from the command line via `crowdb diskdb` subcommands.
Lifecycle stays in `crowdb disk` / `crowdb disk-group`; `diskdb` is
runtime queries only.

```
crowdb diskdb status                          — /api/diskdb/instances
crowdb diskdb usage [--dg <id>] [--disk <id>] [--zone <zi>]
crowdb diskdb scan [--dg <id>]                — trigger
crowdb diskdb scan-status [--dg <id>]
crowdb diskdb recalc [--dg <id>]
crowdb diskdb compact <disk_id> [--zones <zi,...>]
crowdb diskdb rebuild <disk_id> [--zone <zi>]
```

All route through `ConsoleClient` → `crowdb-web` → `DiskdbClient` →
crowdb-rpc; no direct talk to `crowdb-diskdb`.
