// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::{err_500, err_502, map_config_err, map_persist_err, ErrorBody};
use crate::expand::Recursive;
use crate::physical_view::PhysicalBuilder;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::cluster::{DiskGroupId, NodeHealth, NodeId, RackId};
use crowdb_console_shared::config::{
    DiskEntry, DiskGroupEntry, NodeEntry, RackEntry, ServerEntry, ServiceType,
};
use crowdb_console_shared::expand::RecursiveDepth;
use crowdb_console_shared::ops;
use serde::{Deserialize, Serialize};

// ── Physical tree: rack / node / server lifecycle (A3) ──────────────
//
// All handlers mutate the in-memory `ConsoleConfig` under `state.config`
// and persist via `state.persist()` before returning. Lock is held only
// for the synchronous mutation; the persist call writes a small TOML
// file (atomic rename) and runs without holding the lock.

#[derive(Debug, Deserialize)]
pub struct AddRackBody {
    id: RackId,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize)]
pub struct NodeQuery {
    /// Optional `?rack_id=<id>` filter for `GET /api/nodes`.
    #[serde(default)]
    rack_id: Option<RackId>,
}

/// List all racks.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `400` if `?recursive=` is malformed or out of range.
///
/// At `recursive=0` (or absent) returns a flat `Vec<RackEntry>`. At
/// `recursive>=1` returns a wrapper `{ items, truncated_at }` where
/// each item carries an optional `nodes` collection inflated up to the
/// requested depth (rack → node → store → group).
pub async fn http_list_racks(
    State(state): State<AppState>,
    Recursive(depth): Recursive,
) -> Json<serde_json::Value> {
    if matches!(depth, RecursiveDepth::None) {
        let cfg = state.config.read().unwrap();
        return Json(serde_json::to_value(&cfg.racks).expect("serialize racks"));
    }
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    let pids = state.kv_pid_snapshot();
    let diskdb_pids = state.diskdb_pid_snapshot();
    let mut builder = PhysicalBuilder::new_with_diskdb_pids(&cfg, &snap, &pids, &diskdb_pids);
    let limit = depth.effective();
    let racks: Vec<_> = cfg.racks.iter().map(|r| builder.build_rack(r, limit)).collect();
    let trunc = builder.into_truncation();
    Json(serde_json::json!({
        "items": racks,
        "truncated_at": trunc.paths,
    }))
}

fn has_fully_running_group0(state: &AppState) -> bool {
    let replica_nodes = {
        let cfg = state.config.read().unwrap();
        cfg.group(0, 0).map(|group| {
            group
                .replicas
                .iter()
                .map(|replica| replica.node_id)
                .collect::<Vec<_>>()
        })
    };
    replica_nodes.is_some_and(|nodes| {
        !nodes.is_empty() && nodes.iter().all(|node_id| state.runtime_pid(node_id).is_some())
    })
}

/// Add a new rack.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if rack addition or config persistence fails.
pub async fn http_add_rack(
    State(state): State<AppState>,
    Json(body): Json<AddRackBody>,
) -> Result<(StatusCode, Json<RackEntry>), (StatusCode, Json<ErrorBody>)> {
    let entry = RackEntry {
        id: body.id,
        name: body.name,
    };
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_rack(entry.clone()).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    // Avoid the full retry budget before bootstrap or during partial shutdown.
    if has_fully_running_group0(&state) {
        if let Ok(ctx) = state.op_context().await {
            let value = crowdb_protocol::common::RackValue {
                status: crowdb_protocol::common::HwStatus::Up as i32,
                node_ids: Vec::new(),
            };
            let _ = ctx.sysmd().add_rack(body.id, &value).await;
        }
    }
    Ok((StatusCode::CREATED, Json(entry)))
}

/// Remove a rack.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if rack removal or config persistence fails.
pub async fn http_remove_rack(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_rack(id).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    if has_fully_running_group0(&state) {
        if let Ok(ctx) = state.op_context().await {
            let _ = ctx.sysmd().remove_rack_cascade(id).await;
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

/// List nodes, optionally filtered by rack ID.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
pub async fn http_list_nodes(
    State(state): State<AppState>,
    Query(q): Query<NodeQuery>,
    Recursive(_depth): Recursive,
) -> Json<Vec<NodeEntry>> {
    let cfg = state.config.read().unwrap();
    let nodes: Vec<NodeEntry> = match q.rack_id {
        Some(rack_id) => cfg
            .nodes
            .iter()
            .filter(|n| n.rack_id == rack_id)
            .cloned()
            .collect(),
        None => cfg.nodes.clone(),
    };
    Json(nodes)
}

/// Add a new node.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if node addition or config persistence fails.
pub async fn http_add_node(
    State(state): State<AppState>,
    Json(entry): Json<NodeEntry>,
) -> Result<(StatusCode, Json<NodeEntry>), (StatusCode, Json<ErrorBody>)> {
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_node(entry.clone()).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    state
        .prepare_node_workspace(entry.id.to_string())
        .map_err(|e| err_500(e.to_string()))?;
    if has_fully_running_group0(&state) {
        if let Ok(ctx) = state.op_context().await {
            let value = crowdb_protocol::common::NodeValue {
                status: crowdb_protocol::common::HwStatus::Up as i32,
                last_used_dg_id: 0,
                disk_group_ids: Vec::new(),
                status_changed_at_ms: 0,
                temp_failure_since_ms: None,
            };
            let _ = ctx.sysmd().add_node(entry.rack_id, entry.id, &value).await;
        }
    }
    Ok((StatusCode::CREATED, Json(entry)))
}

/// Remove a node.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if node removal or config persistence fails.
pub async fn http_remove_node(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let t0 = std::time::Instant::now();
    // Cascade-stop the server process and drop its deployment record +
    // topology before removing the node, so a direct DELETE /api/nodes/:id
    // does not orphan a running crowdb-kv-server. No-op when no server is
    // deployed (e.g. the UI already called DELETE .../server first).
    stop_and_remove_server_for_node(&state, id).await;
    let t_stop = t0.elapsed().as_millis();
    let rack_id = {
        let cfg = state.config.read().unwrap();
        cfg.nodes.iter().find(|n| n.id == id).map(|n| n.rack_id)
    };
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_node(id).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    let t_cfg = t0.elapsed().as_millis() - t_stop;
    // Refresh the monitor cache for remaining group-0 nodes and poll
    // until a post-election leader is observed, so the sysdata write
    // targets the new leader instead of a stale hint pointing at the
    // just-stopped node (which would trigger a 5s transport-error
    // retry cycle in the KV client).
    let leader_found = state.refresh_group0_leader(Some(id)).await;
    let t_refresh = t0.elapsed().as_millis() - t_stop - t_cfg;
    // Best-effort sysdata sync (non-blocking — config already committed).
    // Skip when refresh_group0_leader found no leader — e.g. deleting the
    // last node leaves no group-0 endpoint, and the RPC would retry against
    // a dead endpoint for 5s+ per attempt, blocking the DELETE response.
    if leader_found {
        if let (Some(rack_id), Ok(ctx)) = (rack_id, state.op_context().await) {
            let _ = ctx.sysmd().remove_node_cascade(rack_id, id).await;
        }
    }
    let t_sysmd = t0.elapsed().as_millis() - t_stop - t_cfg - t_refresh;
    state.monitor_cache.drop_node(&id).await;
    tracing::debug!(
        "http_remove_node: id={id} stop={}ms cfg={}ms refresh={}ms sysmd={}ms total={}ms",
        t_stop,
        t_cfg,
        t_refresh,
        t_sysmd,
        t0.elapsed().as_millis()
    );
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Serialize)]
pub struct PingResult {
    /// `true` when the SSH handshake (or local-loopback equivalent) succeeded.
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// `POST /api/nodes/:id/ping`. For SSH-enabled nodes runs the
/// `crowdb_console_shared::ssh::probe` handshake; for local-fork nodes (`ssh_user=""`)
/// the probe is a no-op success since `lifecycle::deploy_local` does
/// not require any reachability.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the node is not found.
pub async fn http_ping_node(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<PingResult>, (StatusCode, Json<ErrorBody>)> {
    let node = {
        let cfg = state.config.read().unwrap();
        cfg.node(id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("node {id} not found"),
                }),
            )
        })?
    };
    if !node.ssh_enabled() {
        return Ok(Json(PingResult {
            ok: true,
            error: None,
        }));
    }
    match crowdb_console_shared::ssh::probe(&node).await {
        Ok(()) => Ok(Json(PingResult {
            ok: true,
            error: None,
        })),
        Err(e) => Ok(Json(PingResult {
            ok: false,
            error: Some(format!("{e}")),
        })),
    }
}

// ── Rack detail ──────────────────────────────────────────────────────

/// `GET /api/racks/:rack_id`. Rack detail.
///
/// At `recursive=0` (or absent) the legacy shape `{id, name, nodes: [node_ids]}`
/// is preserved. At `recursive>=1` the response shape changes to
/// `{id, name, nodes: [<NodeView>], truncated_at: [...]}` where each
/// node inlines stores / groups up to the requested depth.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the rack does not exist.
pub async fn http_get_rack(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Recursive(depth): Recursive,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let rack_id = id;
    if matches!(depth, RecursiveDepth::None) {
        let cfg = state.config.read().unwrap();
        let rack = cfg.racks.iter().find(|r| r.id == rack_id).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("rack {id} not found"),
                }),
            )
        })?;
        let node_ids: Vec<u64> = cfg
            .nodes
            .iter()
            .filter(|n| n.rack_id == rack_id)
            .map(|n| n.id)
            .collect();
        return Ok(Json(serde_json::json!({
            "id": rack.id,
            "name": rack.name,
            "nodes": node_ids,
        })));
    }
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    let rack = cfg
        .racks
        .iter()
        .find(|r| r.id == rack_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("rack {id} not found"),
                }),
            )
        })?
        .clone();
    let pids = state.kv_pid_snapshot();
    let diskdb_pids = state.diskdb_pid_snapshot();
    let mut builder = PhysicalBuilder::new_with_diskdb_pids(&cfg, &snap, &pids, &diskdb_pids);
    let view = builder.build_rack(&rack, depth.effective());
    let trunc = builder.into_truncation();
    let mut body = serde_json::to_value(&view).expect("serialize rack view");
    body["truncated_at"] = serde_json::to_value(&trunc.paths).expect("serialize truncated_at");
    Ok(Json(body))
}

/// `GET /api/racks/:rack_id/nodes`. List nodes under a specific rack.
///
/// At `recursive=0` returns a flat `Vec<NodeEntry>` (legacy shape). At
/// `recursive>=1` returns `{ items: [NodeView], truncated_at: [...] }`
/// with the per-node store / group tree inflated up to the depth cap.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the rack does not exist.
pub async fn http_list_rack_nodes(
    State(state): State<AppState>,
    Path(rack_id): Path<u64>,
    Recursive(depth): Recursive,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let rack_id_num = rack_id;
    if matches!(depth, RecursiveDepth::None) {
        let cfg = state.config.read().unwrap();
        if !cfg.racks.iter().any(|r| r.id == rack_id_num) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("rack {rack_id} not found"),
                }),
            ));
        }
        let nodes: Vec<NodeEntry> = cfg
            .nodes
            .iter()
            .filter(|n| n.rack_id == rack_id_num)
            .cloned()
            .collect();
        return Ok(Json(serde_json::to_value(&nodes).expect("serialize nodes")));
    }
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    if !cfg.racks.iter().any(|r| r.id == rack_id_num) {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("rack {rack_id} not found"),
            }),
        ));
    }
    let nodes: Vec<NodeEntry> = cfg
        .nodes
        .iter()
        .filter(|n| n.rack_id == rack_id_num)
        .cloned()
        .collect();
    let pids = state.kv_pid_snapshot();
    let diskdb_pids = state.diskdb_pid_snapshot();
    let mut builder = PhysicalBuilder::new_with_diskdb_pids(&cfg, &snap, &pids, &diskdb_pids);
    let limit = depth.effective();
    let views: Vec<_> = nodes.iter().map(|n| builder.build_node(n, limit)).collect();
    let trunc = builder.into_truncation();
    Ok(Json(serde_json::json!({
        "items": views,
        "truncated_at": trunc.paths,
    })))
}

/// `POST /api/racks/:rack_id/nodes`. Create a node under a specific rack.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if node creation or config persistence fails.
pub async fn http_add_rack_node(
    State(state): State<AppState>,
    Path(rack_id): Path<u64>,
    Json(mut entry): Json<NodeEntry>,
) -> Result<(StatusCode, Json<NodeEntry>), (StatusCode, Json<ErrorBody>)> {
    entry.rack_id = rack_id;
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_node(entry.clone()).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    state
        .prepare_node_workspace(entry.id.to_string())
        .map_err(|e| err_500(e.to_string()))?;
    // Best-effort sysdata sync (non-blocking — config already committed).
    if let Ok(ctx) = state.op_context().await {
        let value = crowdb_protocol::common::NodeValue {
            status: crowdb_protocol::common::HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: Vec::new(),
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
        };
        let _ = ctx.sysmd().add_node(entry.rack_id, entry.id, &value).await;
    }
    Ok((StatusCode::CREATED, Json(entry)))
}

// ── Node detail ──────────────────────────────────────────────────────

/// `GET /api/nodes/:node_id`. Node detail including server status.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the node does not exist.
pub async fn http_get_node(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Recursive(_depth): Recursive,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let node_id_num = id;
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    let node = cfg.node(node_id_num).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("node {id} not found"),
            }),
        )
    })?;
    let pids = state.kv_pid_snapshot();
    let diskdb_pids = state.diskdb_pid_snapshot();
    let mut builder = PhysicalBuilder::new_with_diskdb_pids(&cfg, &snap, &pids, &diskdb_pids);
    let view = builder.build_node(node, 0);
    Ok(Json(serde_json::to_value(view).expect("serialize node view")))
}

// ── Server lifecycle (node-addressed) ────────────────────────────────

/// One row of `GET /api/servers`: a deployed `crowdb-kv-server` projected
/// from the persisted config plus the live monitor cache.
#[derive(Debug, Serialize)]
pub struct ServerSummary {
    /// Owning node id (`None` for plain externally-registered servers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<u64>,
    /// KV management URL. Absent for `DiskDB`, which has no public
    /// management URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mgmt_url: Option<String>,
    /// Public service endpoint. Used by `DiskDB`; absent for KV when its
    /// endpoint is represented by `rpc_url`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc_url: Option<String>,
    /// Live pid if the console currently tracks the process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Latest health from the monitor cache (`unknown` until probed).
    pub health: NodeHealth,
    /// Service type: "kv" (crowdb-kv-server) or "diskdb".
    pub service_type: String,
}

/// `GET /api/servers`. Cluster-wide list of deployed servers, one row
/// per `ServerEntry`, with health from the monitor cache and the live
/// pid when tracked. The CLI's `server list` renders this directly.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
pub async fn http_list_servers(State(state): State<AppState>) -> Json<Vec<ServerSummary>> {
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    let rows = cfg
        .servers
        .iter()
        .map(|s| {
            let pid = if s.service_type == ServiceType::Diskdb {
                s.node_id.and_then(|n| state.diskdb_runtime_pid(n.to_string()))
            } else {
                s.node_id.and_then(|n| state.runtime_pid(n.to_string()))
            };
            // KV health comes from the monitor cache (probed via the KV
            // server's /topology), overridden to Down when no PID is
            // tracked. DDB has no topology probe, so its health is derived
            // from PID presence alone — the shared node record reflects KV
            // health and must not flip the DDB badge when KV is stopped or
            // restarted while DDB keeps running.
            let health = if s.service_type == ServiceType::Diskdb {
                if pid.is_some() {
                    NodeHealth::Up
                } else {
                    NodeHealth::Down
                }
            } else if pid.is_some() {
                s.node_id
                    .and_then(|n| snap.get(&n))
                    .map_or(NodeHealth::Unknown, |rec| rec.health)
            } else {
                NodeHealth::Down
            };
            ServerSummary {
                node_id: s.node_id,
                mgmt_url: (s.service_type == ServiceType::Kv).then(|| s.url.clone()),
                endpoint: (s.service_type == ServiceType::Diskdb).then(|| s.url.clone()),
                rpc_url: s.rpc_url.clone(),
                pid,
                health,
                service_type: match s.service_type {
                    crowdb_console_shared::config::ServiceType::Kv => "kv",
                    crowdb_console_shared::config::ServiceType::Diskdb => "diskdb",
                    crowdb_console_shared::config::ServiceType::Chunkdb => "chunkdb",
                    crowdb_console_shared::config::ServiceType::Diskio => "diskio",
                    crowdb_console_shared::config::ServiceType::Rpc => "rpc",
                }
                .to_string(),
            }
        })
        .collect();
    Json(rows)
}

#[derive(Debug, Deserialize)]
pub struct DeployNodeServerBody {
    rest_port: u16,
    rpc_port: u16,
    #[serde(default)]
    binary: Option<String>,
    #[serde(default)]
    election_profile: Option<String>,
    /// `--kv-backend` value (e.g. `"file"`, `"block"`, `"mem-block"`).
    #[serde(default)]
    kv_backend: Option<String>,
    /// `--wal-backend` value (e.g. `"file"`, `"mem-block"`, `"block-device"`).
    #[serde(default)]
    wal_backend: Option<String>,
    /// Sets `--no-fsync` on the spawned server when `true`.
    #[serde(default)]
    no_fsync: bool,
    /// `--metrics-interval` value in seconds.
    #[serde(default)]
    metrics_interval: Option<u64>,
    /// `--max-inflight` value for the proposal admission window.
    #[serde(default)]
    max_inflight: Option<usize>,
    /// `--coalesce-max-keys` value for R45 proposal coalescing.
    #[serde(default)]
    coalesce_max_keys: Option<usize>,
    /// `--peer-pool-size` value for inter-server RPC connection pool.
    #[serde(default)]
    peer_pool_size: Option<usize>,
    /// `--enable-nagle` flag for RPC connections.
    #[serde(default)]
    enable_nagle: Option<bool>,
    /// `--quickack` flag for RPC connections (Linux only).
    #[serde(default)]
    quickack: Option<bool>,
    /// `--event-write` flag for RPC transports.
    #[serde(default)]
    event_write: Option<bool>,
    /// `--send-queue-capacity` value for per-connection send queue.
    #[serde(default)]
    send_queue_capacity: Option<u32>,
    /// Optional `--config` JSON path passed to the spawned `crowdb-kv-server`.
    #[serde(default)]
    config: Option<String>,
    /// `--rpc-workers` value for the spawned `crowdb-kv-server`.
    #[serde(default)]
    rpc_workers: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct DeployResult {
    node_id: NodeId,
    mgmt_url: String,
    rpc_url: String,
    pid: u32,
}

/// `GET /api/nodes/:node_id/server`. Runtime info; 404 if not deployed.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no server is deployed on this node.
pub async fn http_get_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Recursive(_depth): Recursive,
) -> Result<Json<ServerEntry>, (StatusCode, Json<ErrorBody>)> {
    let mut entry = {
        let cfg = state.config.read().unwrap();
        cfg.server_for_node(node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server deployed on node {node_id}"),
                }),
            )
        })?
    };
    entry.pid = state.runtime_pid(node_id);
    Ok(Json(entry))
}

/// `POST /api/nodes/:node_id/server/deploy`. Spawn `crowdb-kv-server` on
/// the node (local fork for `ssh_user=""`, SSH otherwise), wait for
/// health, persist the deployment record.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if deployment, config persistence, or node lookup fails.
pub async fn http_deploy_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Json(body): Json<DeployNodeServerBody>,
) -> Result<(StatusCode, Json<DeployResult>), (StatusCode, Json<ErrorBody>)> {
    use crowdb_console_shared::lifecycle::DeployRequest;

    let workspace_dir = state
        .prepare_node_workspace(node_id)
        .map_err(|e| err_500(e.to_string()))?;
    let req = DeployRequest {
        server_id: node_id.to_string(),
        rest_port: body.rest_port,
        rpc_port: body.rpc_port,
        election_profile: body
            .election_profile
            .clone()
            .or_else(|| std::env::var("CROWDB_KV_SERVER_ELECTION_PROFILE").ok()),
        binary: body.binary.clone().map(std::path::PathBuf::from),
        kv_backend: body.kv_backend.clone(),
        wal_backend: body.wal_backend.clone(),
        no_fsync: body.no_fsync,
        metrics_interval: body.metrics_interval,
        max_inflight: body.max_inflight,
        coalesce_max_keys: body.coalesce_max_keys,
        peer_pool_size: body.peer_pool_size,
        enable_nagle: body.enable_nagle,
        quickack: body.quickack,
        event_write: body.event_write,
        send_queue_capacity: body.send_queue_capacity,
        config: body.config.clone().map(std::path::PathBuf::from),
        rpc_workers: body.rpc_workers,
    };

    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let deployed = ops::kv_server::deploy(&ctx, &req, Some(&workspace_dir))
        .await
        .map_err(map_config_err)?;
    // Apply the server entry directly to state.config instead of
    // commit_op_context — the snapshot-replace pattern loses
    // concurrent updates (e.g. parallel deploys on different nodes).
    let entry = ctx
        .config()
        .server_for_node(node_id)
        .ok_or_else(|| err_500("deploy succeeded but server entry not in ctx"))?
        .clone();
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_server(entry).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    state.set_runtime_pid(node_id, deployed.pid);
    crate::mgmt::refresh_node_cache(&state, node_id).await;
    // A new server is now in the config — re-seed the shared kv_client
    // so topology refresh can reach this node.
    state.reseed_kv_client().await;
    Ok((
        StatusCode::CREATED,
        Json(DeployResult {
            node_id,
            mgmt_url: deployed.mgmt_url,
            rpc_url: deployed.rpc_url,
            pid: deployed.pid,
        }),
    ))
}

/// `POST /api/nodes/:node_id/server/restart`. Stop the tracked
/// `crowdb-kv-server` process on this node (if any) and immediately
/// re-deploy on the same ports recorded in the `ServerEntry`. The
/// binary path falls back to `CROWDB_KV_SERVER_BIN` / `"crowdb-kv-server"`
/// the same way the initial deploy does when no `binary` override
/// is supplied. Returns the new `DeployResult`.
///
/// Idempotent in the sense that calling it when no process is
/// currently running still performs a deploy (so an operator can
/// recover from an out-of-band crash).
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no server is registered for this node, `502` if
/// the SSH/local restart cycle fails.
pub async fn http_restart_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<Json<DeployResult>, (StatusCode, Json<ErrorBody>)> {
    use crowdb_console_shared::lifecycle;

    // Stop the running process first (using the in-memory runtime PID).
    // ops::kv_server::restart also tries to stop via entry.pid, but the
    // runtime PID is authoritative for web-deployed servers.
    if let Some(pid) = state.runtime_pid(node_id) {
        let node = {
            let cfg = state.config.read().unwrap();
            cfg.node(node_id).cloned()
        };
        let _sent = match node {
            Some(n) if n.ssh_enabled() => crowdb_console_shared::ssh::stop_via_ssh(&n, pid)
                .await
                .map_err(|e| err_502(format!("ssh stop (restart): {e}")))?,
            _ => tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
                .await
                .map_err(|e| err_500(format!("spawn_blocking (restart): {e}")))?
                .unwrap_or(false),
        };
    }

    let workspace_dir = state
        .prepare_node_workspace(node_id)
        .map_err(|e| err_500(e.to_string()))?;
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let deployed = ops::kv_server::restart(&ctx, node_id, Some(&workspace_dir))
        .await
        .map_err(map_config_err)?;
    // Apply the updated server entry directly to state.config (avoid
    // commit_op_context snapshot-replace race).
    let new_entry = ctx
        .config()
        .server_for_node(node_id)
        .ok_or_else(|| err_500("restart succeeded but server entry not in ctx"))?
        .clone();
    {
        let mut cfg = state.config.write().unwrap();
        let _ = cfg.remove_server_for_node(node_id);
        cfg.add_server(new_entry).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    state.set_runtime_pid(node_id, deployed.pid);
    // Clear cached KV RPC connections so the next KV request reconnects
    // to the restarted server instead of reusing a stale TCP connection.
    if let Some(t) = state.kv_rpc_transport.read().await.as_ref() {
        t.clear_connections();
    }
    // Mark the node as recovering so the monitor cache preserves
    // previously-known stores until the server's /topology confirms
    // them (WAL replay may still be in progress when the first
    // topology fetch succeeds). Must be set BEFORE
    // restore_persisted_topology_for_node, which calls
    // refresh_node_cache internally.
    state.monitor_cache.mark_recovering(node_id).await;
    crate::mgmt::restore_persisted_topology_for_node(&state, node_id)
        .await
        .map_err(|e| err_502(format!("restore topology after restart: {e}")))?;
    // Refresh the monitor cache so health badges reflect the restarted
    // server. The process may not be listening yet, so retry a few
    // times with short delays until the probe succeeds.
    crate::mgmt::refresh_node_cache(&state, node_id).await;
    let state_clone = state.clone();
    tokio::spawn(async move {
        for _ in 0..10 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            crate::mgmt::refresh_node_cache(&state_clone, node_id).await;
            let snap = state_clone.monitor_cache.snapshot().await;
            if let Some(rec) = snap.get(&node_id) {
                if rec.health == NodeHealth::Up {
                    break;
                }
            }
        }
    });

    Ok(Json(DeployResult {
        node_id,
        mgmt_url: deployed.mgmt_url,
        rpc_url: deployed.rpc_url,
        pid: deployed.pid,
    }))
}

#[derive(Debug, Serialize)]
pub struct StopResult {
    pub sent: bool,
}

/// `POST /api/nodes/:node_id/server/stop`. Stop the server on this node
/// but keep the deployment record so the console can restart / restore it.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the server is not found or has no tracked pid.
pub async fn http_stop_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<Json<StopResult>, (StatusCode, Json<ErrorBody>)> {
    let pid = state.runtime_pid(node_id);
    let node = {
        let cfg = state.config.read().unwrap();
        cfg.node(node_id).cloned()
    };
    // Send SIGTERM without blocking — the server's graceful shutdown can
    // take up to 10s (shutdown_timeout_ms), and blocking here delays the
    // stop response. The process is reaped in the background (SIGKILL
    // after 15s if needed) so ports/WAL files are released for reuse.
    let sent = if let Some(pid) = pid {
        match node {
            Some(n) if n.ssh_enabled() => crowdb_console_shared::ssh::stop_via_ssh(&n, pid)
                .await
                .map_err(|e| err_502(format!("ssh stop: {e}")))?,
            _ => {
                tokio::task::spawn_blocking(move || {
                    crowdb_console_shared::lifecycle::stop_pid(pid).unwrap_or(false)
                });
                true
            }
        }
    } else {
        // No runtime PID — check config for a persisted PID (e.g. server
        // was deployed by a prior web-server instance).
        let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
        ops::kv_server::stop(&ctx, node_id, pid)
            .await
            .map_err(map_config_err)?
    };
    // Clear only the in-memory runtime PID — the config PID is left
    // intact so ops::kv_server::restart() can stop the old process
    // (blocking wait) before redeploying on the same port. The config
    // PID is cleared by resetAll (which removes the server entry) or
    // by the next deploy (which overwrites it with the new PID).
    state.clear_runtime_pid(node_id);
    // Clear cached KV RPC connections — the server is stopping, so any
    // cached TCP connection is now dead. The next KV request (after a
    // restart) must reconnect.
    if let Some(t) = state.kv_rpc_transport.read().await.as_ref() {
        t.clear_connections();
    }
    // Only mark the shared node record Down when no DDB instance is still
    // running on this node. The record is shared between KV and DDB; an
    // unconditional mark_down would flip the node-level badge (and any
    // DDB health derived from the record) even though DDB is unaffected.
    // The KV badge already drops via the no-pid override in
    // build_server_process / http_list_servers, and DDB health in
    // http_list_servers is derived from the DDB pid alone.
    if state.diskdb_runtime_pid(node_id).is_none() {
        state.monitor_cache.mark_down(node_id, "server stopped").await;
    }
    Ok(Json(StopResult { sent }))
}

/// Stop the server process deployed on `node_id` (best-effort) and remove
/// its deployment record + topology from config. Returns `true` if a
/// server was deployed (and is now removed), `false` if no server was
/// deployed on the node. Does NOT persist — the caller persists. Does NOT
/// remove the node itself. Used by both `DELETE /api/nodes/:id/server`
/// and the node-delete cascade so a direct `DELETE /api/nodes/:id` does
/// not orphan a running crowdb-kv-server.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
async fn stop_and_remove_server_for_node(state: &AppState, node_id: u64) -> bool {
    use crowdb_console_shared::lifecycle;

    let (node, config_pid) = {
        let cfg = state.config.read().unwrap();
        if cfg.server_for_node(node_id).is_none() {
            return false;
        }
        (
            cfg.node(node_id).cloned(),
            cfg.server_for_node(node_id).and_then(|s| s.pid),
        )
    };
    // Prefer the in-memory runtime PID (set at deploy time); fall back
    // to the persisted config PID so we can still stop a server whose
    // process was spawned by a prior web-server instance.
    let pid = state.runtime_pid(node_id).or(config_pid);
    if let Some(pid) = pid {
        match node {
            Some(n) if n.ssh_enabled() => {
                let _ = crowdb_console_shared::ssh::stop_via_ssh(&n, pid)
                    .await
                    .unwrap_or(false);
            }
            _ => {
                // Send SIGTERM and reap the process in the background.
                // The server's graceful shutdown can take up to 10s
                // (shutdown_timeout_ms), and blocking here delays the
                // DELETE response until the process fully exits. The
                // node is removed from config below regardless — the
                // tree updates immediately. The background task ensures
                // the process is reaped (SIGKILL after 15s if needed)
                // so ports/WAL files are released for reuse.
                tokio::task::spawn_blocking(move || {
                    let _ = lifecycle::stop_pid(pid);
                });
            }
        }
    }
    {
        let mut cfg = state.config.write().unwrap();
        let _ = cfg.remove_server_for_node(node_id);
        cfg.purge_node_topology(node_id);
    }
    state.clear_runtime_pid(node_id);
    state.monitor_cache.drop_node(&node_id).await;
    true
}

/// `DELETE /api/nodes/:node_id/server`. Stop and remove the deployment
/// record. Returns 204 on success, 404 if no server is deployed, 409
/// if the node still hosts replicas in group-0 sysdata (require-empty).
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no server is deployed on this node; `409` if the
/// node still hosts replicas.
pub async fn http_delete_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    // Require-empty: refuse if the node still hosts replicas in
    // group-0 sysdata. Best-effort — if sysdata is unreachable, the
    // check is skipped (cluster may not be initialized). Skip entirely
    // when no store 0 exists in the local config — the cluster has not
    // been initialized, so there can be no replicas to check, and the
    // sysdata RPC would hang retrying against a bootstrap mgmt URL.
    let cluster_initialized = {
        let cfg = state.config.read().unwrap();
        cfg.stores.iter().any(|s| s.store_id == 0)
    };
    if cluster_initialized {
        let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
        ops::kv_server::check_require_empty(&ctx, node_id)
            .await
            .map_err(map_config_err)?;
    }
    if !stop_and_remove_server_for_node(&state, node_id).await {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("no server deployed on node {node_id}"),
            }),
        ));
    }
    state.persist().map_err(map_persist_err)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/cluster/clean`. Wipe user data on every node + wait for
/// re-election. Preserves group-0 sysdata + topology.
///
/// # Errors
/// Returns `502` if the wipe or re-election wait fails.
pub async fn http_cluster_clean(
    State(state): State<AppState>,
) -> Result<Json<ops::cluster::CleanResult>, (StatusCode, Json<ErrorBody>)> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    ops::cluster::clean(&ctx, 0, 0)
        .await
        .map(Json)
        .map_err(|e| err_502(format!("{e}")))
}

/// `POST /api/cluster/reset`. Remove orphaned sysdata entries
/// (stores/groups/replicas that have no corresponding running server).
/// Does not stop any running servers.
///
/// # Errors
/// Returns `502` if the sysdata scan fails.
pub async fn http_cluster_reset(
    State(state): State<AppState>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    ops::cluster::reset(&ctx)
        .await
        .map_err(|e| err_502(format!("{e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/cluster/destroy` (alias: `/internal/reset`). Tear down
/// the entire cluster in dependency order: groups → stores → server
/// processes → nodes → racks, then clear workspace dirs and caches.
///
/// # Panics
/// Panics if the `RwLock` or `Mutex` is poisoned.
///
/// # Errors
/// Returns an error if workspace cleanup or config persistence fails.
#[allow(clippy::too_many_lines)]
pub async fn http_internal_reset(
    State(state): State<AppState>,
) -> Result<Json<ResetResult>, (StatusCode, Json<ErrorBody>)> {
    // Graceful shutdown in dependency order:
    //   1-4. shutdown_kv_data — remove user groups → user stores →
    //        clean group-0 sysdata → remove group-0/store-0.
    //   5.   stop_all_services — SIGTERM all KV + DDB processes.
    //   6-8. config cleanup — remove nodes, racks, caches, workspaces.
    let mut stopped = shutdown_kv_data(&state).await;
    stopped.extend(stop_all_services(&state).await);

    // 6. Remove all nodes from config + drop monitor cache entries.
    let node_ids: Vec<NodeId> = {
        let cfg = state.config.read().unwrap();
        cfg.nodes.iter().map(|n| n.id).collect()
    };
    for nid in &node_ids {
        {
            let mut cfg = state.config.write().unwrap();
            let _ = cfg.remove_server_for_node(*nid);
            let pos = cfg
                .servers
                .iter()
                .position(|s| s.node_id == Some(*nid) && s.service_type == ServiceType::Diskdb);
            if let Some(p) = pos {
                cfg.servers.remove(p);
            }
            cfg.purge_node_topology(*nid);
            let _ = cfg.remove_node(*nid);
        }
        state.monitor_cache.drop_node(nid).await;
    }

    // 7. Remove all racks from config.
    let rack_ids: Vec<String> = {
        let cfg = state.config.read().unwrap();
        cfg.racks.iter().map(|r| r.id.to_string()).collect()
    };
    {
        let mut cfg = state.config.write().unwrap();
        for rid in &rack_ids {
            let _ = cfg.remove_rack(rid.parse().unwrap());
        }
        // Clear disk-groups and disks from config — the rack/node
        // cascade above removed them from group-0 sysdata, but the
        // config file still carries the stale entries from before
        // the reset. Without this, a restart reloads stale DGs/disks.
        cfg.disk_groups.clear();
        cfg.disks.clear();
    }

    // 8. Clear caches and workspace directories.
    state.clear_kv_client().await;
    // Defer workspace cleanup to a background task — the KV server
    // processes are still shutting down (async SIGTERM in
    // stop_all_services), and removing their WAL/engine files while
    // they are open would fail or race. The background task removes
    // the dirs best-effort; the next deploy recreates them as needed.
    // Defer workspace cleanup — processes may still be shutting down.
    let state_clone = state.clone();
    tokio::task::spawn_blocking(move || {
        let _ = state_clone.clear_workspaces();
    });
    state.persist().map_err(map_persist_err)?;

    Ok(Json(ResetResult { stopped }))
}

/// Steps 1-4: gracefully shut down all KV data in dependency order.
/// - Step 1: remove user groups (non-zero) via mgmt API RPC to each node.
/// - Step 2: remove user stores (non-zero) via mgmt API RPC to each node.
/// - Step 3: clean group-0 sysdata (rack cascade, store records, diskdb
///   unregister) via group-0 RPC — group-0 still alive.
/// - Step 4: remove group-0/store-0 via mgmt API RPC — last KV data
///   gracefully shut down (flushes WAL, closes engine).
///
/// Skips all RPC steps when no KV servers are running
/// (`kv_pid_snapshot().is_empty()`). This is the key optimization for
/// E2E tests: the test's `finally` block already stopped the servers,
/// so `resetAll` skips doomed RPC retries (10-20s of backoff) and goes
/// straight to config cleanup.
#[allow(clippy::too_many_lines)]
async fn shutdown_kv_data(state: &AppState) -> Vec<String> {
    let rack_ids: Vec<RackId> = {
        let cfg = state.config.read().unwrap();
        cfg.racks.iter().map(|r| r.id).collect()
    };
    let stores: Vec<u64> = {
        let snap = state.monitor_cache.snapshot().await;
        let mut ids: Vec<u64> = snap.values().flat_map(|rec| rec.stores.keys().copied()).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    };
    let any_kv_running = !state.kv_pid_snapshot().is_empty();
    if !any_kv_running {
        tracing::info!("reset: no KV servers running, skipping data shutdown RPC steps");
        return Vec::new();
    }

    // Step 1: remove user groups (non-zero) from each hosting node.
    let user_stores: Vec<u64> = stores.iter().copied().filter(|&sid| sid != 0).collect();
    for sid in &user_stores {
        if let Some(view) = state.monitor_cache.resolve_store(*sid).await {
            let group_ids: Vec<u64> = view.groups.iter().map(|g| g.group_id).collect();
            for gid in group_ids {
                if let Some(gv) = state.monitor_cache.resolve_group(*sid, gid).await {
                    let node_ids: Vec<NodeId> = gv.replicas.iter().map(|r| r.node_id).collect();
                    // Only contact running servers — skip stopped ones
                    // (no runtime pid) to avoid connection-refused delays.
                    let live: Vec<NodeId> = node_ids
                        .iter()
                        .copied()
                        .filter(|n| state.runtime_pid(*n).is_some())
                        .collect();
                    for nid in &live {
                        if let Ok(url) = crate::mgmt::mgmt_url_for_node(state, *nid) {
                            if let Ok(client) = crate::mgmt::build_server_client(url) {
                                let _ = client.remove_group(*sid, gid).await;
                            }
                        }
                    }
                    for nid in &live {
                        crate::mgmt::refresh_node_cache(state, *nid).await;
                    }
                }
                {
                    let mut cfg = state.config.write().unwrap();
                    cfg.remove_group_record(*sid, gid);
                }
            }
        }
    }

    // Step 2: remove user stores (non-zero) from each hosting node.
    for sid in &user_stores {
        if let Some(view) = state.monitor_cache.resolve_store(*sid).await {
            let live: Vec<NodeId> = view
                .nodes
                .iter()
                .copied()
                .filter(|n| state.runtime_pid(*n).is_some())
                .collect();
            for nid in &live {
                if let Ok(url) = crate::mgmt::mgmt_url_for_node(state, *nid) {
                    if let Ok(client) = crate::mgmt::build_server_client(url) {
                        let _ = client.remove_store(*sid).await;
                    }
                }
            }
            for nid in &live {
                crate::mgmt::refresh_node_cache(state, *nid).await;
            }
        }
        {
            let mut cfg = state.config.write().unwrap();
            cfg.remove_store_record(*sid);
        }
    }

    // Step 3: clean group-0 sysdata — rack cascade, store records,
    // diskdb unregister. Group-0 is still alive at this point.
    // Refresh the monitor cache for all running nodes first so the
    // group-0 leader endpoint is current (a prior test may have
    // stopped/restarted nodes, leaving stale cache entries).
    let running_nodes: Vec<NodeId> = {
        let snap = state.monitor_cache.snapshot().await;
        snap.keys()
            .copied()
            .filter(|n| state.runtime_pid(*n).is_some())
            .collect()
    };
    for nid in &running_nodes {
        crate::mgmt::refresh_node_cache(state, *nid).await;
    }
    if let Some(hw) = crate::mgmt::build_hardware_client(state).await {
        for rid in &rack_ids {
            if let Err(e) = hw.remove_rack_cascade(*rid).await {
                tracing::warn!(rack_id = rid, error = %e, "reset: remove_rack_cascade failed");
            }
        }
        let meta = crowdb_kv_client::KVClusterMetaClient::from_shared(hw.shared_kv());
        for sid in &stores {
            if let Err(e) = meta.remove_store(*sid).await {
                tracing::warn!(store_id = sid, error = %e, "reset: remove_store from sysdata failed");
            }
        }
        let svc = crowdb_kv_client::ServiceRegistryClient::from_shared(hw.shared_kv());
        match svc.read_all_diskdb_instances().await {
            Ok(instances) => {
                for (instance_id, _) in &instances {
                    if let Err(e) = svc.unregister("diskdb", *instance_id).await {
                        tracing::warn!(instance_id, error = %e, "reset: unregister diskdb failed");
                    }
                }
                tracing::info!("reset: unregistered {} diskdb instances", instances.len());
            }
            Err(e) => {
                tracing::warn!(error = %e, "reset: read_all_diskdb_instances failed; skipping unregister");
            }
        }
        tracing::info!(
            "reset: group-0 sysdata cleanup complete for {} racks",
            rack_ids.len()
        );
    } else {
        tracing::warn!("reset: no group-0 endpoint, skipping sysdata cleanup");
    }

    // Step 4: remove group-0/store-0 via mgmt API — last KV data,
    // triggers graceful PxGroup/PxKvStore shutdown (flush WAL, close
    // engine) on each node that hosts store 0.
    if stores.contains(&0) {
        if let Some(view) = state.monitor_cache.resolve_store(0).await {
            let live: Vec<NodeId> = view
                .nodes
                .iter()
                .copied()
                .filter(|n| state.runtime_pid(*n).is_some())
                .collect();
            for nid in &live {
                if let Ok(url) = crate::mgmt::mgmt_url_for_node(state, *nid) {
                    if let Ok(client) = crate::mgmt::build_server_client(url) {
                        let _ = client.remove_group(0, 0).await;
                    }
                }
            }
            for nid in &live {
                if let Ok(url) = crate::mgmt::mgmt_url_for_node(state, *nid) {
                    if let Ok(client) = crate::mgmt::build_server_client(url) {
                        let _ = client.remove_store(0).await;
                    }
                }
            }
            for nid in &live {
                crate::mgmt::refresh_node_cache(state, *nid).await;
            }
        }
        {
            let mut cfg = state.config.write().unwrap();
            cfg.remove_store_record(0);
        }
    }

    Vec::new()
}

/// Step 5: graceful stop all KV server + DDB processes (SIGTERM →
/// graceful shutdown). Clears runtime PIDs. Returns the list of node
/// IDs whose KV server process was stopped.
async fn stop_all_services(state: &AppState) -> Vec<String> {
    use crowdb_console_shared::lifecycle;

    let node_ids: Vec<NodeId> = {
        let cfg = state.config.read().unwrap();
        cfg.nodes.iter().map(|n| n.id).collect()
    };
    let mut stopped: Vec<String> = Vec::new();

    for nid in &node_ids {
        // Stop the KV server process if a PID is tracked.
        // Send SIGTERM and reap in the background — the server's
        // graceful shutdown can take up to 10s (shutdown_timeout_ms),
        // and blocking here serializes N nodes × 10s = N×10s delays.
        // The process is reaped in the background (SIGKILL after 15s
        // if needed) so ports/WAL files are released for reuse.
        if let Some(pid) = state.runtime_pid(nid) {
            let ssh = state
                .config
                .read()
                .unwrap()
                .node(*nid)
                .is_some_and(crowdb_console_shared::config::NodeEntry::ssh_enabled);
            if ssh {
                let node = state.config.read().unwrap().node(*nid).cloned().unwrap();
                let _ = crowdb_console_shared::ssh::stop_via_ssh(&node, pid).await;
            } else {
                tokio::task::spawn_blocking(move || {
                    let _ = lifecycle::stop_pid(pid);
                });
            }
            stopped.push(nid.to_string());
            state.clear_runtime_pid(nid);
        }

        // Stop the DDB process if a PID is tracked.
        if let Some(pid) = state.diskdb_runtime_pid(nid) {
            tokio::task::spawn_blocking(move || {
                let _ = lifecycle::stop_pid(pid);
            });
            state.clear_diskdb_runtime_pid(nid);
        }
    }

    stopped
}

#[derive(Serialize)]
pub struct ResetResult {
    pub stopped: Vec<String>,
}

// ── Disk-group lifecycle ──────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct AddDiskGroupBody {
    id: DiskGroupId,
    #[serde(default)]
    name: String,
}

/// `GET /api/nodes/:node_id/disk-groups`.
///
/// When group-0 is available, reads the authoritative disk-group list
/// from group-0 sysdata (the source of truth). Falls back to the
/// console config file only when group-0 is not reachable. The config
/// `name` field is merged in as metadata where available.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the node does not exist.
pub async fn http_list_node_disk_groups(
    State(state): State<AppState>,
    Path(node_id): Path<NodeId>,
) -> Result<Json<Vec<DiskGroupEntry>>, (StatusCode, Json<ErrorBody>)> {
    let (rack_id, node_exists) = {
        let cfg = state.config.read().unwrap();
        let rack = cfg.nodes.iter().find(|n| n.id == node_id);
        (rack.map(|n| n.rack_id), cfg.nodes.iter().any(|n| n.id == node_id))
    };
    if !node_exists {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("node {node_id} not found"),
            }),
        ));
    }
    // Try group-0 first (authoritative source of truth), but only if
    // the cluster is initialized — avoids logging warnings on every
    // poll when group-0 doesn't exist yet.
    if crate::mgmt::group0_available(&state).await {
        if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
            if let Some(rack_id) = rack_id {
                match hw.list_disk_groups_on_node(rack_id, node_id).await {
                    Ok(g0_dgs) => {
                        // Merge config names as metadata, and include
                        // config-only DGs not yet synced to group-0 (the
                        // add handler's sysdata sync is best-effort, so a
                        // DG may exist in config before appearing in
                        // group-0). Conversely, filter out group-0 DGs
                        // no longer in config: the delete handler's
                        // group-0 cascade-remove is also best-effort, so
                        // a deleted DG can linger in group-0. Using config
                        // as the existence filter keeps add and delete
                        // consistent.
                        let cfg_dgs: Vec<DiskGroupEntry> = {
                            let cfg = state.config.read().unwrap();
                            cfg.disk_groups
                                .iter()
                                .filter(|dg| dg.node_id == node_id)
                                .cloned()
                                .collect()
                        };
                        let name_map: std::collections::HashMap<DiskGroupId, String> =
                            cfg_dgs.iter().map(|dg| (dg.id, dg.name.clone())).collect();
                        let cfg_ids: std::collections::HashSet<DiskGroupId> =
                            cfg_dgs.iter().map(|dg| dg.id).collect();
                        let g0_ids: std::collections::HashSet<DiskGroupId> =
                            g0_dgs.iter().map(|dg| dg.dg_id).collect();
                        let mut entries: Vec<DiskGroupEntry> = g0_dgs
                            .into_iter()
                            .filter(|dg| cfg_ids.contains(&dg.dg_id))
                            .map(|dg| DiskGroupEntry {
                                id: dg.dg_id,
                                rack_id: dg.rack_id,
                                node_id: dg.node_id,
                                name: name_map.get(&dg.dg_id).cloned().unwrap_or_default(),
                            })
                            .collect();
                        entries.extend(cfg_dgs.into_iter().filter(|dg| !g0_ids.contains(&dg.id)));
                        return Ok(Json(entries));
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, node_id, "list_node_disk_groups: group-0 query failed; falling back to config");
                    }
                }
            }
        }
    }
    // Fallback: read from console config.
    let cfg = state.config.read().unwrap();
    Ok(Json(
        cfg.disk_groups
            .iter()
            .filter(|dg| dg.node_id == node_id)
            .cloned()
            .collect(),
    ))
}

/// `GET /api/nodes/:node_id/disk-groups/:dg_id`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the disk-group does not exist.
pub async fn http_get_node_disk_group(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(NodeId, DiskGroupId)>,
) -> Result<Json<DiskGroupEntry>, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    let dg = cfg
        .disk_groups
        .iter()
        .find(|dg| dg.node_id == node_id && dg.id == dg_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("disk-group {dg_id} not found on node {node_id}"),
                }),
            )
        })?;
    Ok(Json(dg.clone()))
}

/// `POST /api/nodes/:node_id/disk-groups`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if disk-group addition or config persistence fails.
pub async fn http_add_node_disk_group(
    State(state): State<AppState>,
    Path(node_id): Path<NodeId>,
    Json(body): Json<AddDiskGroupBody>,
) -> Result<(StatusCode, Json<DiskGroupEntry>), (StatusCode, Json<ErrorBody>)> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let entry = ops::hardware::add_disk_group(&ctx, node_id, body.id, &body.name)
        .await
        .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;

    let hw = crate::mgmt::build_hardware_client(&state)
        .await
        .ok_or_else(|| err_502("no group-0 endpoint; disk-group owner cannot be assigned"))?;
    if let Err(error) = auto_assign_owner(&hw, entry.rack_id, node_id, entry.id).await {
        let rollback = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
        let _ = ops::hardware::remove_disk_group(&rollback, node_id, entry.id).await;
        let _ = state.commit_op_context(&rollback);
        return Err(err_502(format!("auto-assign owner: {error}")));
    }

    Ok((StatusCode::CREATED, Json(entry)))
}

/// Pick the diskdb instance with the fewest owned DGs from a list of
/// live instance IDs and the current ownership map. Ties are broken by
/// lowest `instance_id`. Returns `None` if `instance_ids` is empty.
///
/// Pure function — no I/O — so it can be unit-tested without a live
/// group-0 connection.
/// Auto-assign a newly created disk-group to the diskdb instance with
/// the fewest owned DGs. Reads the service registry for live diskdb
/// instances and the current ownership map, counts DGs per instance,
/// picks the one with the lowest count, and writes the ownership entry
/// to group-0. Fails if no diskdb instances are registered.
async fn auto_assign_owner(
    hw: &crowdb_kv_client::HardwareClient,
    rack_id: RackId,
    node_id: NodeId,
    dg_id: DiskGroupId,
) -> Result<(), String> {
    let svc = crowdb_kv_client::ServiceRegistryClient::from_shared(hw.shared_kv());
    let instances = svc
        .read_all_diskdb_instances()
        .await
        .map_err(|e| format!("read_all_diskdb_instances: {e}"))?;
    if instances.is_empty() {
        return Err("no live diskdb instances registered".to_string());
    }
    let owners = hw.list_owners().await.map_err(|e| format!("list_owners: {e}"))?;
    let instance_ids: Vec<u64> = instances.iter().map(|(id, _)| *id).collect();
    let instance_id = crate::owner_assignment::pick_least_loaded_instance(&instance_ids, &owners)
        .ok_or_else(|| "no eligible diskdb instance".to_string())?;
    // Lease = 1 hour from now (the diskdb keepalive will refresh it).
    #[allow(clippy::cast_possible_truncation)]
    let lease_expiry_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
        + 3_600_000;
    let disk_group = hw
        .get_disk_group(rack_id, node_id, dg_id)
        .await
        .map_err(|e| format!("get_disk_group: {e}"))?
        .ok_or_else(|| format!("disk-group {dg_id} missing from group 0"))?;
    hw.add_disk_group_with_owner(
        rack_id,
        node_id,
        dg_id,
        &disk_group.value,
        instance_id,
        lease_expiry_ms,
    )
    .await
    .map_err(|e| format!("add_disk_group_with_owner: {e}"))?;
    tracing::info!(dg_id, instance_id, "auto-assign: assigned DG to diskdb instance");
    Ok(())
}

/// `DELETE /api/nodes/:node_id/disk-groups/:dg_id`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if disk-group removal or config persistence fails.
pub async fn http_remove_node_disk_group(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(NodeId, DiskGroupId)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    ops::hardware::remove_disk_group(&ctx, node_id, dg_id)
        .await
        .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;
    Ok(StatusCode::NO_CONTENT)
}

// ── Disk lifecycle ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct AddDiskBody {
    disk_id: String,
    disk_type: String,
    capacity_bytes: u64,
    zone_size_bytes: u64,
    unit_size_bytes: u32,
    #[serde(default)]
    device_path: String,
}

/// `GET /api/nodes/:node_id/disk-groups/:dg_id/disks`.
///
/// When group-0 is available, reads the authoritative disk list from
/// group-0 sysdata. Falls back to the console config file only when
/// group-0 is not reachable.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the disk-group does not exist.
#[allow(clippy::too_many_lines)]
pub async fn http_list_disks_in_group(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(NodeId, DiskGroupId)>,
) -> Result<Json<Vec<DiskEntry>>, (StatusCode, Json<ErrorBody>)> {
    // Resolve rack_id from config (needed for the group-0 key path).
    let rack_id = {
        let cfg = state.config.read().unwrap();
        cfg.nodes.iter().find(|n| n.id == node_id).map(|n| n.rack_id)
    };
    // Try group-0 first (authoritative source of truth), but only if
    // the cluster is initialized.
    if crate::mgmt::group0_available(&state).await {
        if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
            if let Some(rack_id) = rack_id {
                match hw.list_disks_in_group(rack_id, node_id, dg_id).await {
                    Ok(g0_disks) => {
                        // Merge config-only disks not yet synced to group-0
                        // (the add handler's sysdata sync is best-effort).
                        // Conversely, filter out group-0 disks no longer in
                        // config: the delete handler's group-0 cascade-remove
                        // is also best-effort, so a deleted disk can linger
                        // in group-0. Config is the existence filter.
                        // Normalize config disk IDs to dashed format for
                        // dedup comparison — config stores the raw client
                        // string (bare hex), group-0 uses `{high:016x}-{low:016x}`.
                        let cfg_disks: Vec<DiskEntry> = {
                            let cfg = state.config.read().unwrap();
                            cfg.disks
                                .iter()
                                .filter(|d| d.disk_group_id == dg_id && d.node_id == node_id)
                                .cloned()
                                .collect()
                        };
                        let cfg_ids: std::collections::HashSet<String> = cfg_disks
                            .iter()
                            .filter_map(|d| {
                                <crowdb_protocol::common::DiskId as crowdb_protocol::diskdb_type_util::DiskIdExt>::from_display_string(&d.disk_id)
                                    .ok()
                                    .map(|id| {
                                        crowdb_protocol::diskdb_type_util::DiskIdExt::to_display_string(&id)
                                    })
                            })
                            .collect();
                        let g0_ids: std::collections::HashSet<String> = g0_disks
                            .iter()
                            .map(|(id, _)| {
                                crowdb_protocol::diskdb_type_util::DiskIdExt::to_display_string(id)
                            })
                            .collect();
                        let mut entries: Vec<DiskEntry> = g0_disks
                            .into_iter()
                            .filter(|(id, _)| {
                                cfg_ids.contains(
                                    &crowdb_protocol::diskdb_type_util::DiskIdExt::to_display_string(id),
                                )
                            })
                            .map(|(disk_id, val)| {
                                let unit_size = u64::from(val.unit_size_bytes);
                                DiskEntry {
                                    disk_id: crowdb_protocol::diskdb_type_util::DiskIdExt::to_display_string(
                                        &disk_id,
                                    ),
                                    disk_group_id: dg_id,
                                    rack_id,
                                    node_id,
                                    disk_type: disk_type_proto_to_str(val.disk_type),
                                    capacity_bytes: val.capacity_units * unit_size,
                                    zone_size_bytes: val.zone_size_units * unit_size,
                                    unit_size_bytes: val.unit_size_bytes,
                                    device_path: val.device_path,
                                }
                            })
                            .collect();
                        entries.extend(cfg_disks.into_iter().filter_map(|mut d| {
                            let normalized =
                                <crowdb_protocol::common::DiskId as crowdb_protocol::diskdb_type_util::DiskIdExt>::from_display_string(&d.disk_id)
                                    .ok()
                                    .map(|id| {
                                        crowdb_protocol::diskdb_type_util::DiskIdExt::to_display_string(&id)
                                    });
                            if let Some(ref norm) = normalized {
                                if g0_ids.contains(norm) {
                                    return None;
                                }
                                d.disk_id.clone_from(norm);
                            }
                            Some(d)
                        }));
                        return Ok(Json(entries));
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, node_id, dg_id, "list_disks_in_group: group-0 query failed; falling back to config");
                    }
                }
            }
        }
    }
    // Fallback: read from console config.
    let cfg = state.config.read().unwrap();
    if !cfg
        .disk_groups
        .iter()
        .any(|dg| dg.node_id == node_id && dg.id == dg_id)
    {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("disk-group {dg_id} not found on node {node_id}"),
            }),
        ));
    }
    Ok(Json(
        cfg.disks
            .iter()
            .filter(|d| d.disk_group_id == dg_id && d.node_id == node_id)
            .cloned()
            .collect(),
    ))
}

/// Map a proto `DiskType` i32 to the console string representation.
fn disk_type_proto_to_str(disk_type: i32) -> String {
    match disk_type {
        0 => "Hdd",
        1 => "Ssd",
        2 => "ZONE_SSD",
        3 => "SMR_HDD",
        _ => "Unknown",
    }
    .to_string()
}

/// `GET /api/nodes/:node_id/disk-groups/:dg_id/disks/:disk_id`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the disk does not exist.
pub async fn http_get_disk(
    State(state): State<AppState>,
    Path((node_id, dg_id, disk_id)): Path<(NodeId, DiskGroupId, String)>,
) -> Result<Json<DiskEntry>, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    let disk = cfg
        .disks
        .iter()
        .find(|d| d.node_id == node_id && d.disk_group_id == dg_id && d.disk_id == disk_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("disk {disk_id} not found"),
                }),
            )
        })?;
    Ok(Json(disk.clone()))
}

/// `POST /api/nodes/:node_id/disk-groups/:dg_id/disks`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if disk addition or config persistence fails.
pub async fn http_add_disk(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(NodeId, DiskGroupId)>,
    Json(body): Json<AddDiskBody>,
) -> Result<(StatusCode, Json<DiskEntry>), (StatusCode, Json<ErrorBody>)> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let input = ops::hardware::AddDiskInput {
        disk_id: body.disk_id,
        disk_type: body.disk_type,
        capacity_bytes: body.capacity_bytes,
        zone_size_bytes: body.zone_size_bytes,
        unit_size_bytes: body.unit_size_bytes,
        device_path: body.device_path,
    };
    let entry = ops::hardware::add_disk(&ctx, node_id, dg_id, &input)
        .await
        .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;
    Ok((StatusCode::CREATED, Json(entry)))
}

/// `POST /api/nodes/:node_id/disk-groups/:dg_id/disks/batch` —
/// add multiple disks in one request (R77). Validates all inputs
/// and checks for duplicates (against config and within the batch)
/// before mutating config; best-effort sysdata sync per disk.
/// Atomic all-or-nothing on the config mutation: if any `add_disk`
/// fails, the ones already added are rolled back.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the disk-group doesn't exist, `400` on the
/// first invalid disk input, `409` on a duplicate `disk_id` (in
/// config or within the batch).
pub async fn http_add_disks_batch(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(NodeId, DiskGroupId)>,
    Json(body): Json<AddDisksBatchBody>,
) -> Result<(StatusCode, Json<AddDisksBatchResult>), (StatusCode, Json<ErrorBody>)> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let inputs: Vec<ops::hardware::AddDiskInput> = body
        .disks
        .into_iter()
        .map(|d| ops::hardware::AddDiskInput {
            disk_id: d.disk_id,
            disk_type: d.disk_type,
            capacity_bytes: d.capacity_bytes,
            zone_size_bytes: d.zone_size_bytes,
            unit_size_bytes: d.unit_size_bytes,
            device_path: d.device_path,
        })
        .collect();
    let added = ops::hardware::add_disks_batch(&ctx, node_id, dg_id, &inputs)
        .await
        .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;
    Ok((
        StatusCode::CREATED,
        Json(AddDisksBatchResult {
            added,
            sysdata_errors: Vec::new(),
        }),
    ))
}

#[derive(Debug, Deserialize)]
pub struct AddDisksBatchBody {
    pub disks: Vec<AddDiskBody>,
}

#[derive(Debug, Serialize)]
pub struct AddDisksBatchResult {
    pub added: Vec<DiskEntry>,
    pub sysdata_errors: Vec<String>,
}

/// `DELETE /api/nodes/:node_id/disk-groups/:dg_id/disks/:disk_id`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if disk removal or config persistence fails.
pub async fn http_remove_disk(
    State(state): State<AppState>,
    Path((node_id, dg_id, disk_id)): Path<(NodeId, DiskGroupId, String)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    ops::hardware::remove_disk(&ctx, node_id, dg_id, &disk_id)
        .await
        .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;
    Ok(StatusCode::NO_CONTENT)
}
