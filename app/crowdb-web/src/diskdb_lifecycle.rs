// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskDB` instance deploy / restart / stop / delete handlers (R77).
//! These manage the `crowdb-diskdb` process lifecycle via
//! `lifecycle::deploy_diskdb_local` (local-fork
//! only; SSH is a C4 follow-up). The runtime REST proxy
//! (`/api/diskdb/*`) lives in `crate::diskdb`.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::{err_400, err_500, err_502, ErrorBody};
use crate::state::AppState;
use crowdb_console_shared::config::ServiceType;
use crowdb_console_shared::lifecycle::{self, DiskdbDeployRequest};

#[derive(Debug, Deserialize)]
pub struct DeployDiskdbBody {
    pub rpc_port: u16,
    /// Main listener port. When omitted, derived from `rpc_port`.
    #[serde(default)]
    pub listen_port: Option<u16>,
    /// Internal HTTP health port. When omitted, derived from `rpc_port`.
    #[serde(default)]
    pub http_port: Option<u16>,
}

fn validate_diskdb_ports(body: &DeployDiskdbBody) -> Result<(u16, u16, u16), (StatusCode, Json<ErrorBody>)> {
    let listen_port = body.listen_port.unwrap_or(body.rpc_port);
    let http_port = body.http_port.unwrap_or_else(|| body.rpc_port.saturating_add(1));
    let rpc_listen_port = body.rpc_port.saturating_add(2);
    let ports = [listen_port, http_port, rpc_listen_port];
    if ports.contains(&0) || body.rpc_port > 65_533 {
        return Err(err_400("DiskDB ports must be in the range 1..=65535"));
    }
    if ports[0] == ports[1] || ports[0] == ports[2] || ports[1] == ports[2] {
        return Err(err_400("DiskDB listener ports must be distinct"));
    }
    Ok((listen_port, http_port, rpc_listen_port))
}

/// `POST /api/nodes/:id/diskdb/deploy` — spawn `crowdb-diskdb` on the
/// node's workspace. Registers a `ServerEntry` with
/// `service_type: Diskdb` and tracks the PID.
///
/// Local-fork only (mirrors `deploy_diskdb_local`); `node.host` is
/// used for URL construction but the process spawns on the console
/// host. SSH deploy is a follow-up (C4 status — see
/// `lib/crowdb-console-shared/src/lifecycle.rs`).
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `409` if a diskdb instance is already deployed on the
/// node, `404` if the node doesn't exist, `502` on spawn failure.
pub async fn http_deploy_diskdb(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Json(body): Json<DeployDiskdbBody>,
) -> Result<(StatusCode, Json<DiskdbDeployResult>), (StatusCode, Json<ErrorBody>)> {
    let (listen_port, http_port, rpc_listen_port) = validate_diskdb_ports(&body)?;
    let node = {
        let cfg = state.config.read().unwrap();
        // Check for existing diskdb instance on this node.
        if cfg
            .servers
            .iter()
            .any(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Diskdb)
        {
            return Err((
                StatusCode::CONFLICT,
                Json(ErrorBody {
                    error: format!("node {node_id} already hosts a deployed diskdb instance"),
                }),
            ));
        }
        cfg.node(node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("node {node_id} not found"),
                }),
            )
        })?
    };

    // Look up the kv-server management URL(s) on this node so the
    // diskdb can discover group-0. If no kv-server is deployed, the
    // diskdb will fall back to the default seed port.
    let kv_server_mgmt_seeds: Vec<String> = {
        let cfg = state.config.read().unwrap();
        cfg.servers
            .iter()
            .filter(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Kv)
            .map(|s| s.url.clone())
            .collect()
    };

    let req = DiskdbDeployRequest {
        instance_id: None,
        metrics_interval: None,
        rpc_workers: None,
        kv_connections: None,
        kv_client_rpc_workers: None,
        keepalive_interval_secs: state.test_mode.then_some(1),
        server_id: format!("diskdb-{node_id}"),
        listen_port,
        http_port,
        rpc_port: rpc_listen_port,
        kv_server_mgmt_seeds,
    };

    let workspace_dir = state
        .prepare_node_workspace(node_id)
        .map_err(|e| err_500(e.to_string()))?;
    let deployed = lifecycle::deploy_diskdb_local(&req, &node, &workspace_dir)
        .await
        .map_err(|e| err_502(format!("diskdb deploy: {e}")))?;

    let entry = crowdb_console_shared::config::ServerEntry {
        id: format!("diskdb-{node_id}"),
        // DiskDB has no public HTTP management URL; retain its RPC endpoint
        // as the service URL and endpoint for persisted topology metadata.
        url: deployed.endpoint.clone(),
        node_id: Some(node_id),
        rpc_url: Some(deployed.endpoint.clone()),
        rest_port: None,
        rpc_port: Some(body.rpc_port),
        auto_start: true,
        binary: None,
        election_profile: None,
        pid: None,
        service_type: ServiceType::Diskdb,
        rpc_workers: None,
        no_fsync: false,
    };
    state.set_diskdb_runtime_pid(node_id, deployed.pid);
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_server(entry).map_err(|e| err_500(format!("{e}")))?;
    }
    state.persist().map_err(|e| err_500(format!("{e}")))?;
    // Refresh the monitor cache so health badges reflect the new DDB
    // process. The process may not be listening yet, so retry a few
    // times with short delays until the probe succeeds.
    crate::mgmt::refresh_node_cache(&state, node_id).await;
    let state_clone = state.clone();
    tokio::spawn(async move {
        for _ in 0..10 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            crate::mgmt::refresh_node_cache(&state_clone, node_id).await;
            let snap = state_clone.monitor_cache.snapshot().await;
            if let Some(rec) = snap.get(&node_id) {
                if rec.health == crowdb_console_shared::cluster::NodeHealth::Up {
                    break;
                }
            }
        }
    });

    Ok((
        StatusCode::CREATED,
        Json(DiskdbDeployResult {
            node_id,
            endpoint: deployed.endpoint,
            pid: deployed.pid,
        }),
    ))
}

/// `POST /api/nodes/:id/diskdb/restart` — stop and redeploy the
/// diskdb instance on a node, preserving ports and binary path from
/// the persisted `ServerEntry`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no diskdb instance is registered on the node,
/// `502` on spawn/stop failure.
#[allow(clippy::too_many_lines)]
pub async fn http_restart_diskdb(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<Json<DiskdbDeployResult>, (StatusCode, Json<ErrorBody>)> {
    let (entry, node) = {
        let cfg = state.config.read().unwrap();
        let entry = cfg
            .servers
            .iter()
            .find(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Diskdb)
            .cloned()
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorBody {
                        error: format!("no diskdb instance registered on node {node_id}"),
                    }),
                )
            })?;
        let node = cfg.node(node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("node {node_id} not found"),
                }),
            )
        })?;
        (entry, node)
    };

    let rpc_port = entry
        .rpc_port
        .or_else(|| entry.rpc_url.as_deref().and_then(crate::mgmt::port_of))
        .ok_or_else(|| err_500(format!("diskdb entry has malformed rpc_url: {:?}", entry.rpc_url)))?;

    // Stop existing process.
    if let Some(pid) = state.diskdb_runtime_pid(node_id) {
        let _ = tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
            .await
            .map_err(|e| err_500(format!("spawn_blocking (diskdb restart): {e}")))?
            .map_err(|e| err_500(format!("stop_pid (diskdb restart): {e}")))?;
    }

    // Look up the kv-server management URL(s) on this node so the
    // diskdb can discover group-0 after restart.
    let kv_server_mgmt_seeds: Vec<String> = {
        let cfg = state.config.read().unwrap();
        cfg.servers
            .iter()
            .filter(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Kv)
            .map(|s| s.url.clone())
            .collect()
    };

    // Backward-compat: derive from the old paired-port scheme.
    let listen_port = rpc_port;
    let http_port = rpc_port.saturating_add(1);
    let rpc_listen_port = rpc_port.saturating_add(2);
    let req = DiskdbDeployRequest {
        instance_id: None,
        metrics_interval: None,
        rpc_workers: None,
        kv_connections: None,
        kv_client_rpc_workers: None,
        keepalive_interval_secs: state.test_mode.then_some(1),
        server_id: format!("diskdb-{node_id}"),
        listen_port,
        http_port,
        rpc_port: rpc_listen_port,
        kv_server_mgmt_seeds,
    };
    let workspace_dir = state
        .prepare_node_workspace(node_id)
        .map_err(|e| err_500(e.to_string()))?;
    let deployed = lifecycle::deploy_diskdb_local(&req, &node, &workspace_dir)
        .await
        .map_err(|e| err_502(format!("diskdb redeploy (restart): {e}")))?;

    let new_entry = crowdb_console_shared::config::ServerEntry {
        id: entry.id.clone(),
        // DiskDB has no public HTTP management URL; retain its RPC endpoint
        // as the service URL and endpoint for persisted topology metadata.
        url: deployed.endpoint.clone(),
        node_id: Some(node_id),
        rpc_url: Some(deployed.endpoint.clone()),
        rest_port: None,
        rpc_port: Some(rpc_port),
        auto_start: entry.auto_start,
        binary: None,
        election_profile: None,
        pid: None,
        service_type: ServiceType::Diskdb,
        rpc_workers: None,
        no_fsync: false,
    };
    state.set_diskdb_runtime_pid(node_id, deployed.pid);
    {
        let mut cfg = state.config.write().unwrap();
        // Remove only the DDB entry (not the KV entry) before adding
        // the refreshed one. `remove_server_for_node` is KV-only.
        let pos = cfg
            .servers
            .iter()
            .position(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Diskdb);
        if let Some(p) = pos {
            cfg.servers.remove(p);
        }
        cfg.add_server(new_entry).map_err(|e| err_500(format!("{e}")))?;
    }
    state.persist().map_err(|e| err_500(format!("{e}")))?;
    // Refresh the monitor cache so health badges reflect the restarted
    // DDB process. The process may not be listening yet, so retry a few
    // times with short delays until the probe succeeds.
    crate::mgmt::refresh_node_cache(&state, node_id).await;
    let state_clone = state.clone();
    tokio::spawn(async move {
        for _ in 0..10 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            crate::mgmt::refresh_node_cache(&state_clone, node_id).await;
            let snap = state_clone.monitor_cache.snapshot().await;
            if let Some(rec) = snap.get(&node_id) {
                if rec.health == crowdb_console_shared::cluster::NodeHealth::Up {
                    break;
                }
            }
        }
    });

    Ok(Json(DiskdbDeployResult {
        node_id,
        endpoint: deployed.endpoint,
        pid: deployed.pid,
    }))
}

/// `POST /api/nodes/:id/diskdb/stop` — stop the diskdb process on a
/// node but keep its `ServerEntry` so it can be restarted later.
/// Mirrors `http_stop_node_server` for KV: stop process + clear PID
/// only; the deployment record is preserved for restart.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no diskdb instance is registered, `400` if no
/// tracked PID, `502` on stop failure.
pub async fn http_stop_diskdb(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<Json<crate::lifecycle::StopResult>, (StatusCode, Json<ErrorBody>)> {
    {
        let cfg = state.config.read().unwrap();
        let exists = cfg
            .servers
            .iter()
            .any(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Diskdb);
        if !exists {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no diskdb instance deployed on node {node_id}"),
                }),
            ));
        }
    }
    let Some(pid) = state.diskdb_runtime_pid(node_id) else {
        return Err(err_400(format!("diskdb on node {node_id} has no tracked pid")));
    };
    let sent = tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
        .await
        .map_err(|e| err_500(format!("spawn_blocking: {e}")))?
        .map_err(|e| err_500(format!("stop_pid: {e}")))?;
    state.clear_diskdb_runtime_pid(node_id);
    // Only mark the shared node record Down when no KV server is still
    // running on this node. The record is shared between KV and DDB; an
    // unconditional mark_down would flip the KV health badge (derived from
    // the record in build_server_process) even though KV is unaffected.
    // The DDB badge already drops via the no-pid override in
    // http_list_servers, which derives DDB health from the DDB pid alone.
    if state.runtime_pid(node_id).is_none() {
        state.monitor_cache.mark_down(node_id, "diskdb stopped").await;
    }
    Ok(Json(crate::lifecycle::StopResult { sent }))
}

/// `DELETE /api/nodes/:id/diskdb` — stop the diskdb instance (if a
/// PID is tracked) and remove its `ServerEntry`. Best-effort stop:
/// if no PID is tracked (e.g. console restarted after deploy), the
/// entry is still removed. Mirrors `http_delete_node_server` for KV.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no diskdb instance is registered on the node.
pub async fn http_delete_diskdb(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let pid = {
        let cfg = state.config.read().unwrap();
        if !cfg
            .servers
            .iter()
            .any(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Diskdb)
        {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no diskdb instance deployed on node {node_id}"),
                }),
            ));
        }
        state.diskdb_runtime_pid(node_id)
    };

    // Remove the deployment record first so the UI and /api/servers reflect
    // the deletion immediately. The process stop and cache refresh are
    // best-effort and run in the background.
    {
        let mut cfg = state.config.write().unwrap();
        let pos = cfg
            .servers
            .iter()
            .position(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Diskdb);
        if let Some(p) = pos {
            cfg.servers.remove(p);
        }
    }
    state.persist().map_err(|e| err_500(format!("{e}")))?;
    state.clear_diskdb_runtime_pid(node_id);

    let state_clone = state.clone();
    tokio::spawn(async move {
        // Best-effort stop: the PID may be gone (console restart).
        if let Some(pid) = pid {
            let _ = tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid)).await;
        }
        // Refresh the monitor cache: if KV is still running it stays Up;
        // if this was a DDB-only node, it marks Down.
        crate::mgmt::refresh_node_cache(&state_clone, node_id).await;
    });

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Serialize)]
pub struct DiskdbDeployResult {
    pub node_id: u64,
    /// Public crowdb-rpc endpoint used by `DiskDB` clients.
    pub endpoint: String,
    pub pid: u32,
}
