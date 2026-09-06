// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Topology restore: startup three-way fallback + per-node restore.

use crate::mgmt::{
    build_server_client, mgmt_url_for_node, port_of, refresh_node_cache, rpc_endpoint_for_node,
    rpc_is_conflict, rpc_is_not_found,
};
use crate::state::AppState;
use crowdb_console_shared::clients::http::ServerClient;
use crowdb_console_shared::cluster::NodeId;
use crowdb_console_shared::config::{GroupEntry, NodeEntry, ServerEntry, ServiceType, StoreEntry};
use crowdb_console_shared::lifecycle::{self, DeployRequest, DiskdbDeployRequest};
use crowdb_console_shared::mgmt::{AddGroupInitialRole, AddGroupRequest, AddStoreRequest};
use tracing::{info, warn};

/// Result of the three-way group 0 state check at console startup.
enum Group0State {
    /// No nodes deployed yet — first-run scenario.
    NoNodes,
    /// Group 0 not found on any reachable node — phase 1 (TOML mode).
    Missing,
    /// Group 0 exists — group 0 authoritative.
    Ready,
}

/// Check group 0 state across all deployed nodes to determine the
/// topology source at console startup.
async fn check_group0_state(state: &AppState) -> Group0State {
    let node_ids: Vec<NodeId> = {
        let cfg = state.config.read().unwrap();
        cfg.servers.iter().filter_map(|s| s.node_id).collect()
    };
    if node_ids.is_empty() {
        return Group0State::NoNodes;
    }
    for nid in &node_ids {
        let Ok(url) = mgmt_url_for_node(state, *nid) else {
            continue;
        };
        let Ok(client) = build_server_client(url) else {
            continue;
        };
        // Check if group 0 exists by listing stores.
        if let Ok(stores) = client.list_stores().await {
            if stores.iter().any(|s| s.store_id == 0) {
                return Group0State::Ready;
            }
        }
    }
    Group0State::Missing
}

/// Console startup three-way fallback. Checks group 0 state and picks
/// the right topology source before restoring.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
pub async fn startup_topology_check(state: &AppState) {
    match check_group0_state(state).await {
        Group0State::NoNodes => {
            info!("no nodes deployed; first-run scenario, skipping topology restore");
        }
        Group0State::Missing => {
            info!("group 0 not found on any node; TOML mode (phase 1)");
            restore_persisted_topology(state).await;
        }
        Group0State::Ready => {
            info!("group 0 is ready; loading topology from group 0 KV");
            restore_persisted_topology(state).await;
        }
    }
}

/// Restore persisted topology (servers, stores, groups, replicas) on startup.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
pub(crate) async fn restore_persisted_topology(state: &AppState) {
    let (nodes, servers, stores, groups) = {
        let cfg = state.config.read().unwrap();
        (
            cfg.nodes.clone(),
            cfg.servers.clone(),
            cfg.stores.clone(),
            cfg.groups.clone(),
        )
    };
    for server in &servers {
        let Some(node_id) = server.node_id else {
            continue;
        };
        let Some(node) = nodes.iter().find(|n| n.id == node_id) else {
            warn!(
                server_id = server.id,
                node_id, "skipping restore for server with missing node"
            );
            continue;
        };
        let result = if server.service_type == ServiceType::Diskdb {
            ensure_diskdb_running(state, node, server).await
        } else {
            ensure_server_running(state, node, server).await
        };
        if let Err(err) = result {
            warn!(server_id = server.id, node_id, error = %err, "failed to restore server process");
        }
    }
    for StoreEntry { store_id, nodes } in &stores {
        for node_id in nodes {
            if let Err(err) = ensure_store_on_node(state, *node_id, *store_id).await {
                warn!(store_id, node_id, error = %err, "failed to restore store");
            }
        }
    }
    for group in &groups {
        let mut replicas = group.replicas.clone();
        replicas.sort_by_key(|r| r.replica_id);
        // Defer the election driver for multi-replica groups until remotes are
        // wired.
        let start_election = Some(replicas.len() <= 1);
        for (index, replica) in replicas.iter().enumerate() {
            let initial_role = if index == 0 {
                AddGroupInitialRole::Leader
            } else {
                AddGroupInitialRole::Follower
            };
            if let Err(err) = ensure_group_local(
                state,
                replica.node_id,
                group.store_id,
                group.group_id,
                replica.replica_id,
                initial_role,
                start_election,
            )
            .await
            {
                warn!(
                    store_id = group.store_id,
                    group_id = group.group_id,
                    replica_id = replica.replica_id,
                    node_id = replica.node_id,
                    error = %err,
                    "failed to restore local group replica"
                );
            }
        }
        if let Err(err) = ensure_group_remotes(state, group).await {
            warn!(store_id = group.store_id, group_id = group.group_id, error = %err, "failed to restore group remotes");
        }
    }
    for server in &servers {
        if let Some(node_id) = server.node_id {
            refresh_node_cache(state, node_id).await;
        }
    }
    info!(
        servers = servers.len(),
        stores = stores.len(),
        groups = groups.len(),
        "restore reconcile finished"
    );
}

/// Restores persisted topology (stores and groups) for a specific node.
///
/// This function ensures that all stores and groups configured for the given node
/// are properly set up on the node after a restart.
///
/// # Panics
/// Panics if the config read lock is poisoned (should not happen in normal operation).
///
/// # Errors
/// Returns an error if store or group restoration fails.
pub(crate) async fn restore_persisted_topology_for_node(
    state: &AppState,
    node_id: NodeId,
) -> Result<(), String> {
    let (stores, groups) = {
        let cfg = state.config.read().unwrap();
        (cfg.stores.clone(), cfg.groups.clone())
    };

    for store in stores
        .iter()
        .filter(|store| store.nodes.iter().any(|id| id == &node_id))
    {
        ensure_store_on_node(state, node_id, store.store_id).await?;
    }

    for group in groups
        .iter()
        .filter(|group| group.replicas.iter().any(|replica| replica.node_id == node_id))
    {
        let Some(local_replica) = group.replicas.iter().find(|replica| replica.node_id == node_id) else {
            continue;
        };
        ensure_group_local(
            state,
            node_id,
            group.store_id,
            group.group_id,
            local_replica.replica_id,
            AddGroupInitialRole::Follower,
            // Defer for multi-replica groups until remotes are wired.
            Some(group.replicas.len() <= 1),
        )
        .await?;
        if let Err(err) = ensure_group_remotes(state, group).await {
            warn!(
                store_id = group.store_id,
                group_id = group.group_id,
                node_id,
                error = %err,
                "failed to restore group remotes for restarted node"
            );
        }
    }

    refresh_node_cache(state, node_id).await;
    Ok(())
}

async fn ensure_server_running(
    state: &AppState,
    node: &NodeEntry,
    server: &ServerEntry,
) -> Result<(), String> {
    let client = ServerClient::new(server.url.clone()).map_err(|e| e.to_string())?;
    if client.health().await.is_ok() {
        refresh_node_cache(state, node.id).await;
        return Ok(());
    }
    if !server.auto_start {
        return Ok(());
    }
    let rest_port = server
        .rest_port
        .ok_or_else(|| format!("server {} missing persisted rest_port", server.id))?;
    let rpc_port = server
        .rpc_port
        .ok_or_else(|| format!("server {} missing persisted rpc_port", server.id))?;
    let req = DeployRequest {
        server_id: server.id.clone(),
        rest_port,
        rpc_port,
        election_profile: server.election_profile.clone(),
        binary: server.binary.clone().map(std::path::PathBuf::from),
        ..Default::default()
    };
    let deployed = if node.ssh_enabled() {
        let server_bin = server.binary.clone().unwrap_or_else(|| {
            std::env::var("CROWDB_KV_SERVER_BIN").unwrap_or_else(|_| "crowdb-kv-server".to_string())
        });
        crowdb_console_shared::ssh::deploy_via_ssh(&req, node, &server_bin)
            .await
            .map_err(|e| e.to_string())?
    } else {
        let workspace_dir = state
            .prepare_node_workspace(node.id.to_string())
            .map_err(|e| e.to_string())?;
        lifecycle::deploy_local_in_dir(&req, node, &workspace_dir)
            .await
            .map_err(|e| e.to_string())?
    };
    state.set_runtime_pid(node.id, deployed.pid);
    refresh_node_cache(state, node.id).await;
    Ok(())
}

/// Restore a persisted `DiskDB` instance on startup. Mirrors
/// `ensure_server_running` but spawns `crowdb-diskdb` via
/// `deploy_diskdb_local` instead of the KV-server deploy path.
async fn ensure_diskdb_running(
    state: &AppState,
    node: &NodeEntry,
    server: &ServerEntry,
) -> Result<(), String> {
    // If the process is already alive, just refresh the cache.
    if let Some(pid) = state.diskdb_runtime_pid(node.id) {
        if lifecycle::process_is_alive(pid) {
            refresh_node_cache(state, node.id).await;
            return Ok(());
        }
        state.clear_diskdb_runtime_pid(node.id);
    }
    if !server.auto_start {
        return Ok(());
    }
    let rpc_port = server
        .rpc_port
        .or_else(|| server.rpc_url.as_deref().and_then(port_of))
        .ok_or_else(|| format!("diskdb entry {} missing persisted rpc_port", server.id))?;
    // Look up the kv-server management URL(s) on this node so the
    // diskdb can discover group-0 after restart.
    let kv_server_mgmt_seeds: Vec<String> = {
        let cfg = state.config.read().unwrap();
        cfg.servers
            .iter()
            .filter(|s| s.node_id == Some(node.id) && s.service_type == ServiceType::Kv)
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
        server_id: server.id.clone(),
        listen_port,
        http_port,
        rpc_port: rpc_listen_port,
        kv_server_mgmt_seeds,
    };
    let workspace_dir = state
        .prepare_node_workspace(node.id.to_string())
        .map_err(|e| e.to_string())?;
    let deployed = lifecycle::deploy_diskdb_local(&req, node, &workspace_dir)
        .await
        .map_err(|e| e.to_string())?;
    state.set_diskdb_runtime_pid(node.id, deployed.pid);
    // Update the persisted entry with the fresh RPC endpoint. The HTTP
    // readiness URL is intentionally lifecycle-local and is not persisted.
    {
        let mut cfg = state.config.write().unwrap();
        if let Some(entry) = cfg
            .servers
            .iter_mut()
            .find(|s| s.node_id == Some(node.id) && s.service_type == ServiceType::Diskdb)
        {
            entry.url.clone_from(&deployed.endpoint);
            entry.rpc_url = Some(deployed.endpoint.clone());
        }
    }
    state.persist().map_err(|e| e.to_string())?;
    refresh_node_cache(state, node.id).await;
    Ok(())
}

async fn ensure_store_on_node(state: &AppState, node_id: NodeId, store_id: u64) -> Result<(), String> {
    let url = mgmt_url_for_node(state, node_id).map_err(|(_, body)| body.0.error.clone())?;
    let client = ServerClient::new(url).map_err(|e| e.to_string())?;
    match client.get_store(store_id).await {
        Ok(_) => Ok(()),
        Err(err) if rpc_is_not_found(&err) => {
            client
                .add_store(&AddStoreRequest { store_id, port: None })
                .await
                .map_err(|e| e.to_string())?;
            refresh_node_cache(state, node_id).await;
            Ok(())
        }
        Err(err) => Err(err.to_string()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn ensure_group_local(
    state: &AppState,
    node_id: NodeId,
    store_id: u64,
    group_id: u64,
    replica_id: u64,
    initial_role: AddGroupInitialRole,
    // `Some(false)` for multi-replica groups so the server does not self-elect
    // at `quorum == 1` before remotes are wired; the
    // following `ensure_group_remotes` rebuild starts the driver with a correct
    // quorum. `None`/`Some(true)` for single-replica groups (no remote-wiring
    // step to start the driver).
    start_election: Option<bool>,
) -> Result<(), String> {
    let url = mgmt_url_for_node(state, node_id).map_err(|(_, body)| body.0.error.clone())?;
    let client = ServerClient::new(url).map_err(|e| e.to_string())?;
    match client.list_groups(store_id).await {
        Ok(groups)
            if groups
                .iter()
                .any(|g| g.group_id == group_id && g.local_replica_id == replica_id) =>
        {
            Ok(())
        }
        Ok(_) => {
            client
                .add_group(
                    store_id,
                    &AddGroupRequest {
                        group_id,
                        replica_id,
                        initial_role: Some(initial_role),
                        start_election,
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
            refresh_node_cache(state, node_id).await;
            Ok(())
        }
        Err(err) if rpc_is_not_found(&err) => {
            ensure_store_on_node(state, node_id, store_id).await?;
            client
                .add_group(
                    store_id,
                    &AddGroupRequest {
                        group_id,
                        replica_id,
                        initial_role: Some(initial_role),
                        start_election,
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
            refresh_node_cache(state, node_id).await;
            Ok(())
        }
        Err(err) if rpc_is_conflict(&err) => Ok(()),
        Err(err) => Err(err.to_string()),
    }
}

async fn ensure_group_remotes(state: &AppState, group: &GroupEntry) -> Result<(), String> {
    for replica in &group.replicas {
        refresh_node_cache(state, replica.node_id).await;
    }
    for replica in &group.replicas {
        let url = mgmt_url_for_node(state, replica.node_id).map_err(|(_, body)| body.0.error.clone())?;
        let client = ServerClient::new(url).map_err(|e| e.to_string())?;
        let existing = client
            .list_remote_replicas(group.store_id, group.group_id)
            .await
            .map_err(|e| e.to_string())?;
        let mut to_update = Vec::new();
        for peer in &group.replicas {
            if peer.replica_id == replica.replica_id {
                continue;
            }
            let Some(current_endpoint) = rpc_endpoint_for_node(state, peer.node_id, group.store_id).await
            else {
                // Peer's store is not up yet; skip rather than overwriting
                // the correct persisted-config endpoint with a stale one.
                continue;
            };
            let existing_entry = existing.iter().find(|r| r.replica_id == peer.replica_id);
            let needs_update = match existing_entry {
                None => true,
                Some(r) => r.endpoint != current_endpoint,
            };
            if needs_update {
                to_update.push(crowdb_console_shared::mgmt::RemoteReplicaInfo {
                    replica_id: peer.replica_id,
                    endpoint: current_endpoint,
                    voting: true,
                });
            }
        }
        if !to_update.is_empty() {
            client
                .add_remote_replicas(group.store_id, group.group_id, &to_update)
                .await
                .map_err(|e| e.to_string())?;
            refresh_node_cache(state, replica.node_id).await;
        }
    }
    Ok(())
}
