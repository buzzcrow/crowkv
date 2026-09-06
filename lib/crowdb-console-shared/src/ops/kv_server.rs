// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-kv-server` lifecycle: deploy, restart, stop, delete, list.
//!
//! Each function mutates the local TOML [`ConsoleConfig`] (adding /
//! removing [`ServerEntry`] records) and spawns / stops the server
//! process via [`lifecycle`] (local-fork) or [`ssh`] (remote SSH).

use std::path::PathBuf;

use crowdb_protocol::NodeId;

use crate::config::{ServerEntry, ServiceType};
use crate::error::{Error, Result};
use crate::lifecycle::{self, DeployRequest, DeployedServer};
use crate::ops::OpContext;

/// Deploy a `crowdb-kv-server` on a node.
///
/// `workspace_dir` is used for local-fork deploys (the server's data
/// directory); `None` uses the current directory. SSH deploys ignore it.
///
/// # Errors
/// Returns [`Error::NotFound`] if the node does not exist;
/// [`Error::Conflict`] if a server is already deployed on the node;
/// [`Error::NodeUnreachable`] if the deploy fails.
pub async fn deploy(
    ctx: &OpContext,
    req: &DeployRequest,
    workspace_dir: Option<&std::path::Path>,
) -> Result<DeployedServer> {
    let node_id: NodeId = req.server_id.parse().unwrap_or(0);
    let node = ctx.node_entry(node_id)?;

    // Check for existing deployment.
    if ctx.config().server_for_node(node_id).is_some() {
        return Err(Error::Conflict {
            kind: "server".into(),
            id: format!("node {node_id} already hosts a deployed server"),
        });
    }

    let binary = req.binary.as_ref().map(|p| p.to_string_lossy().to_string());
    let deployed = if node.ssh_enabled() {
        let server_bin = binary.clone().unwrap_or_else(|| {
            std::env::var("CROWDB_KV_SERVER_BIN").unwrap_or_else(|_| "crowdb-kv-server".into())
        });
        crate::ssh::deploy_via_ssh(req, &node, &server_bin)
            .await
            .map_err(|e| Error::NodeUnreachable {
                node_id: node_id.to_string(),
                reason: format!("ssh deploy: {e}"),
            })?
    } else {
        let result = match workspace_dir {
            Some(dir) => lifecycle::deploy_local_in_dir(req, &node, dir).await,
            None => lifecycle::deploy_local(req, &node).await,
        };
        result.map_err(|e| Error::NodeUnreachable {
            node_id: node_id.to_string(),
            reason: format!("local deploy: {e}"),
        })?
    };

    let entry = ServerEntry {
        id: node_id.to_string(),
        url: deployed.mgmt_url.clone(),
        node_id: Some(node_id),
        rpc_url: Some(deployed.rpc_url.clone()),
        rest_port: Some(req.rest_port),
        rpc_port: Some(req.rpc_port),
        auto_start: true,
        binary,
        election_profile: req.election_profile.clone(),
        pid: Some(deployed.pid),
        service_type: ServiceType::Kv,
        rpc_workers: req.rpc_workers,
        no_fsync: req.no_fsync,
    };
    {
        let mut cfg = ctx.config_mut();
        cfg.add_server(entry).map_err(|e| Error::Config(e.to_string()))?;
    }
    Ok(deployed)
}

/// Stop the `crowdb-kv-server` on a node (keep the deployment record).
///
/// `pid_override` lets callers supply an in-memory PID (e.g. the web's
/// `runtime_pid`) that takes precedence over the persisted `entry.pid`.
///
/// # Errors
/// Returns [`Error::NotFound`] if no server is deployed on the node or
/// no PID is available (neither override nor `entry.pid`).
pub async fn stop(ctx: &OpContext, node_id: NodeId, pid_override: Option<u32>) -> Result<bool> {
    let node = ctx.node_entry(node_id)?;
    let entry = ctx.server_for_node(node_id)?;
    let pid = pid_override.or(entry.pid).ok_or_else(|| Error::NotFound {
        kind: "server".into(),
        id: format!("node {node_id} has no tracked pid"),
    })?;

    let sent = if node.ssh_enabled() {
        crate::ssh::stop_via_ssh(&node, pid)
            .await
            .map_err(|e| Error::NodeUnreachable {
                node_id: node_id.to_string(),
                reason: format!("ssh stop: {e}"),
            })?
    } else {
        tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))??
    };

    // Clear the PID in the config.
    {
        let mut cfg = ctx.config_mut();
        if let Some(s) = cfg.server_for_node_mut(node_id) {
            s.pid = None;
        }
    }
    Ok(sent)
}

/// Restart the `crowdb-kv-server` on a node: stop (if running) and
/// re-deploy on the same recorded ports.
///
/// `workspace_dir` is used for local-fork redeploys; `None` uses the
/// current directory. SSH redeploys ignore it.
///
/// # Errors
/// Returns [`Error::NotFound`] if no server is deployed on the node.
pub async fn restart(
    ctx: &OpContext,
    node_id: NodeId,
    workspace_dir: Option<&std::path::Path>,
) -> Result<DeployedServer> {
    let node = ctx.node_entry(node_id)?;
    let entry = ctx.server_for_node(node_id)?;

    let rest_port = entry
        .rest_port
        .ok_or_else(|| Error::Config(format!("server entry for node {node_id} has no rest_port")))?;
    let rpc_port = entry
        .rpc_port
        .ok_or_else(|| Error::Config(format!("server entry for node {node_id} has no rpc_port")))?;

    // Stop the existing process if a PID is tracked.
    if let Some(pid) = entry.pid {
        if node.ssh_enabled() {
            let _ = crate::ssh::stop_via_ssh(&node, pid).await;
        } else {
            let _ = tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid)).await;
        }
    }

    let req = DeployRequest {
        server_id: node_id.to_string(),
        rest_port,
        rpc_port,
        binary: entry.binary.as_ref().map(PathBuf::from),
        election_profile: entry.election_profile.clone(),
        rpc_workers: entry.rpc_workers,
        no_fsync: entry.no_fsync,
        ..Default::default()
    };

    let deployed = if node.ssh_enabled() {
        let server_bin = std::env::var("CROWDB_KV_SERVER_BIN").unwrap_or_else(|_| "crowdb-kv-server".into());
        crate::ssh::deploy_via_ssh(&req, &node, &server_bin)
            .await
            .map_err(|e| Error::NodeUnreachable {
                node_id: node_id.to_string(),
                reason: format!("ssh redeploy: {e}"),
            })?
    } else {
        let result = match workspace_dir {
            Some(dir) => lifecycle::deploy_local_in_dir(&req, &node, dir).await,
            None => lifecycle::deploy_local(&req, &node).await,
        };
        result.map_err(|e| Error::NodeUnreachable {
            node_id: node_id.to_string(),
            reason: format!("local redeploy: {e}"),
        })?
    };

    // Update the server entry with the new PID + URLs.
    {
        let mut cfg = ctx.config_mut();
        let _ = cfg.remove_server_for_node(node_id);
        let new_entry = ServerEntry {
            id: node_id.to_string(),
            url: deployed.mgmt_url.clone(),
            node_id: Some(node_id),
            rpc_url: Some(deployed.rpc_url.clone()),
            rest_port: Some(rest_port),
            rpc_port: Some(rpc_port),
            auto_start: entry.auto_start,
            binary: entry.binary.clone(),
            election_profile: entry.election_profile.clone(),
            pid: Some(deployed.pid),
            service_type: entry.service_type,
            rpc_workers: entry.rpc_workers,
            no_fsync: entry.no_fsync,
        };
        cfg.add_server(new_entry)
            .map_err(|e| Error::Config(e.to_string()))?;
    }
    Ok(deployed)
}

/// Delete the server deployment on a node: stop the process and remove
/// the [`ServerEntry`] from the config. Refuses to delete if the node
/// still hosts replicas (require-empty check).
///
/// # Errors
/// Returns [`Error::NotFound`] if no server is deployed on the node;
/// [`Error::Conflict`] if the node still hosts replicas in group-0
/// sysdata (require-empty check).
pub async fn delete(ctx: &OpContext, node_id: NodeId) -> Result<()> {
    let node = ctx.node_entry(node_id)?;
    let entry = ctx.server_for_node(node_id)?;

    check_require_empty(ctx, node_id).await?;

    if let Some(pid) = entry.pid {
        if node.ssh_enabled() {
            let _ = crate::ssh::stop_via_ssh(&node, pid).await;
        } else {
            let _ = tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid)).await;
        }
    }

    {
        let mut cfg = ctx.config_mut();
        let _ = cfg.remove_server_for_node(node_id);
        cfg.purge_node_topology(node_id);
    }
    Ok(())
}

/// Check that no replicas in group-0 sysdata reference the given node.
/// Best-effort — if sysdata is unreachable, the check is skipped (the
/// cluster may not be initialized). Used by [`delete`] and by the web
/// handler's `DELETE /api/nodes/:id/server` to enforce the require-empty
/// constraint before stopping the server process.
///
/// # Errors
/// Returns [`Error::Conflict`] if the node still hosts replicas in
/// group-0 sysdata.
pub async fn check_require_empty(ctx: &OpContext, node_id: NodeId) -> Result<()> {
    if ctx.is_test_scenario() {
        return Ok(());
    }
    if let Ok(stores) = ctx.sysmd().list_stores().await {
        for store in &stores {
            if let Ok(groups) = ctx.sysmd().list_groups_in_store(store.store_id).await {
                for group in &groups {
                    if let Ok(replicas) = ctx
                        .sysmd()
                        .list_replicas_in_group(store.store_id, group.group_id)
                        .await
                    {
                        for replica in &replicas {
                            if replica.node_id == node_id {
                                return Err(Error::Conflict {
                                    kind: "server".into(),
                                    id: format!(
                                        "node {node_id} still hosts replica {} in store {}/group {}",
                                        replica.replica_id, store.store_id, group.group_id
                                    ),
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// List all deployed servers from the local config.
pub fn list(ctx: &OpContext) -> Vec<ServerEntry> {
    ctx.config().servers.clone()
}
