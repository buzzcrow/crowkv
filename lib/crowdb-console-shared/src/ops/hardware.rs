// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Hardware hierarchy operations: rack/node/disk-group/disk CRUD.
//!
//! Each function writes to the local TOML [`ConsoleConfig`] first (the
//! operator's intent), then best-effort syncs the change into group-0
//! sysdata via [`CrowdbSysmdClient`]. The sysdata sync is best-effort
//! because the cluster may not be initialized yet (e.g. the first rack
//! and node are added before `cluster init`).

use crowdb_protocol::common::{HwStatus, NodeValue, RackValue};

use crate::config::{DiskEntry, DiskGroupEntry, NodeEntry, RackEntry};
use crate::error::{Error, Result};
use crate::ops::OpContext;

// ── rack ────────────────────────────────────────────────────────

/// Add a rack to the local config and group-0 sysdata.
///
/// # Errors
/// Returns [`Error::Conflict`] if the rack id already exists.
pub async fn add_rack(ctx: &OpContext, rack_id: u64, name: &str) -> Result<RackEntry> {
    let entry = RackEntry {
        id: rack_id,
        name: name.to_string(),
    };
    {
        let mut cfg = ctx.config_mut();
        cfg.add_rack(entry.clone())?;
    }
    if !ctx.is_test_scenario() {
        // Best-effort sysdata sync.
        let value = RackValue {
            status: HwStatus::Up as i32,
            node_ids: Vec::new(),
        };
        let _ = ctx.sysmd().add_rack(rack_id, &value).await;
    }
    Ok(entry)
}

/// Remove a rack from the local config and cascade-remove from sysdata.
///
/// # Errors
/// Returns [`Error::NotFound`] if the rack does not exist; [`Error::Conflict`]
/// if any node still references it.
pub async fn remove_rack(ctx: &OpContext, rack_id: u64) -> Result<()> {
    {
        let mut cfg = ctx.config_mut();
        cfg.remove_rack(rack_id)?;
    }
    if !ctx.is_test_scenario() {
        let _ = ctx.sysmd().remove_rack_cascade(rack_id).await;
    }
    Ok(())
}

/// List racks from the local config.
pub fn list_racks(ctx: &OpContext) -> Vec<RackEntry> {
    ctx.config().racks.clone()
}

// ── node ────────────────────────────────────────────────────────

/// Add a node to the local config and group-0 sysdata.
///
/// # Errors
/// Returns [`Error::Conflict`] if the node id already exists;
/// [`Error::Validation`] if the rack does not exist.
pub async fn add_node(ctx: &OpContext, entry: NodeEntry) -> Result<NodeEntry> {
    {
        let mut cfg = ctx.config_mut();
        cfg.add_node(entry.clone())?;
    }
    if !ctx.is_test_scenario() {
        let value = NodeValue {
            status: HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: Vec::new(),
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
        };
        let _ = ctx.sysmd().add_node(entry.rack_id, entry.id, &value).await;
    }
    Ok(entry)
}

/// Remove a node from the local config and cascade-remove from sysdata.
///
/// # Errors
/// Returns [`Error::NotFound`] if the node does not exist; [`Error::Conflict`]
/// if a server is still deployed on the node.
pub async fn remove_node(ctx: &OpContext, node_id: u64) -> Result<()> {
    let rack_id = {
        let cfg = ctx.config();
        cfg.node(node_id).map(|n| n.rack_id)
    };
    {
        let mut cfg = ctx.config_mut();
        cfg.remove_node(node_id)?;
    }
    if !ctx.is_test_scenario() {
        if let Some(rack_id) = rack_id {
            let _ = ctx.sysmd().remove_node_cascade(rack_id, node_id).await;
        }
    }
    Ok(())
}

/// List nodes from the local config, optionally filtered by rack.
pub fn list_nodes(ctx: &OpContext, rack_id: Option<u64>) -> Vec<NodeEntry> {
    let cfg = ctx.config();
    match rack_id {
        Some(rid) => cfg.nodes.iter().filter(|n| n.rack_id == rid).cloned().collect(),
        None => cfg.nodes.clone(),
    }
}

// ── disk-group ──────────────────────────────────────────────────

/// Add a disk-group to the local config and group-0 sysdata.
///
/// Writes to config first (operator intent), then best-effort syncs
/// to sysdata. The sysdata sync is best-effort because the cluster
/// may not be initialized yet.
///
/// # Errors
/// Returns [`Error::NotFound`] if the node does not exist;
/// [`Error::Conflict`] if the disk-group id already exists.
pub async fn add_disk_group(ctx: &OpContext, node_id: u64, dg_id: u64, name: &str) -> Result<DiskGroupEntry> {
    let rack_id = {
        let cfg = ctx.config();
        cfg.node(node_id)
            .map(|n| n.rack_id)
            .ok_or_else(|| Error::NotFound {
                kind: "node".into(),
                id: node_id.to_string(),
            })?
    };
    let entry = DiskGroupEntry {
        id: dg_id,
        rack_id,
        node_id,
        name: name.to_string(),
    };
    {
        let mut cfg = ctx.config_mut();
        cfg.add_disk_group(entry.clone())?;
    }
    if !ctx.is_test_scenario() {
        let value = crowdb_protocol::diskdb::rpc::DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: Vec::new(),
        };
        let _ = ctx.sysmd().add_disk_group(rack_id, node_id, dg_id, &value).await;
    }
    Ok(entry)
}

/// Remove a disk-group and its child disks from the local config and
/// group-0 sysdata (cascade).
///
/// Removes child disks from config first, then the disk-group, then
/// best-effort cascade-removes from sysdata.
///
/// # Errors
/// Returns [`Error::NotFound`] if the disk-group does not exist;
/// [`Error::Conflict`] if the disk-group still has disks (config-level
/// check prevents orphaned disks).
pub async fn remove_disk_group(ctx: &OpContext, node_id: u64, dg_id: u64) -> Result<()> {
    let rack_id = {
        let cfg = ctx.config();
        let dg = cfg.disk_group(dg_id);
        if dg.is_none() {
            return Err(Error::NotFound {
                kind: "disk_group".into(),
                id: dg_id.to_string(),
            });
        }
        dg.map(|d| d.rack_id)
    };
    let disk_ids: Vec<String> = {
        let cfg = ctx.config();
        cfg.disks_in_group(dg_id)
            .iter()
            .map(|d| d.disk_id.clone())
            .collect()
    };
    {
        let mut cfg = ctx.config_mut();
        for disk_id in &disk_ids {
            cfg.remove_disk(disk_id)?;
        }
        cfg.remove_disk_group(dg_id)?;
    }
    if !ctx.is_test_scenario() {
        if let Some(rack_id) = rack_id {
            let _ = ctx
                .sysmd()
                .remove_disk_group_cascade(rack_id, node_id, dg_id)
                .await;
        }
    }
    Ok(())
}

/// List disk-groups on a node from the local config.
pub fn list_disk_groups(ctx: &OpContext, node_id: u64) -> Vec<DiskGroupEntry> {
    let cfg = ctx.config();
    cfg.disk_groups
        .iter()
        .filter(|dg| dg.node_id == node_id)
        .cloned()
        .collect()
}

// ── disk ────────────────────────────────────────────────────────

/// Input for adding a disk. Mirrors the web handler's `AddDiskBody`.
#[derive(Debug, Clone)]
pub struct AddDiskInput {
    pub disk_id: String,
    pub disk_type: String,
    pub capacity_bytes: u64,
    pub zone_size_bytes: u64,
    pub unit_size_bytes: u32,
    pub device_path: String,
}

/// Validate a single disk-add input, producing the `DiskEntry`, proto
/// `DiskId`, and `DiskValue` for group-0 sysdata sync.
///
/// # Errors
/// Returns [`Error::Validation`] on invalid `disk_id` format, zero
/// unit/zone sizes, non-multiple `zone_size`, or capacity < `zone_size`.
/// Returns [`Error::Validation`] on unknown `disk_type`.
fn validate_disk_input(
    input: &AddDiskInput,
    dg_id: u64,
    rack_id: u64,
    node_id: u64,
) -> Result<(
    DiskEntry,
    crowdb_protocol::common::DiskId,
    crowdb_protocol::diskdb::rpc::DiskValue,
)> {
    use crowdb_protocol::diskdb_type_util::DiskIdExt;

    let disk_id_proto =
        crowdb_protocol::common::DiskId::from_display_string(&input.disk_id).map_err(|e| {
            Error::Validation {
                field: "disk_id".into(),
                message: format!("invalid `disk_id` format for {}: {e}", input.disk_id),
            }
        })?;

    if input.unit_size_bytes == 0 || input.zone_size_bytes == 0 {
        return Err(Error::Validation {
            field: "unit_size_bytes".into(),
            message: format!(
                "disk {}: unit_size_bytes and zone_size_bytes must be non-zero",
                input.disk_id
            ),
        });
    }
    if input.zone_size_bytes % u64::from(input.unit_size_bytes) != 0 {
        return Err(Error::Validation {
            field: "zone_size_bytes".into(),
            message: format!(
                "disk {}: zone_size_bytes must be a multiple of unit_size_bytes",
                input.disk_id
            ),
        });
    }
    if input.capacity_bytes < input.zone_size_bytes {
        return Err(Error::Validation {
            field: "capacity_bytes".into(),
            message: format!(
                "disk {}: capacity_bytes must be >= zone_size_bytes",
                input.disk_id
            ),
        });
    }

    let unit_size = u64::from(input.unit_size_bytes);
    let capacity_units = input.capacity_bytes / unit_size;
    let zone_size_units = input.zone_size_bytes / unit_size;
    let zone_count = u32::try_from(capacity_units / zone_size_units).unwrap_or(u32::MAX);

    let disk_type_proto = match input.disk_type.as_str() {
        "Hdd" | "BLOCK_HDD" => crowdb_protocol::diskdb::rpc::DiskType::BlockHdd as i32,
        "Ssd" | "BLOCK_SSD" => crowdb_protocol::diskdb::rpc::DiskType::BlockSsd as i32,
        "ZONE_SSD" => crowdb_protocol::diskdb::rpc::DiskType::ZoneSsd as i32,
        "SMR_HDD" => crowdb_protocol::diskdb::rpc::DiskType::SmrHdd as i32,
        other => {
            return Err(Error::Validation {
                field: "disk_type".into(),
                message: format!("disk {}: unknown `disk_type`: {other}", input.disk_id),
            });
        }
    };

    let entry = DiskEntry {
        disk_id: input.disk_id.clone(),
        disk_group_id: dg_id,
        rack_id,
        node_id,
        disk_type: input.disk_type.clone(),
        capacity_bytes: input.capacity_bytes,
        zone_size_bytes: input.zone_size_bytes,
        unit_size_bytes: input.unit_size_bytes,
        device_path: input.device_path.clone(),
    };

    let value = crowdb_protocol::diskdb::rpc::DiskValue {
        status: HwStatus::Up as i32,
        disk_type: disk_type_proto,
        capacity_units,
        zone_size_units,
        unit_size_bytes: input.unit_size_bytes,
        zone_count,
        device_path: input.device_path.clone(),
    };

    Ok((entry, disk_id_proto, value))
}

/// Add a single disk to the local config and group-0 sysdata.
///
/// Validates the input, writes to config, then best-effort syncs to
/// sysdata.
///
/// # Errors
/// Returns [`Error::NotFound`] if the disk-group does not exist;
/// [`Error::Validation`] on invalid input; [`Error::Conflict`] on
/// duplicate `disk_id`.
pub async fn add_disk(ctx: &OpContext, node_id: u64, dg_id: u64, input: &AddDiskInput) -> Result<DiskEntry> {
    let rack_id = {
        let cfg = ctx.config();
        cfg.disk_groups
            .iter()
            .find(|dg| dg.node_id == node_id && dg.id == dg_id)
            .map(|dg| dg.rack_id)
            .ok_or_else(|| Error::NotFound {
                kind: "disk_group".into(),
                id: format!("{dg_id} on node {node_id}"),
            })?
    };
    let (entry, disk_id_proto, value) = validate_disk_input(input, dg_id, rack_id, node_id)?;
    {
        let mut cfg = ctx.config_mut();
        cfg.add_disk(entry.clone())?;
    }
    if !ctx.is_test_scenario() {
        let _ = ctx
            .sysmd()
            .add_disk(rack_id, node_id, dg_id, &disk_id_proto, &value)
            .await;
    }
    Ok(entry)
}

/// Add multiple disks in one call. Validates all inputs and checks for
/// duplicates (against config and within the batch) before mutating
/// config. Atomic all-or-nothing on the config mutation: if any
/// `add_disk` fails, the ones already added are rolled back.
/// Best-effort sysdata sync per disk.
///
/// # Errors
/// Returns [`Error::NotFound`] if the disk-group does not exist;
/// [`Error::Validation`] on invalid input; [`Error::Conflict`] on
/// duplicate `disk_id` (in config or within the batch).
pub async fn add_disks_batch(
    ctx: &OpContext,
    node_id: u64,
    dg_id: u64,
    inputs: &[AddDiskInput],
) -> Result<Vec<DiskEntry>> {
    use std::collections::HashSet;

    let rack_id = {
        let cfg = ctx.config();
        cfg.disk_groups
            .iter()
            .find(|dg| dg.node_id == node_id && dg.id == dg_id)
            .map(|dg| dg.rack_id)
            .ok_or_else(|| Error::NotFound {
                kind: "disk_group".into(),
                id: format!("{dg_id} on node {node_id}"),
            })?
    };

    let mut validated = Vec::with_capacity(inputs.len());
    let mut seen_ids: HashSet<String> = HashSet::with_capacity(inputs.len());
    for input in inputs {
        if !seen_ids.insert(input.disk_id.clone()) {
            return Err(Error::Conflict {
                kind: "disk".into(),
                id: format!("duplicate disk_id within batch: {}", input.disk_id),
            });
        }
        validated.push(validate_disk_input(input, dg_id, rack_id, node_id)?);
    }

    let mut added: Vec<DiskEntry> = Vec::with_capacity(validated.len());
    {
        let mut cfg = ctx.config_mut();
        for (entry, _, _) in &validated {
            if let Err(e) = cfg.add_disk(entry.clone()) {
                for a in &added {
                    let _ = cfg.remove_disk(&a.disk_id);
                }
                return Err(e);
            }
            added.push(entry.clone());
        }
    }

    if !ctx.is_test_scenario() {
        for (entry, disk_id_proto, value) in &validated {
            let _ = ctx
                .sysmd()
                .add_disk(rack_id, node_id, dg_id, disk_id_proto, value)
                .await;
            let _ = &entry;
        }
    }

    Ok(added)
}

/// Remove a disk from the local config and group-0 sysdata.
///
/// # Errors
/// Returns [`Error::NotFound`] if the disk does not exist.
pub async fn remove_disk(ctx: &OpContext, node_id: u64, dg_id: u64, disk_id: &str) -> Result<DiskEntry> {
    use crowdb_protocol::diskdb_type_util::DiskIdExt;

    let (rack_id, entry) = {
        let cfg = ctx.config();
        let disk = cfg
            .disks
            .iter()
            .find(|d| d.node_id == node_id && d.disk_group_id == dg_id && d.disk_id == disk_id)
            .ok_or_else(|| Error::NotFound {
                kind: "disk".into(),
                id: disk_id.to_string(),
            })?;
        (disk.rack_id, disk.clone())
    };
    {
        let mut cfg = ctx.config_mut();
        cfg.remove_disk(disk_id)?;
    }
    if !ctx.is_test_scenario() {
        if let Ok(disk_id_proto) = crowdb_protocol::common::DiskId::from_display_string(disk_id) {
            let _ = ctx
                .sysmd()
                .remove_disk(rack_id, node_id, dg_id, &disk_id_proto)
                .await;
        }
    }
    Ok(entry)
}

/// List disks in a disk-group from the local config.
pub fn list_disks(ctx: &OpContext, node_id: u64, dg_id: u64) -> Vec<DiskEntry> {
    let cfg = ctx.config();
    cfg.disks
        .iter()
        .filter(|d| d.node_id == node_id && d.disk_group_id == dg_id)
        .cloned()
        .collect()
}

/// Set a disk's status in group-0 sysdata (best-effort).
///
/// # Errors
/// Returns [`Error::NotFound`] if the disk does not exist in config.
pub async fn set_disk_status(
    ctx: &OpContext,
    node_id: u64,
    dg_id: u64,
    disk_id: &str,
    status: HwStatus,
) -> Result<()> {
    use crowdb_protocol::diskdb_type_util::DiskIdExt;

    let rack_id = {
        let cfg = ctx.config();
        cfg.disks
            .iter()
            .find(|d| d.node_id == node_id && d.disk_group_id == dg_id && d.disk_id == disk_id)
            .map(|d| d.rack_id)
            .ok_or_else(|| Error::NotFound {
                kind: "disk".into(),
                id: disk_id.to_string(),
            })?
    };
    if !ctx.is_test_scenario() {
        if let Ok(disk_id_proto) = crowdb_protocol::common::DiskId::from_display_string(disk_id) {
            let _ = ctx
                .sysmd()
                .set_disk_status(rack_id, node_id, dg_id, &disk_id_proto, status)
                .await;
        }
    }
    Ok(())
}

/// Set a disk-group's status in group-0 sysdata (best-effort).
///
/// # Errors
/// Returns [`Error::NotFound`] if the disk-group does not exist in config.
pub async fn set_disk_group_status(
    ctx: &OpContext,
    node_id: u64,
    dg_id: u64,
    status: HwStatus,
) -> Result<()> {
    let rack_id = {
        let cfg = ctx.config();
        cfg.disk_group(dg_id)
            .filter(|dg| dg.node_id == node_id)
            .map(|d| d.rack_id)
            .ok_or_else(|| Error::NotFound {
                kind: "disk_group".into(),
                id: dg_id.to_string(),
            })?
    };
    if !ctx.is_test_scenario() {
        let _ = ctx
            .sysmd()
            .set_disk_group_status(rack_id, node_id, dg_id, status)
            .await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConsoleConfig, NodeEntry, RackEntry};

    fn test_ctx(config: ConsoleConfig) -> OpContext {
        OpContext::new_for_test("127.0.0.1:59999".into(), Vec::new(), config)
    }

    fn config_with_rack_node_dg() -> ConsoleConfig {
        let mut config = ConsoleConfig::default();
        config
            .add_rack(RackEntry {
                id: 1,
                name: "r1".into(),
            })
            .unwrap();
        config
            .add_node(NodeEntry {
                id: 10,
                rack_id: 1,
                host: "127.0.0.1".into(),
                ssh_port: 22,
                ssh_user: String::new(),
                ssh_key: None,
                ssh_password: None,
            })
            .unwrap();
        config
            .add_disk_group(DiskGroupEntry {
                id: 100,
                rack_id: 1,
                node_id: 10,
                name: "dg1".into(),
            })
            .unwrap();
        config
    }

    fn valid_disk_input(id: &str) -> AddDiskInput {
        AddDiskInput {
            disk_id: id.to_string(),
            disk_type: "Hdd".into(),
            capacity_bytes: 4 * 1024 * 1024 * 1024,
            zone_size_bytes: 32 * 1024 * 1024,
            unit_size_bytes: 1024 * 1024,
            device_path: String::new(),
        }
    }

    #[tokio::test]
    async fn add_disk_group_unknown_node_returns_not_found() {
        let ctx = test_ctx(ConsoleConfig::default());
        let result = add_disk_group(&ctx, 999, 1, "test").await;
        assert!(matches!(result, Err(Error::NotFound { ref kind, .. }) if kind == "node"));
    }

    #[tokio::test]
    async fn add_disk_group_duplicate_returns_conflict() {
        let ctx = test_ctx(config_with_rack_node_dg());
        let result = add_disk_group(&ctx, 10, 100, "dup").await;
        assert!(matches!(result, Err(Error::Conflict { ref kind, .. }) if kind == "disk_group"));
    }

    #[tokio::test]
    async fn add_disk_group_succeeds() {
        let ctx = test_ctx(config_with_rack_node_dg());
        let entry = add_disk_group(&ctx, 10, 200, "new-dg").await.unwrap();
        assert_eq!(entry.id, 200);
        assert_eq!(entry.node_id, 10);
        assert_eq!(entry.rack_id, 1);
        assert_eq!(entry.name, "new-dg");
    }

    #[tokio::test]
    async fn remove_disk_group_with_disks_cascades() {
        let ctx = test_ctx(config_with_rack_node_dg());
        add_disk(
            &ctx,
            10,
            100,
            &valid_disk_input("00000000000000000000000000000001"),
        )
        .await
        .unwrap();
        remove_disk_group(&ctx, 10, 100).await.unwrap();
        assert!(ctx.config().disk_group(100).is_none());
        assert!(ctx.config().disks.is_empty());
    }

    #[tokio::test]
    async fn remove_disk_group_unknown_returns_not_found() {
        let ctx = test_ctx(ConsoleConfig::default());
        let result = remove_disk_group(&ctx, 10, 999).await;
        assert!(matches!(result, Err(Error::NotFound { ref kind, .. }) if kind == "disk_group"));
    }

    #[tokio::test]
    async fn add_disk_unknown_disk_group_returns_not_found() {
        let ctx = test_ctx(config_with_rack_node_dg());
        let result = add_disk(
            &ctx,
            10,
            999,
            &valid_disk_input("00000000000000000000000000000001"),
        )
        .await;
        assert!(matches!(result, Err(Error::NotFound { ref kind, .. }) if kind == "disk_group"));
    }

    #[tokio::test]
    async fn add_disk_invalid_disk_id_returns_validation() {
        let ctx = test_ctx(config_with_rack_node_dg());
        let mut input = valid_disk_input("not-a-uuid");
        input.disk_id = "bad-format".into();
        let result = add_disk(&ctx, 10, 100, &input).await;
        assert!(matches!(result, Err(Error::Validation { ref field, .. }) if field == "disk_id"));
    }

    #[tokio::test]
    async fn add_disk_zero_unit_size_returns_validation() {
        let ctx = test_ctx(config_with_rack_node_dg());
        let mut input = valid_disk_input("00000000000000000000000000000001");
        input.unit_size_bytes = 0;
        let result = add_disk(&ctx, 10, 100, &input).await;
        assert!(matches!(result, Err(Error::Validation { .. })));
    }

    #[tokio::test]
    async fn add_disk_duplicate_returns_conflict() {
        let ctx = test_ctx(config_with_rack_node_dg());
        let input = valid_disk_input("00000000000000000000000000000001");
        add_disk(&ctx, 10, 100, &input).await.unwrap();
        let result = add_disk(&ctx, 10, 100, &input).await;
        assert!(matches!(result, Err(Error::Conflict { ref kind, .. }) if kind == "disk"));
    }

    #[tokio::test]
    async fn add_disks_batch_atomic_rollback_on_duplicate_in_config() {
        let ctx = test_ctx(config_with_rack_node_dg());
        let input1 = valid_disk_input("00000000000000000000000000000001");
        add_disk(&ctx, 10, 100, &input1).await.unwrap();

        let batch = vec![
            valid_disk_input("00000000000000000000000000000002"),
            input1.clone(),
        ];
        let result = add_disks_batch(&ctx, 10, 100, &batch).await;
        assert!(result.is_err());

        // The first disk in the batch (00000002) should NOT be in config
        // because the batch was rolled back.
        assert!(
            !ctx.config()
                .disks
                .iter()
                .any(|d| d.disk_id == "00000000000000000000000000000002"),
            "batch rollback must not leave partial disks in config"
        );
    }

    #[tokio::test]
    async fn add_disks_batch_rejects_duplicate_within_batch() {
        let ctx = test_ctx(config_with_rack_node_dg());
        let input = valid_disk_input("00000000000000000000000000000001");
        let batch = vec![input.clone(), input];
        let result = add_disks_batch(&ctx, 10, 100, &batch).await;
        assert!(matches!(result, Err(Error::Conflict { ref kind, .. }) if kind == "disk"));
        assert!(ctx.config().disks.is_empty());
    }

    #[tokio::test]
    async fn add_disks_batch_succeeds_for_valid_inputs() {
        let ctx = test_ctx(config_with_rack_node_dg());
        let batch = vec![
            valid_disk_input("00000000000000000000000000000001"),
            valid_disk_input("00000000000000000000000000000002"),
        ];
        let added = add_disks_batch(&ctx, 10, 100, &batch).await.unwrap();
        assert_eq!(added.len(), 2);
        assert_eq!(ctx.config().disks.len(), 2);
    }

    #[tokio::test]
    async fn remove_disk_unknown_returns_not_found() {
        let ctx = test_ctx(config_with_rack_node_dg());
        let result = remove_disk(&ctx, 10, 100, "nonexistent").await;
        assert!(matches!(result, Err(Error::NotFound { ref kind, .. }) if kind == "disk"));
    }

    #[tokio::test]
    async fn remove_disk_succeeds() {
        let ctx = test_ctx(config_with_rack_node_dg());
        let input = valid_disk_input("00000000000000000000000000000001");
        add_disk(&ctx, 10, 100, &input).await.unwrap();
        let removed = remove_disk(&ctx, 10, 100, &input.disk_id).await.unwrap();
        assert_eq!(removed.disk_id, input.disk_id);
        assert!(ctx.config().disks.is_empty());
    }

    #[tokio::test]
    async fn list_disk_groups_filters_by_node() {
        let ctx = test_ctx(config_with_rack_node_dg());
        let dgs = list_disk_groups(&ctx, 10);
        assert_eq!(dgs.len(), 1);
        assert_eq!(dgs[0].id, 100);
        let dgs_other = list_disk_groups(&ctx, 999);
        assert!(dgs_other.is_empty());
    }

    #[tokio::test]
    async fn list_disks_filters_by_node_and_group() {
        let ctx = test_ctx(config_with_rack_node_dg());
        add_disk(
            &ctx,
            10,
            100,
            &valid_disk_input("00000000000000000000000000000001"),
        )
        .await
        .unwrap();
        let disks = list_disks(&ctx, 10, 100);
        assert_eq!(disks.len(), 1);
        let disks_other = list_disks(&ctx, 10, 999);
        assert!(disks_other.is_empty());
    }
}
