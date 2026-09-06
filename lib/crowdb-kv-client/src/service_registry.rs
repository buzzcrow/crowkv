// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::missing_errors_doc, clippy::cast_possible_truncation)]

//! [`ServiceRegistryClient`]: group-0 service instance registry.
//!
//! Generic across services (diskdb, kv-server, future services). Each
//! instance registers under `/srv/<service>/<instance_id>` and
//! heartbeats periodically. Readers filter expired entries by
//! `last_heartbeat_ms` against a configurable TTL.
//!
//! See `doc/design/kv/design-crowdb-kv-group0.md` §4.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_protocol::common::{DiskGroupUsageSummary, DiskdbExtra, InstanceValue, ServiceExtra};
use crowdb_protocol::common_type::InstanceId;
use crowdb_protocol::key::InstanceKey;

use crate::client::GetOutcome;
use crate::{CrowdbKvClient, Error, Result};

const G0_STORE: u64 = 0;
const G0_GROUP: u64 = 0;

/// Default liveness TTL: 3× a 5s heartbeat interval = 15s.
const DEFAULT_TTL_MS: u64 = 15_000;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Client for the service instance registry in group 0.
///
/// All methods target store 0, group 0. The wrapped `CrowdbKvClient`
/// must have its topology seeded with a group-0 leader endpoint.
#[derive(Clone)]
pub struct ServiceRegistryClient {
    kv: Arc<CrowdbKvClient>,
}

impl ServiceRegistryClient {
    /// Wrap a `CrowdbKvClient` for group-0 service registry access.
    #[must_use]
    pub fn new(kv: CrowdbKvClient) -> Self {
        Self { kv: Arc::new(kv) }
    }

    /// Wrap an already-shared `CrowdbKvClient` for group-0 service registry access.
    #[must_use]
    pub fn from_shared(kv: Arc<CrowdbKvClient>) -> Self {
        Self { kv }
    }

    /// Access the underlying `CrowdbKvClient`.
    #[must_use]
    pub fn kv(&self) -> &CrowdbKvClient {
        &self.kv
    }

    /// Register a service instance (initial registration or
    /// re-registration with updated fields). Sets `last_heartbeat_ms`
    /// to the current time.
    pub async fn register(
        &self,
        service: &str,
        instance_id: InstanceId,
        rpc_endpoint: &str,
        extra: &ServiceExtra,
    ) -> Result<()> {
        let value = InstanceValue {
            instance_id,
            rpc_endpoint: rpc_endpoint.to_string(),
            last_heartbeat_ms: now_ms(),
            extra: Some(extra.clone()),
        };
        self.put_instance(service, instance_id, &value).await
    }

    /// Heartbeat an existing instance (updates `last_heartbeat_ms`
    /// and any changed fields in `extra`). Read-modify-write to
    /// preserve `rpc_endpoint` if the caller only wants to update
    /// the heartbeat timestamp + extra.
    pub async fn heartbeat(
        &self,
        service: &str,
        instance_id: InstanceId,
        rpc_endpoint: &str,
        extra: &ServiceExtra,
    ) -> Result<()> {
        // Heartbeat is a full overwrite (same as register) — the
        // caller supplies the current endpoint + extra each time.
        self.register(service, instance_id, rpc_endpoint, extra).await
    }

    /// Unregister a service instance (clean shutdown).
    pub async fn unregister(&self, service: &str, instance_id: InstanceId) -> Result<()> {
        let key = InstanceKey {
            service: service.to_string(),
            instance_id,
        };
        self.kv
            .delete(G0_STORE, G0_GROUP, key.to_path().as_bytes(), None)
            .await
            .map(|_| ())
    }

    /// Read a single service instance.
    pub async fn read_instance(
        &self,
        service: &str,
        instance_id: InstanceId,
    ) -> Result<Option<InstanceValue>> {
        let key = InstanceKey {
            service: service.to_string(),
            instance_id,
        };
        let path = key.to_path();
        match self
            .kv
            .get(
                G0_STORE,
                G0_GROUP,
                path.as_bytes(),
                crate::ReadMode::Linearizable,
                None,
            )
            .await?
        {
            GetOutcome::Found { value, .. } => {
                let v: InstanceValue = serde_json::from_slice(&value).map_err(|e| Error::SysdataDecode {
                    key: path.clone(),
                    reason: e.to_string(),
                })?;
                Ok(Some(v))
            }
            GetOutcome::NotFound => Ok(None),
        }
    }

    /// Read all instances of a service (prefix scan
    /// `/srv/<service>/`). Filters out expired entries (whose
    /// `last_heartbeat_ms` is older than `now - ttl_ms`).
    pub async fn read_all_instances(&self, service: &str) -> Result<Vec<(InstanceId, InstanceValue)>> {
        self.read_all_instances_with_ttl(service, DEFAULT_TTL_MS).await
    }

    /// Like [`read_all_instances`](Self::read_all_instances) but with
    /// a custom liveness TTL in milliseconds.
    pub async fn read_all_instances_with_ttl(
        &self,
        service: &str,
        ttl_ms: u64,
    ) -> Result<Vec<(InstanceId, InstanceValue)>> {
        let prefix = InstanceKey::text_prefix_for_service(service);
        let cutoff = now_ms().saturating_sub(ttl_ms);
        let mut out: Vec<(InstanceId, InstanceValue)> = Vec::new();
        let mut start_after: Vec<u8> = Vec::new();
        loop {
            let outcome = self
                .kv
                .scan(
                    G0_STORE,
                    G0_GROUP,
                    prefix.as_bytes(),
                    &start_after,
                    &[],
                    0,
                    crate::ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await?;
            for (k, v) in &outcome.items {
                let path = std::str::from_utf8(k).map_err(|e| Error::SysdataDecode {
                    key: prefix.clone(),
                    reason: e.to_string(),
                })?;
                let val: InstanceValue = serde_json::from_slice(v).map_err(|e| Error::SysdataDecode {
                    key: path.to_string(),
                    reason: e.to_string(),
                })?;
                if val.last_heartbeat_ms >= cutoff {
                    out.push((val.instance_id, val));
                }
            }
            if !outcome.truncated || outcome.items.is_empty() {
                break;
            }
            if let Some((last_key, _)) = outcome.items.last() {
                start_after = last_key.to_vec();
            } else {
                break;
            }
        }
        Ok(out)
    }

    async fn put_instance(
        &self,
        service: &str,
        instance_id: InstanceId,
        value: &InstanceValue,
    ) -> Result<()> {
        let key = InstanceKey {
            service: service.to_string(),
            instance_id,
        };
        let path = key.to_path();
        let payload = serde_json::to_vec(value).map_err(|e| Error::SysdataDecode {
            key: path.clone(),
            reason: e.to_string(),
        })?;
        self.kv
            .put(G0_STORE, G0_GROUP, path.as_bytes(), &payload, None)
            .await
            .map(|_| ())
    }
}

// ── diskdb convenience wrappers ─────────────────────────────────

impl ServiceRegistryClient {
    /// Register a diskdb instance with `owned_dg_ids` and optional
    /// per-disk-group usage summaries (piggybacked on keepalive).
    pub async fn register_diskdb(
        &self,
        instance_id: InstanceId,
        rpc_endpoint: &str,
        owned_dg_ids: &[u64],
        group_usages: &[DiskGroupUsageSummary],
    ) -> Result<()> {
        let extra = ServiceExtra {
            diskdb: Some(DiskdbExtra {
                owned_dg_ids: owned_dg_ids.to_vec(),
                group_usages: group_usages.to_vec(),
            }),
            kv_server: None,
        };
        self.register("diskdb", instance_id, rpc_endpoint, &extra).await
    }

    /// Heartbeat a diskdb instance with updated `owned_dg_ids` and
    /// per-disk-group usage summaries (§11 keepalive piggyback).
    pub async fn heartbeat_diskdb(
        &self,
        instance_id: InstanceId,
        rpc_endpoint: &str,
        owned_dg_ids: &[u64],
        group_usages: &[DiskGroupUsageSummary],
    ) -> Result<()> {
        self.register_diskdb(instance_id, rpc_endpoint, owned_dg_ids, group_usages)
            .await
    }

    /// Read all live diskdb instances.
    pub async fn read_all_diskdb_instances(&self) -> Result<Vec<(InstanceId, InstanceValue)>> {
        self.read_all_instances("diskdb").await
    }
}

// ── diskio convenience wrappers ─────────────────────────────────

impl ServiceRegistryClient {
    /// Read all live diskio instances.
    pub async fn read_all_diskio_instances(&self) -> Result<Vec<(InstanceId, InstanceValue)>> {
        self.read_all_instances("diskio").await
    }

    /// Register a diskio instance with `owned_dg_ids` and optional
    /// per-disk-group usage summaries. Uses the "diskio" service group
    /// so it doesn't pollute the "diskdb" group.
    pub async fn heartbeat_diskio(
        &self,
        instance_id: InstanceId,
        rpc_endpoint: &str,
        owned_dg_ids: &[u64],
        group_usages: &[DiskGroupUsageSummary],
    ) -> Result<()> {
        let extra = ServiceExtra {
            diskdb: Some(DiskdbExtra {
                owned_dg_ids: owned_dg_ids.to_vec(),
                group_usages: group_usages.to_vec(),
            }),
            kv_server: None,
        };
        self.register("diskio", instance_id, rpc_endpoint, &extra).await
    }
}

// ── kv-server convenience wrappers ──────────────────────────────

impl ServiceRegistryClient {
    /// Register a kv-server instance with `hosted_stores`,
    /// `hosted_groups`, aggregate `health`, and the node `data_root`.
    pub async fn register_kv_server(
        &self,
        instance_id: InstanceId,
        rpc_endpoint: &str,
        hosted_stores: &[u64],
        hosted_groups: &[crowdb_protocol::common::HostedGroup],
        health: &str,
        data_root: &str,
    ) -> Result<()> {
        let extra = ServiceExtra {
            diskdb: None,
            kv_server: Some(crowdb_protocol::common::KvServerExtra {
                hosted_stores: hosted_stores.to_vec(),
                hosted_groups: hosted_groups.to_vec(),
                health: health.to_string(),
                data_root: data_root.to_string(),
            }),
        };
        self.register("kv-server", instance_id, rpc_endpoint, &extra)
            .await
    }

    /// Heartbeat a kv-server instance.
    pub async fn heartbeat_kv_server(
        &self,
        instance_id: InstanceId,
        rpc_endpoint: &str,
        hosted_stores: &[u64],
        hosted_groups: &[crowdb_protocol::common::HostedGroup],
        health: &str,
        data_root: &str,
    ) -> Result<()> {
        self.register_kv_server(
            instance_id,
            rpc_endpoint,
            hosted_stores,
            hosted_groups,
            health,
            data_root,
        )
        .await
    }

    /// Read all live kv-server instances.
    pub async fn read_all_kv_server_instances(&self) -> Result<Vec<(InstanceId, InstanceValue)>> {
        self.read_all_instances("kv-server").await
    }
}

// ── chunkdb convenience wrappers ────────────────────────────────

impl ServiceRegistryClient {
    /// Register a chunkdb instance. chunkdb is stateless (design §3.6),
    /// so the only extra field is the crowdb-rpc endpoint; the binding
    /// monitor (`BindingMonitor` with `ChunkdbRangeStrategy`) reads
    /// these entries to compute the range binding table.
    pub async fn register_chunkdb(&self, instance_id: InstanceId, rpc_endpoint: &str) -> Result<()> {
        let extra = ServiceExtra {
            diskdb: None,
            kv_server: None,
        };
        self.register("chunkdb", instance_id, rpc_endpoint, &extra).await
    }

    /// Heartbeat a chunkdb instance (same as register — full overwrite).
    pub async fn heartbeat_chunkdb(&self, instance_id: InstanceId, rpc_endpoint: &str) -> Result<()> {
        self.register_chunkdb(instance_id, rpc_endpoint).await
    }

    /// Read all live chunkdb instances.
    pub async fn read_all_chunkdb_instances(&self) -> Result<Vec<(InstanceId, InstanceValue)>> {
        self.read_all_instances("chunkdb").await
    }
}
