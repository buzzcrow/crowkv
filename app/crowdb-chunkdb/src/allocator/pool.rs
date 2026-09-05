// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Diskdb client pool — caches crowdb-rpc transports to diskdb instances.
//!
//! Routes `AllocateBlocks` / `FreeBlocks` to the correct diskdb
//! instance per disk-group, using `ServiceRegistryClient` for endpoint
//! discovery.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;

use crowdb_diskdb_client::{DiskdbClientError, DiskdbRpcTransport};
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::{
    AllocateBlocksRequest, AllocateResponse, CommitBlocksRequest, FreeBlocksRequest, Segment,
};

/// Pool of diskdb crowdb-rpc transports, keyed by disk-group ID.
pub struct DiskdbClientPool {
    svc: ServiceRegistryClient,
    /// `disk_group_id -> rpc_endpoint` cache.
    endpoints: DashMap<u64, String>,
    /// `disk_id -> disk_group_id` reverse lookup cache (GAP-4).
    /// Populated from the topology cache's `DiskGroupEntry` list.
    /// Used for precise `free_blocks` routing.
    disk_id_to_dg: ArcSwap<HashMap<DiskId, u64>>,
    /// Shared crowdb-rpc transport.
    transport: Arc<DiskdbRpcTransport>,
}

impl DiskdbClientPool {
    #[must_use]
    pub fn new(svc: ServiceRegistryClient) -> Self {
        Self::with_transport(svc, 1, 2)
    }

    /// Construct a pool with a configured DiskDB connection count and RPC workers.
    #[must_use]
    pub fn with_transport(svc: ServiceRegistryClient, pool_size: usize, workers: u32) -> Self {
        Self {
            svc,
            endpoints: DashMap::new(),
            disk_id_to_dg: ArcSwap::from_pointee(HashMap::new()),
            transport: Arc::new(DiskdbRpcTransport::with_pool_size(pool_size, workers)),
        }
    }

    /// Update the `disk_id → disk_group_id` reverse lookup cache from
    /// a topology snapshot. Called by the topology refresh loop.
    pub fn update_disk_id_lookup(&self, entries: &[crowdb_protocol::sysdata::DiskGroupEntry]) {
        let mut refreshed = HashMap::new();
        for entry in entries {
            for disk_id in &entry.value.disk_ids {
                refreshed.insert(*disk_id, entry.dg_id);
            }
        }
        self.disk_id_to_dg.store(Arc::new(refreshed));
    }

    /// Look up the disk-group ID for a disk_id (reverse lookup).
    pub(crate) fn dg_for_disk(&self, disk_id: &DiskId) -> Option<u64> {
        self.disk_id_to_dg.load().get(disk_id).copied()
    }

    /// Resolve the endpoint for the diskdb instance owning
    /// `disk_group_id`.
    async fn endpoint_for_dg(&self, dg_id: u64) -> Result<String, String> {
        // Check endpoint cache.
        if let Some(endpoint) = self.endpoints.get(&dg_id) {
            return Ok(endpoint.value().clone());
        }

        // Cache miss — refresh from service registry and retry.
        self.refresh_endpoints().await?;
        if let Some(endpoint) = self.endpoints.get(&dg_id) {
            return Ok(endpoint.value().clone());
        }
        Err(format!("no endpoint cached for disk_group {dg_id}"))
    }

    /// Warm the endpoint cache by reading all diskdb instances from the
    /// service registry. Maps each instance's `owned_dg_ids` to its
    /// RPC endpoint so `endpoint_for_dg` can route by disk-group ID.
    ///
    /// # Errors
    /// Returns a String error if the service registry read fails.
    pub async fn refresh_endpoints(&self) -> Result<(), String> {
        let instances = self
            .svc
            .read_all_instances("diskdb")
            .await
            .map_err(|e| format!("read_all_instances: {e}"))?;
        let mut refreshed = HashMap::new();
        for (_id, value) in instances {
            if let Some(ref extra) = value.extra {
                if let Some(ref diskdb) = extra.diskdb {
                    for dg_id in &diskdb.owned_dg_ids {
                        refreshed.insert(*dg_id, value.rpc_endpoint.clone());
                    }
                }
            }
        }
        self.endpoints.retain(|dg_id, _| refreshed.contains_key(dg_id));
        for (dg_id, endpoint) in refreshed {
            self.endpoints.insert(dg_id, endpoint);
        }
        Ok(())
    }

    /// Allocate blocks on the diskdb instance owning `disk_group_id`.
    ///
    /// The mutation is sent once. A transport failure is ambiguous because
    /// DiskDB may already have persisted the tentative blocks, so retrying
    /// here could allocate a second physical set for the same chunk.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Unreachable` if the endpoint is not cached
    /// or the RPC transport fails, or `DiskdbClientError::Rpc` for a server
    /// error.
    pub async fn allocate_blocks(
        &self,
        dg_id: u64,
        count: u32,
        unit_count: u32,
        owner_chunk: &ChunkId,
    ) -> Result<AllocateResponse, DiskdbClientError> {
        let req = AllocateBlocksRequest {
            disk_group_id: dg_id,
            unit_count,
            count,
            exclude_disk_ids: vec![],
            owner_chunk: Some(*owner_chunk),
        };

        let endpoint = self.endpoint_for_dg(dg_id).await.map_err(|e| {
            DiskdbClientError::Unreachable(format!("no endpoint for disk_group {dg_id}: {e}"))
        })?;
        self.transport.allocate_blocks(&endpoint, &req).await
    }

    /// Commit blocks on the DiskDB instances that own them.
    ///
    /// # Errors
    /// Returns a String error if routing is incomplete or any commit fails.
    pub async fn commit_blocks(&self, segments: Vec<Segment>) -> Result<(), String> {
        if segments.is_empty() {
            return Ok(());
        }

        let grouped = self.group_segments(segments, "commit_blocks")?;
        let mut futures = Vec::with_capacity(grouped.len());
        for (dg_id, segs) in grouped {
            let endpoint = self.endpoint_for_dg(dg_id).await?;
            let transport = Arc::clone(&self.transport);
            futures.push(async move {
                let req = CommitBlocksRequest { segments: segs };
                transport
                    .commit_blocks(&endpoint, &req)
                    .await
                    .map_err(|e| format!("commit_blocks RPC: {e}"))
            });
        }

        let results = futures::future::join_all(futures).await;
        let errors: Vec<_> = results.into_iter().filter_map(Result::err).collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    /// Free blocks via the diskdb instances that own them.
    ///
    /// Segments are grouped by disk-group (via `disk_id → dg_id`
    /// reverse lookup) and freed in parallel to the owning instances.
    /// # Errors
    /// Returns a String error if routing is incomplete or any free RPC fails.
    pub async fn free_blocks(&self, segments: Vec<Segment>) -> Result<(), String> {
        if segments.is_empty() {
            return Ok(());
        }

        let grouped = self.group_segments(segments, "free_blocks")?;
        let mut futures = Vec::with_capacity(grouped.len());
        for (dg_id, segs) in grouped {
            let endpoint = self.endpoint_for_dg(dg_id).await?;
            let transport = Arc::clone(&self.transport);
            futures.push(async move {
                let req = FreeBlocksRequest { segments: segs };
                transport
                    .free_blocks(&endpoint, &req)
                    .await
                    .map_err(|e| format!("free_blocks RPC: {e}"))
            });
        }

        let results = futures::future::join_all(futures).await;
        let errors: Vec<_> = results.into_iter().filter_map(Result::err).collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    fn group_segments(
        &self,
        segments: Vec<Segment>,
        operation: &str,
    ) -> Result<HashMap<u64, Vec<Segment>>, String> {
        let mut grouped = HashMap::new();
        for segment in segments {
            let disk_id = segment
                .disk_id
                .as_ref()
                .ok_or_else(|| format!("{operation}: segment has no disk_id"))?;
            let dg_id = self
                .dg_for_disk(disk_id)
                .ok_or_else(|| format!("{operation}: disk_id {disk_id:?} has no disk-group mapping"))?;
            grouped.entry(dg_id).or_insert_with(Vec::new).push(segment);
        }
        Ok(grouped)
    }
}
