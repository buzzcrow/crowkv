// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk allocator — orchestrates strip layout → selector → parallel
//! diskdb `AllocateBlocks` with rollback on partial failure.
//!
//! Design §8: parallel allocation via `futures::join_all`; rollback
//! frees successfully-allocated segments on any failure.

pub mod pool;

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::join_all;
use tracing::{info, warn};

use crowdb_common::metrics::LatencyHistogram;
use crowdb_protocol::chunkdb::rpc::StripType as ProtoStripType;
use crowdb_protocol::chunkdb::rpc::{ChunkStrip, EcStrip, MirrorStrip};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;

use crate::metrics::ChunkdbMetrics;
use crate::selector::{EcPlacement, MirrorPlacement, PlacementConstraints, PlacementPlan};
use crate::topology::TopologySnapshot;

pub use pool::DiskdbClientPool;

const MAX_ALLOC_RETRIES: usize = 3;

/// Allocator error.
#[derive(Debug, thiserror::Error)]
pub enum AllocError {
    #[error("placement failed: {0}")]
    Placement(#[from] crate::selector::PlacementError),
    #[error("diskdb allocate failed for disk_group {dg_id}: {error}")]
    AllocateFailed { dg_id: u64, error: String },
    #[error("partial allocation: requested {requested}, got {got}")]
    PartialAllocation { requested: u32, got: u32 },
    #[error("invalid diskdb allocation response for disk_group {dg_id}: {reason}")]
    InvalidResponse { dg_id: u64, reason: String },
    #[error("rollback failed: {0}")]
    Rollback(String),
}

/// Strip type for allocation.
#[derive(Debug, Clone, Copy)]
pub enum StripAllocType {
    Mirror { copy_count: usize },
    Ec { data_num: usize, code_num: usize },
}

/// Chunk allocator — orchestrates placement + parallel diskdb calls.
pub struct ChunkAllocator {
    pool: Arc<DiskdbClientPool>,
    metrics: Option<Arc<ChunkdbMetrics>>,
}

impl ChunkAllocator {
    #[must_use]
    pub fn new(pool: Arc<DiskdbClientPool>) -> Self {
        Self { pool, metrics: None }
    }

    /// Attach allocation workflow metrics.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<ChunkdbMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Get a reference to the diskdb client pool.
    pub fn pool(&self) -> &DiskdbClientPool {
        &self.pool
    }

    /// Allocate a single strip.
    ///
    /// # Errors
    /// Returns `AllocError` on placement failure, diskdb RPC failure,
    /// or partial allocation (triggers rollback).
    pub async fn allocate_strip(
        &self,
        snap: &TopologySnapshot,
        owner_chunk: &ChunkId,
        strip_type: StripAllocType,
        unit_count: u32,
        strip_sequence: u32,
        constraints: &PlacementConstraints,
    ) -> Result<ChunkStrip, AllocError> {
        self.pool.update_disk_id_lookup(&snap.disk_groups());
        let placement_started = std::time::Instant::now();
        let plan = match strip_type {
            StripAllocType::Mirror { copy_count } => MirrorPlacement::select(snap, copy_count, constraints)?,
            StripAllocType::Ec { data_num, code_num } => {
                EcPlacement::select(snap, data_num, code_num, constraints)?
            }
        };
        if let Some(metrics) = &self.metrics {
            observe_elapsed(&metrics.allocate_placement, placement_started);
            metrics
                .allocate_diskdb_calls
                .inc_by(u64::try_from(plan.entries.len()).unwrap_or(u64::MAX));
        }

        let diskdb_started = std::time::Instant::now();
        let segments = self
            .allocate_blocks_parallel(owner_chunk, &plan, unit_count)
            .await?;
        if let Some(metrics) = &self.metrics {
            observe_elapsed(&metrics.allocate_diskdb_round, diskdb_started);
            metrics
                .allocate_blocks
                .inc_by(u64::try_from(segments.len()).unwrap_or(u64::MAX));
            metrics.allocate_strips.inc();
        }

        let strip = assemble_strip(
            &segments,
            strip_type,
            strip_sequence,
            unit_count,
            (snap.unit_size_bytes() / 1024).max(1),
        );
        Ok(strip)
    }

    /// Allocate blocks in parallel across all placement entries.
    ///
    /// Per-instance verification: each diskdb response is checked for
    /// the requested segment count. Partial responses trigger a retry
    /// for just the missing blocks (up to `MAX_ALLOC_RETRIES`). On
    /// final failure, all successfully-allocated segments are freed.
    async fn allocate_blocks_parallel(
        &self,
        owner_chunk: &ChunkId,
        plan: &PlacementPlan,
        unit_count: u32,
    ) -> Result<Vec<Segment>, AllocError> {
        let mut all_segments: Vec<Segment> = Vec::new();
        // One request per DiskDB/data group. The selector still chooses each
        // EC position independently; aggregation only combines the counts for
        // positions placed on the same node.
        let mut pending = grouped_requests(plan);

        for attempt in 0..=MAX_ALLOC_RETRIES {
            if pending.is_empty() {
                break;
            }

            let mut futures = Vec::new();
            for (dg_id, count) in &pending {
                let pool = Arc::clone(&self.pool);
                let owner = *owner_chunk;
                let dg = *dg_id;
                let cnt = *count;
                futures.push(async move {
                    pool.allocate_blocks(dg, cnt, unit_count, &owner)
                        .await
                        .map_err(|e| AllocError::AllocateFailed {
                            dg_id: dg,
                            error: e.to_string(),
                        })
                });
            }

            let results = join_all(futures).await;

            // Check for hard failures and per-instance count mismatches.
            let mut errors = Vec::new();
            let mut next_pending: Vec<(u64, u32)> = Vec::new();
            for (result, (dg_id, requested)) in results.into_iter().zip(&pending) {
                match result {
                    Ok(resp) => {
                        let got = u32::try_from(resp.segments.len()).unwrap_or(u32::MAX);
                        if got > *requested {
                            all_segments.extend(resp.segments);
                            let error = AllocError::InvalidResponse {
                                dg_id: *dg_id,
                                reason: format!("requested {requested} segments, got {got}"),
                            };
                            self.rollback_or_error(&all_segments, error).await?;
                            unreachable!("rollback_or_error always returns Err")
                        }
                        if let Some(reason) = resp.segments.iter().find_map(|segment| {
                            self.validate_segment(segment, *dg_id, owner_chunk, unit_count)
                        }) {
                            all_segments.extend(resp.segments);
                            let error = AllocError::InvalidResponse {
                                dg_id: *dg_id,
                                reason,
                            };
                            self.rollback_or_error(&all_segments, error).await?;
                            unreachable!("rollback_or_error always returns Err")
                        }
                        if got < *requested {
                            warn!(
                                disk_group_id = *dg_id,
                                requested, got, attempt, "partial response from diskdb, will retry missing"
                            );
                            all_segments.extend(resp.segments);
                            next_pending.push((*dg_id, *requested - got));
                        } else {
                            all_segments.extend(resp.segments);
                        }
                    }
                    Err(e) => {
                        errors.push(e);
                    }
                }
            }

            if !errors.is_empty() {
                // Hard failure — free everything allocated so far.
                let error = errors.into_iter().next().expect("at least one error");
                return self.rollback_or_error(&all_segments, error).await;
            }

            pending = next_pending;
            if !pending.is_empty() && attempt < MAX_ALLOC_RETRIES {
                self.record_diskdb_retry();
                warn!(
                    pending_count = pending.len(),
                    attempt = attempt + 1,
                    "retrying partial allocation"
                );
            }
        }

        if !pending.is_empty() {
            let expected: u32 = plan.entries.iter().map(|e| e.block_count).sum();
            let got = u32::try_from(all_segments.len()).unwrap_or(u32::MAX);
            warn!(expected, got, "allocation retries exhausted, freeing all");
            return self
                .rollback_or_error(
                    &all_segments,
                    AllocError::PartialAllocation {
                        requested: expected,
                        got,
                    },
                )
                .await;
        }

        info!(segment_count = all_segments.len(), "strip allocated");
        Ok(all_segments)
    }

    fn record_diskdb_retry(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.allocate_diskdb_retries.inc();
        }
    }

    /// Free all allocated segments (rollback). Logs failures for the
    /// orphan scanner but does not propagate the free error.
    pub async fn rollback_strips(&self, strips: &[ChunkStrip]) -> Result<(), AllocError> {
        let segments: Vec<_> = strips.iter().flat_map(extract_segments).collect();
        let started = std::time::Instant::now();
        let result = self.free_all(&segments).await;
        if let Some(metrics) = &self.metrics {
            observe_elapsed(&metrics.allocate_rollback, started);
            metrics
                .allocate_rollback_blocks
                .inc_by(u64::try_from(segments.len()).unwrap_or(u64::MAX));
        }
        result
    }

    fn validate_segment(
        &self,
        segment: &Segment,
        dg_id: u64,
        owner_chunk: &ChunkId,
        unit_count: u32,
    ) -> Option<String> {
        if segment.owner_chunk.as_ref() != Some(owner_chunk) {
            return Some("owner_chunk does not match request".to_string());
        }
        if segment.unit_count != unit_count {
            return Some(format!(
                "requested unit_count {unit_count}, got {}",
                segment.unit_count
            ));
        }
        let Some(disk_id) = segment.disk_id.as_ref() else {
            return Some("segment has no disk_id".to_string());
        };
        if self.pool.dg_for_disk(disk_id) != Some(dg_id) {
            return Some("segment disk does not belong to requested disk_group".to_string());
        }
        None
    }

    async fn rollback_or_error<T>(&self, segments: &[Segment], cause: AllocError) -> Result<T, AllocError> {
        match self.free_all(segments).await {
            Ok(()) => Err(cause),
            Err(rollback) => Err(AllocError::Rollback(format!("{cause}; {rollback}"))),
        }
    }

    async fn free_all(&self, segments: &[Segment]) -> Result<(), AllocError> {
        if segments.is_empty() {
            return Ok(());
        }
        self.pool
            .free_blocks(segments.to_vec())
            .await
            .map_err(AllocError::Rollback)
    }
}

fn grouped_requests(plan: &PlacementPlan) -> Vec<(u64, u32)> {
    let mut grouped = HashMap::<u64, u32>::new();
    for entry in &plan.entries {
        grouped
            .entry(entry.disk_group_id)
            .and_modify(|count| *count = count.saturating_add(entry.block_count))
            .or_insert(entry.block_count);
    }
    grouped.into_iter().collect()
}

fn extract_segments(strip: &ChunkStrip) -> Vec<Segment> {
    use crowdb_protocol::chunkdb::rpc::Strip;
    match &strip.strip {
        Some(Strip::MirrorStrip(mirror)) => mirror.segments.clone(),
        Some(Strip::EcStrip(ec)) => ec.segments.clone(),
        None => Vec::new(),
    }
}

fn observe_elapsed(metric: &LatencyHistogram, started: std::time::Instant) {
    metric.observe(started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
}

/// Assemble a `ChunkStrip` from allocated segments.
fn assemble_strip(
    segments: &[Segment],
    strip_type: StripAllocType,
    strip_sequence: u32,
    unit_count: u32,
    unit_kb: u32,
) -> ChunkStrip {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

    match strip_type {
        StripAllocType::Mirror { .. } => ChunkStrip {
            chunk_offset: 0,
            strip_sequence,
            unit_kb,
            capacity: unit_count.saturating_mul(unit_kb),
            create_ts_ms: now_ms,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: ProtoStripType::Mirror as i32,
            strip: Some(crowdb_protocol::chunkdb::rpc::Strip::MirrorStrip(MirrorStrip {
                segments: segments.to_vec(),
            })),
            usage_bitmap: Vec::new(),
        },
        StripAllocType::Ec { data_num, code_num } => ChunkStrip {
            chunk_offset: 0,
            strip_sequence,
            unit_kb,
            capacity: unit_count
                .saturating_mul(unit_kb)
                .saturating_mul(u32::try_from(data_num).unwrap_or(u32::MAX)),
            create_ts_ms: now_ms,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: ProtoStripType::Ec as i32,
            strip: Some(crowdb_protocol::chunkdb::rpc::Strip::EcStrip(EcStrip {
                data_num: u32::try_from(data_num).unwrap_or(u32::MAX),
                code_num: u32::try_from(code_num).unwrap_or(u32::MAX),
                ec_state: crowdb_protocol::chunkdb::rpc::EcState::NoParity as i32,
                segments: segments.to_vec(),
            })),
            usage_bitmap: Vec::new(),
        },
    }
}
