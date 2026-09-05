// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk lifecycle management — state machine + crowdb-rpc handlers.
//!
//! Design §9: `Init → Active → Sealed → Deleted` state machine.
//! Transitions are validated; invalid transitions return
//! `InvalidStateTransition`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use quick_cache::sync::Cache;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{info, warn};

use crowdb_common::metrics::LatencyHistogram;
use crowdb_protocol::chunkdb::rpc::{
    Chunk, ChunkState as ProtoChunkState, ChunkStrip, ChunkType, StripType as ProtoStripType,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::generate_chunk_id;

use crate::allocator::{AllocError, ChunkAllocator, StripAllocType};
use crate::metrics::ChunkdbMetrics;
use crate::metrics::LifecycleMetrics;
use crate::range_guard::RangeGuard;
use crate::routing::hash_to_bucket;
use crate::selector::PlacementConstraints;
use crate::storage::{ChunkStore, StoreError};
use crate::topology::TopologyCache;

use super::state::{ChunkState, StateTransitionError};

/// Default lock wait time for `LockPolicy::default()`.
const DEFAULT_LOCK_WAIT: Duration = Duration::from_secs(10);

/// Lifecycle error — maps to crowdb-rpc status codes in the service layer.
#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("invalid state transition: {0}")]
    InvalidStateTransition(#[from] StateTransitionError),
    #[error("chunk not found")]
    ChunkNotFound,
    #[error("chunk already exists")]
    ChunkAlreadyExists,
    #[error("state conflict — concurrent modification")]
    StateConflict,
    #[error("allocation failed: {0}")]
    Allocation(#[from] AllocError),
    #[error("storage error: {0}")]
    Storage(#[from] StoreError),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("chunk bucket {bucket} not in owned ranges")]
    NotMyRange { bucket: u16 },
    #[error("chunk lock busy — retry later")]
    LockBusy,
    #[error("chunk lock acquire timed out")]
    LockTimeout,
    #[error("strip index {index} out of range (chunk has {len} strips)")]
    StripIndexOutOfRange { index: u32, len: usize },
    #[error("diskdb commit failed: {0}")]
    Commit(String),
    #[error("diskdb cleanup failed after metadata publication: {0}")]
    Cleanup(String),
}

/// Lock policy — how to handle contention on a per-chunk mutex.
#[derive(Debug, Clone)]
pub enum LockPolicy {
    /// Fail fast with `LockBusy` on contention.
    TryLock,
    /// Park the task up to `duration`, then `LockTimeout`.
    Wait(Duration),
}

impl Default for LockPolicy {
    fn default() -> Self {
        Self::Wait(DEFAULT_LOCK_WAIT)
    }
}

/// Cache hint — whether to populate the payload cache.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CacheHint {
    /// Populate cache on miss, write to cache on refresh (default).
    #[default]
    Cache,
    /// Skip cache population — always fetch from store.
    NoCache,
}

#[path = "lock_map.rs"]
mod lock_map;

pub use lock_map::{ChunkGuard, ChunkLockMap};

/// Lifecycle handler — orchestrates allocate/append/seal/delete/query/list.
pub struct LifecycleHandler {
    store: Arc<ChunkStore>,
    allocator: Arc<ChunkAllocator>,
    topology: TopologyCache,
    /// Range guard — enforces chunkdb instance sharding. `None` for
    /// v1 single-instance mode (no binding table); `Some` for R99
    /// sharded mode.
    range_guard: Option<Arc<RangeGuard>>,
    /// Per-chunk lock map + payload cache. `None` when R100 is not
    /// configured (no lifecycle section in config).
    locks: Option<Arc<ChunkLockMap>>,
    allow_unsafe_ec: bool,
    metrics: Option<Arc<ChunkdbMetrics>>,
}

struct AllocationMetricGuard {
    metrics: Option<Arc<ChunkdbMetrics>>,
    success: bool,
}

impl AllocationMetricGuard {
    fn new(metrics: Option<Arc<ChunkdbMetrics>>) -> Self {
        if let Some(metrics) = &metrics {
            metrics.allocate_inflight.inc();
        }
        Self {
            metrics,
            success: false,
        }
    }

    fn mark_success(&mut self) {
        self.success = true;
    }
}

impl Drop for AllocationMetricGuard {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.allocate_inflight.dec();
            if !self.success {
                metrics.allocate_errors.inc();
            }
        }
    }
}

impl LifecycleHandler {
    #[must_use]
    pub fn new(store: Arc<ChunkStore>, allocator: Arc<ChunkAllocator>, topology: TopologyCache) -> Self {
        Self {
            store,
            allocator,
            topology,
            range_guard: None,
            locks: None,
            allow_unsafe_ec: false,
            metrics: None,
        }
    }

    /// Attach a range guard for R99 sharded mode.
    #[must_use]
    pub fn with_range_guard(mut self, guard: Arc<RangeGuard>) -> Self {
        self.range_guard = Some(guard);
        self
    }

    /// Attach a per-chunk lock map (R100).
    #[must_use]
    pub fn with_locks(mut self, locks: Arc<ChunkLockMap>) -> Self {
        self.locks = Some(locks);
        self
    }

    /// Permit explicitly configured unsafe EC placement fallback.
    #[must_use]
    pub fn with_allow_unsafe_ec(mut self, allow: bool) -> Self {
        self.allow_unsafe_ec = allow;
        self
    }

    /// Attach allocation workflow metrics.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<ChunkdbMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Get a reference to the lock map (if attached).
    #[must_use]
    pub fn locks(&self) -> Option<&Arc<ChunkLockMap>> {
        self.locks.as_ref()
    }

    /// Check the range guard (if present) before processing a
    /// mutating RPC. Read-only RPCs (query, list) bypass the guard.
    fn check_range(&self, chunk_id: &ChunkId) -> Result<(), LifecycleError> {
        if let Some(guard) = &self.range_guard {
            guard
                .check(chunk_id)
                .map_err(|e| LifecycleError::NotMyRange { bucket: e.bucket })?;
        }
        Ok(())
    }

    /// Allocate a new chunk.
    #[allow(clippy::too_many_arguments)]
    pub async fn allocate_chunk(
        &self,
        chunk_id: Option<ChunkId>,
        write_granularity_kb: u32,
        strip_count: u32,
        strip_type: ProtoStripType,
        data_num: u32,
        code_num: u32,
        copy_count: u32,
        chunk_type: ChunkType,
    ) -> Result<Chunk, LifecycleError> {
        let id = chunk_id.unwrap_or_else(|| {
            let parts = generate_chunk_id(chunk_type as u8);
            parts.to_proto()
        });
        self.check_range(&id)?;
        let mut allocation_guard = AllocationMetricGuard::new(self.metrics.clone());

        // Caller-supplied ID: acquire lock + existence check.
        // Auto-generated ID: skip lock (UUID collision negligible).
        let mut guard = if chunk_id.is_some() {
            if let Some(locks) = &self.locks {
                let g = locks
                    .acquire_for_create(&id, &LockPolicy::default(), CacheHint::Cache)
                    .await?;
                // Existence check inside the lock.
                match self.store.get_chunk(&id).await {
                    Ok(_) => return Err(LifecycleError::ChunkAlreadyExists),
                    Err(StoreError::ChunkNotFound) => {}
                    Err(e) => return Err(LifecycleError::Storage(e)),
                }
                Some(g)
            } else {
                None
            }
        } else {
            None
        };

        let snap = self.topology.snapshot();

        let mirror_copies = if copy_count == 0 { 3 } else { copy_count as usize };
        let strip_alloc_type = match strip_type {
            ProtoStripType::Mirror => StripAllocType::Mirror {
                copy_count: mirror_copies,
            },
            ProtoStripType::Ec => StripAllocType::Ec {
                data_num: data_num as usize,
                code_num: code_num as usize,
            },
        };

        let constraints = self.placement_constraints();
        // Convert write_granularity (KB) to unit_count using the unit
        // size from the topology snapshot. Fall back to treating KB as
        // units if unit_size_bytes is unavailable (0).
        let unit_size_kb = snap.unit_size_bytes() / 1024;
        let unit_count = write_granularity_kb
            .checked_div(unit_size_kb)
            .unwrap_or(write_granularity_kb)
            .max(1);

        let mut strips = Vec::with_capacity(strip_count as usize);
        for seq in 0..strip_count {
            let mut strip = match self
                .allocator
                .allocate_strip(&snap, &id, strip_alloc_type, unit_count, seq, &constraints)
                .await
            {
                Ok(strip) => strip,
                Err(error) => {
                    self.allocator.rollback_strips(&strips).await?;
                    return Err(error.into());
                }
            };
            strip.chunk_offset = strips.iter().map(|strip: &ChunkStrip| strip.capacity).sum();
            strips.push(strip);
        }

        let record_started = std::time::Instant::now();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

        let mut chunk = Chunk {
            id: Some(id),
            state: ProtoChunkState::Init as i32,
            create_ts_ms: now_ms,
            sealed_ts_ms: 0,
            capacity: strips.iter().map(|s| s.capacity).sum(),
            sealed_length: 0,
            strips,
            chunk_type: chunk_type as i32,
        };
        if let Some(metrics) = &self.metrics {
            observe_elapsed(&metrics.allocate_record_build, record_started);
        }

        self.persist_allocated_chunk(&mut chunk).await?;

        // Update cache.
        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        } else if chunk_id.is_none() {
            // Auto-generated ID: populate cache directly (no guard).
            if let Some(locks) = &self.locks {
                locks.populate_cache(&id, chunk.clone());
            }
        }
        info!(chunk_id = ?id, strips = strip_count, "chunk allocated");
        allocation_guard.mark_success();
        Ok(chunk)
    }

    async fn persist_allocated_chunk(&self, chunk: &mut Chunk) -> Result<(), LifecycleError> {
        let init_started = std::time::Instant::now();
        if let Err(error) = self.store.put_chunk(chunk).await {
            self.allocator.rollback_strips(&chunk.strips).await?;
            return Err(error.into());
        }
        if let Some(metrics) = &self.metrics {
            observe_elapsed(&metrics.allocate_kv_init_persist, init_started);
        }

        let commit_started = std::time::Instant::now();
        self.commit_strip_segments(&chunk.strips).await?;
        if let Some(metrics) = &self.metrics {
            observe_elapsed(&metrics.allocate_commit, commit_started);
            let blocks = chunk.strips.iter().flat_map(extract_segments).count();
            metrics
                .allocate_commit_blocks
                .inc_by(u64::try_from(blocks).unwrap_or(u64::MAX));
        }

        chunk.state = ProtoChunkState::Active as i32;
        let active_started = std::time::Instant::now();
        for attempt in 0..100_u32 {
            match self.store.put_chunk(chunk).await {
                Ok(()) => break,
                Err(error) if attempt < 99 => {
                    let backoff_ms = 1_u64.checked_shl(attempt).unwrap_or(u64::MAX).min(50);
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    drop(error);
                }
                Err(error) => return Err(error.into()),
            }
        }
        if let Some(metrics) = &self.metrics {
            observe_elapsed(&metrics.allocate_kv_active_persist, active_started);
        }
        Ok(())
    }

    /// Append strips to an active chunk.
    #[allow(clippy::too_many_arguments)]
    pub async fn append_chunk(
        &self,
        chunk_id: &ChunkId,
        strip_count: u32,
        strip_type: ProtoStripType,
        data_num: u32,
        code_num: u32,
        copy_count: u32,
        unit_count: u32,
    ) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;

        let mut guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };

        let mut chunk = match &guard {
            Some(g) => g
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let current_state = ChunkState::from_proto(chunk.state);
        current_state.check_can_append()?;

        let snap = self.topology.snapshot();
        let mirror_copies = if copy_count == 0 { 3 } else { copy_count as usize };
        let strip_alloc_type = match strip_type {
            ProtoStripType::Mirror => StripAllocType::Mirror {
                copy_count: mirror_copies,
            },
            ProtoStripType::Ec => StripAllocType::Ec {
                data_num: data_num as usize,
                code_num: code_num as usize,
            },
        };

        let constraints = self.placement_constraints();
        let start_seq = u32::try_from(chunk.strips.len()).unwrap_or(u32::MAX);

        let mut appended = Vec::with_capacity(strip_count as usize);
        for i in 0..strip_count {
            let seq = start_seq + i;
            let mut strip = match self
                .allocator
                .allocate_strip(&snap, chunk_id, strip_alloc_type, unit_count, seq, &constraints)
                .await
            {
                Ok(strip) => strip,
                Err(error) => {
                    self.allocator.rollback_strips(&appended).await?;
                    return Err(error.into());
                }
            };
            strip.chunk_offset = chunk
                .capacity
                .saturating_add(appended.iter().map(|strip: &ChunkStrip| strip.capacity).sum());
            appended.push(strip);
        }

        if let Err(error) = self.commit_strip_segments(&appended).await {
            self.allocator.rollback_strips(&appended).await?;
            return Err(error);
        }
        chunk.strips.extend(appended.iter().cloned());
        chunk.capacity = chunk.strips.iter().map(|s| s.capacity).sum();
        if let Err(error) = self.store.put_chunk(&chunk).await {
            self.allocator.rollback_strips(&appended).await?;
            return Err(error.into());
        }

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, added_strips = strip_count, "chunk appended");
        Ok(chunk)
    }

    /// Seal a chunk — no more appends allowed.
    pub async fn seal_chunk(&self, chunk_id: &ChunkId, seal_length: u32) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;

        let mut guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };

        let mut chunk = match &guard {
            Some(g) => g
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let current_state = ChunkState::from_proto(chunk.state);
        current_state.check_can_seal()?;
        if seal_length > chunk.capacity {
            return Err(LifecycleError::InvalidRequest(format!(
                "seal_length {seal_length} exceeds chunk capacity {}",
                chunk.capacity
            )));
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

        chunk.state = ProtoChunkState::Sealed as i32;
        chunk.sealed_length = seal_length;
        chunk.sealed_ts_ms = now_ms;

        self.store.put_chunk(&chunk).await?;

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, seal_length, "chunk sealed");
        Ok(chunk)
    }

    /// Delete a chunk — marks deleted and frees segments.
    /// Repeated deletion returns the existing tombstone so retries are idempotent.
    pub async fn delete_chunk(&self, chunk_id: &ChunkId) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;

        let mut guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };

        let mut chunk = match &guard {
            Some(g) => g
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let current_state = ChunkState::from_proto(chunk.state);

        // A Deleted record that still owns strips is a durable cleanup intent.
        if current_state == ChunkState::Deleted {
            if chunk.strips.is_empty() {
                return Ok(chunk);
            }
            let segments: Vec<_> = chunk.strips.iter().flat_map(extract_segments).collect();
            self.allocator
                .pool()
                .free_blocks(segments)
                .await
                .map_err(LifecycleError::Cleanup)?;
            chunk.strips.clear();
            chunk.capacity = 0;
            self.store.put_chunk(&chunk).await?;
            if let Some(ref mut g) = guard {
                g.refresh(chunk.clone());
            }
            return Ok(chunk);
        }

        current_state.check_can_delete()?;

        // Publish Deleted before making any referenced block reusable.
        let all_segments: Vec<_> = chunk.strips.iter().flat_map(extract_segments).collect();
        chunk.state = ProtoChunkState::Deleted as i32;
        self.store.put_chunk(&chunk).await?;

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        if !all_segments.is_empty() {
            self.allocator
                .pool()
                .free_blocks(all_segments)
                .await
                .map_err(LifecycleError::Cleanup)?;
        }
        chunk.strips.clear();
        chunk.capacity = 0;
        self.store.put_chunk(&chunk).await?;
        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, "chunk deleted");
        Ok(chunk)
    }

    /// Delete a range within a chunk (partial delete). Frees the
    /// segments of strips whose `[chunk_offset, chunk_offset +
    /// capacity)` range overlaps with `[offset, offset + size)`, then
    /// removes those strips from the chunk record. The chunk must be
    /// Active.
    pub async fn delete_chunk_range(
        &self,
        chunk_id: &ChunkId,
        offset: u32,
        size: u32,
    ) -> Result<(), LifecycleError> {
        self.check_range(chunk_id)?;
        if size == 0 {
            return Err(LifecycleError::InvalidRequest(
                "delete range size must be non-zero".into(),
            ));
        }

        let mut guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };

        let mut chunk = match &guard {
            Some(g) => g
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let current_state = ChunkState::from_proto(chunk.state);
        current_state.check_can_append()?;

        let end = offset
            .checked_add(size)
            .ok_or_else(|| LifecycleError::InvalidRequest("delete range overflows u32".into()))?;
        // Find strips that overlap with [offset, end).
        let (to_remove, to_keep): (Vec<_>, Vec<_>) = chunk.strips.into_iter().partition(|s| {
            let s_start = s.chunk_offset;
            let s_end = s_start.saturating_add(s.capacity);
            s_start < end && offset < s_end
        });

        // Remove references durably before making their blocks reusable.
        let all_segments: Vec<_> = to_remove.iter().flat_map(extract_segments).collect();
        let removed_count = to_remove.len();
        chunk.strips = to_keep;
        chunk.capacity = chunk.strips.iter().map(|s| s.capacity).sum();
        self.store.put_chunk(&chunk).await?;

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        if !all_segments.is_empty() {
            self.allocator
                .pool()
                .free_blocks(all_segments)
                .await
                .map_err(LifecycleError::Cleanup)?;
        }
        info!(chunk_id = ?chunk_id, offset, size, removed_strips = removed_count, "chunk range deleted");
        Ok(())
    }

    /// Update a single strip within a chunk (e.g. after EC parity
    /// computation). Replaces the strip at `strip_index` with the new
    /// strip, freeing the old strip's segments and committing the new
    /// strip's segments. The chunk must be Active or Sealed.
    pub async fn update_chunk_strip(
        &self,
        chunk_id: &ChunkId,
        strip_index: u32,
        new_strip: ChunkStrip,
    ) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;

        let mut guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };

        let mut chunk = match &guard {
            Some(g) => g
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let current_state = ChunkState::from_proto(chunk.state);
        // Strip updates can happen on Active (EC encoding) or Sealed
        // (parity rebuild after seal) chunks.
        if current_state != ChunkState::Active && current_state != ChunkState::Sealed {
            return Err(LifecycleError::InvalidStateTransition(StateTransitionError::new(
                current_state,
                "Active|Sealed",
            )));
        }

        let idx = usize::try_from(strip_index).unwrap_or(usize::MAX);
        if idx >= chunk.strips.len() {
            return Err(LifecycleError::StripIndexOutOfRange {
                index: strip_index,
                len: chunk.strips.len(),
            });
        }
        if new_strip.strip_sequence != chunk.strips[idx].strip_sequence {
            return Err(LifecycleError::InvalidRequest(format!(
                "replacement strip sequence {} does not match existing sequence {}",
                new_strip.strip_sequence, chunk.strips[idx].strip_sequence
            )));
        }
        let expected_segments = match &new_strip.strip {
            Some(crowdb_protocol::chunkdb::rpc::Strip::MirrorStrip(mirror)) => mirror.segments.len(),
            Some(crowdb_protocol::chunkdb::rpc::Strip::EcStrip(ec)) => {
                let expected = usize::try_from(ec.data_num.saturating_add(ec.code_num)).unwrap_or(usize::MAX);
                if ec.data_num == 0 || ec.code_num == 0 || ec.segments.len() != expected {
                    return Err(LifecycleError::InvalidRequest(
                        "replacement EC strip shape is invalid".into(),
                    ));
                }
                expected
            }
            None => {
                return Err(LifecycleError::InvalidRequest(
                    "replacement strip has no segments".into(),
                ))
            }
        };
        if expected_segments == 0
            || extract_segments(&new_strip)
                .iter()
                .any(|segment| segment.owner_chunk.as_ref() != Some(chunk_id) || segment.unit_count == 0)
        {
            return Err(LifecycleError::InvalidRequest(
                "replacement segments must be non-empty and owned by the chunk".into(),
            ));
        }

        // Commit the replacement before publishing it.
        let old_segments = extract_segments(&chunk.strips[idx]);
        self.commit_strip_segments(std::slice::from_ref(&new_strip))
            .await?;

        // Replace the strip.
        chunk.strips[idx] = new_strip.clone();
        chunk.capacity = chunk.strips.iter().map(|s| s.capacity).sum();
        if let Err(error) = self.store.put_chunk(&chunk).await {
            self.allocator
                .rollback_strips(std::slice::from_ref(&new_strip))
                .await?;
            return Err(error.into());
        }

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        if !old_segments.is_empty() {
            self.allocator
                .pool()
                .free_blocks(old_segments)
                .await
                .map_err(LifecycleError::Cleanup)?;
        }
        info!(chunk_id = ?chunk_id, strip_index, "chunk strip updated");
        Ok(chunk)
    }

    /// Query a chunk by ID.
    pub async fn query_chunk(&self, chunk_id: &ChunkId) -> Result<Chunk, LifecycleError> {
        self.store.get_chunk(chunk_id).await.map_err(|e| match e {
            StoreError::ChunkNotFound => LifecycleError::ChunkNotFound,
            other => LifecycleError::Storage(other),
        })
    }

    /// List chunks with pagination.
    pub async fn list_chunks(
        &self,
        start_after: Option<&ChunkId>,
        max_keys: u32,
    ) -> Result<Vec<Chunk>, LifecycleError> {
        if max_keys == 0 {
            return Ok(Vec::new());
        }
        self.store
            .list_chunks(start_after, max_keys)
            .await
            .map_err(LifecycleError::Storage)
    }

    /// Finish durable `Init` allocation intents left by an interrupted commit.
    pub async fn reconcile_pending_chunks(&self) -> Result<u64, LifecycleError> {
        let mut start_after = None;
        let mut reconciled = 0u64;
        loop {
            let chunks = self.list_chunks(start_after.as_ref(), 1_000).await?;
            if chunks.is_empty() {
                break;
            }
            for mut chunk in chunks.iter().cloned() {
                match ChunkState::from_proto(chunk.state) {
                    ChunkState::Init => {
                        self.commit_strip_segments(&chunk.strips).await?;
                        chunk.state = ProtoChunkState::Active as i32;
                    }
                    ChunkState::Deleted if !chunk.strips.is_empty() => {
                        let segments = chunk.strips.iter().flat_map(extract_segments).collect();
                        self.allocator
                            .pool()
                            .free_blocks(segments)
                            .await
                            .map_err(LifecycleError::Cleanup)?;
                        chunk.strips.clear();
                        chunk.capacity = 0;
                    }
                    _ => continue,
                }
                self.store.put_chunk(&chunk).await?;
                if let (Some(locks), Some(chunk_id)) = (&self.locks, chunk.id) {
                    locks.populate_cache(&chunk_id, chunk);
                }
                reconciled += 1;
            }
            start_after = chunks.last().and_then(|chunk| chunk.id);
            if chunks.len() < 1_000 {
                break;
            }
        }
        Ok(reconciled)
    }

    /// Commit all segments in the given strips to diskdb (two-phase
    /// commit: mark tentative blocks as permanent after chunk persist).
    async fn commit_strip_segments(&self, strips: &[ChunkStrip]) -> Result<(), LifecycleError> {
        let all_segments: Vec<_> = strips.iter().flat_map(extract_segments).collect();
        if all_segments.is_empty() {
            return Ok(());
        }
        self.allocator
            .pool()
            .commit_blocks(all_segments)
            .await
            .map_err(LifecycleError::Commit)
    }

    fn placement_constraints(&self) -> PlacementConstraints {
        let constraints = PlacementConstraints::new();
        if self.allow_unsafe_ec {
            constraints.allow_unsafe_ec()
        } else {
            constraints
        }
    }
}

/// Extract all segments from a strip (mirror or EC).
fn extract_segments(strip: &ChunkStrip) -> Vec<crowdb_protocol::diskdb::rpc::Segment> {
    use crowdb_protocol::chunkdb::rpc::Strip;
    match &strip.strip {
        Some(Strip::MirrorStrip(m)) => m.segments.clone(),
        Some(Strip::EcStrip(ec)) => ec.segments.clone(),
        None => Vec::new(),
    }
}

fn observe_elapsed(metric: &LatencyHistogram, started: std::time::Instant) {
    metric.observe(started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
}
