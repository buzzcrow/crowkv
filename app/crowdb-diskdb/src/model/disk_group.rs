// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DdbDiskGroup` — per-disk-group manager: owns the disks, the RCU
//! allocatable-disk context, and the round-robin cursor.

use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwap;
use crowdb_protocol::common::{DiskId, HwStatus};
use crowdb_protocol::diskdb::rpc::BusyBlockValue;
use crowdb_protocol::DiskGroupId;
use dashmap::DashMap;

use crate::metrics::DiskMetrics;
use crate::model::disk::{DdbDisk, DiskUsage};
use crate::model::zone::{AllocatedRange, DdbZone, ZoneUsage};

/// RCU-published set of allocatable disks within the named
/// disk-group, replaced via `Arc` swap on add/remove/status-change.
pub type AllocateDiskContext = Vec<Arc<DdbDisk>>;
pub type Bind = (u64, u64);

/// Result of a successful allocation: `(disk, zone, range)`.
pub type AllocClaim = (Arc<DdbDisk>, Arc<DdbZone>, AllocatedRange);

// Keeps abandoned tentative allocations from growing the process without
// bound. Eviction is safe: commit falls back to the durable KV record.
const MAX_TENTATIVE_BLOCKS: usize = 262_144;

/// Tentative allocation retained until the normal near-term commit arrives.
#[derive(Clone)]
pub struct TentativeBlock {
    pub disk_id: DiskId,
    pub zone_index: u32,
    pub unit_offset: u64,
    pub value: BusyBlockValue,
}

/// A disk-group manager — one per owned disk-group.
pub struct DdbDiskGroup {
    pub disk_group_id: DiskGroupId,
    pub node_id: u64,
    pub rack_id: u64,
    status: AtomicI32,
    /// `(store_id, group_id)` for the bound paxos data group.
    bind: ArcSwap<Bind>,
    pub disks: RwLock<Vec<Arc<DdbDisk>>>,
    /// O(1) disk-id → disk lookup for the free path.
    disk_index: DashMap<DiskId, Arc<DdbDisk>>,
    /// RCU context of allocatable disks within this disk-group.
    allocating_disks: ArcSwap<AllocateDiskContext>,
    /// Round-robin cursor over `allocating_disks`.
    pos_v_disk_ctx: AtomicU64,
    /// Per-disk-group monotonic allocation-incarnation source.
    allocation_ts_source: AtomicU64,
    /// `allocation_ts -> tentative allocation`; recovery-safe KV reads are
    /// used when an entry is absent after restart or eviction.
    tentative_blocks: DashMap<u64, TentativeBlock>,
}

impl DdbDiskGroup {
    pub fn new(disk_group_id: DiskGroupId, node_id: u64, rack_id: u64) -> Self {
        Self {
            disk_group_id,
            node_id,
            rack_id,
            // A.1: start at Init — the sync loop applies the real
            // group-0 status on the first tick.
            status: AtomicI32::new(HwStatus::Init as i32),
            bind: ArcSwap::from_pointee((0, 0)),
            disks: RwLock::new(Vec::new()),
            disk_index: DashMap::new(),
            allocating_disks: ArcSwap::from_pointee(Vec::new()),
            pos_v_disk_ctx: AtomicU64::new(0),
            allocation_ts_source: AtomicU64::new(now_nanos()),
            tentative_blocks: DashMap::new(),
        }
    }

    pub fn cache_tentative(&self, block: TentativeBlock) {
        if self.tentative_blocks.len() >= MAX_TENTATIVE_BLOCKS {
            let eviction_key = self.tentative_blocks.iter().next().map(|entry| *entry.key());
            if let Some(allocation_ts) = eviction_key {
                self.tentative_blocks.remove(&allocation_ts);
            }
        }
        self.tentative_blocks.insert(block.value.allocation_ts, block);
    }

    pub fn tentative(&self, allocation_ts: u64) -> Option<TentativeBlock> {
        self.tentative_blocks
            .get(&allocation_ts)
            .map(|entry| entry.clone())
    }

    pub fn remove_tentative(&self, allocation_ts: u64) -> bool {
        self.tentative_blocks.remove(&allocation_ts).is_some()
    }

    pub fn remove_matching_tentative(
        &self,
        allocation_ts: u64,
        disk_id: DiskId,
        zone_index: u32,
        unit_offset: u64,
    ) -> bool {
        self.tentative_blocks
            .remove_if(&allocation_ts, |_, block| {
                block.disk_id == disk_id && block.zone_index == zone_index && block.unit_offset == unit_offset
            })
            .is_some()
    }

    /// Add a disk to this disk-group. Rebuilds the allocatable disk set.
    pub fn add_disk(&self, disk: Arc<DdbDisk>) {
        self.disk_index.insert(disk.disk_id, Arc::clone(&disk));
        self.disks.write().unwrap().push(disk);
        self.rebuild_allocating_disks();
    }

    /// Remove a disk from in-memory state (`disks` vec + `disk_index`).
    /// Used when a disk is absent from sync and its status is
    /// `Offline`, `Maintenance`, or `Init` — the disk's `DiskKey` was
    /// deleted from group 0 (moved or removed), so absence means it's
    /// gone. Rebuilds the allocatable disk set.
    pub fn remove_disk_from_memory(&self, disk_id: &DiskId) {
        {
            let mut disks = self.disks.write().unwrap();
            disks.retain(|d| d.disk_id != *disk_id);
        }
        self.disk_index.remove(disk_id);
        self.rebuild_allocating_disks();
    }

    /// Rebuild the RCU-published allocatable disk set.
    pub fn rebuild_allocating_disks(&self) {
        let disks = self.disks.read().unwrap();
        let new_ctx: Vec<Arc<DdbDisk>> = disks.iter().filter(|d| d.allocatable()).cloned().collect();
        self.allocating_disks.store(Arc::new(new_ctx));
    }

    /// Generate the next monotonic allocation incarnation.
    /// Advances the source by `max(now(), last + 1)` to guarantee
    /// monotonicity even if the wall clock jumps backwards.
    pub fn next_allocation_ts(&self) -> u64 {
        let now = now_nanos();
        loop {
            let prev = self.allocation_ts_source.load(Ordering::Acquire);
            // Saturating add avoids wrapping to a reusable incarnation.
            let next = now.max(prev.saturating_add(1));
            if self
                .allocation_ts_source
                .compare_exchange(prev, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return next;
            }
        }
    }

    /// Initialize the incarnation source above every recovered incarnation.
    pub fn init_allocation_ts_source_after_load(&self, max_allocation_ts: u64) {
        let now = now_nanos();
        let target = now.max(max_allocation_ts.saturating_add(1));
        self.allocation_ts_source.fetch_max(target, Ordering::AcqRel);
    }

    /// Whether this disk-group can accept allocations.
    pub fn allocatable(&self) -> bool {
        self.status() == HwStatus::Up
    }

    pub fn status(&self) -> HwStatus {
        HwStatus::try_from(self.status.load(Ordering::Acquire)).unwrap_or(HwStatus::Init)
    }

    pub fn bind(&self) -> Bind {
        **self.bind.load()
    }

    pub fn set_bind(&self, bind: Bind) {
        self.bind.store(Arc::new(bind));
    }

    pub fn set_status(&self, status: HwStatus) {
        self.status.store(status as i32, Ordering::Release);
    }

    /// Allocate a single block — round-robin over allocatable disks
    /// within this disk-group, skipping `exclude_disks`.
    ///
    /// Returns `NoSpace` if no disk can satisfy the request.
    pub fn allocate_block(
        &self,
        unit_count: u32,
        exclude_disks: &[DiskId],
        cas_retry_limit: u32,
        zone_rotate_count: u32,
    ) -> Result<AllocClaim, AllocError> {
        if !self.allocatable() {
            return Err(AllocError::NoSpace);
        }
        let ctx = self.allocating_disks.load_full();
        if ctx.is_empty() {
            return Err(AllocError::NoSpace);
        }
        let ctx_len = ctx.len();
        #[allow(clippy::cast_possible_truncation)]
        let start = self.pos_v_disk_ctx.fetch_add(1, Ordering::Relaxed) as usize % ctx_len;
        for i in 0..ctx_len {
            let disk = &ctx[(start + i) % ctx_len];
            if exclude_disks.contains(&disk.disk_id) {
                continue;
            }
            if let Some((zone, range)) = disk.disk_allocate(unit_count, cas_retry_limit, zone_rotate_count) {
                return Ok((Arc::clone(disk), zone, range));
            }
        }
        Err(AllocError::NoSpace)
    }

    /// Allocate `count` blocks of `unit_count` units each, spreading
    /// across disks (anti-affinity via `exclude_disks`).
    ///
    /// Tries round-robin first; if not all `count` claimed, retries
    /// remaining with a full scan.
    pub fn allocate_blocks(
        &self,
        unit_count: u32,
        count: u32,
        exclude_disks: &[DiskId],
        cas_retry_limit: u32,
        zone_rotate_count: u32,
    ) -> Result<Vec<AllocClaim>, AllocError> {
        let mut results: Vec<AllocClaim> = Vec::new();
        let mut used_disks: Vec<DiskId> = exclude_disks.to_vec();

        // First pass: round-robin.
        for _ in 0..count {
            match self.allocate_block(unit_count, &used_disks, cas_retry_limit, zone_rotate_count) {
                Ok((disk, zone, range)) => {
                    used_disks.push(disk.disk_id);
                    results.push((disk, zone, range));
                }
                Err(AllocError::NoSpace) => break,
                Err(error @ AllocError::Persistence) => return Err(error),
            }
        }

        if results.len() == count as usize {
            return Ok(results);
        }

        // Second pass: full scan (random start, skip excluded + used).
        let ctx = self.allocating_disks.load_full();
        while results.len() < count as usize {
            let mut claimed = false;
            #[allow(clippy::cast_possible_truncation)]
            let start = rand::random_range(0..ctx.len().max(1));
            for i in 0..ctx.len() {
                let disk = &ctx[(start + i) % ctx.len()];
                if used_disks.contains(&disk.disk_id) {
                    continue;
                }
                if let Some((zone, range)) =
                    disk.disk_allocate(unit_count, cas_retry_limit, zone_rotate_count)
                {
                    used_disks.push(disk.disk_id);
                    results.push((Arc::clone(disk), zone, range));
                    claimed = true;
                    if results.len() >= count as usize {
                        break;
                    }
                }
            }
            if !claimed {
                break;
            }
        }

        if results.len() == count as usize {
            Ok(results)
        } else {
            for (_, zone, range) in &results {
                if !zone.rollback_allocate(range.unit_offset, range.unit_count) {
                    tracing::error!(
                        disk_group_id = self.disk_group_id,
                        zone_index = zone.zone_index,
                        unit_offset = range.unit_offset,
                        unit_count = range.unit_count,
                        "partial allocation rollback failed; range remains conservatively busy"
                    );
                }
            }
            Err(AllocError::NoSpace)
        }
    }

    /// Free a block by `(disk_id, zone_index, unit_offset, unit_count)`.
    pub fn free_block(&self, disk_id: &DiskId, zone_index: u32, unit_offset: u64, unit_count: u32) -> bool {
        let disk = self
            .disk_index
            .get(disk_id)
            .map(|entry| Arc::clone(entry.value()));
        match disk {
            Some(d) => d.free(zone_index, unit_offset, unit_count),
            None => false,
        }
    }

    // ── R74 space-metrics accessors ───────────────────────────────

    /// Aggregated usage across all disks (R74 §2). `disk_count` =
    /// total disks; `allocatable_disk_count` = RCU `allocating_disks`
    /// size (disks currently `Up` and allocatable). A `Bad` disk's
    /// capacity still counts in the total.
    #[must_use]
    pub fn aggregate_usage(&self) -> DiskGroupUsage {
        let disks_guard = self.disks.read().unwrap();
        let mut capacity_bytes = 0u64;
        let mut busy_bytes = 0u64;
        let mut disk_usages: Vec<DiskUsage> = Vec::with_capacity(disks_guard.len());
        for disk in disks_guard.iter() {
            let u = disk.usage();
            capacity_bytes += u.capacity_bytes;
            busy_bytes += u.busy_bytes;
            disk_usages.push(u);
        }
        #[allow(clippy::cast_possible_truncation)]
        let disk_count = disks_guard.len() as u32;
        #[allow(clippy::cast_possible_truncation)]
        let allocatable_disk_count = self.allocating_disks.load().len() as u32;
        let free_bytes = capacity_bytes.saturating_sub(busy_bytes);
        DiskGroupUsage {
            disk_group_id: self.disk_group_id,
            capacity_bytes,
            busy_bytes,
            free_bytes,
            disk_count,
            allocatable_disk_count,
            disks: disk_usages,
        }
    }

    /// Brief per-zone usage for `(disk_id, zone_index)` (R74 §2).
    /// Returns `None` for an unknown disk or out-of-range zone.
    #[must_use]
    pub fn zone_usage(&self, disk_id: DiskId, zone_index: u32) -> Option<ZoneUsage> {
        let disk = self
            .disk_index
            .get(&disk_id)
            .map(|entry| Arc::clone(entry.value()))?;
        let zones = disk.zones.load();
        let idx = zone_index as usize;
        if idx >= zones.len() {
            return None;
        }
        let unit_size_bytes = disk.disk_value.unit_size_bytes;
        Some(ZoneUsage::from_zone(&zones[idx], unit_size_bytes))
    }

    /// Per-disk hot-path metrics handle for `disk_id` (R74 §3).
    /// Returns `None` for an unknown disk or a disk with no metrics
    /// attached (test disks).
    #[must_use]
    pub fn disk_metrics(&self, disk_id: DiskId) -> Option<Arc<DiskMetrics>> {
        self.disk_index
            .get(&disk_id)
            .and_then(|disk| disk.metrics.clone())
    }

    /// The disk's `unit_size_bytes` (from `disk_value`), or `None` for
    /// an unknown disk. Used by the free path to record byte counters.
    #[must_use]
    pub fn disk_unit_size(&self, disk_id: DiskId) -> Option<u32> {
        let disk = self.disk_index.get(&disk_id)?;
        let unit_size = disk.disk_value.unit_size_bytes;
        Some(unit_size)
    }

    /// Get a cloned `Arc<DdbDisk>` by `disk_id` (R74 query handler).
    /// Returns `None` for an unknown disk.
    #[must_use]
    pub fn get_disk(&self, disk_id: DiskId) -> Option<Arc<DdbDisk>> {
        self.disk_index
            .get(&disk_id)
            .map(|entry| Arc::clone(entry.value()))
    }
}

/// Per-disk-group usage (aggregated across disks, R74 §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskGroupUsage {
    pub disk_group_id: DiskGroupId,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub disk_count: u32,
    pub allocatable_disk_count: u32,
    pub disks: Vec<DiskUsage>,
}

/// Allocation errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocError {
    /// No disk/zone can satisfy the request.
    NoSpace,
    /// Durable allocation record persistence failed.
    Persistence,
}

/// Current wall-clock time in nanoseconds.
pub(crate) fn now_nanos() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(0))
}
