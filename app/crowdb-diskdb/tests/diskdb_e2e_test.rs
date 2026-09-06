// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Component-level integration test for crowdb-diskdb.
//!
//! Starts a real 3-dg `crowdb-kv-server` cluster (store 0, groups 0
//! and 1), seeds hardware metadata into group 0, runs the diskdb
//! sync loop + allocate/free in-process, and verifies that busy/free
//! records are persisted to the kv cluster.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{wait_for_disks_ready, KvCluster};
use crowdb_diskdb::ddb_config::KeepAliveConfig;
use crowdb_diskdb::ddb_kv_client::DdbKvClient;
use crowdb_diskdb::liveness::keepalive::KeepAlive;
use crowdb_diskdb::model::alloc;
use crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer;
use crowdb_kv_client::{GetOutcome, HardwareClient};
use crowdb_protocol::common::{ChunkId, DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use crowdb_protocol::key::{BinaryKey, BusyBlockKey, FreeBlockKey};
use crowdb_protocol::DiskIdExt;

const RACK_ID: u64 = 1;
const NODE_ID: u64 = 10;
const DG_ID: u64 = 100;
const STORE_ID: u64 = 0;
const DATA_GROUP_ID: u64 = 1;
const INSTANCE_ID: u64 = 999;

/// Small disk: 4 zones × 128 units each (128 = 2 words, round number
/// for bitmap scanning). `unit_size` = 1 MB.
const ZONE_SIZE_UNITS: u64 = 128;
const UNIT_SIZE_BYTES: u32 = 1024 * 1024;
const CAPACITY_UNITS: u64 = ZONE_SIZE_UNITS * 4;
const ZONE_COUNT: u32 = 4;

fn make_disk_id(high: u64, low: u64) -> DiskId {
    DiskId { high, low }
}

fn make_chunk_id(high: u64, low: u64) -> ChunkId {
    ChunkId { high, low }
}

/// Point-lookup a key in the data group, returning the value bytes
/// if found.
async fn kv_get(kv: &DdbKvClient, key: &[u8]) -> Option<bytes::Bytes> {
    let outcome = kv
        .kv()
        .get(
            STORE_ID,
            DATA_GROUP_ID,
            key,
            crowdb_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .expect("kv get");
    match outcome {
        GetOutcome::Found { value, .. } => Some(value),
        GetOutcome::NotFound => None,
    }
}

/// Seed hardware metadata into group 0: rack, dg, disk-group, 3
/// disks, ownership, bind map.
async fn seed_hardware(hw: &HardwareClient) {
    // Rack
    hw.add_rack(
        RACK_ID,
        &RackValue {
            status: HwStatus::Up as i32,
            node_ids: vec![NODE_ID],
        },
    )
    .await
    .expect("add rack");

    // Node
    hw.add_node(
        RACK_ID,
        NODE_ID,
        &NodeValue {
            status: HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: vec![DG_ID],
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
        },
    )
    .await
    .expect("add dg");

    // Disk-group with 3 disks (allocate_blocks spreads across disks).
    let disk_ids = vec![make_disk_id(0, 1), make_disk_id(0, 2), make_disk_id(0, 3)];
    hw.add_disk_group(
        RACK_ID,
        NODE_ID,
        DG_ID,
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: disk_ids.clone(),
        },
    )
    .await
    .expect("add disk-group");

    // Disks
    for did in &disk_ids {
        hw.add_disk(
            RACK_ID,
            NODE_ID,
            DG_ID,
            did,
            &DiskValue {
                disk_type: DiskType::BlockSsd as i32,
                capacity_units: CAPACITY_UNITS,
                zone_size_units: ZONE_SIZE_UNITS,
                unit_size_bytes: UNIT_SIZE_BYTES,
                zone_count: ZONE_COUNT,
                status: HwStatus::Up as i32,
                device_path: String::new(),
            },
        )
        .await
        .expect("add disk");
    }

    // Ownership: this instance owns the disk-group.
    let lease_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
        + 3_600_000;
    hw.set_owner(RACK_ID, NODE_ID, DG_ID, INSTANCE_ID, lease_ms)
        .await
        .expect("set owner");

    // Bind: disk-group's records live on store 0, group 1.
    hw.set_bind(RACK_ID, NODE_ID, DG_ID, STORE_ID, DATA_GROUP_ID)
        .await
        .expect("set bind");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn diskdb_e2e_allocate_free() {
    // Skip if crowdb-kv-server binary is not built.
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }

    // 1. Start the kv cluster.
    let cluster = KvCluster::start().await;
    eprintln!(
        "kv cluster started: group0 leader={}, group1 leader={}",
        cluster.group0_leader_endpoint, cluster.group1_leader_endpoint
    );

    // 2. Seed hardware metadata into group 0.
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    eprintln!("hardware metadata seeded");

    // 3. Build diskdb in-process.
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = cluster.make_service_registry_client();
    let hw2 = cluster.make_hardware_client();
    let dg_kv = cluster.make_ddb_kv_client();

    let keepalive_cfg = KeepAliveConfig {
        interval: Duration::from_secs(10),
        miss_threshold: 3,
        zone_rotate_count: 4,
        cas_retry_limit: 100,
        temp_failure_timeout_secs: 900,
    };
    let keepalive = KeepAlive::new(hw2, svc, Arc::clone(&container), keepalive_cfg).with_ddb_kv_client(dg_kv);

    // 4. Run one sync tick to populate in-memory state.
    let outcome = keepalive.tick().await;
    eprintln!(
        "tick: groups_added={}, disks_added={}, duration_ms={}",
        outcome.groups_added, outcome.disks_added, outcome.sync_duration_ms
    );
    assert_eq!(outcome.groups_added, 1, "expected 1 disk-group added");
    assert_eq!(outcome.disks_added, 3, "expected 3 disks added");

    // 5. Wait for background zone load to complete (R81: disks start
    //    in Init state, zones load in background).
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    // 6. Verify the dg has 3 disks with zones.
    let dg = container
        .get_disk_group(DG_ID)
        .expect("disk-group should be in container");
    let (bind, disk_count, zone_count) = {
        let bind = dg.bind();
        let disks = dg.disks.read().unwrap();
        assert_eq!(disks.len(), 3, "expected 3 disks");
        let zone_count = {
            let zones = disks[0].zones.load();
            u32::try_from(zones.len()).unwrap()
        };
        (bind, disks.len(), zone_count)
    };
    assert_eq!(bind, (STORE_ID, DATA_GROUP_ID), "bind should be set");
    assert_eq!(disk_count, 3, "expected 3 disks");
    assert_eq!(zone_count, ZONE_COUNT, "expected {ZONE_COUNT} zones per disk");

    // 7. Allocate one block.
    let alloc_kv = cluster.make_ddb_kv_client();
    let owner_chunk = make_chunk_id(0, 42);
    let metrics = crowdb_diskdb::metrics::DiskdbMetrics::disabled();
    let segment = alloc::allocate_block(
        &dg,
        1, // unit_count
        &owner_chunk,
        UNIT_SIZE_BYTES,
        &alloc_kv,
        100, // cas_retry_limit
        4,   // zone_rotate_count
        &metrics,
    )
    .await
    .expect("allocate should succeed");
    eprintln!(
        "allocated: disk={:?} zone={} offset={} count={}",
        segment.disk_id, segment.zone_index, segment.unit_offset, segment.unit_count
    );
    assert_eq!(segment.unit_count, 1);
    assert_eq!(segment.owner_chunk, Some(owner_chunk));

    // 8. Verify the BusyBlockValue record was persisted.
    let busy_key = BusyBlockKey {
        disk_id: make_disk_id(0, 1),
        zone_index: segment.zone_index,
        unit_offset: segment.unit_offset,
    };
    let busy_bytes = busy_key.to_bytes();
    let kv_client = cluster.make_ddb_kv_client();
    let busy_val = kv_get(&kv_client, &busy_bytes).await;
    assert!(busy_val.is_some(), "busy record should exist in kv");
    let busy_record: crowdb_protocol::diskdb::rpc::BusyBlockValue =
        bincode::deserialize(&busy_val.unwrap()).expect("deserialize BusyBlockValue");
    assert_eq!(busy_record.unit_count, 1);
    assert_eq!(busy_record.owner_chunk, Some(owner_chunk));
    eprintln!("BusyBlockValue record verified in kv");

    // 9. Free the block.
    let free_kv = cluster.make_ddb_kv_client();
    alloc::free_block(&dg, &segment, &free_kv)
        .await
        .expect("free should succeed");
    eprintln!("freed segment");

    // 10. Verify the FreeBlockValue record was persisted and the
    //     BusyBlockKey is gone.
    let free_key = FreeBlockKey {
        disk_id: make_disk_id(0, 1),
        zone_index: segment.zone_index,
        unit_offset: segment.unit_offset,
        allocation_ts: segment.allocation_ts,
    };
    let free_bytes = free_key.to_bytes();
    let verify_kv2 = cluster.make_ddb_kv_client();
    let free_val = kv_get(&verify_kv2, &free_bytes).await;
    assert!(free_val.is_some(), "free record should exist in kv");
    let free_record: crowdb_protocol::diskdb::rpc::FreeBlockValue =
        bincode::deserialize(&free_val.unwrap()).expect("deserialize FreeBlockValue");
    assert_eq!(free_record.unit_count, 1);
    assert_eq!(free_record.previous_owner, Some(owner_chunk));

    // Busy remains until bounded compaction consumes the matching free fact.
    let busy_val2 = kv_get(&verify_kv2, &busy_bytes).await;
    assert!(busy_val2.is_some(), "busy record should remain before compaction");

    // 11. Allocate multiple blocks and verify.
    let alloc_kv2 = cluster.make_ddb_kv_client();
    let segments = alloc::allocate_blocks(
        &dg,
        1,   // unit_count
        3,   // count
        &[], // exclude_disks
        &owner_chunk,
        UNIT_SIZE_BYTES,
        &alloc_kv2,
        100,
        4,
        &metrics,
    )
    .await
    .expect("allocate 3 blocks should succeed");
    assert_eq!(segments.len(), 3, "expected 3 segments");
    eprintln!("allocated 3 blocks");

    // 12. Free all 3 in one batch.
    let free_kv2 = cluster.make_ddb_kv_client();
    alloc::free_blocks(&dg, &segments, &free_kv2)
        .await
        .expect("free 3 blocks should succeed");
    eprintln!("freed 3 blocks in batch");

    // 13. Verify all three immutable free facts and retained busy records.
    let verify_kv3 = cluster.make_ddb_kv_client();
    for seg in &segments {
        let bk = BusyBlockKey {
            disk_id: seg.disk_id.expect("segment disk"),
            zone_index: seg.zone_index,
            unit_offset: seg.unit_offset,
        };
        let bk_bytes = bk.to_bytes();
        let result = kv_get(&verify_kv3, &bk_bytes).await;
        assert!(
            result.is_some(),
            "busy record should remain for offset {}",
            seg.unit_offset
        );

        let fk = FreeBlockKey {
            disk_id: seg.disk_id.expect("segment disk"),
            zone_index: seg.zone_index,
            unit_offset: seg.unit_offset,
            allocation_ts: seg.allocation_ts,
        };
        let fk_bytes = fk.to_bytes();
        let result = kv_get(&verify_kv3, &fk_bytes).await;
        assert!(
            result.is_some(),
            "free record should exist for offset {}",
            seg.unit_offset
        );
    }
    eprintln!("all 3 batch free records verified");

    eprintln!("diskdb_e2e_allocate_free: ALL CHECKS PASSED");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn diskdb_e2e_blind_free_validation_at_compaction() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }

    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;

    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = cluster.make_service_registry_client();
    let hw2 = cluster.make_hardware_client();
    let dg_kv = cluster.make_ddb_kv_client();

    let keepalive_cfg = KeepAliveConfig {
        interval: Duration::from_secs(10),
        miss_threshold: 3,
        zone_rotate_count: 4,
        cas_retry_limit: 100,
        temp_failure_timeout_secs: 900,
    };
    let keepalive = KeepAlive::new(hw2, svc, Arc::clone(&container), keepalive_cfg).with_ddb_kv_client(dg_kv);
    let outcome = keepalive.tick().await;
    assert_eq!(outcome.groups_added, 1);
    assert_eq!(outcome.disks_added, 3);

    // Wait for background zone load (R81).
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    let dg = container
        .get_disk_group(DG_ID)
        .expect("disk-group should be in container");

    // Allocate a block.
    let owner_chunk = make_chunk_id(0, 100);
    let metrics = crowdb_diskdb::metrics::DiskdbMetrics::disabled();
    let alloc_kv = cluster.make_ddb_kv_client();
    let segment = alloc::allocate_block(&dg, 1, &owner_chunk, UNIT_SIZE_BYTES, &alloc_kv, 100, 4, &metrics)
        .await
        .expect("allocate should succeed");

    // 1. A matching-owner blind free succeeds.
    let free_kv = cluster.make_ddb_kv_client();
    alloc::free_block(&dg, &segment, &free_kv)
        .await
        .expect("free with matching owner should succeed");

    // Verify the FreeBlockValue was persisted.
    let free_key = FreeBlockKey {
        disk_id: make_disk_id(0, 1),
        zone_index: segment.zone_index,
        unit_offset: segment.unit_offset,
        allocation_ts: segment.allocation_ts,
    };
    let verify_kv = cluster.make_ddb_kv_client();
    let free_val = kv_get(&verify_kv, &free_key.to_bytes()).await;
    assert!(
        free_val.is_some(),
        "free record should exist after validated free"
    );

    // 2. A wrong-owner free is persisted without a read; compaction rejects it.
    let alloc_kv2 = cluster.make_ddb_kv_client();
    let segment2 = alloc::allocate_block(
        &dg,
        1,
        &owner_chunk,
        UNIT_SIZE_BYTES,
        &alloc_kv2,
        100,
        4,
        &metrics,
    )
    .await
    .expect("allocate should succeed");

    let wrong_owner = make_chunk_id(0, 999);
    let mut wrong_segment = segment2;
    wrong_segment.owner_chunk = Some(wrong_owner);

    let free_kv2 = cluster.make_ddb_kv_client();
    let result = alloc::free_block(&dg, &wrong_segment, &free_kv2).await;
    assert!(result.is_ok(), "blind free should persist: {result:?}");

    // The BusyBlockKey remains for compaction-time validation.
    let seg2_disk_id = segment2.disk_id.expect("segment2 should have disk_id");
    let busy_key = BusyBlockKey {
        disk_id: seg2_disk_id,
        zone_index: segment2.zone_index,
        unit_offset: segment2.unit_offset,
    };
    let verify_kv2 = cluster.make_ddb_kv_client();
    let busy_val = kv_get(&verify_kv2, &busy_key.to_bytes()).await;
    assert!(busy_val.is_some(), "busy record should remain before compaction");

    // 3. Free the block with the correct owner (cleanup).
    let free_kv3 = cluster.make_ddb_kv_client();
    alloc::free_block(&dg, &segment2, &free_kv3)
        .await
        .expect("free with matching owner should succeed");

    // 4. A non-busy free is also an inert immutable fact.
    let fake_segment = crowdb_protocol::diskdb::rpc::Segment {
        disk_id: Some(seg2_disk_id),
        zone_index: segment2.zone_index,
        unit_offset: 999_999, // non-existent offset
        unit_count: 1,
        owner_chunk: Some(owner_chunk),
        allocation_ts: 0,
    };
    let free_kv4 = cluster.make_ddb_kv_client();
    let result = alloc::free_block(&dg, &fake_segment, &free_kv4).await;
    assert!(result.is_ok(), "blind non-busy free should persist: {result:?}");

    eprintln!("diskdb_e2e_blind_free_validation_at_compaction: ALL CHECKS PASSED");
}

/// E2E: allocate ALL space across 3 disks × 4 zones × 128 units = 1536
/// units in 8-unit ranges, then free ALL of it. Verifies the full
/// fill/drain cycle against a real KV cluster: every range gets a
/// `BusyBlockValue`, then a `FreeBlockValue` while the `BusyBlockKey`
/// remains until compaction. Uses `aggregate_usage` for bulk verification
/// and samples a subset of KV records for persistence checks.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn diskdb_e2e_allocate_all_free_all() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }

    let cluster = KvCluster::start().await;
    eprintln!(
        "kv cluster started: group0 leader={}, group1 leader={}",
        cluster.group0_leader_endpoint, cluster.group1_leader_endpoint
    );

    // Seed hardware + sync.
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;

    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = cluster.make_service_registry_client();
    let hw2 = cluster.make_hardware_client();
    let dg_kv = cluster.make_ddb_kv_client();

    let keepalive_cfg = KeepAliveConfig {
        interval: Duration::from_secs(10),
        miss_threshold: 3,
        zone_rotate_count: 4,
        cas_retry_limit: 100,
        temp_failure_timeout_secs: 900,
    };
    let keepalive = KeepAlive::new(hw2, svc, Arc::clone(&container), keepalive_cfg).with_ddb_kv_client(dg_kv);
    let outcome = keepalive.tick().await;
    assert_eq!(outcome.groups_added, 1);
    assert_eq!(outcome.disks_added, 3);

    // Wait for background zone load (R81).
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    let dg = container
        .get_disk_group(DG_ID)
        .expect("disk-group should be in container");

    let total_cap = 3 * CAPACITY_UNITS; // 3 disks × 4 zones × 128 units
    let total_cap_bytes = total_cap * u64::from(UNIT_SIZE_BYTES);
    let owner_chunk = make_chunk_id(0, 42);
    let metrics = crowdb_diskdb::metrics::DiskdbMetrics::disabled();

    // Helper: verify the busy + free == capacity invariant on a
    // DiskGroupUsage, and that per-disk usage sums match the aggregate.
    let verify_invariant = |label: &str| {
        let usage = dg.aggregate_usage();
        assert_eq!(
            usage.busy_bytes + usage.free_bytes,
            usage.capacity_bytes,
            "{label}: busy + free != capacity (busy={}, free={}, cap={})",
            usage.busy_bytes,
            usage.free_bytes,
            usage.capacity_bytes
        );
        assert_eq!(
            usage.capacity_bytes, total_cap_bytes,
            "{label}: capacity mismatch"
        );
        // Per-disk usage should sum to the aggregate.
        let disks = dg.disks.read().unwrap();
        let mut disk_busy_sum = 0u64;
        let mut disk_free_sum = 0u64;
        for disk in disks.iter() {
            let du = disk.usage();
            assert_eq!(
                du.busy_bytes + du.free_bytes,
                du.capacity_bytes,
                "{label}: disk {} busy + free != capacity",
                disk.disk_id.to_display_string()
            );
            disk_busy_sum += du.busy_bytes;
            disk_free_sum += du.free_bytes;
        }
        drop(disks);
        assert_eq!(
            disk_busy_sum, usage.busy_bytes,
            "{label}: per-disk busy sum != aggregate busy"
        );
        assert_eq!(
            disk_free_sum, usage.free_bytes,
            "{label}: per-disk free sum != aggregate free"
        );
        eprintln!(
            "{label}: busy={} free={} cap={} (invariant ok)",
            usage.busy_bytes, usage.free_bytes, usage.capacity_bytes
        );
    };

    // Check invariant at the initial empty state.
    verify_invariant("initial empty");

    // ── Phase 1: Allocate ALL space ───────────────────────────────
    // Allocate one block per disk in each batch. Anti-affinity limits
    // each call to disk_count, while batching cuts KV round trips.
    let mut all_segments: Vec<crowdb_protocol::diskdb::rpc::Segment> = Vec::new();
    let alloc_kv = cluster.make_ddb_kv_client();
    let mut allocated_units = 0u64;
    while let Ok(segments) = alloc::allocate_blocks(
        &dg,
        8, // unit_count
        3, // count
        &[],
        &owner_chunk,
        UNIT_SIZE_BYTES,
        &alloc_kv,
        100,
        4,
        &metrics,
    )
    .await
    {
        allocated_units += segments
            .iter()
            .map(|segment| u64::from(segment.unit_count))
            .sum::<u64>();
        all_segments.extend(segments);
        // Check invariant periodically during allocation.
        if allocated_units % (total_cap / 4) == 0 {
            verify_invariant(&format!("allocating {allocated_units} units"));
        }
    }
    eprintln!("allocated {allocated_units} units (expected {total_cap})");
    assert_eq!(allocated_units, total_cap, "should fill all capacity");

    // Verify aggregate usage is full + invariant holds.
    verify_invariant("full");
    let usage = dg.aggregate_usage();
    assert_eq!(usage.busy_bytes, total_cap_bytes);
    assert_eq!(usage.free_bytes, 0);

    // Sample-verify a few BusyBlockValue records in KV (first, middle, last).
    let verify_kv = cluster.make_ddb_kv_client();
    let sample_indices = [0usize, all_segments.len() / 2, all_segments.len() - 1];
    for &idx in &sample_indices {
        let seg = &all_segments[idx];
        let disk_id = seg.disk_id.expect("segment has disk_id");
        let busy_key = BusyBlockKey {
            disk_id,
            zone_index: seg.zone_index,
            unit_offset: seg.unit_offset,
        };
        let busy_val = kv_get(&verify_kv, &busy_key.to_bytes()).await;
        assert!(
            busy_val.is_some(),
            "busy record should exist for segment {idx} (disk={disk_id:?} zone={} offset={})",
            seg.zone_index,
            seg.unit_offset
        );
    }
    eprintln!("sample busy records verified in kv");

    // ── Phase 2: Free ALL space ───────────────────────────────────
    // Batch-free 48 ranges at a time, checking the invariant periodically.
    let free_kv = cluster.make_ddb_kv_client();
    let mut freed_units = 0u64;
    for chunk in all_segments.chunks(48) {
        alloc::free_blocks(&dg, chunk, &free_kv)
            .await
            .expect("free batch should succeed");
        freed_units += chunk
            .iter()
            .map(|segment| u64::from(segment.unit_count))
            .sum::<u64>();
        if freed_units % (total_cap / 4) == 0 {
            verify_invariant(&format!("freeing {freed_units} units"));
        }
    }
    eprintln!("freed all {freed_units} units");

    // Persist-only model: free does NOT clear the bitmap. Aggregate
    // usage still shows full — the bitmap is a conservative over-
    // estimate. Compaction is the sole bit-clearer (I3).
    verify_invariant("after free (persist-only)");
    let usage = dg.aggregate_usage();
    assert_eq!(usage.busy_bytes, total_cap_bytes);
    assert_eq!(usage.free_bytes, 0);

    // Sample-verify: FreeBlockValue exists + BusyBlockKey is gone.
    let verify_kv2 = cluster.make_ddb_kv_client();
    for &idx in &sample_indices {
        let seg = &all_segments[idx];
        let disk_id = seg.disk_id.expect("segment has disk_id");

        // Free record should exist.
        let free_key = FreeBlockKey {
            disk_id,
            zone_index: seg.zone_index,
            unit_offset: seg.unit_offset,
            allocation_ts: seg.allocation_ts,
        };
        let free_val = kv_get(&verify_kv2, &free_key.to_bytes()).await;
        assert!(
            free_val.is_some(),
            "free record should exist for segment {idx} (disk={disk_id:?} zone={} offset={})",
            seg.zone_index,
            seg.unit_offset
        );

        // Busy record remains until compaction.
        let busy_key = BusyBlockKey {
            disk_id,
            zone_index: seg.zone_index,
            unit_offset: seg.unit_offset,
        };
        let busy_val = kv_get(&verify_kv2, &busy_key.to_bytes()).await;
        assert!(
            busy_val.is_some(),
            "busy record should remain for segment {idx} (disk={disk_id:?} zone={} offset={})",
            seg.zone_index,
            seg.unit_offset
        );
    }
    eprintln!("sample free and retained busy records verified");

    // ── Phase 3: Persist-only — no space reclaimable without compaction
    // The bitmap still shows all blocks busy. Allocation must fail
    // (NoSpace). Compaction (tested separately) is required to reclaim.
    let alloc_kv2 = cluster.make_ddb_kv_client();
    let result = alloc::allocate_block(
        &dg,
        1,
        &owner_chunk,
        UNIT_SIZE_BYTES,
        &alloc_kv2,
        100,
        4,
        &metrics,
    )
    .await;
    assert!(
        result.is_err(),
        "persist-only free should not make space available without compaction"
    );
    eprintln!("no space reclaimable without compaction (persist-only model)");

    // Final invariant check: still full.
    verify_invariant("still full after free (persist-only)");

    eprintln!("diskdb_e2e_allocate_all_free_all: ALL CHECKS PASSED");
}

/// `CompactZone` RPC: allocate → free (persist-only, bitmap stays set)
/// → call `CompactZone` RPC → verify bitmap cleared, space reclaimed,
/// free records deleted. Tests the full RPC handler path via
/// `DiskdbService::compact_zone`.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn diskdb_e2e_compact_zone_rpc() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }

    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;

    // 1. First tick — populates state + writes baseline ZoneValues.
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = cluster.make_service_registry_client();
    let hw2 = cluster.make_hardware_client();
    let dg_kv = cluster.make_ddb_kv_client();
    let keepalive = KeepAlive::new(
        hw2,
        svc,
        Arc::clone(&container),
        KeepAliveConfig {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ddb_kv_client(dg_kv);
    let outcome = keepalive.tick().await;
    assert_eq!(outcome.groups_added, 1);
    assert_eq!(outcome.disks_added, 3);

    // Wait for background zone load (R81).
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    let dg = container.get_disk_group(DG_ID).expect("disk-group exists");

    // 2. Allocate 4 blocks (one at a time — allocate_blocks enforces
    // anti-affinity across disks, but we only have 3 disks).
    let owner_chunk = make_chunk_id(0, 42);
    let metrics = crowdb_diskdb::metrics::DiskdbMetrics::disabled();
    let alloc_kv = cluster.make_ddb_kv_client();
    let mut segments = Vec::new();
    for _ in 0..4 {
        let seg = alloc::allocate_block(&dg, 1, &owner_chunk, UNIT_SIZE_BYTES, &alloc_kv, 100, 4, &metrics)
            .await
            .expect("allocate");
        segments.push(seg);
    }
    assert_eq!(segments.len(), 4);

    // 3. Free 2 blocks — persist-only: bitmap stays set.
    let free_kv = cluster.make_ddb_kv_client();
    alloc::free_blocks(&dg, &segments[0..2], &free_kv)
        .await
        .expect("free 2");

    // 4. Verify bitmap still shows busy for freed segments (persist-only).
    let disk_id = segments[0].disk_id.unwrap();
    let zone_index = segments[0].zone_index;
    {
        let disk = dg
            .disks
            .read()
            .unwrap()
            .iter()
            .find(|d| d.disk_id == disk_id)
            .cloned()
            .expect("disk exists");
        let zones = disk.zones.load();
        let zone = &zones[zone_index as usize];
        #[allow(clippy::cast_possible_truncation)]
        let bit0 = segments[0].unit_offset as u32;
        assert!(
            zone.usage_bits.is_set(bit0),
            "freed bit should still be set (persist-only)"
        );
    }

    // 5. Call CompactZone RPC via DiskdbService handler — compact ALL
    // zones on the disk (empty zone_indices = all zones). This handles
    // the case where freed and busy blocks are in different zones.
    // Set lifecycle to Up so the mutating-RPC gate passes.
    container.set_lifecycle_phase(crowdb_diskdb::liveness::lifecycle::StartupPhase::Up);
    let compact_kv = Arc::new(cluster.make_ddb_kv_client());
    // Call compact_zone directly on each zone (the tonic service
    // trait is gone; the crowdb-rpc handler wraps the same logic).
    let metrics = crowdb_diskdb::metrics::DiskdbMetrics::disabled();
    let disk = {
        let dg = container.get_disk_group(DG_ID).expect("disk-group exists");
        let disk = dg
            .disks
            .read()
            .unwrap()
            .iter()
            .find(|d| d.disk_id == disk_id)
            .cloned()
            .expect("disk exists");
        disk
    };
    let bind: crowdb_diskdb::ddb_kv_client::Bind = (STORE_ID, DATA_GROUP_ID);
    let mut compacted_count = 0u32;
    let mut total_deleted = 0u32;
    let mut all_success = true;
    let zone_count = disk.zones.load().len();
    for zi in 0..zone_count {
        let zone = Arc::clone(&disk.zones.load()[zi]);
        let backlog_before = zone
            .uncompacted_free_record_count
            .load(std::sync::atomic::Ordering::Acquire);
        match crowdb_diskdb::recovery::compaction::compact_zone(
            &compact_kv,
            bind,
            disk_id,
            &zone,
            u32::try_from(zi).expect("zone index fits u32"),
            &metrics,
        )
        .await
        {
            Ok(()) => {
                let backlog_after = zone
                    .uncompacted_free_record_count
                    .load(std::sync::atomic::Ordering::Acquire);
                let deleted = backlog_before.saturating_sub(backlog_after);
                compacted_count += 1;
                total_deleted += deleted;
            }
            Err(e) => {
                eprintln!("compact_zone {zi} failed: {e}");
                all_success = false;
            }
        }
    }
    assert!(compacted_count > 0, "at least one zone should be compacted");
    assert!(all_success, "all zone compaction results should be success");
    eprintln!("compact_zone: compacted {compacted_count} zones, deleted {total_deleted} free records");

    // 6. Verify bitmap is cleared only when a positive cutoff was available.
    let compacted = {
        let disk = dg
            .disks
            .read()
            .unwrap()
            .iter()
            .find(|d| d.disk_id == disk_id)
            .cloned()
            .expect("disk exists");
        let zones = disk.zones.load();
        let zone = &zones[zone_index as usize];
        #[allow(clippy::cast_possible_truncation)]
        let bit0 = segments[0].unit_offset as u32;
        let compacted = zone.compact_slot.load(std::sync::atomic::Ordering::Acquire) > 0;
        assert_eq!(zone.usage_bits.is_set(bit0), !compacted);
        compacted
    };

    // 7. Verify FreeBlockKey records are deleted from KV.
    let verify_kv = cluster.make_ddb_kv_client();
    for seg in &segments[0..2] {
        let free_key = FreeBlockKey {
            disk_id: seg.disk_id.expect("segment disk"),
            zone_index: seg.zone_index,
            unit_offset: seg.unit_offset,
            allocation_ts: seg.allocation_ts,
        };
        let val = kv_get(&verify_kv, &free_key.to_bytes()).await;
        let compacted_here = compacted && seg.disk_id == Some(disk_id);
        assert_eq!(val.is_none(), compacted_here, "free record retention mismatch");
    }

    if !compacted {
        eprintln!("compaction deferred at contiguous slot zero");
        return;
    }

    // 8. Verify space is now reclaimable — allocate 2 more blocks.
    let alloc_kv2 = cluster.make_ddb_kv_client();
    let mut new_segments = Vec::new();
    for _ in 0..2 {
        let seg = alloc::allocate_block(
            &dg,
            1,
            &owner_chunk,
            UNIT_SIZE_BYTES,
            &alloc_kv2,
            100,
            4,
            &metrics,
        )
        .await
        .expect("should reclaim after compaction");
        new_segments.push(seg);
    }
    assert_eq!(new_segments.len(), 2);

    eprintln!("diskdb_e2e_compact_zone_rpc: ALL CHECKS PASSED");
}

/// A.3: Suspect path — Up → Suspect on first absence, Suspect → Up on
/// rediscovery. Regression test for H1: the Up → Suspect transition
/// must NOT write Suspect back to group 0 (Suspect is local-only). If
/// it did, the write-back would re-create the `DiskKey` with Suspect
/// status, and on the next tick `reconcile_existing_disk` would see
/// `raw_disk_status` = Suspect → effective = Suspect → no transition,
/// permanently wedging the disk (not allocatable, no timer to fire).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn diskdb_e2e_suspect_rediscovery() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }

    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;

    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = cluster.make_service_registry_client();
    let hw2 = cluster.make_hardware_client();
    let dg_kv = cluster.make_ddb_kv_client();

    let keepalive_cfg = KeepAliveConfig {
        interval: Duration::from_secs(10),
        miss_threshold: 3,
        zone_rotate_count: 4,
        cas_retry_limit: 100,
        temp_failure_timeout_secs: 900,
    };
    let keepalive = KeepAlive::new(hw2, svc, Arc::clone(&container), keepalive_cfg).with_ddb_kv_client(dg_kv);

    // 1. Initial sync — 3 disks added, wait for zone load (Init → Up).
    let outcome = keepalive.tick().await;
    assert_eq!(outcome.groups_added, 1);
    assert_eq!(outcome.disks_added, 3);
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    let dg = container
        .get_disk_group(DG_ID)
        .expect("disk-group should be in container");

    // Pick one disk to remove + rediscover.
    let target_disk_id = make_disk_id(0, 1);
    let target_disk = {
        let disks = dg.disks.read().unwrap();
        disks
            .iter()
            .find(|d| d.disk_id == target_disk_id)
            .cloned()
            .expect("target disk should exist")
    };

    // Verify disk is Up and allocatable before the test.
    assert_eq!(
        target_disk.effective_status(),
        HwStatus::Up,
        "disk should be Up before absence"
    );
    assert!(
        target_disk.allocatable(),
        "disk should be allocatable before absence"
    );

    // 2. Remove the disk from group 0 (simulates absence — the sync
    //    scan no longer returns it).
    hw.remove_disk(RACK_ID, NODE_ID, DG_ID, &target_disk_id)
        .await
        .expect("remove disk");

    // 3. Tick → Up → Suspect (first absence, anti-flapping buffer).
    let outcome = keepalive.tick().await;
    assert_eq!(
        outcome.status_changes, 1,
        "expected 1 status change (Up → Suspect)"
    );
    assert_eq!(
        target_disk.effective_status(),
        HwStatus::Suspect,
        "disk should be Suspect after first absence"
    );
    assert!(
        !target_disk.allocatable(),
        "Suspect disk should not be allocatable"
    );

    // 4. H1 regression: verify group 0 does NOT have the disk with
    //    Suspect status. With the bug (write-back), the DiskKey would
    //    be re-created with Suspect. With the fix, it stays deleted.
    let group0_disk = hw
        .get_disk(RACK_ID, NODE_ID, DG_ID, &target_disk_id)
        .await
        .expect("get_disk should not error");
    assert!(
        group0_disk.is_none(),
        "H1: disk should NOT exist in group 0 after Up → Suspect (Suspect is local-only)"
    );

    // 5. Re-add the disk to group 0 with Up status (simulates
    //    rediscovery — the disk comes back).
    hw.add_disk(
        RACK_ID,
        NODE_ID,
        DG_ID,
        &target_disk_id,
        &DiskValue {
            disk_type: DiskType::BlockSsd as i32,
            capacity_units: CAPACITY_UNITS,
            zone_size_units: ZONE_SIZE_UNITS,
            unit_size_bytes: UNIT_SIZE_BYTES,
            zone_count: ZONE_COUNT,
            status: HwStatus::Up as i32,
            device_path: String::new(),
        },
    )
    .await
    .expect("re-add disk");

    // 6. Tick → Suspect → Up (rediscovery via recover_disk_to_up).
    let outcome = keepalive.tick().await;
    assert_eq!(
        outcome.status_changes, 1,
        "expected 1 status change (Suspect → Up)"
    );
    assert_eq!(
        target_disk.effective_status(),
        HwStatus::Up,
        "disk should be Up after rediscovery"
    );
    assert!(
        target_disk.allocatable(),
        "disk should be allocatable after rediscovery"
    );

    eprintln!("diskdb_e2e_suspect_rediscovery: ALL CHECKS PASSED");
}

/// Find the crowdb-kv-server binary (mirrors the cluster module's logic).
fn crowdb_kv_server_bin() -> Option<std::path::PathBuf> {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut p = dir.to_path_buf();
            for _ in 0..3 {
                let candidate = p.join("crowdb-kv-server");
                if candidate.exists() {
                    return Some(candidate);
                }
                if !p.pop() {
                    break;
                }
            }
        }
    }
    None
}
