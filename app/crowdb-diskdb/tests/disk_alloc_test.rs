// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DdbDisk` + disk-group allocator tests — disk round-robin, zone
//! rotation, multi-disk spread, free-by-disk-id.

use std::collections::HashSet;
use std::sync::Arc;

use crowdb_diskdb::model::disk::DdbDisk;
use crowdb_diskdb::model::disk_group::{AllocError, DdbDiskGroup, TentativeBlock};
use crowdb_diskdb::model::zone::DdbZone;
use crowdb_protocol::common::{DiskId, HwStatus};
use crowdb_protocol::diskdb::rpc::{BusyBlockValue, DiskValue};

fn disk_id(n: u64) -> DiskId {
    DiskId { high: 0, low: n }
}

fn make_disk_value() -> DiskValue {
    DiskValue {
        capacity_units: 16 * 1024,
        zone_size_units: 16 * 1024,
        unit_size_bytes: 1024 * 1024,
        device_path: String::new(),
        ..Default::default()
    }
}

const DG: u64 = 1;
const CAS_RETRY: u32 = 100;
const ZONE_ROTATE: u32 = 4;

fn make_disk(disk_low: u64, zone_count: u32, zone_capacity: u32) -> Arc<DdbDisk> {
    let disk = Arc::new(DdbDisk::new(disk_id(disk_low), DG, 1, 1, make_disk_value()));
    disk.set_effective_status(HwStatus::Up);
    for zi in 0..zone_count {
        let zone = Arc::new(DdbZone::new(disk_id(disk_low), zi, DG, zone_capacity));
        disk.add_zone(&zone);
    }
    disk.rebuild_active_zones(ZONE_ROTATE);
    disk
}

fn make_dg_with_disks(disk_specs: &[(u64, u32, u32)]) -> Arc<DdbDiskGroup> {
    let dg = Arc::new(DdbDiskGroup::new(DG, 1, 1));
    // Default group status is Init; set to Up for allocation tests.
    dg.set_status(HwStatus::Up);
    for &(disk_low, zone_count, zone_capacity) in disk_specs {
        let disk = make_disk(disk_low, zone_count, zone_capacity);
        dg.add_disk(disk);
    }
    dg
}

#[test]
fn tentative_cache_removes_only_matching_block() {
    let dg = DdbDiskGroup::new(DG, 1, 1);
    let block = TentativeBlock {
        disk_id: disk_id(7),
        zone_index: 3,
        unit_offset: 11,
        value: BusyBlockValue {
            allocation_ts: 99,
            unit_count: 2,
            ..Default::default()
        },
    };
    dg.cache_tentative(block);

    assert!(dg.tentative(99).is_some());
    assert!(!dg.remove_matching_tentative(99, disk_id(8), 3, 11));
    assert!(dg.tentative(99).is_some());
    assert!(dg.remove_matching_tentative(99, disk_id(7), 3, 11));
    assert!(dg.tentative(99).is_none());
}

// ── DdbDisk ────────────────────────────────────────────────────

#[test]
fn disk_allocate_single_zone() {
    let disk = make_disk(1, 1, 128);
    let (zone, range) = disk.disk_allocate(4, CAS_RETRY, ZONE_ROTATE).expect("allocate");
    assert_eq!(range.unit_count, 4);
    assert_eq!(range.unit_offset, 0);
    assert_eq!(zone.zone_index, 0);
}

#[test]
fn disk_allocate_rotates_across_active_zones() {
    let disk = make_disk(1, 4, 64);
    // First 4 allocations should land on different zones (round-robin).
    let mut zone_indices = Vec::new();
    for _ in 0..4 {
        let (zone, _) = disk.disk_allocate(1, CAS_RETRY, ZONE_ROTATE).expect("allocate");
        zone_indices.push(zone.zone_index);
    }
    zone_indices.sort_unstable();
    zone_indices.dedup();
    // Should have used at least 2 different zones.
    assert!(
        zone_indices.len() >= 2,
        "expected zone rotation, got {zone_indices:?}"
    );
}

#[test]
fn disk_allocate_returns_none_when_all_zones_full() {
    let disk = make_disk(1, 1, 8);
    for _ in 0..8 {
        disk.disk_allocate(1, CAS_RETRY, ZONE_ROTATE).expect("allocate");
    }
    assert!(disk.disk_allocate(1, CAS_RETRY, ZONE_ROTATE).is_none());
}

#[test]
fn disk_free_is_persist_only() {
    // In the persist-only model, disk.free increments
    // uncompacted_free_record_count but does NOT clear the bitmap.
    // The block stays busy in memory until compaction reclaims it.
    let disk = make_disk(1, 1, 64);
    // Fill the zone completely.
    while disk.disk_allocate(1, CAS_RETRY, ZONE_ROTATE).is_some() {}
    let zones = disk.zones.load();
    assert_eq!(zones[0].used_count.load(std::sync::atomic::Ordering::Acquire), 64);
    let backlog_before = zones[0]
        .uncompacted_free_record_count
        .load(std::sync::atomic::Ordering::Acquire);
    // Free 4 units — persist-only: bitmap NOT cleared.
    assert!(disk.free(0, 0, 4));
    let backlog_after = zones[0]
        .uncompacted_free_record_count
        .load(std::sync::atomic::Ordering::Acquire);
    assert_eq!(backlog_after, backlog_before + 1);
    // Bitmap NOT cleared — used_count unchanged.
    assert_eq!(zones[0].used_count.load(std::sync::atomic::Ordering::Acquire), 64);
    drop(zones);
    // disk_allocate cannot find the freed space (bitmap still shows busy).
    let result = disk.disk_allocate(1, CAS_RETRY, ZONE_ROTATE);
    assert!(
        result.is_none(),
        "persist-only free should not make space available without compaction"
    );
}

#[test]
fn disk_free_wrong_zone_index_fails() {
    let disk = make_disk(1, 2, 64);
    assert!(!disk.free(99, 0, 1));
}

#[test]
fn rotate_clears_compacted_ready_on_published_zones() {
    // When zones are published into the active set via rotation,
    // compacted_ready must be cleared — they will need re-compaction
    // after being allocated from and freed (I5).
    let disk = make_disk(1, 8, 64);
    // rebuild_active_zones picks the first 4 zones. Mark all zones
    // compacted_ready = true first (simulating recovery / compaction).
    {
        let zones = disk.zones.load();
        for zone in zones.iter() {
            zone.mark_compacted_ready();
        }
    }
    // Fill all 4 active zones (256 units) to force rotation.
    for _ in 0..(64 * 4) {
        disk.disk_allocate(1, CAS_RETRY, ZONE_ROTATE);
    }
    // One more allocate triggers rotation (all active zones full).
    // This picks the next 4 zones (4-7) and clears compacted_ready.
    disk.disk_allocate(1, CAS_RETRY, ZONE_ROTATE);
    // After rotation, the new active zones should have
    // compacted_ready = false (cleared on publish).
    let active = disk.active_zone_context.load();
    assert!(
        !active.is_empty(),
        "active set should not be empty after rotation"
    );
    for zone in active.iter() {
        assert!(
            !zone.compacted_ready.load(std::sync::atomic::Ordering::Acquire),
            "zone {} in active set should have compacted_ready = false after rotation",
            zone.zone_index
        );
    }
}

// ── DdbDiskGroup ────────────────────────────────────────────────

#[test]
fn dg_allocate_block_single_disk() {
    let dg = make_dg_with_disks(&[(1, 1, 128)]);
    let (disk, _, range) = dg
        .allocate_block(4, &[], CAS_RETRY, ZONE_ROTATE)
        .expect("allocate");
    assert_eq!(disk.disk_id, disk_id(1));
    assert_eq!(range.unit_count, 4);
}

#[test]
fn dg_allocate_block_round_robins_across_disks() {
    let dg = make_dg_with_disks(&[(1, 1, 128), (2, 1, 128), (3, 1, 128)]);
    let mut disk_ids = Vec::new();
    for _ in 0..3 {
        let (disk, _, _) = dg.allocate_block(1, &[], CAS_RETRY, ZONE_ROTATE).unwrap();
        disk_ids.push(disk.disk_id);
    }
    let disk_set: HashSet<DiskId> = disk_ids.into_iter().collect();
    // Should have used at least 2 different disks.
    assert!(disk_set.len() >= 2, "expected disk round-robin, got {disk_set:?}");
}

#[test]
fn dg_allocate_block_respects_exclude_disks() {
    let dg = make_dg_with_disks(&[(1, 1, 128), (2, 1, 128)]);
    // Exclude disk 1 — should land on disk 2.
    let (disk, _, _) = dg
        .allocate_block(1, &[disk_id(1)], CAS_RETRY, ZONE_ROTATE)
        .expect("allocate");
    assert_eq!(disk.disk_id, disk_id(2));
}

#[test]
fn dg_allocate_block_no_space_when_all_excluded() {
    let dg = make_dg_with_disks(&[(1, 1, 128), (2, 1, 128)]);
    let result = dg.allocate_block(1, &[disk_id(1), disk_id(2)], CAS_RETRY, ZONE_ROTATE);
    assert!(matches!(result, Err(AllocError::NoSpace)));
}

#[test]
fn dg_allocate_blocks_spreads_across_disks() {
    let dg = make_dg_with_disks(&[(1, 1, 128), (2, 1, 128), (3, 1, 128)]);
    let results = dg
        .allocate_blocks(1, 3, &[], CAS_RETRY, ZONE_ROTATE)
        .expect("allocate 3");
    assert_eq!(results.len(), 3);
    let disk_ids: HashSet<DiskId> = results.iter().map(|(d, _, _)| d.disk_id).collect();
    // Anti-affinity: all 3 should be on different disks.
    assert_eq!(disk_ids.len(), 3, "expected 3 different disks, got {disk_ids:?}");
}

#[test]
fn dg_allocate_blocks_no_space_when_count_exceeds_disks() {
    let dg = make_dg_with_disks(&[(1, 1, 128), (2, 1, 128)]);
    // 3 blocks, 2 disks, no exclusions — anti-affinity prevents
    // reusing disks, so the 3rd block can't be placed.
    let result = dg.allocate_blocks(1, 3, &[], CAS_RETRY, ZONE_ROTATE);
    assert!(matches!(result, Err(AllocError::NoSpace)));
}

#[test]
fn dg_allocate_blocks_no_space_rolls_back_partial_claims() {
    let dg = make_dg_with_disks(&[(1, 1, 128), (2, 1, 128)]);
    let usage_before = dg.aggregate_usage();

    let result = dg.allocate_blocks(1, 3, &[], CAS_RETRY, ZONE_ROTATE);

    assert!(matches!(result, Err(AllocError::NoSpace)));
    assert_eq!(dg.aggregate_usage().busy_bytes, usage_before.busy_bytes);
    assert!(dg.allocate_blocks(1, 2, &[], CAS_RETRY, ZONE_ROTATE).is_ok());
}

#[test]
fn dg_allocate_blocks_succeeds_when_count_equals_disks() {
    let dg = make_dg_with_disks(&[(1, 1, 128), (2, 1, 128)]);
    let results = dg
        .allocate_blocks(1, 2, &[], CAS_RETRY, ZONE_ROTATE)
        .expect("allocate 2");
    assert_eq!(results.len(), 2);
}

#[test]
fn node_free_block_by_disk_id() {
    let dg = make_dg_with_disks(&[(1, 1, 128)]);
    let (disk, _, r) = dg.allocate_block(4, &[], CAS_RETRY, ZONE_ROTATE).unwrap();
    // Persist-only free: always succeeds (increments backlog). Double-
    // free detection is deferred to the KV persist layer + scanner.
    assert!(dg.free_block(&disk.disk_id, 0, r.unit_offset, r.unit_count));
    assert!(dg.free_block(&disk.disk_id, 0, r.unit_offset, r.unit_count));
}

#[test]
fn node_free_block_unknown_disk_id_fails() {
    let dg = make_dg_with_disks(&[(1, 1, 128)]);
    assert!(!dg.free_block(&disk_id(999), 0, 0, 1));
}

// ── Status-change refresh ───────────────────────────────────────

#[test]
fn node_rebuild_allocating_disks_on_status_change() {
    use crowdb_protocol::common::HwStatus;
    let dg = make_dg_with_disks(&[(1, 1, 128), (2, 1, 128), (3, 1, 128)]);
    // All 3 disks are allocatable initially — round-robin should
    // eventually pick all 3.
    let mut picked = std::collections::HashSet::new();
    for _ in 0..6 {
        let (disk, _, _) = dg.allocate_block(1, &[], CAS_RETRY, ZONE_ROTATE).unwrap();
        picked.insert(disk.disk_id);
    }
    assert_eq!(picked.len(), 3, "expected all 3 disks before status change");

    // Transition disk 1 to Missing — rebuild_allocating_disks should
    // remove it from the RCU context.
    {
        let all_disks = dg.disks.read().unwrap();
        let target = all_disks.iter().find(|d| d.disk_id == disk_id(1)).unwrap();
        target.set_effective_status(HwStatus::Missing);
    }
    dg.rebuild_allocating_disks();

    // Round-robin should now only pick disks 2 and 3 (never disk 1).
    let mut picked_after = std::collections::HashSet::new();
    for _ in 0..4 {
        let (disk, _, _) = dg.allocate_block(1, &[], CAS_RETRY, ZONE_ROTATE).unwrap();
        picked_after.insert(disk.disk_id);
    }
    assert!(
        !picked_after.contains(&disk_id(1)),
        "disk 1 should never be picked after Missing"
    );
    assert!(
        picked_after.len() >= 2,
        "expected disks 2 and 3 to be picked, got {picked_after:?}"
    );
}
