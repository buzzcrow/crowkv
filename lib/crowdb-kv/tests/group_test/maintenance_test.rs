// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Engine-durability + WAL GC maintenance loop (`group_maintenance`)
//! integration test: verifies `PxGroup::run_maintenance_pass_for_tests`
//! wires a real `CrowdbTreeEngine`'s `persist_snapshot` into the group's WAL
//! `snapshot_slot`, and that WAL segment GC only fires once the group
//! safe-slot (not just the engine snapshot) allows it.
//!
//! Also covers `PxLocalReplica::shutdown` flush + `persist_snapshot` (R20).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::group_election::LeaderElection;
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::common::config::PxElectionConfig;
use crowdb_kv::kv::{CrowdbTreeEngine, CrowdbTreeOptions, KVEngine};
use crowdb_kv::paxos::roles::{Learner, PxBallot, PxLogEntry};
use crowdb_kv::wal::record::WALRecord;
use crowdb_kv::wal::replay::replay_group;
use crowdb_kv::wal::wal_engine::WalEngine;
use crowdb_kv::wal::{IoBackend, MemBlockDevice, WalConfig};

fn sim_backend() -> Arc<IoBackend> {
    Arc::new(IoBackend::MemBlock(MemBlockDevice::new()))
}

/// A file-backed `CrowdbTreeEngine`: `persist_snapshot` (`Crowdbtree::snapshot`)
/// requires a real `page_store` and fails (`invalid_argument: no
/// page_store`) for the default in-memory (`path: None`) engine, so tests
/// exercising it need a real durable backend.
fn open_file_engine(dir: &std::path::Path) -> CrowdbTreeEngine {
    let path = dir.join("data");
    std::fs::create_dir_all(&path).unwrap();
    CrowdbTreeEngine::open(&CrowdbTreeOptions {
        path: Some(path.display().to_string()),
        ..Default::default()
    })
    .unwrap()
}

fn encode_put_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u16.to_le_bytes());
    buf.push(0);
    let key_len = u32::try_from(key.len()).expect("key length exceeds u32");
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key);
    let value_len = u32::try_from(value.len()).expect("value length exceeds u32");
    buf.extend_from_slice(&value_len.to_le_bytes());
    buf.extend_from_slice(value);
    buf
}

/// Learn slots `1..=upto` with real `Put` payloads (unlike a `NoOp`/empty
/// payload, this actually drives `KVEngine::apply` on the replica's engine,
/// which is what a durable engine's `persist_snapshot` reflects).
async fn apply_through_with_engine(replica: &PxLocalReplica, upto: u64) {
    for slot in 1..=upto {
        replica
            .learner
            .learn(
                PxLogEntry {
                    slot,
                    ballot: PxBallot::new(0, 1),
                    term: 1,
                    payload: Bytes::from(encode_put_payload(format!("k{slot}").as_bytes(), b"v")),
                },
                &[],
            )
            .await;
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn maintenance_pass_persists_snapshot_and_gcs_wal_segments_once_safe() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = WalConfig {
        wal_disks: disks.clone(),
        wal_segment_size: 200, // small segments so several accumulate
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    for slot in 1..=20u64 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::from("data"),
        };
        wal.append(&WALRecord::from_accepted(1, &entry)).await.unwrap();
    }
    wal.seal_all().await.unwrap();
    let seg_count_before = wal.index().lock().segments().count();
    assert!(seg_count_before >= 2, "need multiple segments for GC test");

    // Real file-backed CrowdbTreeEngine, attached to a fresh replica with an
    // empty replay result.
    let engine_dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("maintenance");
    let engine = open_file_engine(engine_dir.path());
    let empty_replay = replay_group(&backend, &[PathBuf::from("/empty")], 1)
        .await
        .unwrap();
    let mut replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Leader,
        &empty_replay,
        Box::new(engine),
    )
    .await
    .unwrap();
    replica.set_wal(wal.clone());

    // Drive the engine to durably reflect slots 1..=15 (real Put payloads).
    apply_through_with_engine(&replica, 15).await;
    assert_eq!(replica.contiguous_applied(), 15);

    let mut group = PxGroup::new(1, replica);
    group.set_election_config(PxElectionConfig {
        snapshot_slot_threshold: 1,
        ..PxElectionConfig::for_tests()
    });

    // No real peers, so the recompute uses the local replica's own
    // contiguous_applied as the group safe-slot.
    group.note_peer_applied_for_tests(999, 999);
    assert_eq!(group.group_safe_slot(), 15);

    group.run_maintenance_pass_for_tests().await;

    // `persist_snapshot` durably covers exactly the 15 slots that were
    // `learn`ed with real payloads, and that's what gets fed into the
    // WAL's own snapshot_slot marker.
    assert_eq!(wal.snapshot_slot(), 15);

    // GC watermark is min(engine_snapshot=15, safe_slot=15) = 15: segments
    // fully below slot 15 are unlinked.
    let seg_count_after = wal.index().lock().segments().count();
    assert!(
        seg_count_after < seg_count_before,
        "wal segments below the maintenance-pass watermark should be GC'd"
    );

    // Replay after GC should still work (only surviving segments).
    let result = replay_group(&backend, &disks, 1).await.unwrap();
    assert!(!result.records.is_empty());
}

/// follow-up: the maintenance loop's tick interval is a
/// normal per-group tunable (`PxElectionConfig::maintenance_tick_ms`), not
/// a hardcoded constant. Configure a very short tick, start the *real*
/// periodic loop (not `run_maintenance_pass_for_tests`'s direct call), and
/// confirm a pass actually ran once paused virtual time crosses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_loop_uses_configured_tick_interval() {
    let backend = sim_backend();
    let config = WalConfig {
        wal_disks: vec![PathBuf::from("/wal")],
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    let engine_dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("maintenance");
    let engine = open_file_engine(engine_dir.path());
    let empty_replay = replay_group(&backend, &[PathBuf::from("/empty")], 1)
        .await
        .unwrap();
    let mut replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Leader,
        &empty_replay,
        Box::new(engine),
    )
    .await
    .unwrap();
    replica.set_wal(wal.clone());
    apply_through_with_engine(&replica, 15).await;

    let mut group = PxGroup::new(1, replica);
    group.set_election_config(PxElectionConfig {
        maintenance_tick_ms: 5,
        snapshot_slot_threshold: 1,
        ..PxElectionConfig::for_tests()
    });
    group.note_peer_applied_for_tests(999, 999);
    let group = Arc::new(group);

    assert_eq!(wal.snapshot_slot(), 0, "nothing persisted before the loop runs");

    group.start_engine_maintenance_loop().await;
    // Use a real timer (no paused time) since spawn_blocking uses real
    // OS threads that don't respect tokio's virtual clock. Poll until
    // the loop's first pass persists the snapshot rather than guessing a
    // fixed delay — under heavy parallel test load the spawn_blocking
    // thread may not complete within a naive 50 ms budget.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if wal.snapshot_slot() == 15 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the periodic loop should have run a maintenance pass using the configured tick \
             (snapshot_slot still {} after 2 s)",
            wal.snapshot_slot(),
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn maintenance_pass_does_not_gc_wal_when_safe_slot_lags_snapshot() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = WalConfig {
        wal_disks: disks.clone(),
        wal_segment_size: 200,
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    for slot in 1..=20u64 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::from("data"),
        };
        wal.append(&WALRecord::from_accepted(1, &entry)).await.unwrap();
    }
    wal.seal_all().await.unwrap();
    let seg_count_before = wal.index().lock().segments().count();

    let engine_dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("maintenance");
    let engine = open_file_engine(engine_dir.path());
    let empty_replay = replay_group(&backend, &[PathBuf::from("/empty")], 1)
        .await
        .unwrap();
    let mut replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Leader,
        &empty_replay,
        Box::new(engine),
    )
    .await
    .unwrap();
    replica.set_wal(wal.clone());

    // This replica's own engine is durably caught up through slot 15, but a
    // (simulated) lagging voting peer has only applied through slot 2 --
    // `group_safe_slot` must hold GC back regardless of engine progress.
    apply_through_with_engine(&replica, 15).await;
    let mut group = PxGroup::new(1, replica);
    group.set_election_config(PxElectionConfig {
        snapshot_slot_threshold: 1,
        ..PxElectionConfig::for_tests()
    });
    group.add_remote_replica(crowdb_kv::cluster::PxRemoteReplica::new(
        2,
        "127.0.0.1:2".to_string(),
    ));
    group.note_peer_applied_for_tests(2, 2);
    assert_eq!(group.group_safe_slot(), 2);

    group.run_maintenance_pass_for_tests().await;

    // The engine still persists its own snapshot regardless (purely local
    // decision)...
    assert_eq!(wal.snapshot_slot(), 15);
    //...but WAL GC stays fully blocked by the lagging peer's safe_slot.
    let seg_count_after = wal.index().lock().segments().count();
    assert_eq!(
        seg_count_after, seg_count_before,
        "a lagging voting peer's safe_slot must hold WAL GC back even though this replica's engine is far ahead"
    );
}

/// R20: `PxLocalReplica::shutdown` must flush + persist the engine
/// snapshot so data reaches the block file before the process exits.
/// After shutdown, reopening the engine from the same directory should
/// report a non-zero `resume_from_slot` (the snapshot covers all
/// applied slots), proving the durable prefix was written.
#[tokio::test]
async fn shutdown_persists_engine_snapshot() {
    let engine_dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("maintenance");
    let engine_path = engine_dir.path().join("data");
    std::fs::create_dir_all(&engine_path).unwrap();

    let backend = sim_backend();
    let empty_replay = replay_group(&backend, &[PathBuf::from("/empty")], 1)
        .await
        .unwrap();

    let engine = CrowdbTreeEngine::open(&CrowdbTreeOptions {
        path: Some(engine_path.display().to_string()),
        ..Default::default()
    })
    .unwrap();

    let replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Leader,
        &empty_replay,
        Box::new(engine),
    )
    .await
    .unwrap();

    // Apply real Put payloads through slots 1..=10.
    apply_through_with_engine(&replica, 10).await;
    assert_eq!(replica.contiguous_applied(), 10);

    // Graceful shutdown — should flush + persist_snapshot.
    let report = replica.shutdown(Duration::from_secs(3)).await;
    assert!(
        report.is_clean(),
        "shutdown should be clean, got: {:?}",
        report.errors
    );

    // Reopen the engine from the same directory. The snapshot persisted
    // by shutdown should make resume_from_slot non-zero.
    let reopened = CrowdbTreeEngine::open(&CrowdbTreeOptions {
        path: Some(engine_path.display().to_string()),
        ..Default::default()
    })
    .unwrap();

    let resume_slot = reopened.resume_from_slot();
    assert_eq!(
        resume_slot, 10,
        "resume_from_slot should be 10 after shutdown persisted the snapshot, got {resume_slot}"
    );
}
