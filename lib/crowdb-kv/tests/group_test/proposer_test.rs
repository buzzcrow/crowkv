// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! In-flight proposal admission and background-repair tests.
//!
//! These drive crate-internal mechanisms (the inflight-window semaphore and a
//! single repair step) through the `test-util` feature hooks on `PxGroup`,
//! using a single-voter leader group so quorum is 1 and no peer RPCs are
//! needed.

use crowdb_kv::cluster::group::{ProposeResult, PxGroup};
use crowdb_kv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::common::config::PaxosConfig;
use crowdb_kv::paxos::roles::{Learner, PxBallot, PxLogEntry};

/// Single-voter leader group: quorum is 1, so propose / repair complete
/// against the local acceptor with no peer RPCs.
fn single_leader_group() -> PxGroup {
    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let mut group = PxGroup::new(1, local);
    group.set_inflight_config(PaxosConfig::DEFAULT.max_inflight_proposals, true);
    group
}

#[tokio::test]
async fn propose_returns_busy_when_window_is_full() {
    let group = single_leader_group();

    // Exhaust every window permit so the next admission must fail fast.
    let held = group.try_acquire_all_inflight_permits();
    assert_eq!(
        held.len(),
        PaxosConfig::DEFAULT.max_inflight_proposals,
        "should acquire all default window permits"
    );
    match group.propose(b"v".to_vec(), Some(1), Some(1)).await {
        ProposeResult::Busy => {}
        other => panic!("expected Busy with a full window, got {other:?}"),
    }

    // Releasing the permits reopens admission.
    drop(held);
    match group.propose(b"v".to_vec(), Some(1), Some(2)).await {
        ProposeResult::Chosen { .. } => {}
        other => panic!("expected Chosen after window drained, got {other:?}"),
    }
}

#[tokio::test]
async fn repair_once_fills_gap_and_advances_frontier() {
    let group = single_leader_group();

    // Learn slot 2 directly, leaving slot 1 as an abandoned gap: the
    // contiguous frontier stays at 0 while the highest-seen slot is 2.
    group
        .local_replica()
        .learner
        .learn(
            PxLogEntry {
                slot: 2,
                ballot: PxBallot::new(1, 1),
                term: 0,
                payload: bytes::Bytes::from_static(b""),
            },
            &[],
        )
        .await;
    assert_eq!(group.local_replica().contiguous_chosen(), 0, "gap below slot 2");
    assert_eq!(group.local_replica().last_chosen_slot(), 2);

    // One repair step closes slot 1; the frontier then drains through the
    // already-learned slot 2.
    assert_eq!(
        group.repair_once_for_tests().await,
        Some(1),
        "repair should fill the gap at slot 1"
    );
    assert_eq!(
        group.local_replica().contiguous_chosen(),
        2,
        "frontier advances past the filled gap and the trailing learned slot"
    );

    // A second repair has nothing to do.
    assert_eq!(
        group.repair_once_for_tests().await,
        None,
        "no gap remains, so repair is a no-op"
    );
}

/// Single-voter leader group with queue-mode admission and a small window.
fn queue_leader_group(max_inflight: usize) -> PxGroup {
    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let mut group = PxGroup::new(1, local);
    group.set_inflight_config(max_inflight, false);
    group
}

#[tokio::test]
async fn propose_queues_when_policy_is_queue() {
    // Window=1, queue mode: a second concurrent proposal must block
    // until the first one's permit is released.
    let group = queue_leader_group(1);
    let group = std::sync::Arc::new(group);

    // Exhaust the single permit.
    let held = group.try_acquire_all_inflight_permits();
    assert_eq!(held.len(), 1, "window=1 should have 1 permit");

    // Launch a propose in the background — it should block.
    let g = group.clone();
    let task = tokio::spawn(async move { g.propose(b"v".to_vec(), Some(2), Some(1)).await });

    // Give it a moment to ensure it's blocked.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!task.is_finished(), "propose should be blocked on queue");

    // Release the permit — the queued propose should now complete.
    drop(held);
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), task)
        .await
        .expect("propose should complete within 5s after permit release")
        .expect("task should not panic");
    match result {
        ProposeResult::Chosen { .. } => {}
        other => panic!("expected Chosen after queue drain, got {other:?}"),
    }
}
