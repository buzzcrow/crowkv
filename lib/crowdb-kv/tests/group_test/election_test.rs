// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Leader-election integration tests.
//!
//! Two groups of tests live here:
//!
//! 1. **E2E multi-node** (`single_leader_elected_3_nodes`,
//!    `single_node_auto_promotes`) — drive real clusters via
//!    `common::cluster::start_cluster_no_leader`.
//!
//! 2. **Driver unit tests** (migrated from the inline `#[cfg(test)]`
//!    module in `crowdb_kv/src/cluster/election.rs`) — exercise the
//!    election driver, lease renewal, and admin step-down in
//!    isolation with `tokio::test(start_paused = true)`.

use crate::common::cluster::start_cluster_no_leader;
use crowdb_kv::cluster::group::{ProposeResult, PxGroup};
use crowdb_kv::cluster::group_election::{spawn, LeaderElection, XorShift64};
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::cluster::replica::StepDownRequestPayload;
use crowdb_kv::common::config::PxElectionConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

fn test_cfg() -> PxElectionConfig {
    PxElectionConfig::for_tests()
}

/// Yield to let the election driver start and register its `sleep_until`
/// timer, then advance past the election deadline and poll until the
/// replica reaches the expected role.
///
/// The election driver registers a `sleep_until` timer on its first
/// poll. If that registration happens *after* a single `advance` call
/// (because one `yield_now` didn't give the driver enough time to reach
/// `sleep_until`), the timer deadline is computed from the
/// already-advanced clock and never fires. Yielding multiple times
/// before `advance` ensures the timer is registered first.
async fn advance_until_role(group: &Arc<PxGroup>, cfg: &PxElectionConfig, expected: PxLocalReplicaRole) {
    // Let the election driver start and register its timer.
    for _ in 0..10 {
        if group.local_replica().role() == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    // Advance past the election deadline.
    tokio::time::advance(Duration::from_millis(cfg.election_max_ms + 10)).await;
    // Poll until the role transitions.
    for _ in 0..100 {
        if group.local_replica().role() == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
}

// ------------------------------------------------------------------
// E2E multi-node
// ------------------------------------------------------------------

/// Wait up to `timeout` for *any* node in `cluster` to enter the
/// `Leader` role. Returns the elected node's id on success.
async fn wait_for_leader(cluster: &crate::common::cluster::TestCluster, timeout: Duration) -> Option<u64> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Some(node) = cluster.elected_leader() {
            let group = node.get_group(1).expect("group exists");
            return Some(group.local_replica().id);
        }
        sleep(Duration::from_millis(5)).await;
    }
    None
}

/// A 3-node cluster with no pre-set leader must converge on exactly one
/// `Leader` (the election driver picks via `PreVote` → `RequestVote`).
#[tokio::test]
async fn single_leader_elected_3_nodes() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    let leader_id = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("a leader should be elected within 5s");

    // Verify exactly one leader and that the term has advanced past 0.
    let leaders: Vec<u64> = cluster
        .nodes()
        .iter()
        .filter_map(|n| {
            let g = n.get_group(1).expect("group exists");
            let r = g.local_replica();
            if r.is_leader() {
                Some(r.id)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(leaders.len(), 1, "exactly one leader expected, got: {leaders:?}");
    assert_eq!(leaders[0], leader_id);

    let term = {
        let leader_node = cluster.elected_leader().expect("leader present");
        leader_node
            .get_group(1)
            .expect("group exists")
            .local_replica()
            .current_term_snapshot()
    };
    assert!(term >= 1, "elected leader should have term >= 1, got {term}");

    cluster.shutdown().await;
}

/// A single-node cluster auto-promotes on the first election tick (quorum = 1).
#[tokio::test]
async fn single_node_auto_promotes() {
    let cluster = start_cluster_no_leader(&[42]).await;
    let leader_id = wait_for_leader(&cluster, Duration::from_secs(2))
        .await
        .expect("lone node should self-elect within 2s");
    assert_eq!(leader_id, 42);
    cluster.shutdown().await;
}

/// Drive a fresh `n`-replica cluster (ids `1..=n`) and assert it
/// converges on *exactly one* leader. Exercises odd and even replica
/// counts: with `n` voters the quorum is `n / 2 + 1`, which a single
/// connected cluster can always satisfy (the even-`n` limitation only
/// bites under a perfectly balanced split, which we do not inject).
async fn assert_single_leader_for_replica_count(n: u64) {
    let ids: Vec<u64> = (1..=n).collect();
    let cluster = start_cluster_no_leader(&ids).await;

    let leader_id = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .unwrap_or_else(|| panic!("a leader should be elected for {n}-replica cluster within 10s"));

    let leaders: Vec<u64> = cluster
        .nodes()
        .iter()
        .filter_map(|node| {
            let group = node.get_group(1).expect("group exists");
            let replica = group.local_replica();
            replica.is_leader().then_some(replica.id)
        })
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "exactly one leader expected for {n}-replica cluster, got: {leaders:?}"
    );
    assert_eq!(leaders[0], leader_id);

    cluster.shutdown().await;
}

/// End-to-end: a leader must be elected for every replica count from 1
/// through 7 (covering both odd and even group sizes).
#[tokio::test]
async fn leader_elected_for_replica_counts_1_through_7() {
    for n in 1..=7 {
        assert_single_leader_for_replica_count(n).await;
    }
}

// ------------------------------------------------------------------
// Driver unit tests (migrated from inline `#[cfg(test)]` module)
// ------------------------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn election_driver_scaffold_starts_and_stops() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    let group = Arc::new(PxGroup::new(42, replica));
    let cancel = group.tenure_cancel();

    let weak = Arc::downgrade(&group);
    let handle = spawn(weak, test_cfg(), cancel.clone());

    // Advance past one election deadline so the driver records a tick.
    tokio::time::advance(Duration::from_millis(test_cfg().election_max_ms + 5)).await;
    assert!(
        !handle.is_finished(),
        "driver should still be running before cancel"
    );

    cancel.cancel();
    let join = tokio::time::timeout(Duration::from_secs(1), handle).await;
    assert!(join.is_ok(), "driver did not exit within 1s of cancel");
    assert!(join.unwrap().is_ok(), "driver task panicked");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn election_driver_exits_when_group_dropped() {
    let replica = PxLocalReplica::new(2, PxLocalReplicaRole::Follower);
    let group = Arc::new(PxGroup::new(7, replica));
    let weak = Arc::downgrade(&group);
    let cancel = group.tenure_cancel();
    let handle = spawn(weak, test_cfg(), cancel);

    drop(group);
    tokio::time::advance(Duration::from_millis(test_cfg().election_max_ms + 5)).await;

    let join = tokio::time::timeout(Duration::from_secs(1), handle).await;
    assert!(join.is_ok(), "driver did not notice dropped group within 1s");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn single_voter_candidate_becomes_leader() {
    // Trivial-cluster fast path: local replica is the only voter, so
    // quorum = 1 and the candidate wins its election immediately.
    let replica = PxLocalReplica::new(5, PxLocalReplicaRole::Follower);
    let group = Arc::new(PxGroup::new(13, replica));
    let cancel = group.tenure_cancel();

    let handle = spawn(Arc::downgrade(&group), test_cfg(), cancel.clone());

    advance_until_role(&group, &test_cfg(), PxLocalReplicaRole::Leader).await;

    assert_eq!(
        group.local_replica().role(),
        PxLocalReplicaRole::Leader,
        "expected Leader after self-quorum election"
    );
    assert_eq!(
        group.local_replica().current_term_snapshot(),
        1,
        "term should be bumped to 1 by become_candidate"
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn single_voter_with_prevote_enabled_becomes_leader() {
    // PreVote enabled in `for_tests`. Single voter wins the PreVote
    // trivially (self-grant), bumps term, wins RequestVote, and
    // becomes leader. Verifies the 9.4 path does not regress the
    // 9.3 outcome.
    let cfg = PxElectionConfig {
        prevote_enabled: true,
        ..PxElectionConfig::for_tests()
    };
    let replica = PxLocalReplica::new(9, PxLocalReplicaRole::Follower);
    let group = Arc::new(PxGroup::new(21, replica));
    let cancel = group.tenure_cancel();

    let handle = spawn(Arc::downgrade(&group), cfg, cancel.clone());
    advance_until_role(&group, &cfg, PxLocalReplicaRole::Leader).await;

    assert_eq!(
        group.local_replica().role(),
        PxLocalReplicaRole::Leader,
        "PreVote path should still reach Leader for a single-voter group"
    );
    assert_eq!(group.local_replica().current_term_snapshot(), 1);

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn leader_heartbeat_tick_renews_lease() {
    // Single-voter cluster: become_leader, then the heartbeat ticker
    // bootstraps + extends the lease without sending RPCs.
    let cfg = PxElectionConfig::for_tests();
    let replica = PxLocalReplica::new(31, PxLocalReplicaRole::Follower);
    let group = Arc::new(PxGroup::new(99, replica));
    let cancel = group.tenure_cancel();

    let handle = spawn(Arc::downgrade(&group), cfg, cancel.clone());

    // First election deadline -> Leader.
    advance_until_role(&group, &cfg, PxLocalReplicaRole::Leader).await;
    assert_eq!(group.local_replica().role(), PxLocalReplicaRole::Leader);

    // Run several heartbeat ticks; lease_read_until should advance
    // past the moment of becoming leader. Yield first to let the
    // leader state register its heartbeat timer, then advance.
    let lease_before = group.local_replica().lease_state_snapshot().lease_read_until;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_millis(cfg.heartbeat_interval_ms * 4 + 5)).await;
    for _ in 0..100 {
        if group.local_replica().lease_state_snapshot().lease_read_until > lease_before {
            break;
        }
        tokio::task::yield_now().await;
    }
    let lease_after = group.local_replica().lease_state_snapshot().lease_read_until;
    assert!(
        lease_after > lease_before,
        "lease_read_until should be extended by heartbeat ticks; before={lease_before:?} after={lease_after:?}"
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn admin_step_down_drops_leader_to_follower() {
    let cfg = PxElectionConfig::for_tests();
    let replica = PxLocalReplica::new(17, PxLocalReplicaRole::Follower);
    let group = Arc::new(PxGroup::new(64, replica));
    let cancel = group.tenure_cancel();
    let handle = spawn(Arc::downgrade(&group), cfg, cancel.clone());

    // Single-voter election: Follower -> Leader.
    advance_until_role(&group, &cfg, PxLocalReplicaRole::Leader).await;
    assert_eq!(group.local_replica().role(), PxLocalReplicaRole::Leader);
    let term_before = group.local_replica().current_term_snapshot();

    // Admin step-down via the strict-fence handler.
    let reply = group.local_replica().handle_step_down(&StepDownRequestPayload {
        term: term_before,
        target_leader_id: group.local_replica().id,
        reason: "manual transfer".into(),
    });
    assert!(reply.accepted, "strict-fence StepDown should be accepted");

    // The handler flips role + signals the driver; driver runs the
    // canonical step-down sequence on its next wakeup.
    for _ in 0..100 {
        if group.local_replica().role() == PxLocalReplicaRole::Follower {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(group.local_replica().role(), PxLocalReplicaRole::Follower);
    // Term is preserved on admin step-down.
    assert_eq!(group.local_replica().current_term_snapshot(), term_before);

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn propose_after_admin_step_down_returns_not_leader() {
    let cfg = PxElectionConfig::for_tests();
    let replica = PxLocalReplica::new(23, PxLocalReplicaRole::Follower);
    let group = Arc::new(PxGroup::new(77, replica));
    let cancel = group.tenure_cancel();
    let handle = spawn(Arc::downgrade(&group), cfg, cancel.clone());

    // Single-voter election: Follower -> Leader. Stamp_proposing_term
    // fires inside finalize_leader, so a proposal here would be admitted.
    advance_until_role(&group, &cfg, PxLocalReplicaRole::Leader).await;
    assert_eq!(group.local_replica().role(), PxLocalReplicaRole::Leader);
    let term = group.local_replica().current_term_snapshot();
    assert_eq!(
        group.proposing_term(),
        term,
        "proposing_term should match current_term after election win"
    );

    // Admin step-down via the strict-fence handler.
    let reply = group.local_replica().handle_step_down(&StepDownRequestPayload {
        term,
        target_leader_id: group.local_replica().id,
        reason: "test step-down".into(),
    });
    assert!(reply.accepted);
    for _ in 0..100 {
        if group.local_replica().role() == PxLocalReplicaRole::Follower {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(group.local_replica().role(), PxLocalReplicaRole::Follower);

    // Step 9.8: a fresh propose must short-circuit with NotLeader
    // because role != Leader (despite proposing_term still equalling
    // the stale tenure's term).
    match group.propose(b"after-stepdown".to_vec(), Some(1), Some(1)).await {
        ProposeResult::NotLeader { .. } => {}
        other => panic!("expected NotLeader after step-down, got {other:?}"),
    }

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[test]
fn xorshift_random_between_ms_stays_in_range() {
    let mut rng = XorShift64::new(0x1234_5678_9ABC_DEF0);
    for _ in 0..200 {
        let v = rng.random_between_ms(30, 60);
        assert!((30..=60).contains(&v), "v={v} out of [30, 60]");
    }
}
