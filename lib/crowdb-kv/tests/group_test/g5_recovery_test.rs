// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Recovery above durable-commit watermark via bulk Phase 1.
//!
//! When a new leader takes over, it runs `run_bulk_phase1` to repair
//! any slots in the open prefix `[floor+1, ceiling]` that it hasn't
//! seen chosen. This test verifies:
//!
//! - After a leader step-down and re-election, the new leader recovers
//!   all previously committed values via bulk Phase 1.
//! - Values written before the step-down are readable through the new
//!   leader, even if the new leader was a follower that missed some
//!   `ChosenNotification` frames.

use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_kv::cluster::replica::StepDownRequestPayload;
use crowdb_kv::rpc::{KvGetRequest, KvSetRequest};

use crate::common::cluster::{start_cluster_no_leader, TestCluster};

async fn wait_for_leader(cluster: &TestCluster, timeout: Duration) -> Option<u64> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(node) = cluster.elected_leader() {
            return Some(node.get_group(1).expect("group").local_replica().id);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    None
}

async fn put_via_leader(cluster: &TestCluster, key: &[u8], val: &[u8], req_id: u64) -> bool {
    let leader = cluster.elected_leader().expect("leader present");
    let client = cluster.kv_client(leader).await;
    let resp = client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(val),
            ttl_ms: 0,
            request_id: req_id,
            request_create_ms: req_id,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("put rpc")
        .into_inner();
    resp.ok
}

async fn read_via_leader(cluster: &TestCluster, key: &[u8]) -> Option<Vec<u8>> {
    let leader = cluster.elected_leader()?;
    let client = cluster.kv_client(leader).await;
    let resp = client
        .get(KvGetRequest {
            version: 1,
            key: Bytes::copy_from_slice(key),
            request_id: 9001,
            request_create_ms: 9001,
            group_id: 1,
            read_mode: 0,
            min_slot: 0,
        })
        .await
        .ok()?
        .into_inner();
    if resp.ok && !resp.not_found {
        Some(resp.value.to_vec())
    } else {
        None
    }
}

async fn poll_for_value(cluster: &TestCluster, key: &[u8], expected: &[u8], timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(v) = read_via_leader(cluster, key).await {
            assert_eq!(v.as_slice(), expected, "key {key:?} mismatch");
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("key {key:?} not readable within {timeout:?}");
}

fn force_step_down(cluster: &TestCluster, leader_id: u64, reason: &str) -> bool {
    let node = cluster.elected_leader().expect("leader present");
    let term = node
        .get_group(1)
        .expect("group")
        .local_replica()
        .current_term_snapshot();
    let reply = node
        .get_group(1)
        .expect("group")
        .local_replica()
        .handle_step_down(&StepDownRequestPayload {
            term,
            target_leader_id: leader_id,
            reason: reason.into(),
        });
    reply.accepted
}

/// After a leader step-down and re-election, the new leader runs
/// bulk Phase 1 to recover any slots it hasn't seen chosen. This
/// test verifies that all previously committed values survive the
/// leader change and are readable through the new leader.
///
/// This is distinct from `g3_leader_change_simulation` which does
/// two consecutive step-downs: this test writes more data (10 keys)
/// to exercise a larger bulk Phase 1 repair range, and verifies
/// every key individually.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_phase1_recovers_all_committed_values() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    let leader1 = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("initial leader elected");

    // Write 10 keys through the first leader.
    for i in 0u64..10 {
        let key = format!("recover-{i}");
        let val = format!("val-{i}");
        assert!(
            put_via_leader(&cluster, key.as_bytes(), val.as_bytes(), i + 1).await,
            "write {i} should commit"
        );
    }

    // Force step-down of the first leader.
    assert!(
        force_step_down(&cluster, leader1, "bulk phase 1 test"),
        "step-down should be accepted"
    );

    // Wait for a new leader to be elected. The new leader will run
    // bulk Phase 1 to recover any slots it hasn't seen chosen.
    let _leader2 = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("second leader elected after step-down");

    // All 10 keys must be readable through the new leader.
    // poll_for_value retries until bulk Phase 1 completes — no
    // fixed sleep needed.
    for i in 0u64..10 {
        let key = format!("recover-{i}");
        let val = format!("val-{i}");
        poll_for_value(&cluster, key.as_bytes(), val.as_bytes(), Duration::from_secs(3)).await;
    }

    // Write a new key through the new leader to verify it's functional.
    assert!(
        put_via_leader(&cluster, b"after-recovery", b"ok", 100).await,
        "write after recovery should commit"
    );
    poll_for_value(&cluster, b"after-recovery", b"ok", Duration::from_secs(3)).await;

    cluster.shutdown().await;
}

/// A second step-down after recovery verifies that the bulk Phase 1
/// repair from the first re-election produced a consistent state:
/// the third leader must also see all values, including the one
/// written after the first recovery.
#[tokio::test]
async fn bulk_phase1_after_second_step_down() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    let leader1 = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("initial leader elected");

    // Write keys through first leader.
    assert!(put_via_leader(&cluster, b"b1", b"v1", 1).await);
    assert!(put_via_leader(&cluster, b"b2", b"v2", 2).await);

    // First step-down.
    assert!(force_step_down(&cluster, leader1, "first"));
    let leader2 = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("second leader elected");

    // Write more keys through second leader.
    assert!(put_via_leader(&cluster, b"b3", b"v3", 3).await);

    // Second step-down.
    assert!(force_step_down(&cluster, leader2, "second"));
    wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("third leader elected");

    // All keys must survive two leader changes.
    poll_for_value(&cluster, b"b1", b"v1", Duration::from_secs(3)).await;
    poll_for_value(&cluster, b"b2", b"v2", Duration::from_secs(3)).await;
    poll_for_value(&cluster, b"b3", b"v3", Duration::from_secs(3)).await;

    cluster.shutdown().await;
}
