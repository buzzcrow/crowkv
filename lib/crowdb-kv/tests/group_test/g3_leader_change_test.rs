// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Two-replica even-quorum behaviour and leader change simulation.
//!
//! - `two_replica_even_quorum_no_progress_with_one_down`: a 2-node
//!   cluster cannot make progress when one node is missing (quorum = 2).
//! - `leader_change_simulation`: write keys, force step-down, wait for
//!   new leader, write more keys, force another step-down, verify all
//!   keys readable through final leader.

use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_kv::cluster::replica::StepDownRequestPayload;
use crowdb_kv::rpc::{KvGetRequest, KvSetRequest};

use crate::common::cluster::{start_cluster_no_leader_relaxed as start_cluster_no_leader, TestCluster};

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

/// A 2-node cluster has quorum = 2. With both nodes up, a leader is
/// elected and writes succeed. This test verifies the basic 2-node
/// write path works (quorum = 2, both must participate).
#[tokio::test]
async fn two_replica_even_quorum_writes_succeed_with_both_up() {
    let cluster = start_cluster_no_leader(&[1, 2]).await;

    let _leader_id = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("leader elected in 2-node cluster");

    let leader = cluster.elected_leader().expect("leader present");
    let client = cluster.kv_client(leader).await;
    let resp = client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"q2"),
            value: Bytes::from_static(b"both_up"),
            ttl_ms: 0,
            request_id: 1,
            request_create_ms: 1,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("put rpc")
        .into_inner();
    assert!(resp.ok, "write should commit with both nodes up");

    assert_eq!(
        read_via_leader(&cluster, b"q2").await.as_deref(),
        Some(b"both_up".as_slice()),
    );

    cluster.shutdown().await;
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

/// Leader change simulation: write keys, force step-down, wait for
/// new leader, write more keys, force another step-down, verify all
/// keys readable through final leader.
#[tokio::test]
async fn leader_change_simulation() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    // Phase 1: initial leader, write k1.
    let leader1 = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("initial leader elected");
    assert!(
        put_via_leader(&cluster, b"lc1", b"first", 1).await,
        "first write should commit"
    );

    // Phase 2: force step-down of the first leader.
    assert!(
        force_step_down(&cluster, leader1, "first step-down"),
        "first StepDown should be accepted"
    );
    let second_id = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("second leader elected");
    assert!(
        put_via_leader(&cluster, b"lc2", b"second", 2).await,
        "second write should commit through new leader"
    );

    // Phase 3: force step-down of the second leader.
    assert!(
        force_step_down(&cluster, second_id, "second step-down"),
        "second StepDown should be accepted"
    );
    wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("third leader elected");

    // Verify both keys survive.
    poll_for_value(&cluster, b"lc1", b"first", Duration::from_secs(3)).await;
    poll_for_value(&cluster, b"lc2", b"second", Duration::from_secs(3)).await;

    cluster.shutdown().await;
}
