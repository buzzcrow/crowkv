// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! G1 (P1 exit criterion, / a value written
//! to a 3-node cluster survives a forced leader step-down — after a new leader
//! is elected, the committed write is still readable.

use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_kv::cluster::replica::StepDownRequestPayload;
use crowdb_kv::rpc::{KvGetRequest, KvSetRequest};

use crate::common::cluster::{start_cluster_no_leader_relaxed as start_cluster_no_leader, TestCluster};

/// Poll until some node reports the `Leader` role, returning its node id.
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

/// Linearizable read of `key` via whichever node is currently leader. Returns
/// `Some(value)` on a hit, `None` if there is no leader yet or the key reads as
/// not-found / error this round.
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
            read_mode: 0, // Linearizable
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

#[tokio::test]
async fn write_survives_forced_leader_step_down() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    // 1. Wait for the initial leader and commit a write through it.
    let leader_id = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("initial leader elected");

    let leader_node = cluster.elected_leader().expect("leader present");
    let leader_client = cluster.kv_client(leader_node).await;
    let put = leader_client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"g1"),
            value: Bytes::from_static(b"survives"),
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
    assert!(put.ok, "write should commit on the initial leader: {put:?}");

    // Confirm it is readable before the step-down.
    assert_eq!(
        read_via_leader(&cluster, b"g1").await.as_deref(),
        Some(b"survives".as_slice()),
        "value should be readable before step-down"
    );
    drop(leader_client);

    // 2. Force the current leader to step down via the admin StepDown path.
    let term = leader_node
        .get_group(1)
        .expect("group")
        .local_replica()
        .current_term_snapshot();
    let reply = leader_node
        .get_group(1)
        .expect("group")
        .local_replica()
        .handle_step_down(&StepDownRequestPayload {
            term,
            target_leader_id: leader_id,
            reason: "g1 forced step-down".into(),
        });
    assert!(reply.accepted, "strict-fence StepDown should be accepted");

    // 3. The cluster must re-elect a leader (the surviving quorum) and the
    //    committed write must still be readable through it. The new leader
    //    recovers the slot via bulk Phase 1, so poll until it surfaces.
    wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("a leader is re-elected after step-down");

    let start = Instant::now();
    let mut recovered = None;
    while start.elapsed() < Duration::from_secs(3) {
        if let Some(value) = read_via_leader(&cluster, b"g1").await {
            recovered = Some(value);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        recovered.as_deref(),
        Some(b"survives".as_slice()),
        "the committed write must survive the forced leader step-down"
    );

    cluster.shutdown().await;
}
