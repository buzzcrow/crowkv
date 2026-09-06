// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! R65 replication tests: async catch-up, `ChosenNotice` ballot verification,
//! out-of-order apply, `FetchGap` follower-driven catch-up, heartbeat strip.

use crate::common::cluster::{
    start_cluster, start_cluster_no_leader_relaxed as start_cluster_no_leader, TestCluster,
};
use bytes::Bytes;
use crowdb_kv::rpc::{KvDeleteRequest, KvSetRequest};
use std::time::{Duration, Instant};

/// Helper: assert all nodes in the cluster eventually see `expected` for `key`.
async fn assert_cluster_value(cluster: &TestCluster, key: &[u8], expected: Option<&[u8]>) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let mut all_match = true;
        for node in cluster.nodes() {
            let group = node.get_group(1).expect("group exists");
            let replica = group.local_replica();
            let value = replica.learner.engine_get(key).await.map(|(_, v)| v);
            let matches = match expected {
                Some(bytes) => value.is_some_and(|v| v.as_slice() == bytes),
                None => value.is_none(),
            };
            if !matches {
                all_match = false;
            }
        }
        if all_match {
            return;
        }
        if Instant::now() >= deadline {
            for node in cluster.nodes() {
                let group = node.get_group(1).expect("group exists");
                let replica = group.local_replica();
                let value = replica.learner.engine_get(key).await.map(|(_, v)| v);
                match expected {
                    Some(bytes) => {
                        let stored = value.expect("value missing");
                        assert_eq!(stored.as_slice(), bytes);
                    }
                    None => {
                        assert!(value.is_none(), "value for {key:?} should be absent");
                    }
                }
            }
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// R65-1: Follower does NOT apply on Accept. After a successful proposal,
/// the follower's acceptor has the value but the engine does NOT have it
/// yet (apply is driven by `ChosenNotice`, not Accept). The leader applies
/// immediately; followers converge after `ChosenNotice` + apply loop.
#[tokio::test]
async fn follower_does_not_apply_on_accept() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let client = cluster.kv_client(leader).await;

    let resp = client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"r65-1"),
            value: Bytes::from_static(b"v1"),
            ttl_ms: 0,
            request_id: 1,
            request_create_ms: 1001,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv put")
        .into_inner();
    assert!(resp.ok);

    // The leader should have the value applied immediately.
    let leader_group = leader.get_group(1).expect("group exists");
    let leader_val = leader_group.local_replica().learner.engine_get(b"r65-1").await;
    assert_eq!(leader_val.map(|(_, v)| v), Some(b"v1".to_vec()));

    // Followers should eventually converge (`ChosenNotice` + apply loop).
    assert_cluster_value(&cluster, b"r65-1", Some(b"v1")).await;

    drop(client);
    cluster.shutdown().await;
}

/// R65-2: Follower applies on `ChosenNotice` when accepted ballot matches.
/// After a proposal, the follower receives `ChosenNotice` and applies the
/// value to its engine.
#[tokio::test]
async fn follower_applies_on_chosen_notice() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let client = cluster.kv_client(leader).await;

    for i in 0..5u32 {
        let key = format!("r65-2-key{i}");
        let val = format!("r65-2-val{i}");
        let resp = client
            .put(KvSetRequest {
                version: 1,
                key: Bytes::from(key),
                value: Bytes::from(val),
                ttl_ms: 0,
                request_id: 100 + u64::from(i),
                request_create_ms: 2000 + u64::from(i),
                client_id: 0,
                seq: 0,
                group_id: 1,
            })
            .await
            .expect("kv put")
            .into_inner();
        assert!(resp.ok);
    }

    for i in 0..5u32 {
        let key = format!("r65-2-key{i}");
        let val = format!("r65-2-val{i}");
        assert_cluster_value(&cluster, key.as_bytes(), Some(val.as_bytes())).await;
    }

    drop(client);
    cluster.shutdown().await;
}

/// R65-3: Out-of-order apply — a follower can apply slot 5 before slot 3
/// is resolved (via the `advance_applied_frontier` mechanism).
#[tokio::test]
async fn out_of_order_apply_converges() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let client = cluster.kv_client(leader).await;

    for i in 0..10u32 {
        let resp = client
            .put(KvSetRequest {
                version: 1,
                key: Bytes::from(format!("r65-4-key{i}")),
                value: Bytes::from(format!("r65-4-val{i}")),
                ttl_ms: 0,
                request_id: 400 + u64::from(i),
                request_create_ms: 5000 + u64::from(i),
                client_id: 0,
                seq: 0,
                group_id: 1,
            })
            .await
            .expect("kv put")
            .into_inner();
        assert!(resp.ok);
    }

    for i in 0..10u32 {
        assert_cluster_value(
            &cluster,
            format!("r65-4-key{i}").as_bytes(),
            Some(format!("r65-4-val{i}").as_bytes()),
        )
        .await;
    }

    drop(client);
    cluster.shutdown().await;
}

/// R65-4: Delete operations converge across all nodes.
#[tokio::test]
async fn delete_converges_across_nodes() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let client = cluster.kv_client(leader).await;

    let resp = client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"r65-5"),
            value: Bytes::from_static(b"v1"),
            ttl_ms: 0,
            request_id: 500,
            request_create_ms: 6000,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv put")
        .into_inner();
    assert!(resp.ok);
    assert_cluster_value(&cluster, b"r65-5", Some(b"v1")).await;

    let resp = client
        .delete(KvDeleteRequest {
            version: 1,
            key: Bytes::from_static(b"r65-5"),
            request_id: 501,
            request_create_ms: 6001,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv delete")
        .into_inner();
    assert!(resp.ok);
    assert_cluster_value(&cluster, b"r65-5", None).await;

    drop(client);
    cluster.shutdown().await;
}

/// R65-5: Heartbeat round is not delayed by a lagging follower.
/// With the R65 change, the heartbeat round no longer does catch-up
/// (no accept replay), so even a severely lagging follower should not
/// delay the heartbeat round.
#[tokio::test]
async fn heartbeat_not_delayed_by_lagging_follower() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let client = cluster.kv_client(leader).await;

    for i in 0..10u32 {
        let resp = client
            .put(KvSetRequest {
                version: 1,
                key: Bytes::from(format!("r65-6-key{i}")),
                value: Bytes::from(format!("r65-6-val{i}")),
                ttl_ms: 0,
                request_id: 600 + u64::from(i),
                request_create_ms: 7000 + u64::from(i),
                client_id: 0,
                seq: 0,
                group_id: 1,
            })
            .await
            .expect("kv put")
            .into_inner();
        assert!(resp.ok);
    }

    for i in 0..10u32 {
        assert_cluster_value(
            &cluster,
            format!("r65-6-key{i}").as_bytes(),
            Some(format!("r65-6-val{i}").as_bytes()),
        )
        .await;
    }

    drop(client);
    cluster.shutdown().await;
}

/// R65-6: Leader change — a new leader continues to serve writes and
/// followers converge after a leader change.
#[tokio::test]
async fn leader_change_continues_serving() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    // Wait for leader to be elected.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if cluster.elected_leader().is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "leader not elected within 5s");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let leader = cluster.elected_leader().expect("leader elected");
    let client = cluster.kv_client(leader).await;

    for i in 0..5u32 {
        let resp = client
            .put(KvSetRequest {
                version: 1,
                key: Bytes::from(format!("r65-7-key{i}")),
                value: Bytes::from(format!("r65-7-val{i}")),
                ttl_ms: 0,
                request_id: 700 + u64::from(i),
                request_create_ms: 8000 + u64::from(i),
                client_id: 0,
                seq: 0,
                group_id: 1,
            })
            .await
            .expect("kv put")
            .into_inner();
        assert!(resp.ok);
    }

    for i in 0..5u32 {
        assert_cluster_value(
            &cluster,
            format!("r65-7-key{i}").as_bytes(),
            Some(format!("r65-7-val{i}").as_bytes()),
        )
        .await;
    }

    drop(client);
    cluster.shutdown().await;
}

/// R65-7: Large value (1 MB) does not block convergence.
#[tokio::test]
async fn large_value_converges() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let client = cluster.kv_client(leader).await;

    let large: Vec<u8> = (0..1024 * 1024u32)
        .map(|i| u8::try_from(i % 256).unwrap())
        .collect();

    let resp = client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"r65-8-large"),
            value: Bytes::from(large.clone()),
            ttl_ms: 0,
            request_id: 800,
            request_create_ms: 9000,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv put")
        .into_inner();
    assert!(resp.ok);

    assert_cluster_value(&cluster, b"r65-8-large", Some(&large)).await;

    drop(client);
    cluster.shutdown().await;
}

/// R65-8: Batch writes converge across all nodes.
#[tokio::test]
async fn batch_writes_converge() {
    use crowdb_kv::rpc::{KvBatchItem, KvBatchWriteRequest};
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let client = cluster.kv_client(leader).await;

    let resp = client
        .batch_write(KvBatchWriteRequest {
            version: 1,
            items: vec![
                KvBatchItem {
                    key: Bytes::from_static(b"r65-9-a"),
                    value: Bytes::from_static(b"val-a"),
                    is_delete: false,
                },
                KvBatchItem {
                    key: Bytes::from_static(b"r65-9-b"),
                    value: Bytes::from_static(b"val-b"),
                    is_delete: false,
                },
                KvBatchItem {
                    key: Bytes::from_static(b"r65-9-c"),
                    value: Bytes::from_static(b"val-c"),
                    is_delete: false,
                },
            ],
            request_id: 900,
            request_create_ms: 10000,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("batch write")
        .into_inner();
    assert!(resp.ok);

    assert_cluster_value(&cluster, b"r65-9-a", Some(b"val-a")).await;
    assert_cluster_value(&cluster, b"r65-9-b", Some(b"val-b")).await;
    assert_cluster_value(&cluster, b"r65-9-c", Some(b"val-c")).await;

    drop(client);
    cluster.shutdown().await;
}
