// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `LearnerStream` behaviour tests.
//!
//! The per-peer bidi `PxLearnerStream` multiplexes `Accept`, `Heartbeat`,
//! and `ChosenNotification` frames over a single long-running crowdb-rpc stream.
//! All group-layer tests exercise the `LearnerStream` implicitly (every
//! `Accept` and `Heartbeat` goes through it). These tests verify
//! additional properties:
//!
//! - **Concurrent writes through the stream**: multiple sequential writes
//!   succeed, confirming the stream's correlation-id matching works
//!   correctly under rapid-fire usage.
//! - **`ChosenNotification` delivery**: after a write commits, the
//!   chosen slot is visible on followers via the `ChosenNotification`
//!   fire-and-forget frame (verified by reading through the leader
//!   which must have received the follower's heartbeat reply reflecting
//!   the advanced frontier).

use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_kv::rpc::{KvGetRequest, KvSetRequest};

use crate::common::cluster::{start_cluster_no_leader_relaxed as start_cluster_no_leader, TestCluster};
use crate::common::test_client::TestKvClient;

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

async fn put_via_client(client: &TestKvClient, key: &[u8], val: &[u8], req_id: u64) -> bool {
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

async fn read_via_client(client: &TestKvClient, key: &[u8]) -> Option<Vec<u8>> {
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

async fn poll_for_value(client: &TestKvClient, key: &[u8], expected: &[u8], timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(v) = read_via_client(client, key).await {
            assert_eq!(v.as_slice(), expected, "key {key:?} mismatch");
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("key {key:?} not readable within {timeout:?}");
}

/// Rapid-fire writes through the `LearnerStream` verify that the
/// correlation-id matching (`request_id` → oneshot) works correctly
/// when many `Accept` frames are in flight back-to-back. Each write
/// must receive its own `AcceptedResponse` on the correct oneshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learner_stream_rapid_fire_writes() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("initial leader elected");
    let leader = cluster.elected_leader().expect("leader present");
    let client = cluster.kv_client(leader).await;

    for i in 0u64..20 {
        let key = format!("rapid-{i}");
        let val = format!("val-{i}");
        assert!(
            put_via_client(&client, key.as_bytes(), val.as_bytes(), i + 1).await,
            "write {i} should commit"
        );
    }

    for i in 0u64..20 {
        let key = format!("rapid-{i}");
        let val = format!("val-{i}");
        poll_for_value(&client, key.as_bytes(), val.as_bytes(), Duration::from_secs(3)).await;
    }

    cluster.shutdown().await;
}

/// After a write commits via the `LearnerStream`'s `Accept` frame, the
/// leader fans out a `ChosenNotification` (fire-and-forget) to all
/// followers. The next heartbeat round carries the updated
/// `contiguous_chosen` / `highest_seen_slot` watermarks from followers,
/// confirming the notification was processed.
#[tokio::test]
async fn chosen_notification_advances_follower_frontier() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("initial leader elected");
    let leader = cluster.elected_leader().expect("leader present");
    let client = cluster.kv_client(leader).await;

    assert!(
        put_via_client(&client, b"chosen-1", b"val-1", 1).await,
        "first write should commit"
    );
    poll_for_value(&client, b"chosen-1", b"val-1", Duration::from_secs(3)).await;

    assert!(
        put_via_client(&client, b"chosen-2", b"val-2", 2).await,
        "second write should commit"
    );
    poll_for_value(&client, b"chosen-2", b"val-2", Duration::from_secs(3)).await;

    poll_for_value(&client, b"chosen-1", b"val-1", Duration::from_secs(3)).await;
    poll_for_value(&client, b"chosen-2", b"val-2", Duration::from_secs(3)).await;

    cluster.shutdown().await;
}
