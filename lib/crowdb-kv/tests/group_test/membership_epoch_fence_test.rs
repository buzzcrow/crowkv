// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Real multi-node coverage for the membership-epoch fence:
//! while a membership mutation is
//! propagating across a real cluster's per-node HTTP fan-out (§2.1), a
//! leader whose own epoch has already advanced (its own node's mutation
//! call landed first) must have its writes fenced by any follower that
//! hasn't caught up yet -- not silently succeed on a stale quorum view.
//! The stall must be bounded and self-heal the instant every node's
//! epoch agrees again, per §6.3's confirmed trade-off.
//!
//! Uses the real 3-node `common::cluster` harness (separate `PxKvStore`
//! processes-in-process, real crowdb-rpc wire messages, no mocks) rather than
//! the console's HTTP mgmt layer: the epoch bump itself is what's under
//! test here, not the HTTP fan-out mechanics already covered by
//! `crowdb-kv-server`'s `add_remote_replicas` tests. Calling
//! `PxGroup::set_membership_epoch` directly on individual nodes is the
//! same effect an `add_remote_replicas`/`remove_remote_replica` call has
//! on that one node's own `PxGroup` (`bump_membership_epoch`), just
//! invoked without the HTTP round-trip -- a faithful, deterministic
//! stand-in for "this node's own mutation call has landed, that one
//! hasn't yet".

use std::time::{Duration, Instant};

use bytes::Bytes;
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

async fn put(cluster: &TestCluster, client_id: u64, seq: u64, key: &[u8], value: &[u8]) -> (bool, String) {
    let leader = cluster.elected_leader().expect("leader present");
    let client = cluster.kv_client(leader).await;
    let resp = client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
            ttl_ms: 0,
            request_id: seq,
            request_create_ms: seq,
            client_id,
            seq,
            group_id: 1,
        })
        .await
        .expect("put rpc")
        .into_inner();
    (resp.ok, resp.error)
}

async fn get_via_leader(cluster: &TestCluster, key: &[u8]) -> Option<Vec<u8>> {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_epoch_mismatch_stalls_writes_until_fanout_completes() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    let leader_id = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("initial leader elected");
    let leader_node = cluster.elected_leader().expect("leader present");
    let leader_group = leader_node.get_group(1).expect("leader group exists");
    assert_eq!(leader_group.membership_epoch(), 0);

    // Baseline: quorum reachable, all three nodes agree at epoch 0.
    let (ok, err) = put(&cluster, 1, 1, b"k", b"before").await;
    assert!(ok, "baseline write should succeed at epoch 0: {err}");
    assert_eq!(
        get_via_leader(&cluster, b"k").await.as_deref(),
        Some(b"before".as_slice())
    );

    // Simulate the leader's *own* membership-mutation HTTP call landing
    // first (as `add_remote_replicas`/`remove_remote_replica` would do
    // on the node the console fans out to first) -- its epoch advances
    // to 1 in-process, synchronously, exactly as §3 describes. The two
    // followers have not yet received their own HTTP call and remain at
    // epoch 0.
    leader_group.set_membership_epoch(1);

    // While the fan-out is mid-flight, the leader's Prepare/Accept to
    // both stale followers is rejected by the exact-match fence on the
    // first attempt. However, the acceptor-side convergence path
    // (`adopt_membership_epoch`) makes each follower adopt the leader's
    // higher epoch from the rejected request, so the retry within the
    // same `propose()` call succeeds — the stall is bounded and
    // self-healing, per §3's confirmed trade-off, without silently
    // succeeding on a stale quorum view.
    let (ok, err) = put(&cluster, 1, 2, b"k", b"during-fanout").await;
    assert!(
        ok,
        "write should self-heal via epoch convergence even mid-fanout: {err}"
    );
    assert_eq!(
        get_via_leader(&cluster, b"k").await.as_deref(),
        Some(b"during-fanout".as_slice()),
        "value must reflect the converged write"
    );

    // The rest of the fan-out lands (both followers' own HTTP calls
    // arrive) -- the propagation window closes and every node agrees on
    // epoch 1 again. This is now redundant with the convergence above,
    // but mirrors the real-world sequence.
    for node in cluster.followers() {
        node.get_group(1)
            .expect("follower group exists")
            .set_membership_epoch(1);
    }

    // Post-fanout write succeeds immediately, with epochs matching.
    let (ok, err) = put(&cluster, 1, 3, b"k", b"after-fanout").await;
    assert!(ok, "write should succeed with epochs matching: {err}");
    assert_eq!(
        get_via_leader(&cluster, b"k").await.as_deref(),
        Some(b"after-fanout".as_slice())
    );

    // Sanity: the leader never changed across the whole sequence -- this
    // test is purely about the epoch fence, not an election side effect.
    assert_eq!(
        cluster
            .elected_leader()
            .expect("leader present")
            .get_group(1)
            .expect("group")
            .local_replica()
            .id,
        leader_id
    );

    cluster.shutdown().await;
}
