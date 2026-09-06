// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reconfiguration: remove the current leader.
//!
//! `g6_reconfig_test` covers adding a replica and removing a non-leader
//! follower; `g1_step_down_survival_test` covers a forced step-down in
//! isolation. This suite combines the two: step the leader down, then remove
//! the stepped-down node from the membership, and verify a new leader is
//! elected on the remaining 2 nodes with CRUD still working.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::group_election::LeaderElection;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxRemoteReplica};
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

/// Discover a leader among a subset of nodes (the survivors after a
/// reconfig). Returns the replica id of the leader, or `None` if no node in
/// the subset currently holds the `Leader` role.
async fn wait_for_leader_in(nodes: &[&Arc<PxKvStore>], timeout: Duration) -> Option<u64> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        for node in nodes {
            let g = node.get_group(1).expect("group");
            if g.local_replica().is_leader() {
                return Some(g.local_replica().id);
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    None
}

async fn put_via_node(
    cluster: &TestCluster,
    node: &Arc<PxKvStore>,
    key: &[u8],
    val: &[u8],
    req_id: u64,
) -> bool {
    let client = cluster.kv_client(node).await;
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

async fn read_via_node(cluster: &TestCluster, node: &Arc<PxKvStore>, key: &[u8]) -> Option<Vec<u8>> {
    let client = cluster.kv_client(node).await;
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

/// Rebuild a group on a node with a remote replica removed (same pattern as
/// `g6_reconfig_test::remove_replica_from_node`).
fn remove_replica_from_node(node: &Arc<PxKvStore>, remove_id: u64) {
    let prior = node.get_group(1).expect("group exists");
    let group_id = prior.group_id;
    let cfg = prior.election_config();

    let local_replica = PxLocalReplica::new(prior.local_replica().id, prior.local_replica().role());

    let mut new_group = PxGroup::new(group_id, local_replica);
    new_group.inherit_local_state_from(&prior);
    new_group.set_election_config(cfg);

    let remotes: Vec<PxRemoteReplica> = prior
        .remote_replica_info()
        .into_iter()
        .filter(|(id, _, _)| *id != remove_id)
        .map(|(id, ep, voting)| {
            let r = PxRemoteReplica::new(id, ep.to_string());
            if voting {
                r
            } else {
                r.with_voting(false)
            }
        })
        .collect();

    new_group.set_remote_replicas(remotes);
    node.add_group(new_group);
}

/// Isolate a node from the group: rebuild its group with zero remotes so its
/// election driver can no longer fan out `RequestVote`/`Heartbeat` to the
/// survivors. This models the decommissioning step of a real remove-leader
/// reconfig — without it the removed node (still alive) keeps disrupting the
/// surviving quorum's election via higher-term `RequestVote` rounds (the vote
/// path has no membership-epoch fence).
fn isolate_node(node: &Arc<PxKvStore>) {
    let prior = node.get_group(1).expect("group exists");
    let group_id = prior.group_id;
    let cfg = prior.election_config();

    let local_replica = PxLocalReplica::new(prior.local_replica().id, prior.local_replica().role());

    let mut new_group = PxGroup::new(group_id, local_replica);
    new_group.inherit_local_state_from(&prior);
    new_group.set_election_config(cfg);
    // No remotes — the node becomes a standalone 1-voter group.
    node.add_group(new_group);
}

/// Step the current leader down, remove it from the membership, and verify a
/// new leader is elected on the remaining 2 nodes with CRUD still working.
#[tokio::test]
async fn reconfig_remove_leader() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    let leader_id = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("initial leader elected");

    // Write data before reconfig.
    let leader_node = cluster.elected_leader().expect("leader present");
    assert!(put_via_node(&cluster, leader_node, b"rc-rmldr-1", b"val-1", 1).await);

    // 1. Force the current leader to step down.
    let reply = leader_node
        .get_group(1)
        .expect("group")
        .step_down_if_leader("g7 remove-leader reconfig");
    assert!(reply.accepted, "leader should accept its own step-down");

    // 2. Remove the stepped-down node from every other node's membership,
    //    and isolate the stepped-down node itself (rebuild its group with
    //    zero remotes) so it can no longer disrupt the survivors' election.
    for node in cluster.nodes() {
        let nid = node.get_group(1).expect("group").local_replica().id;
        if nid != leader_id {
            remove_replica_from_node(node, leader_id);
        }
    }
    isolate_node(leader_node);

    // Collect the surviving (non-removed) nodes.
    let survivors: Vec<&Arc<PxKvStore>> = cluster
        .nodes()
        .iter()
        .filter(|n| n.get_group(1).expect("group").local_replica().id != leader_id)
        .collect();
    assert_eq!(survivors.len(), 2, "two survivors after leader removal");

    // 3. A new leader must be elected on the surviving 2 nodes.
    let new_leader_id = wait_for_leader_in(&survivors, Duration::from_secs(3))
        .await
        .expect("a new leader is elected after leader removal");
    assert_ne!(
        new_leader_id, leader_id,
        "the removed leader must not still be leader"
    );

    // 4. CRUD still works on the remaining 2 nodes: original data survives
    //    and a new write commits with the reduced quorum.
    let new_leader = survivors
        .iter()
        .copied()
        .find(|n| n.get_group(1).expect("group").local_replica().is_leader())
        .expect("new leader node");

    // Poll for the pre-reconfig write to surface through the new leader.
    let start = Instant::now();
    let mut recovered = None;
    while start.elapsed() < Duration::from_secs(3) {
        if let Some(v) = read_via_node(&cluster, new_leader, b"rc-rmldr-1").await {
            recovered = Some(v);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        recovered.as_deref(),
        Some(b"val-1".as_slice()),
        "pre-reconfig write must survive leader removal"
    );

    // New write commits with the 2-node quorum.
    assert!(
        put_via_node(&cluster, new_leader, b"rc-rmldr-2", b"val-2", 2).await,
        "write after leader removal should commit"
    );

    let start = Instant::now();
    let mut confirmed = None;
    while start.elapsed() < Duration::from_secs(3) {
        if let Some(v) = read_via_node(&cluster, new_leader, b"rc-rmldr-2").await {
            confirmed = Some(v);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        confirmed.as_deref(),
        Some(b"val-2".as_slice()),
        "post-reconfig write must be readable through the new leader"
    );

    cluster.shutdown().await;
}
