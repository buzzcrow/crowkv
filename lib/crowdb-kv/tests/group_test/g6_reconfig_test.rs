// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reconfiguration: add and remove replicas.
//!
//! `PxGroup::add_remote_replica` / `remove_remote_replica` are `&mut self`
//! methods. Since the store holds `Arc<PxGroup>`, the reconfig pattern is
//! to build a new `PxGroup` with the modified membership, call
//! `inherit_local_state_from` to preserve election state, and replace via
//! `add_group()`. These tests verify:
//!
//! - **Add replica**: a 3-node cluster writes data, a 4th node is added,
//!   and writes continue to commit with the expanded quorum.
//! - **Remove non-leader**: a 4-node cluster writes data, a non-leader
//!   follower is removed, and writes continue with the reduced quorum.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::group_election::LeaderElection;
use crowdb_kv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowdb_kv::common::config::PxElectionConfig;
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

/// Retry `put_via_leader` with a deadline — used after reconfig where
/// the leader may briefly reject writes while membership propagates.
async fn put_via_leader_retry(
    cluster: &TestCluster,
    key: &[u8],
    val: &[u8],
    req_id: u64,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if put_via_leader(cluster, key, val, req_id).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
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

/// Rebuild a group on a node with an additional remote replica.
/// Uses the same pattern as the common harness: create a new `PxGroup`,
/// inherit election state, set updated remotes, and replace via `add_group`.
fn add_replica_to_node(node: &Arc<PxKvStore>, new_id: u64, new_endpoint: String) {
    let prior = node.get_group(1).expect("group exists");
    let group_id = prior.group_id;
    let cfg = prior.election_config();

    let local_replica = PxLocalReplica::new(prior.local_replica().id, prior.local_replica().role());

    let mut new_group = PxGroup::new(group_id, local_replica);
    new_group.inherit_local_state_from(&prior);
    new_group.set_election_config(cfg);

    // Collect existing remotes + add the new one.
    let mut remotes: Vec<PxRemoteReplica> = prior
        .remote_replica_info()
        .into_iter()
        .map(|(id, ep, voting)| {
            let r = PxRemoteReplica::new(id, ep.to_string());
            if voting {
                r
            } else {
                r.with_voting(false)
            }
        })
        .collect();
    remotes.push(PxRemoteReplica::new(new_id, new_endpoint));

    new_group.set_remote_replicas(remotes);
    node.add_group(new_group);
}

/// Rebuild a group on a node with a remote replica removed.
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

/// Add a new node to the cluster: create a `PxKvStore`, wire it as a
/// replica of group 1 with all existing nodes as remotes, start it,
/// then update all existing nodes to include the new node.
async fn add_node_to_cluster(cluster: &TestCluster, new_id: u64) -> Arc<PxKvStore> {
    let cfg = PxElectionConfig::for_tests();
    let store = PxKvStore::new(new_id, "127.0.0.1:0".parse().unwrap());
    let server = Arc::new(store);

    let replica = PxLocalReplica::new(new_id, PxLocalReplicaRole::Follower);

    // Collect endpoints from existing nodes.
    let mut remotes: Vec<PxRemoteReplica> = Vec::new();
    for node in cluster.nodes() {
        let g = node.get_group(1).expect("group");
        let rid = g.local_replica().id;
        let ep = node.listen_addr().expect("server started").to_string();
        remotes.push(PxRemoteReplica::new(rid, ep));
    }

    let mut group = PxGroup::new(1, replica);
    group.set_remote_replicas(remotes);
    group.set_election_config(cfg);
    server.add_group(group);
    server.start().await.expect("start new node");

    // Now update all existing nodes to include the new node.
    let new_endpoint = server.listen_addr().expect("new node started").to_string();
    for node in cluster.nodes() {
        add_replica_to_node(node, new_id, new_endpoint.clone());
    }

    server
}

/// Adding a 4th replica to a running 3-node cluster: existing data
/// survives, and new writes commit with the expanded membership.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconfig_add_replica() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    let _leader_id = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("initial leader elected");

    // Write data before reconfig.
    assert!(put_via_leader(&cluster, b"rc-add-1", b"val-1", 1).await);
    poll_for_value(&cluster, b"rc-add-1", b"val-1", Duration::from_secs(3)).await;

    // Add a 4th node. We can't add it to the TestCluster struct (it's
    // immutable), but we can start it independently and wire it in.
    // The key verification is that writes still commit after the reconfig.
    let _node4 = add_node_to_cluster(&cluster, 4).await;

    // Write after reconfig — retry until the leader accepts (quorum
    // is now 3-of-4, but membership may take a moment to propagate).
    assert!(
        put_via_leader_retry(&cluster, b"rc-add-2", b"val-2", 2, Duration::from_secs(3)).await,
        "write after add-replica should commit"
    );
    poll_for_value(&cluster, b"rc-add-2", b"val-2", Duration::from_secs(3)).await;

    // Original data survives.
    poll_for_value(&cluster, b"rc-add-1", b"val-1", Duration::from_secs(3)).await;

    cluster.shutdown().await;
}

/// Removing a non-leader follower from a 3-node cluster: writes continue
/// to commit with the reduced quorum (2-of-2 after removal, since the
/// removed node is no longer counted).
#[tokio::test]
async fn reconfig_remove_non_leader() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    let _leader_id = wait_for_leader(&cluster, Duration::from_secs(3))
        .await
        .expect("initial leader elected");

    // Write data before reconfig.
    assert!(put_via_leader(&cluster, b"rc-rm-1", b"val-1", 1).await);
    poll_for_value(&cluster, b"rc-rm-1", b"val-1", Duration::from_secs(3)).await;

    // Find a non-leader follower to remove.
    let follower_id = cluster
        .nodes()
        .iter()
        .find(|n| {
            let g = n.get_group(1).expect("group");
            !g.local_replica().is_leader()
        })
        .expect("found follower")
        .get_group(1)
        .expect("group")
        .local_replica()
        .id;

    // Remove the follower from all remaining nodes' groups.
    for node in cluster.nodes() {
        let g = node.get_group(1).expect("group");
        if g.local_replica().id != follower_id {
            remove_replica_from_node(node, follower_id);
        }
    }

    // Write after reconfig — retry until the leader accepts with
    // the reduced quorum.
    assert!(
        put_via_leader_retry(&cluster, b"rc-rm-2", b"val-2", 2, Duration::from_secs(3)).await,
        "write after remove-replica should commit"
    );
    poll_for_value(&cluster, b"rc-rm-2", b"val-2", Duration::from_secs(3)).await;

    // Original data survives.
    poll_for_value(&cluster, b"rc-rm-1", b"val-1", Duration::from_secs(3)).await;

    cluster.shutdown().await;
}
