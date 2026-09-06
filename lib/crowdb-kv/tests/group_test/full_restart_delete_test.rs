// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Full-cluster restart durability for deletes.
//!
//! Reproduces the web-console `cluster_restart_recovery_test` failure at the
//! `crowdb_kv` layer (no console / crowdb-rpc proxy): a 3-node WAL-backed group commits
//! puts and deletes, then EVERY node is killed and restarted from its own WAL
//! dir. After restart the deleted keys must stay deleted (no resurrection) and
//! the surviving keys must remain readable.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::group_config::GroupConfigStore;
use crowdb_kv::cluster::group_election::LeaderElection;
use crowdb_kv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::common::config::{PxElectionConfig, WalConfig};
use crowdb_kv::rpc::{KvDeleteRequest, KvGetRequest, KvSetRequest};
use crowdb_kv::wal::replay::replay_group;
use crowdb_kv::wal::{IoBackend, WalEngine};
use crowdb_kv_client::KvRpcTransport;

use crate::common::test_client::TestKvClient;

const GROUP: u64 = 1;

struct WalNode {
    id: u64,
    store: Arc<PxKvStore>,
    wal_dir: PathBuf,
}

struct WalCluster {
    nodes: Vec<WalNode>,
    kv_transport: Arc<KvRpcTransport>,
    _tmp: crowdb_test_harness::test_dirs::TestDir,
    _net: tokio::sync::MutexGuard<'static, ()>,
}

fn node_wal_dir(root: &Path, id: u64) -> PathBuf {
    root.join(format!("node-{id}")).join("wal")
}

fn node_conf_dir(root: &Path, id: u64) -> PathBuf {
    root.join(format!("node-{id}")).join("conf")
}

async fn build_wal_group(
    id: u64,
    wal_dir: &Path,
    conf_dir: &Path,
    peers: &[(u64, String)],
    cfg: PxElectionConfig,
) -> PxGroup {
    let backend = Arc::new(IoBackend::File);
    let config = WalConfig::with_root(wal_dir.to_path_buf());
    let replay = replay_group(&backend, &config.wal_disks, GROUP)
        .await
        .expect("replay group");
    let next_seg = replay.max_segment_id.saturating_add(1).max(1);
    let wal = WalEngine::create_with_next_segment_id(backend, config, GROUP, next_seg)
        .await
        .expect("create wal");

    let mut replica = PxLocalReplica::restore_from_replay(id, PxLocalReplicaRole::Follower, &replay)
        .await
        .expect("restore replica");
    replica.set_wal(wal);

    let mut group = PxGroup::new(GROUP, replica);
    let store = GroupConfigStore::new(conf_dir, 0, GROUP);
    if let Some(persisted) = store.load().await.expect("load config") {
        group.apply_config(&persisted);
    }
    group.set_config_store(store);
    // Caller-supplied endpoints (e.g., after restart rewiring) override the
    // persisted endpoints so ephemeral ports can be reconciled.
    for (peer_id, endpoint) in peers {
        if *peer_id != id {
            group.update_member_endpoint(*peer_id, endpoint.clone());
        }
    }
    group.set_election_config(cfg);
    let next_slot = group
        .local_replica()
        .highest_seen_slot()
        .max(group.local_replica().last_chosen_slot())
        .max(group.local_replica().contiguous_applied())
        .saturating_add(1)
        .max(1);
    group.set_next_slot(next_slot);
    group
}

async fn start_wal_cluster(ids: &[u64]) -> WalCluster {
    let net = crate::common::net_lock::lock().await;
    crate::common::logging::init_test_subscriber();
    let tmp = crowdb_test_harness::test_dirs::tempdir_in_test_data("full-restart-delete");
    let cfg = PxElectionConfig::for_tests();

    let mut nodes = Vec::with_capacity(ids.len());
    for &id in ids {
        let wal_dir = node_wal_dir(tmp.path(), id);
        let conf_dir = node_conf_dir(tmp.path(), id);
        let placeholders: Vec<(u64, String)> = ids
            .iter()
            .map(|&other| {
                (
                    other,
                    format!("127.0.0.1:{}", crate::common::net_lock::unique_port()),
                )
            })
            .collect();
        let group = build_wal_group(id, &wal_dir, &conf_dir, &placeholders, cfg).await;

        let store = Arc::new(PxKvStore::new(id, "127.0.0.1:0".parse().unwrap()));
        store.add_group(group);
        store.start().await.expect("start store");
        nodes.push(WalNode { id, store, wal_dir });
    }

    let endpoints: Vec<(u64, String)> = nodes
        .iter()
        .map(|n| (n.id, n.store.listen_addr().expect("bound addr").to_string()))
        .collect();
    for node in &nodes {
        let conf_dir = node_conf_dir(tmp.path(), node.id);
        let group = build_wal_group(node.id, &node.wal_dir, &conf_dir, &endpoints, cfg).await;
        node.store.add_group(group);
    }

    WalCluster {
        nodes,
        kv_transport: Arc::new(KvRpcTransport::new()),
        _tmp: tmp,
        _net: net,
    }
}

impl WalCluster {
    fn elected_leader(&self) -> Option<&WalNode> {
        self.nodes.iter().find(|n| {
            n.store
                .get_group(GROUP)
                .expect("group")
                .local_replica()
                .is_leader()
        })
    }

    fn kv_client(&self, node: &WalNode) -> TestKvClient {
        TestKvClient::with_transport(
            Arc::clone(&self.kv_transport),
            format!("http://{}", node.store.listen_addr().expect("bound addr")),
        )
    }

    async fn wait_for_leader(&self, timeout: Duration) -> Option<u64> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Some(node) = self.elected_leader() {
                return Some(node.id);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        None
    }

    /// Read a key from EVERY node's local engine (best-effort, served locally).
    async fn read_local_everywhere(&self, key: &[u8]) -> Vec<(u64, Option<Vec<u8>>)> {
        let mut out = Vec::new();
        for node in &self.nodes {
            let client = self.kv_client(node);
            let resp = client
                .get(KvGetRequest {
                    version: 1,
                    key: Bytes::copy_from_slice(key),
                    request_id: 1,
                    request_create_ms: 1,
                    group_id: GROUP,
                    read_mode: 1, // MinSlot: serve from local engine
                    min_slot: 0,
                })
                .await
                .expect("get")
                .into_inner();
            let val = if resp.ok && !resp.not_found {
                Some(resp.value.to_vec())
            } else {
                None
            };
            out.push((node.id, val));
        }
        out
    }

    async fn restart_all(&mut self) {
        let cfg = PxElectionConfig::for_tests();
        // Snapshot ids + wal dirs, shut every node down, then rebuild all.
        let mut ids_dirs: Vec<(u64, PathBuf)> =
            self.nodes.iter().map(|n| (n.id, n.wal_dir.clone())).collect();
        ids_dirs.sort_by_key(|(id, _)| *id);

        for node in self.nodes.drain(..) {
            node.store.shutdown(Duration::from_secs(2)).await;
        }
        // Pass 1: bind each store with NO remotes wired yet — exactly the
        // web-console restore window where `add_group` runs before
        // `add_remote_replicas`. With zero remotes the group is quorum=1 and
        // self-promotes to leader, then runs bulk Phase 1 / repair ALONE. This
        // is the dangerous window: a lone leader missing a committed slot can
        // mis-decide it before peers wire in.
        let mut nodes = Vec::new();
        for (id, wal_dir) in &ids_dirs {
            let conf_dir = wal_dir.parent().expect("wal_dir parent").join("conf");
            let group = build_wal_group(*id, wal_dir, &conf_dir, &[], cfg).await;
            let store = Arc::new(PxKvStore::new(*id, "127.0.0.1:0".parse().unwrap()));
            store.add_group(group);
            store.start().await.expect("restart store");
            nodes.push(WalNode {
                id: *id,
                store,
                wal_dir: wal_dir.clone(),
            });
        }
        // Wait until every lone (quorum=1) replica has self-promoted to
        // leader before wiring remotes — reproducing the restore-window
        // race. Poll instead of a fixed sleep so we proceed as soon as
        // the election driver has promoted all nodes.
        let promote_start = Instant::now();
        loop {
            let all_leaders = nodes.iter().all(|n| {
                n.store
                    .get_group(GROUP)
                    .expect("group")
                    .local_replica()
                    .is_leader()
            });
            if all_leaders {
                break;
            }
            assert!(
                promote_start.elapsed() < Duration::from_secs(3),
                "lone replicas did not all self-promote within 5s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Pass 2: rewire peers to the actual bound endpoints.
        let endpoints: Vec<(u64, String)> = nodes
            .iter()
            .map(|n| (n.id, n.store.listen_addr().expect("bound addr").to_string()))
            .collect();
        for node in &nodes {
            let conf_dir = node.wal_dir.parent().expect("wal_dir parent").join("conf");
            let group = build_wal_group(node.id, &node.wal_dir, &conf_dir, &endpoints, cfg).await;
            node.store.add_group(group);
        }
        self.nodes = nodes;
    }

    async fn shutdown(self) {
        for node in self.nodes {
            node.store.shutdown(Duration::from_secs(2)).await;
        }
    }
}

async fn put(cluster: &WalCluster, key: &str, value: &str, seq: u64) {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(3) {
        if let Some(leader) = cluster.elected_leader() {
            let client = cluster.kv_client(leader);
            let resp = client
                .put(KvSetRequest {
                    version: 1,
                    key: Bytes::copy_from_slice(key.as_bytes()),
                    value: Bytes::copy_from_slice(value.as_bytes()),
                    seq,
                    client_id: 0,
                    ttl_ms: 0,
                    request_id: seq,
                    request_create_ms: 1,
                    group_id: GROUP,
                })
                .await
                .expect("put rpc")
                .into_inner();
            if resp.ok {
                return;
            }
            assert!(resp.error == "not leader", "put {key} should commit: {resp:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("put {key} should commit before timeout");
}

async fn delete(cluster: &WalCluster, key: &str, seq: u64) {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(3) {
        if let Some(leader) = cluster.elected_leader() {
            let client = cluster.kv_client(leader);
            let resp = client
                .delete(KvDeleteRequest {
                    version: 1,
                    key: Bytes::copy_from_slice(key.as_bytes()),
                    seq,
                    client_id: 0,
                    request_id: seq,
                    request_create_ms: 1,
                    group_id: GROUP,
                })
                .await
                .expect("delete rpc")
                .into_inner();
            if resp.ok {
                return;
            }
            assert!(resp.error == "not leader", "delete {key} should commit: {resp:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("delete {key} should commit before timeout");
}

async fn wait_until_deleted_everywhere(cluster: &WalCluster, key: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let reads = cluster.read_local_everywhere(key.as_bytes()).await;
        if reads.iter().all(|(_, v)| v.is_none()) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// Core scenario: seed `n` puts, delete `deleted`, require every replica to
/// reflect the deletes, restart ALL nodes (through the quorum=1 restore
/// window), then require no resurrection and surviving keys intact.
async fn run_restart_delete_scenario(ids: &[u64], n: u64, deleted: &[u64]) {
    let mut cluster = start_wal_cluster(ids).await;
    cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("initial leader");

    for i in 1..=n {
        put(&cluster, &format!("k{i}"), &format!("v{i}"), i).await;
    }
    let mut seq = n;
    for &i in deleted {
        seq += 1;
        delete(&cluster, &format!("k{i}"), seq).await;
    }

    // Every replica must reflect the deletes before we restart.
    for &i in deleted {
        assert!(
            wait_until_deleted_everywhere(&cluster, &format!("k{i}"), Duration::from_secs(3)).await,
            "k{i} should be deleted on every replica before restart"
        );
    }

    cluster.restart_all().await;
    cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("leader after full restart");

    // After full restart the deletes must NOT resurrect.
    for &i in deleted {
        let converged =
            wait_until_deleted_everywhere(&cluster, &format!("k{i}"), Duration::from_secs(3)).await;
        let reads = cluster.read_local_everywhere(format!("k{i}").as_bytes()).await;
        assert!(
            converged,
            "k{i} resurrected after full cluster restart: {reads:?}"
        );
    }

    // Surviving keys remain present.
    for i in 1..=n {
        if deleted.contains(&i) {
            continue;
        }
        let reads = cluster.read_local_everywhere(format!("k{i}").as_bytes()).await;
        assert!(
            reads
                .iter()
                .any(|(_, v)| v.as_deref() == Some(format!("v{i}").as_bytes())),
            "k{i} should survive restart on at least one replica: {reads:?}"
        );
    }

    cluster.shutdown().await;
}

#[tokio::test]
async fn full_cluster_restart_keeps_deletes() {
    run_restart_delete_scenario(&[1, 2, 3, 4, 5], 30, &[5, 10, 15, 20, 25]).await;
}

/// Mirror the web e2e's store-12 group (3 replicas) at higher scale: 60 keys
/// with deletes spread across the range (low, mid, and the highest slots).
#[tokio::test]
async fn full_cluster_restart_keeps_deletes_3node_scaled() {
    let deleted: Vec<u64> = (1..=60).filter(|i| i % 5 == 0).collect();
    run_restart_delete_scenario(&[1, 2, 3], 60, &deleted).await;
}
