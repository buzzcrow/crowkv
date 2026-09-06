// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A5 / G2 (freeze gate): multi-node kill/restart/re-elect with
//! no data loss.
//!
//! Spins up a 3-node cluster whose replicas each own a per-node `tempfile` WAL
//! dir over the real `File` backend (the same wiring `create_group_with_wal`
//! performs at server startup). Commits a batch of writes through the elected
//! leader — each accepted entry is durably logged on every replica via the
//! ack contract — then kills the leader. Two things must hold:
//!
//! 1. **Live cluster:** the surviving quorum re-elects a leader and every
//!    committed value is still readable through it.
//! 2. **Restart durability:** restarting the killed leader from the same WAL dir
//!    rejoins the cluster with every committed value recovered; an offline
//!    replay check also verifies the restored election state.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::group_config::GroupConfigStore;
use crowdb_kv::cluster::group_election::LeaderElection;
use crowdb_kv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::common::config::{PxElectionConfig, WalConfig};
use crowdb_kv::rpc::{KvGetRequest, KvSetRequest};
use crowdb_kv::wal::replay::replay_group;
use crowdb_kv::wal::{IoBackend, WalEngine, WalRecordFormat};
use crowdb_kv_client::KvRpcTransport;

use crate::common::test_client::TestKvClient;

const GROUP: u64 = 1;

struct WalNode {
    id: u64,
    store: Arc<PxKvStore>,
    wal_dir: PathBuf,
}

/// A 3-node cluster where every replica logs to its own `File`-backed WAL dir.
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
    let mut config = WalConfig::with_root(wal_dir.to_path_buf());
    config.wal_record_format = WalRecordFormat::Binary;
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
    let tmp = crowdb_test_harness::test_dirs::tempdir_in_test_data("g2-crash-restart");
    let cfg = PxElectionConfig::for_tests();

    // Pass 1: bind each store on an ephemeral port (peers are placeholders).
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

    // Pass 2: rewire peers to the actual bound endpoints.
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

    async fn read_via_leader(&self, key: &[u8]) -> Option<Vec<u8>> {
        let leader = self.elected_leader()?;
        let client = self.kv_client(leader);
        let resp = client
            .get(KvGetRequest {
                version: 1,
                key: Bytes::copy_from_slice(key),
                request_id: 9001,
                request_create_ms: 9001,
                group_id: GROUP,
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

    /// Kill the node with `id`: stop the server, drop it from the live set, and
    /// return its WAL dir so the crash can be replayed.
    async fn kill(&mut self, id: u64) -> PathBuf {
        let idx = self.nodes.iter().position(|n| n.id == id).expect("node present");
        let node = self.nodes.remove(idx);
        // Full cascade shutdown: stops the crowdb-rpc server *and* cancels the
        // election driver / heartbeat loop. A bare `stop` would leave the
        // driver heartbeating forever, starving the survivors' election
        // deadline so they could never re-elect.
        node.store.shutdown(Duration::from_secs(2)).await;
        let wal_dir = node.wal_dir.clone();
        drop(node);
        wal_dir
    }

    async fn restart(&mut self, id: u64, wal_dir: PathBuf) {
        let cfg = PxElectionConfig::for_tests();
        let conf_dir = wal_dir.parent().expect("wal_dir parent").join("conf");
        let peers: Vec<(u64, String)> = self
            .nodes
            .iter()
            .map(|n| (n.id, n.store.listen_addr().expect("bound addr").to_string()))
            .collect();
        let group = build_wal_group(id, &wal_dir, &conf_dir, &peers, cfg).await;
        let store = Arc::new(PxKvStore::new(id, "127.0.0.1:0".parse().unwrap()));
        store.add_group(group);
        store.start().await.expect("restart store");
        self.nodes.push(WalNode { id, store, wal_dir });

        let endpoints: Vec<(u64, String)> = self
            .nodes
            .iter()
            .map(|n| (n.id, n.store.listen_addr().expect("bound addr").to_string()))
            .collect();
        for node in &self.nodes {
            let conf_dir = node.wal_dir.parent().expect("wal_dir parent").join("conf");
            let group = build_wal_group(node.id, &node.wal_dir, &conf_dir, &endpoints, cfg).await;
            node.store.add_group(group);
        }
    }

    async fn shutdown(self) {
        for node in self.nodes {
            node.store.shutdown(Duration::from_secs(2)).await;
        }
    }
}

fn sample_kvs() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..8u64)
        .map(|i| {
            (
                format!("g2-key-{i}").into_bytes(),
                format!("g2-value-{i}").into_bytes(),
            )
        })
        .collect()
}

async fn commit_writes(cluster: &WalCluster, kvs: &[(Vec<u8>, Vec<u8>)]) {
    for (i, (key, value)) in kvs.iter().enumerate() {
        let seq = u64::try_from(i + 1).unwrap();
        let start = Instant::now();
        loop {
            assert!(
                start.elapsed() < Duration::from_secs(3),
                "write {i} should commit on the leader before timeout"
            );
            if let Some(leader) = cluster.elected_leader() {
                let client = cluster.kv_client(leader);
                let resp = client
                    .put(KvSetRequest {
                        version: 1,
                        key: Bytes::from(key.clone()),
                        value: Bytes::from(value.clone()),
                        seq,
                        ttl_ms: 0,
                        client_id: 77,
                        request_id: seq,
                        request_create_ms: 1,
                        group_id: GROUP,
                    })
                    .await
                    .expect("put rpc")
                    .into_inner();
                if resp.ok {
                    break;
                }
                assert!(
                    resp.error == "not leader",
                    "write {i} should commit on the leader: {resp:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn read_until_leader_has(cluster: &WalCluster, key: &[u8], timeout: Duration) -> Option<Vec<u8>> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(v) = cluster.read_via_leader(key).await {
            return Some(v);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    None
}

async fn assert_cluster_reads(cluster: &WalCluster, kvs: &[(Vec<u8>, Vec<u8>)], message: &str) {
    for (key, value) in kvs {
        assert_eq!(
            read_until_leader_has(cluster, key, Duration::from_secs(3))
                .await
                .as_deref(),
            Some(value.as_slice()),
            "{message}"
        );
    }
}

async fn assert_restarted_node_has_values(cluster: &WalCluster, node_id: u64, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let restarted = cluster
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .expect("restarted node present");
    let restarted_group = restarted.store.get_group(GROUP).expect("restarted group");
    for (key, value) in kvs {
        let got = restarted_group.local_replica().learner.engine_get(key).await;
        assert_eq!(
            got.map(|(_, v)| v).as_deref(),
            Some(value.as_slice()),
            "restarted live node must recover committed value for {:?} from WAL",
            String::from_utf8_lossy(key)
        );
    }
}

async fn assert_offline_replay_has_values(node_id: u64, wal_dir: PathBuf, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let backend = Arc::new(IoBackend::File);
    let disks = vec![wal_dir];
    let replay = replay_group(&backend, &disks, GROUP)
        .await
        .expect("replay killed leader wal");
    let restored = PxLocalReplica::restore_from_replay(node_id, PxLocalReplicaRole::Follower, &replay)
        .await
        .expect("restore killed leader");

    assert!(
        restored.current_term() >= 1,
        "restored replica recovered an election term"
    );
    let min_expected = u64::try_from(kvs.len()).expect("kvs length exceeds u64");
    let tip = restored.accepted_log_tip().0;
    assert!(
        tip >= min_expected,
        "offline replay should recover every accepted slot (tip={tip}, expected>={min_expected})"
    );
    // WAL replay now fully restores the learner: every accepted entry is
    // replayed into the state machine, so last_chosen_slot and
    // contiguous_chosen reflect the accepted slots, and the engine has the
    // values pre-applied.
    assert!(
        restored.last_chosen_slot() >= min_expected,
        "replay replays all accepted entries into the learner (last_chosen={}, expected>={min_expected})",
        restored.last_chosen_slot()
    );
    assert!(
        restored.contiguous_chosen() >= min_expected,
        "replay advances chosen frontier for contiguous accepted slots (contiguous={}, expected>={min_expected})",
        restored.contiguous_chosen()
    );
    for (slot, (key, value)) in (1u64..).zip(kvs.iter()) {
        let accepted = restored.accepted_at(slot).await;
        let got = restored.learner.engine_get(key).await;
        assert_eq!(
            got.map(|(_, v)| v),
            Some(value.clone()),
            "engine should have value after replay for {:?}",
            String::from_utf8_lossy(key)
        );
        assert!(
            accepted.is_some(),
            "accepted slot {slot} should survive offline replay"
        );
    }
}

#[tokio::test]
async fn cluster_survives_leader_kill_and_restart_with_no_data_loss() {
    let mut cluster = start_wal_cluster(&[1, 2, 3]).await;
    let leader_id = cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("initial leader elected");
    let kvs = sample_kvs();

    commit_writes(&cluster, &kvs).await;
    assert_cluster_reads(&cluster, &kvs, "key should be readable before the crash").await;

    let dead_wal_dir = cluster.kill(leader_id).await;
    assert_eq!(cluster.nodes.len(), 2, "two replicas survive the crash");

    let new_leader = cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("surviving quorum re-elects a leader");
    assert_ne!(new_leader, leader_id, "a survivor took over leadership");
    assert_cluster_reads(&cluster, &kvs, "committed value must survive the leader kill").await;

    let dead_wal_dir_for_offline_replay = dead_wal_dir.clone();
    cluster.restart(leader_id, dead_wal_dir).await;
    assert_eq!(cluster.nodes.len(), 3, "killed replica restarted");
    cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("cluster has a leader after restart");
    assert_cluster_reads(
        &cluster,
        &kvs,
        "committed value must remain readable after restart",
    )
    .await;
    assert_restarted_node_has_values(&cluster, leader_id, &kvs).await;
    assert_offline_replay_has_values(leader_id, dead_wal_dir_for_offline_replay, &kvs).await;

    cluster.shutdown().await;
}
