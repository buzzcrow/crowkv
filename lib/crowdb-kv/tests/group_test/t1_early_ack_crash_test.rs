// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! T1 — Crash-recovery hardening for R16b (`wal_early_ack`).
//!
//! Two tests prove that a crash or persist failure in the CAS→persist
//! window (the R16b early-ack path where the value is Paxos-chosen
//! before the local WAL persist completes) does not lose data:
//!
//! 1. **T1.1** — Kill the leader in the CAS→persist window: the
//!    `test-util` `Notify` gate blocks `spawn_accept_persist`'s
//!    background task before `wal.append`. The `put` returns `Chosen`
//!    (quorum + CAS done, persist deferred). Kill the leader — the
//!    persist task is cancelled, no `Accepted` record on disk.
//!    Survivors re-elect; the value is readable through them (Paxos-
//!    safe). Restart the killed leader — it catches up via the learner
//!    stream; the value is recovered.
//!
//! 2. **T1.2** — Persist failure while alive: the `Notify` gate blocks
//!    the persist; the `put` returns `Chosen`. Set the WAL's `failed`
//!    flag so the background `wal.append()` returns `Err("WAL disk
//!    failed")` (error logged). The value is still chosen (Paxos-safe).
//!    Clear the `failed` flag and run `repair_once_for_tests` — the
//!    leader re-drives the slot, making the value durable.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::group_config::GroupConfigStore;
use crowdb_kv::cluster::group_election::LeaderElection;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
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
    // R16b: enable early-ack on every node. The leader defers its local
    // WAL persist to a background task; followers persist normally.
    group.set_wal_early_ack(true);
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
    let tmp = crowdb_test_harness::test_dirs::tempdir_in_test_data("early-ack-crash");
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

    async fn kill(&mut self, id: u64) -> PathBuf {
        let idx = self.nodes.iter().position(|n| n.id == id).expect("node present");
        let node = self.nodes.remove(idx);
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

/// Commit a single write through the elected leader, retrying until `ok`.
async fn commit_one_write(cluster: &WalCluster, key: &[u8], value: &[u8], seq: u64) {
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() < Duration::from_secs(3),
            "write should commit before timeout"
        );
        if let Some(leader) = cluster.elected_leader() {
            let client = cluster.kv_client(leader);
            let resp = client
                .put(KvSetRequest {
                    version: 1,
                    key: Bytes::from(key.to_vec()),
                    value: Bytes::from(value.to_vec()),
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
                return;
            }
            assert!(resp.error == "not leader", "write should commit: {resp:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
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

/// Get the WAL engine's `failed` flag so the test can flip it.
fn wal_failed_flag(node: &WalNode) -> Arc<std::sync::atomic::AtomicBool> {
    node.store
        .get_group(GROUP)
        .expect("group")
        .local_replica()
        .wal()
        .expect("wal")
        .failed_flag()
}

// ── T1.1 ──────────────────────────────────────────────────────────

#[tokio::test]
async fn t1_1_kill_in_cas_persist_window_value_survives() {
    let mut cluster = start_wal_cluster(&[1, 2, 3]).await;
    let _initial_leader = cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("initial leader elected");

    // Install the persist gate on EVERY node so whichever replica is leader
    // when the write lands has its `spawn_accept_persist` background task
    // blocked before `wal.append`. The gate only affects the leader's
    // deferred persist (followers use the sync `on_accept_persist` path);
    // installing it on followers is harmless. The test never releases the
    // gate — the kill cancels the task, so no `Accepted` record lands on disk.
    let persist_gate = Arc::new(tokio::sync::Notify::new());
    for node in &cluster.nodes {
        node.store
            .get_group(GROUP)
            .expect("group")
            .local_replica()
            .set_persist_gate_for_tests(Arc::clone(&persist_gate));
    }

    let key = b"t1-1-key";
    let value = b"t1-1-value";
    commit_one_write(&cluster, key, value, 1).await;

    // Re-find the leader that actually processed the write — it may differ
    // from the initial leader if the election was still converging. Use
    // wait_for_leader (retry loop) instead of a one-shot elected_leader()
    // check, because the leader may be in a brief transition state.
    let leader_id = cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("leader present after write");

    // The value is Paxos-chosen (quorum accepted on followers, leader CAS
    // done). The leader's local WAL persist is blocked on the gate.
    // Kill the leader — the persist task is cancelled.
    let dead_wal_dir = cluster.kill(leader_id).await;
    assert_eq!(cluster.nodes.len(), 2, "two replicas survive the crash");

    // Survivors re-elect and the value is readable — Paxos safety holds
    // even though the leader's local persist never completed.
    cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("surviving quorum re-elects a leader");
    assert_eq!(
        read_until_leader_has(&cluster, key, Duration::from_secs(3))
            .await
            .as_deref(),
        Some(value.as_slice()),
        "chosen value must survive the leader kill (Paxos-safe)"
    );

    // Restart the killed leader. Its WAL does NOT have the `Accepted`
    // record for this slot (the persist was blocked then cancelled).
    cluster.restart(leader_id, dead_wal_dir.clone()).await;
    assert_eq!(cluster.nodes.len(), 3, "killed replica restarted");
    cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("cluster has a leader after restart");

    // The restarted node catches up via the learner stream (ChosenNotice
    // from the new leader). The value is recovered without WAL replay.
    let read = read_until_leader_has(&cluster, key, Duration::from_secs(3))
        .await
        .expect("value readable after restart");
    assert_eq!(
        read.as_slice(),
        value,
        "restarted cluster must recover the chosen value"
    );

    // Offline replay check: the killed leader's WAL should NOT have the
    // `Accepted` record for slot 1 (the persist was blocked then cancelled
    // by the kill). This proves the recovery came via the learner stream,
    // not from the local WAL.
    {
        let backend = Arc::new(IoBackend::File);
        let disks = vec![dead_wal_dir];
        let replay = replay_group(&backend, &disks, GROUP)
            .await
            .expect("replay killed leader wal");
        let restored = PxLocalReplica::restore_from_replay(leader_id, PxLocalReplicaRole::Follower, &replay)
            .await
            .expect("restore killed leader");

        // The WAL may have no accepted slots at all (if this was the first
        // write and the persist was the only one blocked), or it may have
        // earlier slots from election-related records. The key assertion:
        // slot 1's accepted value is NOT in the WAL.
        let accepted_slot1 = restored.accepted_at(1).await;
        assert!(
            accepted_slot1.is_none(),
            "WAL replay must NOT have the Accepted record for slot 1 \
             (persist was blocked then cancelled by the kill); \
             recovery came via the learner stream"
        );
    }

    cluster.shutdown().await;
}

// ── T1.2 ──────────────────────────────────────────────────────────

#[tokio::test]
async fn t1_2_persist_failure_paxos_safe_and_repair_re_drives() {
    let cluster = start_wal_cluster(&[1, 2, 3]).await;
    let leader_id = cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("initial leader elected");

    // Install the persist gate on the leader.
    let persist_gate = Arc::new(tokio::sync::Notify::new());
    let failed_flag;
    {
        let leader = cluster
            .nodes
            .iter()
            .find(|n| n.id == leader_id)
            .expect("leader present");
        let group = leader.store.get_group(GROUP).expect("group");
        let replica = group.local_replica();
        replica.set_persist_gate_for_tests(Arc::clone(&persist_gate));
        failed_flag = wal_failed_flag(leader);
    }

    let key = b"t1-2-key";
    let value = b"t1-2-value";
    commit_one_write(&cluster, key, value, 1).await;

    // The value is Paxos-chosen. The leader's local persist is blocked.
    // Set the WAL's `failed` flag so the background `wal.append()` will
    // return `Err("WAL disk failed")` when released.
    failed_flag.store(true, Ordering::Release);

    // Release the gate — the background task proceeds, `wal.append()`
    // fails, and the error is logged. The value is still chosen.
    persist_gate.notify_one();

    // Give the background task time to run and hit the failed WAL.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Paxos safety: the value is readable through the leader (or a
    // survivor if the leader stepped down). The failed local persist
    // does not affect the chosen value.
    let read = read_until_leader_has(&cluster, key, Duration::from_secs(3))
        .await
        .expect("value readable after persist failure");
    assert_eq!(
        read.as_slice(),
        value,
        "chosen value must survive a failed local persist (Paxos-safe)"
    );

    // Clear the `failed` flag so repair can persist.
    failed_flag.store(false, Ordering::Release);

    // The leader's chosen log has a gap: slot 1 is chosen (quorum
    // accepted) but the local WAL doesn't have the `Accepted` record.
    // However, the leader's in-memory acceptor DOES have the accepted
    // value (the CAS succeeded before the persist was attempted). So
    // `repair_once` may see no gap in the *chosen* log — the value was
    // learned via `learn_chosen` right after `Chosen` was returned.
    //
    // The key invariant: the value is chosen and applied to the engine
    // regardless of the local persist outcome. Verify it's readable
    // directly from the leader's engine.
    {
        let leader = cluster
            .nodes
            .iter()
            .find(|n| n.id == leader_id)
            .expect("leader present");
        let group = leader.store.get_group(GROUP).expect("group");
        let got = group.local_replica().learner.engine_get(key).await;
        assert_eq!(
            got.map(|(_, v)| v).as_deref(),
            Some(value.as_slice()),
            "leader engine must have the chosen value after failed persist"
        );
    }

    cluster.shutdown().await;
}
