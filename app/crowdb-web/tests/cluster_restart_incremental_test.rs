// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Incremental restart-recovery tests, each with explicit verification steps:
//! 1. Seed KV data and wait for convergence
//! 2. Verify WAL contents (Accepted records, slot count)
//! 3. Stop all nodes
//! 4. Restart all nodes
//! 5. Verify WAL replay
//! 6. Verify KV state is fully restored
//!
//! Tests are ordered by complexity:
//! - `restart_3node_1group` — smallest quorum, fewest leader changes
//! - `restart_5node_1group` — more replicas, more leader changes
//! - `restart_5node_2group` — multi-store, multi-group

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_console_shared::lifecycle::{crowdb_kv_server_bin, stop_pid_with_timeout};
use crowdb_console_shared::ConsoleConfig;
use crowdb_kv::wal::io_backend::IoBackend;
use crowdb_kv::wal::record::{RecordType, WALRecord};
use crowdb_kv::wal::segment::SegmentReader;
use crowdb_web::{router, AppState};
use serde_json::{json, Value};

// ── Shared helpers ─────────────────────────────────────────────────────────

struct ProcessGuard {
    pids: BTreeMap<String, u32>,
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        for pid in self.pids.values() {
            let _ = stop_pid_with_timeout(*pid, Duration::from_millis(100));
        }
    }
}

/// Convert epoch millis to UTC (year, month, day, hour, min, sec, ms).
/// Pure-Rust civil time calculation — no unsafe, no external deps.
fn epoch_to_local(millis: u128) -> (u32, u32, u32, u32, u32, u32, u32) {
    let secs = u64::try_from(millis / 1000).unwrap_or(u64::MAX);
    let ms = u32::try_from(millis % 1000).unwrap_or(u32::MAX);
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let hour = u32::try_from(rem / 3600).unwrap_or(u32::MAX);
    let min = u32::try_from((rem % 3600) / 60).unwrap_or(u32::MAX);
    let sec = u32::try_from(rem % 60).unwrap_or(u32::MAX);

    // Civil from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = i64::try_from(days).unwrap_or(i64::MAX) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = u32::try_from(y + i64::from(m <= 2)).unwrap_or(u32::MAX);
    (
        year,
        u32::try_from(m).unwrap_or(u32::MAX),
        u32::try_from(d).unwrap_or(u32::MAX),
        hour,
        min,
        sec,
        ms,
    )
}

fn tempdir(tag: &str) -> PathBuf {
    // Use a fixed `test-logs/` directory inside the project root so logs,
    // WAL files, and config persist for inspection after test runs.
    // A timestamp postfix prevents conflicts when the same test is run twice
    // in parallel (e.g. from different terminals).
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("test-logs");
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let (year, month, day, hour, min, sec, ms) = epoch_to_local(millis);
    let dir = base.join(format!(
        "{tag}-{year:04}{month:02}{day:02}-{hour:02}{min:02}{sec:02}.{ms:03}"
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn spawn_web_with_path(path: PathBuf) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let cfg = ConsoleConfig::load(&path).unwrap_or_default();
    let state = AppState::with_config(cfg, Some(path.clone()));
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn json_get(client: &reqwest::Client, url: &str) -> (reqwest::StatusCode, Value) {
    let r = client.get(url).send().await.unwrap();
    let status = r.status();
    let v = r.json::<Value>().await.unwrap_or(Value::Null);
    (status, v)
}

async fn json_post(client: &reqwest::Client, url: &str, body: Value) -> (reqwest::StatusCode, Value) {
    let r = client.post(url).json(&body).send().await.unwrap();
    let status = r.status();
    let v = r.json::<Value>().await.unwrap_or(Value::Null);
    (status, v)
}

async fn create_rack(client: &reqwest::Client, base: &str, rack_id: u64) {
    let (status, body) = json_post(client, &format!("{base}/api/racks"), json!({ "id": rack_id })).await;
    assert_eq!(status.as_u16(), 201, "create rack {rack_id}: {body}");
}

async fn create_node(client: &reqwest::Client, base: &str, node_id: u64, rack_id: u64) {
    let (status, body) = json_post(
        client,
        &format!("{base}/api/nodes"),
        json!({ "id": node_id, "rack_id": rack_id, "host": "127.0.0.1", "ssh_user": "" }),
    )
    .await;
    assert_eq!(status.as_u16(), 201, "create node {node_id}: {body}");
}

async fn deploy_server(
    client: &reqwest::Client,
    base: &str,
    node_id: u64,
    binary: &Path,
    election_profile: &str,
    rest_port: u16,
    rpc_port: u16,
) -> u32 {
    eprintln!("deploy node {node_id}: rest_port={rest_port} rpc_port={rpc_port}");
    let (status, body) = json_post(
        client,
        &format!("{base}/api/nodes/{node_id}/server/deploy"),
        json!({
            "rest_port": rest_port,
            "rpc_port": rpc_port,
            "binary": binary.to_string_lossy().to_string(),
            "election_profile": election_profile,
        }),
    )
    .await;
    assert!(status.is_success(), "deploy {node_id}: {status} {body}");
    u32::try_from(body["pid"].as_u64().expect("pid")).unwrap()
}

async fn stop_server(client: &reqwest::Client, base: &str, node_id: u64) {
    let (status, body) = json_post(
        client,
        &format!("{base}/api/nodes/{node_id}/server/stop"),
        json!({}),
    )
    .await;
    assert!(status.is_success(), "stop {node_id}: {status} {body}");
}

async fn restart_server(client: &reqwest::Client, base: &str, node_id: u64) -> u32 {
    let (status, body) = json_post(
        client,
        &format!("{base}/api/nodes/{node_id}/server/restart"),
        json!({}),
    )
    .await;
    assert!(status.is_success(), "restart {node_id}: {status} {body}");
    u32::try_from(body["pid"].as_u64().expect("pid")).unwrap()
}

async fn create_store(client: &reqwest::Client, base: &str, store_id: u64, nodes: &[u64]) {
    // Retry with backoff — a node may be momentarily unreachable
    // right after concurrent deploy (the REST server may not have
    // started listening yet). Mirrors the retry in create_group.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (status, body) = json_post(
            client,
            &format!("{base}/api/stores"),
            json!({ "store_id": store_id, "nodes": nodes }),
        )
        .await;
        if status.as_u16() == 201 {
            return;
        }
        if Instant::now() >= deadline {
            assert_eq!(status.as_u16(), 201, "create store {store_id}: {body}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn create_group(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    group_id: u64,
    replica_id: u64,
    nodes: &[u64],
) {
    // Retry with backoff — the kv-server on the target node may be
    // momentarily unreachable right after concurrent deploy.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (status, body) = json_post(
            client,
            &format!("{base}/api/stores/{store_id}/groups"),
            json!({ "group_id": group_id, "replica_id": replica_id, "nodes": nodes }),
        )
        .await;
        if status.as_u16() == 201 {
            return;
        }
        if Instant::now() >= deadline {
            assert_eq!(status.as_u16(), 201, "create group {store_id}/{group_id}: {body}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn kv_put(client: &reqwest::Client, base: &str, store_id: u64, group_id: u64, key: &str, value: &str) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let (status, body) = json_post(
            client,
            &format!("{base}/api/stores/{store_id}/groups/{group_id}/kv/put"),
            json!({ "key": key, "value": value }),
        )
        .await;
        if status.as_u16() == 200 && body["ok"] == true {
            return;
        }
        if Instant::now() >= deadline {
            assert_eq!(status.as_u16(), 200, "kv put {store_id}/{group_id} {key}: {body}");
            assert_eq!(body["ok"], true);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn kv_delete(client: &reqwest::Client, base: &str, store_id: u64, group_id: u64, key: &str) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let (status, body) = json_post(
            client,
            &format!("{base}/api/stores/{store_id}/groups/{group_id}/kv/delete"),
            json!({ "key": key }),
        )
        .await;
        if status.as_u16() == 200 && body["ok"] == true {
            return;
        }
        if Instant::now() >= deadline {
            assert_eq!(
                status.as_u16(),
                200,
                "kv delete {store_id}/{group_id} {key}: {body}"
            );
            assert_eq!(body["ok"], true);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn kv_get(client: &reqwest::Client, base: &str, store_id: u64, group_id: u64, key: &str) -> Value {
    let (status, body) = json_get(
        client,
        &format!("{base}/api/stores/{store_id}/groups/{group_id}/kv/get?key={key}"),
    )
    .await;
    assert_eq!(status.as_u16(), 200, "kv get {store_id}/{group_id} {key}: {body}");
    body
}

async fn kv_scan(client: &reqwest::Client, base: &str, store_id: u64, group_id: u64) -> (u16, Value) {
    let (status, body) = json_get(
        client,
        &format!("{base}/api/stores/{store_id}/groups/{group_id}/kv/scan?limit=10000"),
    )
    .await;
    (status.as_u16(), body)
}

async fn wait_for_group_leader(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    group_id: u64,
    expected_replicas: usize,
    timeout: Duration,
) -> Value {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let (status, body) =
            json_get(client, &format!("{base}/api/stores/{store_id}/groups/{group_id}")).await;
        if status.is_success() {
            let replicas = body["replicas"].as_array().cloned().unwrap_or_default();
            let leaders: Vec<u64> = replicas
                .iter()
                .filter(|r| r["role"].as_str() == Some("leader"))
                .filter_map(|r| r["replica_id"].as_u64())
                .collect();
            let count_ok = expected_replicas == 0 || replicas.len() == expected_replicas;
            if count_ok && leaders.len() == 1 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let (status_b, body_b) =
                    json_get(client, &format!("{base}/api/stores/{store_id}/groups/{group_id}")).await;
                if status_b.is_success() {
                    let replicas_b = body_b["replicas"].as_array().cloned().unwrap_or_default();
                    let leaders_b: Vec<u64> = replicas_b
                        .iter()
                        .filter(|r| r["role"].as_str() == Some("leader"))
                        .filter_map(|r| r["replica_id"].as_u64())
                        .collect();
                    let count_ok_b = expected_replicas == 0 || replicas_b.len() == expected_replicas;
                    if count_ok_b && leaders_b == leaders {
                        return body_b;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("group {store_id}/{group_id} failed to converge to one leader within {timeout:?}");
}

async fn wait_for_store(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    expected_groups: usize,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let (status, body) = json_get(client, &format!("{base}/api/stores/{store_id}")).await;
        if status.is_success() {
            let groups = body["groups"].as_array().cloned().unwrap_or_default();
            if groups.len() == expected_groups {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("store {store_id} failed to report {expected_groups} groups within {timeout:?}");
}

// ── WAL inspection helpers ─────────────────────────────────────────────────

/// Summary of WAL contents for a single node/group.
#[derive(Debug)]
struct WalSummary {
    accepted_count: usize,
    unique_slots: BTreeSet<u64>,
    max_slot: u64,
}

impl WalSummary {
    fn contiguous_chosen(&self) -> u64 {
        let mut expected = 1u64;
        for slot in &self.unique_slots {
            if *slot == expected {
                expected += 1;
            } else {
                break;
            }
        }
        expected - 1
    }
}

/// Read all WAL records from a node's WAL directory for a given store/group.
async fn read_wal_records(wal_dir: &Path, store_id: u64, group_id: u64) -> Vec<WALRecord> {
    let group_dir = wal_dir
        .join(format!("store{store_id}"))
        .join(format!("group{group_id}"));
    if !group_dir.exists() {
        return Vec::new();
    }

    let backend = Arc::new(IoBackend::File);
    let mut segments: Vec<(u64, PathBuf)> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&group_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(seg_id) = name
                .strip_prefix("seg-")
                .and_then(|s| s.strip_suffix(".ck"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                segments.push((seg_id, entry.path()));
            }
        }
    }
    segments.sort_by_key(|(id, _)| *id);

    let mut records = Vec::new();
    for (_seg_id, path) in &segments {
        let mut seg = SegmentReader::open(&backend, path).await.unwrap();
        while let Ok(Some((record, _))) = seg.next_record().await {
            records.push(record);
        }
    }
    records
}

/// Summarize WAL records into a `WalSummary`.
fn summarize_wal(records: &[WALRecord]) -> WalSummary {
    let mut unique_slots = BTreeSet::new();
    let mut max_slot = 0u64;
    let mut accepted_count = 0usize;

    for record in records {
        if record.record_type == RecordType::Accepted {
            accepted_count += 1;
            unique_slots.insert(record.slot);
            if record.slot > max_slot {
                max_slot = record.slot;
            }
        }
    }

    WalSummary {
        accepted_count,
        unique_slots,
        max_slot,
    }
}

/// Print a WAL summary for debugging.
fn print_wal_summary(label: &str, summary: &WalSummary) {
    eprintln!(
        "[WAL] {label}: accepted={} unique_slots={} max_slot={} \
         contiguous_chosen={}",
        summary.accepted_count,
        summary.unique_slots.len(),
        summary.max_slot,
        summary.contiguous_chosen(),
    );
}

// ── KV verification helpers ────────────────────────────────────────────────

async fn assert_kv_state(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    group_id: u64,
    expected: &BTreeMap<String, Option<String>>,
) {
    for (key, value) in expected {
        let body = kv_get(client, base, store_id, group_id, key).await;
        match value {
            Some(expected_value) => {
                assert_eq!(
                    body["found"], true,
                    "expected {store_id}/{group_id}/{key} to exist: {body}"
                );
                assert_eq!(body["value_utf8"], expected_value.as_str());
            }
            None => {
                assert_eq!(
                    body["found"], false,
                    "expected {store_id}/{group_id}/{key} to be deleted: {body}"
                );
            }
        }
    }
}

async fn wait_for_kv_state(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    group_id: u64,
    expected: &BTreeMap<String, Option<String>>,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let (status, body) = kv_scan(client, base, store_id, group_id).await;
        if status != 200 {
            assert!(
                Instant::now() < deadline,
                "kv scan {store_id}/{group_id} failed (status {status}) within {timeout:?}: {body}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        let items = body["items"].as_array().cloned().unwrap_or_default();
        let mut actual: BTreeMap<String, String> = BTreeMap::new();
        for item in &items {
            if let (Some(k), Some(v)) = (item["key_utf8"].as_str(), item["value_utf8"].as_str()) {
                actual.insert(k.to_string(), v.to_string());
            }
        }
        let mut all_match = true;
        for (key, value) in expected {
            let matches = match value {
                Some(v) => actual.get(key).is_some_and(|av| av == v),
                None => !actual.contains_key(key),
            };
            if !matches {
                all_match = false;
                break;
            }
        }
        if all_match {
            return;
        }
        if Instant::now() >= deadline {
            assert_kv_state(client, base, store_id, group_id, expected).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ── Cluster setup helper ───────────────────────────────────────────────────

struct Cluster {
    base: String,
    client: reqwest::Client,
    dir: PathBuf,
    guard: ProcessGuard,
    node_ids: Vec<u64>,
}

async fn setup_cluster(tag: &str, rack_nodes: &[(u64, u64)], bin: &Path, election_profile: &str) -> Cluster {
    let dir = tempdir(tag);
    let cfg_path = dir.join("console.toml");
    let addr = spawn_web_with_path(cfg_path).await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let mut racks: BTreeSet<u64> = BTreeSet::new();
    for (_, rack_id) in rack_nodes {
        racks.insert(*rack_id);
    }
    for rack_id in &racks {
        create_rack(&client, &base, *rack_id).await;
    }

    let mut guard = ProcessGuard {
        pids: BTreeMap::new(),
    };
    let mut node_ids = Vec::new();
    for (node_id, rack_id) in rack_nodes {
        create_node(&client, &base, *node_id, *rack_id).await;
        node_ids.push(*node_id);
    }
    // Batch-allocate all deploy ports via the flock-coordinated port
    // allocator. Each node needs a mgmt port + a listen port, allocated
    // from the respective service ranges. The claim file + flock
    // guarantees no TOCTOU between concurrent deploys.
    let n = u16::try_from(node_ids.len()).unwrap_or(u16::MAX);
    let rest_ports =
        crowdb_protocol::port_alloc::alloc_test_port_range(crowdb_protocol::ServicePort::KvServerMgmt, n);
    let rpc_ports =
        crowdb_protocol::port_alloc::alloc_test_port_range(crowdb_protocol::ServicePort::KvServerListen, n);
    // Deploy all nodes concurrently — each deploy polls /health until
    // ready, so parallel deploy overlaps the readiness waits.
    let deploy_futs: Vec<_> = node_ids
        .iter()
        .enumerate()
        .map(|(i, &nid)| {
            deploy_server(
                &client,
                &base,
                nid,
                bin,
                election_profile,
                rest_ports[i],
                rpc_ports[i],
            )
        })
        .collect();
    let pids = futures::future::join_all(deploy_futs).await;
    for (nid, pid) in node_ids.iter().zip(pids) {
        guard.pids.insert(nid.to_string(), pid);
    }

    Cluster {
        base,
        client,
        dir,
        guard,
        node_ids,
    }
}

impl Cluster {
    fn node_wal_dir(&self, node_id: u64) -> PathBuf {
        self.dir.join(format!("N-{node_id}")).join("waldata")
    }

    async fn stop_all(&self) {
        let futs: Vec<_> = self
            .node_ids
            .iter()
            .map(|&nid| stop_server(&self.client, &self.base, nid))
            .collect();
        futures::future::join_all(futs).await;
    }

    async fn restart_all(&mut self) {
        let ids = self.node_ids.clone();
        let futs: Vec<_> = ids
            .iter()
            .map(|&nid| restart_server(&self.client, &self.base, nid))
            .collect();
        let pids = futures::future::join_all(futs).await;
        for (nid, pid) in ids.iter().zip(pids) {
            self.guard.pids.insert(nid.to_string(), pid);
        }
    }

    async fn verify_wal_for_nodes(
        &self,
        label: &str,
        store_id: u64,
        group_id: u64,
        nodes: &[u64],
    ) -> Vec<WalSummary> {
        let mut summaries = Vec::new();
        for node_id in nodes {
            let wal_dir = self.node_wal_dir(*node_id);
            let records = read_wal_records(&wal_dir, store_id, group_id).await;
            let summary = summarize_wal(&records);
            print_wal_summary(
                &format!("{label} node={node_id} store={store_id} group={group_id}"),
                &summary,
            );
            summaries.push(summary);
        }
        summaries
    }

    /// Wait until all nodes in a group have converged to the same WAL
    /// `max_slot`. This prevents a lagging follower from catching up
    /// between the pre-restart WAL snapshot and `stop_all`, which would
    /// cause a false "WAL changed after stop" assertion.
    async fn wait_for_wal_convergence(&self, store_id: u64, group_id: u64, nodes: &[u64], timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            let summaries = self
                .verify_wal_for_nodes("convergence", store_id, group_id, nodes)
                .await;
            let max_slots: Vec<u64> = summaries.iter().map(|s| s.max_slot).collect();
            let min = *max_slots.iter().min().unwrap_or(&0);
            let max = *max_slots.iter().max().unwrap_or(&0);
            if min == max && min > 0 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "WAL did not converge for store {store_id} group {group_id} \
                 within {timeout:?}: slots={max_slots:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

// ── Restart recovery helper ────────────────────────────────────────────────

/// Description of a single group within a restart test.
const REPLAY_PUTS: u64 = 50;

struct GroupSpec {
    store_id: u64,
    group_id: u64,
    replica_id: u64,
    nodes: Vec<u64>,
    n_puts: u64,
    deleted_keys: Vec<u64>,
}

impl GroupSpec {
    fn expected(&self) -> BTreeMap<String, Option<String>> {
        (1..=self.n_puts)
            .map(|i| {
                let key = format!("k{i}");
                if self.deleted_keys.contains(&i) {
                    (key, None)
                } else {
                    (key, Some(format!("v{i}")))
                }
            })
            .collect()
    }
}

/// Run a full restart-recovery scenario with explicit verification steps:
/// 1. Seed KV data and wait for convergence
/// 2. Verify WAL contents (Accepted records, slot count, watermark)
/// 3. Verify KV state before restart
/// 4. Stop all nodes
/// 5. Verify WAL after stop (unchanged)
/// 6. Restart all nodes
/// 7. Verify WAL after restart (unchanged)
/// 8. Verify KV state after restart
#[allow(clippy::too_many_lines)]
async fn restart_recovery(
    tag: &str,
    rack_nodes: &[(u64, u64)],
    groups: &[GroupSpec],
    election_profile: &str,
) {
    let bin = match crowdb_kv_server_bin() {
        Some(p) if p.exists() => p,
        _ => {
            eprintln!("skipping: crowdb-kv-server binary not built");
            return;
        }
    };
    std::env::set_var("CROWDB_KV_WAL_TEXT", "1");

    let mut cluster = setup_cluster(tag, rack_nodes, &bin, election_profile).await;
    eprintln!("test-logs: {}", cluster.dir.display());

    // Step 0: Initialize the system group so non-zero stores can be created.
    // Retry with backoff — a node's KV server may fail to bind its RPC
    // port on the first attempt if a previous test's process hasn't
    // fully released the port (TIME_WAIT) or the node server hasn't
    // started its listener yet.
    let init_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (status, body) = json_post(
            &cluster.client,
            &format!("{}/api/cluster/init", cluster.base),
            json!({ "nodes": cluster.node_ids }),
        )
        .await;
        if status.as_u16() == 201 {
            break;
        }
        if Instant::now() >= init_deadline {
            assert_eq!(status.as_u16(), 201, "cluster init: {body}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Step 1a: Create all stores + groups first (concurrent startup)
    // Collect the union of nodes for each store_id so the store is created
    // on every node that hosts any of its groups.
    let mut store_nodes: std::collections::BTreeMap<u64, Vec<u64>> = std::collections::BTreeMap::new();
    for g in groups {
        store_nodes
            .entry(g.store_id)
            .or_default()
            .extend(g.nodes.iter().copied());
    }
    for (sid, nodes) in &mut store_nodes {
        nodes.sort_unstable();
        nodes.dedup();
        create_store(&cluster.client, &cluster.base, *sid, nodes).await;
    }
    for g in groups {
        create_group(
            &cluster.client,
            &cluster.base,
            g.store_id,
            g.group_id,
            g.replica_id,
            &g.nodes,
        )
        .await;
    }

    // Step 1b: Wait for all groups to elect a leader
    for g in groups {
        wait_for_group_leader(
            &cluster.client,
            &cluster.base,
            g.store_id,
            g.group_id,
            g.nodes.len(),
            Duration::from_secs(3),
        )
        .await;
    }

    // Step 1c: Seed data into all groups (interleaved round-robin so
    // both groups make progress simultaneously without aggressive
    // concurrency that could trigger election churn).
    let max_puts = groups.iter().map(|g| g.n_puts).max().unwrap_or(0);
    for i in 1..=max_puts {
        for g in groups {
            if i <= g.n_puts {
                kv_put(
                    &cluster.client,
                    &cluster.base,
                    g.store_id,
                    g.group_id,
                    &format!("k{i}"),
                    &format!("v{i}"),
                )
                .await;
            }
        }
    }
    for g in groups {
        for &del in &g.deleted_keys {
            kv_delete(
                &cluster.client,
                &cluster.base,
                g.store_id,
                g.group_id,
                &format!("k{del}"),
            )
            .await;
        }
    }

    // Step 2: Verify WAL before restart
    eprintln!("\n=== Step 2: Verify WAL before restart ===");
    let mut pre_wal = Vec::new();
    for g in groups {
        eprintln!(
            "\n-- store {} group {} ({} replicas) --",
            g.store_id,
            g.group_id,
            g.nodes.len()
        );
        // Wait for all replicas to converge to the same max_slot before
        // snapshotting. Without this, a lagging follower can catch up
        // between the snapshot and stop_all, causing a false "WAL changed
        // after stop" failure.
        cluster
            .wait_for_wal_convergence(g.store_id, g.group_id, &g.nodes, Duration::from_secs(5))
            .await;
        let summaries = cluster
            .verify_wal_for_nodes("pre-restart", g.store_id, g.group_id, &g.nodes)
            .await;
        for summary in &summaries {
            assert!(
                summary.max_slot > 0,
                "WAL should have Accepted records before restart"
            );
        }
        pre_wal.push(summaries);
    }

    // Step 3: Verify KV state before restart
    eprintln!("\n=== Step 3: Verify KV state before restart ===");
    for g in groups {
        let expected = g.expected();
        wait_for_kv_state(
            &cluster.client,
            &cluster.base,
            g.store_id,
            g.group_id,
            &expected,
            Duration::from_secs(3),
        )
        .await;
    }

    // Step 4: Stop all nodes
    eprintln!("\n=== Step 4: Stop all nodes ===");
    cluster.stop_all().await;

    // Step 5: Verify WAL after stop (unchanged)
    eprintln!("\n=== Step 5: Verify WAL after stop ===");
    for (gi, g) in groups.iter().enumerate() {
        let post = cluster
            .verify_wal_for_nodes("post-stop", g.store_id, g.group_id, &g.nodes)
            .await;
        for (i, summary) in post.iter().enumerate() {
            assert_eq!(
                summary.max_slot, pre_wal[gi][i].max_slot,
                "WAL max_slot should not change after stop (store {} group {} node {})",
                g.store_id, g.group_id, g.nodes[i]
            );
        }
    }

    // Step 6: Restart all nodes
    eprintln!("\n=== Step 6: Restart all nodes ===");
    cluster.restart_all().await;

    // Count groups per store so we wait for the correct group count.
    let mut store_group_count: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
    for g in groups {
        *store_group_count.entry(g.store_id).or_default() += 1;
    }
    let mut waited_stores: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for g in groups {
        if waited_stores.insert(g.store_id) {
            wait_for_store(
                &cluster.client,
                &cluster.base,
                g.store_id,
                store_group_count[&g.store_id],
                Duration::from_secs(3),
            )
            .await;
        }
        wait_for_group_leader(
            &cluster.client,
            &cluster.base,
            g.store_id,
            g.group_id,
            g.nodes.len(),
            Duration::from_secs(3),
        )
        .await;
    }

    // Confirm leaders are still stable after all nodes are up (no blind sleep).
    for g in groups {
        wait_for_group_leader(
            &cluster.client,
            &cluster.base,
            g.store_id,
            g.group_id,
            g.nodes.len(),
            Duration::from_secs(3),
        )
        .await;
    }

    // Step 7: Verify WAL after restart (unchanged)
    eprintln!("\n=== Step 7: Verify WAL after restart ===");
    for (gi, g) in groups.iter().enumerate() {
        let post = cluster
            .verify_wal_for_nodes("post-restart", g.store_id, g.group_id, &g.nodes)
            .await;
        for (i, summary) in post.iter().enumerate() {
            assert_eq!(
                summary.max_slot, pre_wal[gi][i].max_slot,
                "WAL max_slot should not change after restart (store {} group {} node {})",
                g.store_id, g.group_id, g.nodes[i]
            );
        }
    }

    // Step 8: Verify KV state after restart
    eprintln!("\n=== Step 8: Verify KV state after restart ===");
    for g in groups {
        eprintln!("\n-- store {} group {} --", g.store_id, g.group_id);
        let expected = g.expected();
        wait_for_kv_state(
            &cluster.client,
            &cluster.base,
            g.store_id,
            g.group_id,
            &expected,
            Duration::from_secs(3),
        )
        .await;
    }
    eprintln!("\n=== PASSED: {tag} ===");
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn restart_1node_1group() {
    restart_recovery(
        "restart-1n-1g",
        &[(1, 1)],
        &[GroupSpec {
            store_id: 10,
            group_id: 1,
            replica_id: 1000,
            nodes: vec![1],
            n_puts: REPLAY_PUTS,
            deleted_keys: vec![1, 10, 20, 30, 40, 50],
        }],
        "test",
    )
    .await;
}

#[tokio::test]
async fn restart_3node_1group() {
    restart_recovery(
        "restart-3n-1g",
        &[(1, 1), (2, 1), (3, 1)],
        &[GroupSpec {
            store_id: 10,
            group_id: 1,
            replica_id: 1000,
            nodes: vec![1, 2, 3],
            n_puts: REPLAY_PUTS,
            deleted_keys: vec![1, 10, 20, 30, 40, 50],
        }],
        "test",
    )
    .await;
}

#[tokio::test]
async fn restart_5node_1group() {
    restart_recovery(
        "restart-5n-1g",
        &[(1, 1), (2, 1), (3, 1), (4, 2), (5, 2)],
        &[GroupSpec {
            store_id: 10,
            group_id: 1,
            replica_id: 1000,
            nodes: vec![1, 2, 3, 4, 5],
            n_puts: REPLAY_PUTS,
            deleted_keys: vec![1, 5, 10, 20, 30, 40],
        }],
        "e2e",
    )
    .await;
}

#[tokio::test]
async fn restart_5node_2group() {
    restart_recovery(
        "restart-5n-2g",
        &[(1, 1), (2, 1), (3, 1), (4, 2), (5, 2)],
        &[
            GroupSpec {
                store_id: 11,
                group_id: 1,
                replica_id: 1000,
                nodes: vec![1, 2, 3, 4, 5],
                n_puts: REPLAY_PUTS,
                deleted_keys: vec![1, 5, 10, 20, 30, 40],
            },
            GroupSpec {
                store_id: 11,
                group_id: 2,
                replica_id: 2000,
                nodes: vec![1, 2, 3],
                n_puts: REPLAY_PUTS,
                deleted_keys: vec![2, 8, 15, 22],
            },
        ],
        "e2e",
    )
    .await;
}

#[tokio::test]
async fn restart_6node_2group_overlap() {
    restart_recovery(
        "restart-6n-2g-overlap",
        &[(1, 1), (2, 1), (3, 1), (4, 2), (5, 2), (6, 2)],
        &[
            GroupSpec {
                store_id: 11,
                group_id: 1,
                replica_id: 1000,
                nodes: vec![1, 2, 3, 4, 5],
                n_puts: REPLAY_PUTS,
                deleted_keys: vec![1, 10, 15, 20, 25],
            },
            GroupSpec {
                store_id: 11,
                group_id: 2,
                replica_id: 2000,
                nodes: vec![2, 5, 6],
                n_puts: REPLAY_PUTS,
                deleted_keys: vec![2, 10, 15, 25],
            },
        ],
        "e2e",
    )
    .await;
}
