// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared cluster harness for diskdb integration tests.
//!
//! Starts a real 3-node `crowdb-kv-server` cluster with store 0,
//! group 0 (system) and group 1 (diskdb data), wires topology,
//! waits for leader election, and provides helpers to seed
//! hardware metadata into group 0.

use std::io as std_io;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crowdb_diskdb::ddb_kv_client::DdbKvClient;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient, RetryConfig, ServiceRegistryClient};
use crowdb_protocol::port_alloc;
use crowdb_protocol::ServicePort;
use serde_json::Value;

// ── process management ──────────────────────────────────────────

struct ServerHandle {
    child: Child,
    base_url: String,
    _root: crowdb_test_harness::test_dirs::TestDir,
}

impl ServerHandle {
    fn base_url(&self) -> &str {
        &self.base_url
    }

    async fn wait_for_ready(&self, timeout: Duration) -> std_io::Result<()> {
        let client = reqwest::Client::new();
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Ok(resp) = client.get(format!("{}/health", self.base_url)).send().await {
                if resp.status().is_success() || resp.status().as_u16() == 503 {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(std_io::Error::new(
            std_io::ErrorKind::TimedOut,
            "server was not ready before timeout",
        ))
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let pid = self.child.id();
        let _ = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status();
        let start = Instant::now();
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) | Err(_) => break,
                Ok(None) => {
                    if start.elapsed() >= Duration::from_secs(1) {
                        let _ = std::process::Command::new("kill")
                            .arg("-KILL")
                            .arg(pid.to_string())
                            .status();
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }
}

/// One kv-server node in the test cluster.
#[allow(dead_code)]
pub struct KvNode {
    handle: ServerHandle,
    pub node_id: u64,
    pub replica_id: u64,
}

impl KvNode {
    pub fn base_url(&self) -> &str {
        self.handle.base_url()
    }
}

/// Find the crowdb-kv-server binary.
fn crowdb_kv_server_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CROWDB_KV_SERVER_BIN") {
        let path = PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut p = dir.to_path_buf();
            for _ in 0..3 {
                let candidate = p.join("crowdb-kv-server");
                if candidate.exists() {
                    return Some(candidate);
                }
                if !p.pop() {
                    break;
                }
            }
        }
    }
    None
}

// ── topology helpers ────────────────────────────────────────────

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

async fn topology(node: &KvNode) -> Value {
    client()
        .get(format!("{}/topology", node.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint
        .strip_prefix("0.0.0.0:")
        .map_or_else(|| endpoint.to_string(), |port| format!("127.0.0.1:{port}"))
}

fn node_endpoint(topo: &Value) -> String {
    normalize_endpoint(
        topo["stores"][0]["listen_addr"]
            .as_str()
            .expect("store listen_addr"),
    )
}

async fn combined_topology(nodes: &[KvNode]) -> Value {
    let mut stores = Vec::new();
    for node in nodes {
        let topo = topology(node).await;
        if let Some(arr) = topo["stores"].as_array() {
            for s in arr {
                let mut s = s.clone();
                if let Some(addr) = s["listen_addr"].as_str() {
                    s["listen_addr"] = Value::String(normalize_endpoint(addr));
                }
                stores.push(s);
            }
        }
    }
    serde_json::json!({ "stores": stores })
}

/// Wire all nodes with each other's topology (remotes batch).
async fn wire_topology(nodes: &[KvNode], group_id: u64) {
    let combined = combined_topology(nodes).await;
    for node in nodes {
        let resp = client()
            .post(format!(
                "{}/stores/{}/groups/{group_id}/remotes/batch",
                node.base_url(),
                node.node_id
            ))
            .json(&combined)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "batch wiring failed for node {}",
            node.node_id
        );
    }
}

/// Wait for exactly one leader across all nodes for the given group.
/// Returns the index of the leader node.
pub async fn wait_for_leader(nodes: &[KvNode], group_id: u64, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut leaders = Vec::new();
        for (idx, node) in nodes.iter().enumerate() {
            let topo = topology(node).await;
            let role = topo["stores"][0]["groups"]
                .as_array()
                .and_then(|g| g.iter().find(|gg| gg["group_id"].as_u64() == Some(group_id)))
                .and_then(|gg| gg["local_replica"]["role"].as_str())
                .unwrap_or("");
            if role == "leader" {
                leaders.push(idx);
            }
        }
        if leaders.len() == 1 {
            return leaders[0];
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no unique leader for group {group_id} within {timeout:?}");
}

/// Get the crowdb-rpc endpoint of the leader node for a group.
pub async fn leader_endpoint(nodes: &[KvNode], group_id: u64) -> String {
    let idx = wait_for_leader(nodes, group_id, Duration::from_secs(30)).await;
    node_endpoint(&topology(&nodes[idx]).await)
}

// ── cluster ─────────────────────────────────────────────────────

/// A running 3-node kv-server cluster with group 0 (system) and
/// group 1 (diskdb data).
#[allow(dead_code)]
pub struct KvCluster {
    pub nodes: Vec<KvNode>,
    pub group0_leader_endpoint: String,
    pub group1_leader_endpoint: String,
    /// HTTP management API endpoints for all nodes — used as
    /// `mgmt_seeds` so client topology refresh can recover from a
    /// stale leader hint.
    pub mgmt_endpoints: Vec<String>,
}

impl KvCluster {
    /// Start a 3-node cluster with store 0, groups 0 and 1.
    /// Each node runs both groups.
    pub async fn start() -> Self {
        let mut nodes = Vec::new();
        // Start 3 nodes, each with store 0, groups 0 and 1.
        // The kv-server `--groups` flag accepts a comma list.
        for (idx, nid) in [0u64, 1, 2].iter().enumerate() {
            let replica_id = u64::try_from(idx + 1).unwrap();
            // We pass "0,1" as the groups list so each node hosts
            // both group 0 (system) and group 1 (data).
            let node = start_kv_node_with_groups(*nid, &[0, 1], replica_id)
                .await
                .unwrap_or_else(|e| panic!("start kv node {nid}: {e}"));
            nodes.push(node);
        }
        // Wire topology for both groups.
        wire_topology(&nodes, 0).await;
        wire_topology(&nodes, 1).await;
        // Wait for leader election on both groups.
        let group0_leader_endpoint = leader_endpoint(&nodes, 0).await;
        let group1_leader_endpoint = leader_endpoint(&nodes, 1).await;
        let mgmt_endpoints = nodes.iter().map(|n| n.base_url().to_string()).collect();
        Self {
            nodes,
            group0_leader_endpoint,
            group1_leader_endpoint,
            mgmt_endpoints,
        }
    }

    /// Build a `DdbKvClient` seeded with the group-1 leader endpoint.
    /// Uses `mgmt_endpoints` for topology refresh so the client can
    /// recover if the leader changes.
    #[must_use]
    pub fn make_ddb_kv_client(&self) -> DdbKvClient {
        let kv = CrowdbKvClient::new(test_client_config(self.mgmt_endpoints.clone()));
        kv.seed_leader(0, 1, self.group1_leader_endpoint.clone());
        DdbKvClient::new(kv)
    }

    /// Build a `HardwareClient` seeded with the group-0 leader endpoint.
    #[must_use]
    pub fn make_hardware_client(&self) -> HardwareClient {
        let kv = CrowdbKvClient::new(test_client_config(self.mgmt_endpoints.clone()));
        kv.seed_leader(0, 0, self.group0_leader_endpoint.clone());
        HardwareClient::new(kv)
    }

    /// Build a `ServiceRegistryClient` seeded with the group-0 leader.
    #[must_use]
    pub fn make_service_registry_client(&self) -> ServiceRegistryClient {
        let kv = CrowdbKvClient::new(test_client_config(self.mgmt_endpoints.clone()));
        kv.seed_leader(0, 0, self.group0_leader_endpoint.clone());
        ServiceRegistryClient::new(kv)
    }
}

/// Build a `ClientConfig` with a generous retry budget for E2E tests,
/// where leader election may still be converging right after cluster
/// startup. The production default (`max_retries: 3`, 100ms wait) gives
/// only ~300ms of patience; tests need ~2s to ride out re-elections.
fn test_client_config(mgmt_seeds: Vec<String>) -> ClientConfig {
    let mut cfg = ClientConfig::new(mgmt_seeds);
    cfg.retry = RetryConfig {
        max_retries: 10,
        unknown_leader_wait: Duration::from_millis(200),
        backoff_base: Duration::from_millis(100),
        backoff_max: Duration::from_secs(5),
    };
    cfg
}

/// Start a kv-server node hosting multiple groups on one store.
async fn start_kv_node_with_groups(
    node_id: u64,
    group_ids: &[u64],
    replica_id: u64,
) -> std_io::Result<KvNode> {
    let group_str = group_ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",");
    let root = crowdb_test_harness::test_dirs::TestDir::new("diskdb-node")?;
    let bin = crowdb_kv_server_bin().ok_or_else(|| {
        std_io::Error::new(std_io::ErrorKind::NotFound, "crowdb-kv-server binary not found")
    })?;
    let mgmt_port = port_alloc::alloc_test_port(ServicePort::KvServerMgmt);
    let listen_port = port_alloc::alloc_test_port(ServicePort::KvServerListen);
    let mut cmd = Command::new(bin);
    cmd.args([
        "--root",
        root.path().to_str().unwrap(),
        "--stores",
        &node_id.to_string(),
        "--groups",
        &group_str,
        "--replica",
        &replica_id.to_string(),
        "--management-addr",
        "127.0.0.1",
        "--management-port",
        &mgmt_port.to_string(),
        "--ports",
        &listen_port.to_string(),
        "--election-profile",
        "e2e",
    ])
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    let mut child = cmd.spawn()?;
    let stdout = child.stdout.take().expect("stdout captured");
    let stderr = child.stderr.take().expect("stderr captured");
    let (tx, rx) = mpsc::channel();
    let stderr_buf = Arc::new(Mutex::new(Vec::<String>::new()));
    let stderr_buf_clone = Arc::clone(&stderr_buf);
    thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(idx) = line.find("management_addr=") {
                let after = &line[idx + "management_addr=".len()..];
                let _ = tx.send(after.trim().to_string());
                break;
            }
        }
    });
    thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            stderr_buf_clone.lock().unwrap().push(line);
        }
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    let addr = match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(addr) => addr,
        Err(e) => {
            let _ = child.wait();
            let stderr_lines = stderr_buf.lock().unwrap();
            let msg = format!(
                "no management_addr in stdout: {e}; stderr:\n{}",
                stderr_lines.join("\n")
            );
            drop(stderr_lines);
            return Err(std_io::Error::new(std_io::ErrorKind::BrokenPipe, msg));
        }
    };
    let handle = ServerHandle {
        child,
        base_url: format!("http://{addr}"),
        _root: root,
    };
    handle.wait_for_ready(Duration::from_secs(10)).await?;
    Ok(KvNode {
        handle,
        node_id,
        replica_id,
    })
}

/// Wait for all disks in a disk-group to transition from Init to Up
/// and have their zones loaded. Polls every 10ms up to 15s.
///
/// Used by `diskdb_e2e_test.rs` and `recovery_test.rs`; other test
/// binaries include `common` without using it, so `dead_code` is
/// suppressed for this shared helper.
#[allow(dead_code)]
pub async fn wait_for_disks_ready(
    container: &crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer,
    dg_id: u64,
    expected_disks: usize,
    expected_zones: u32,
) {
    use crowdb_protocol::common::HwStatus;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(dg) = container.get_disk_group(dg_id) {
            let disks = dg.disks.read().unwrap();
            let all_ready = disks.len() == expected_disks
                && disks.iter().all(|d| {
                    d.effective_status() == HwStatus::Up
                        && u32::try_from(d.zones.load().len()).unwrap_or(0) == expected_zones
                });
            if all_ready {
                return;
            }
        }
        if Instant::now() > deadline {
            let dg = container.get_disk_group(dg_id);
            let status = match dg {
                Some(dg) => {
                    let disks = dg.disks.read().unwrap();
                    disks
                        .iter()
                        .map(|d| {
                            let s = d.effective_status();
                            let zc = d.zones.load().len();
                            format!("{s:?}({zc}z)")
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                }
                None => "no dg".to_string(),
            };
            panic!("disks not ready after 5s: {status}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
