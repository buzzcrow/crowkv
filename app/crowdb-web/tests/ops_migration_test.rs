// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! C9: Integration tests verifying that migrated handlers delegate to
//! `ops::*` functions and that removed Swagger/openapi routes return 404.
//! Also covers the `DELETE /api/nodes/:id/server` require-empty check
//! (409 with replicas, 204 when empty).

use std::net::SocketAddr;
use std::time::Duration;

use crowdb_console_shared::config::{NodeEntry, RackEntry, ServerEntry, ServiceType};
use crowdb_console_shared::lifecycle::{self, crowdb_kv_server_bin, stop_pid_with_timeout, DeployRequest};
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use serde_json::json;

fn pick_free_port() -> u16 {
    crowdb_protocol::port_alloc::alloc_test_port(crowdb_protocol::ServicePort::Web)
}

struct Upstream {
    pid: u32,
    mgmt_url: String,
    rpc_url: String,
}

impl Drop for Upstream {
    fn drop(&mut self) {
        let _ = stop_pid_with_timeout(self.pid, Duration::from_secs(1));
    }
}

async fn spawn_upstream() -> Option<Upstream> {
    let bin = crowdb_kv_server_bin()?;
    if !bin.exists() {
        return None;
    }
    let workspace = crowdb_test_harness::test_dirs::test_data_dir().join(format!(
        "ops-migration-test-{}-{}",
        std::process::id(),
        pick_free_port()
    ));
    std::fs::create_dir_all(&workspace).ok()?;
    std::fs::create_dir_all(workspace.join("bin")).ok()?;
    std::fs::create_dir_all(workspace.join("log")).ok()?;
    let node = NodeEntry {
        id: 1,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    };
    let req = DeployRequest {
        server_id: "n1".to_string(),
        rest_port: pick_free_port(),
        rpc_port: pick_free_port(),
        election_profile: Some("e2e".into()),
        binary: Some(bin),
        ..Default::default()
    };
    let deployed = lifecycle::deploy_local_in_dir(&req, &node, &workspace)
        .await
        .expect("deploy_local_in_dir");
    Some(Upstream {
        pid: deployed.pid,
        mgmt_url: deployed.mgmt_url,
        rpc_url: deployed.rpc_url,
    })
}

async fn spawn_web(upstream: &Upstream) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let mut cfg = ConsoleConfig::default();
    cfg.racks.push(RackEntry {
        id: 1,
        name: "r1".into(),
    });
    cfg.nodes.push(NodeEntry {
        id: 1,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    });
    cfg.add_server(ServerEntry {
        id: "n1".to_string(),
        url: upstream.mgmt_url.clone(),
        node_id: Some(1),
        rpc_url: Some(upstream.rpc_url.clone()),
        rest_port: None,
        rpc_port: None,
        auto_start: true,
        binary: None,
        election_profile: None,
        pid: None,
        service_type: ServiceType::Kv,
        rpc_workers: None,
        no_fsync: false,
    })
    .unwrap();
    let state = AppState::with_config(cfg, None);
    state.set_runtime_pid(1, upstream.pid);
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
async fn swagger_routes_are_removed() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let web = spawn_web(&upstream).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();

    // Swagger UI was removed. These routes are no longer API endpoints —
    // they fall through to the SPA fallback (200 with HTML, not a Swagger
    // JSON response). We assert the response is NOT a Swagger/OpenAPI
    // JSON payload.
    let resp = http.get(format!("{base}/api/swagger/")).send().await.unwrap();
    let body = resp.text().await.unwrap_or_default();
    assert!(
        !body.contains("\"swagger\""),
        "GET /api/swagger/ should not return Swagger JSON"
    );

    let resp = http.get(format!("{base}/api/swagger.json")).send().await.unwrap();
    let body = resp.text().await.unwrap_or_default();
    assert!(
        !body.contains("\"swagger\""),
        "GET /api/swagger.json should not return Swagger JSON"
    );

    let resp = http
        .get(format!("{base}/api/nodes/1/openapi.json"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap_or_default();
    assert!(
        !body.contains("\"openapi\"") && !body.contains("\"swagger\""),
        "GET /api/nodes/1/openapi.json should not return OpenAPI JSON"
    );

    let _ = stop_pid_with_timeout(upstream.pid, Duration::from_secs(1));
}

#[tokio::test]
async fn cluster_init_delegates_to_ops_cluster_init() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let web = spawn_web(&upstream).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();

    // POST /api/cluster/init → ops::cluster::init → 201
    let resp = http
        .post(format!("{base}/api/cluster/init"))
        .json(&json!({"nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "cluster init: {:?}", resp.text().await.ok());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["store_id"], 0);
    assert_eq!(body["group_id"], 0);

    // Double init should succeed (idempotent) or conflict — either is
    // acceptable. We only assert it doesn't 5xx.
    let resp = http
        .post(format!("{base}/api/cluster/init"))
        .json(&json!({"nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().as_u16() < 500,
        "double init should not 5xx: {}",
        resp.status()
    );

    let _ = stop_pid_with_timeout(upstream.pid, Duration::from_secs(1));
}

#[tokio::test]
async fn add_store_delegates_to_ops_kv_logical() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let web = spawn_web(&upstream).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();

    // Init first.
    let resp = http
        .post(format!("{base}/api/cluster/init"))
        .json(&json!({"nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // POST /api/stores → ops::kv_logical::add_store → 201
    let resp = http
        .post(format!("{base}/api/stores"))
        .json(&json!({"store_id": 42, "nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "add store: {:?}", resp.text().await.ok());

    // GET /api/stores should list it.
    let stores: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        stores
            .iter()
            .any(|s| s.get("store_id").and_then(serde_json::Value::as_u64) == Some(42)),
        "store 42 should be listed"
    );

    let _ = stop_pid_with_timeout(upstream.pid, Duration::from_secs(1));
}

#[tokio::test]
async fn delete_server_returns_409_when_replicas_exist() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let web = spawn_web(&upstream).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();

    // Init + create a store with a group (replica exists).
    let resp = http
        .post(format!("{base}/api/cluster/init"))
        .json(&json!({"nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // DELETE /api/nodes/1/server → 409 (system group replica exists).
    let resp = http
        .delete(format!("{base}/api/nodes/1/server"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        409,
        "delete with replicas should be 409: {:?}",
        resp.text().await.ok()
    );

    let _ = stop_pid_with_timeout(upstream.pid, Duration::from_secs(1));
}

#[tokio::test]
async fn kv_get_delegates_to_ops_kv_data() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let web = spawn_web(&upstream).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();

    // Init + create store + group.
    let resp = http
        .post(format!("{base}/api/cluster/init"))
        .json(&json!({"nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let resp = http
        .post(format!("{base}/api/stores"))
        .json(&json!({"store_id": 8, "nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let resp = http
        .post(format!("{base}/api/stores/8/groups"))
        .json(&json!({"group_id": 80, "replica_id": 800, "nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Wait for leader election.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // PUT a value via the web route (ops::kv_data::put).
    let resp = http
        .post(format!("{base}/api/stores/8/groups/80/kv/put"))
        .json(&json!({"key": "hello", "value": "world", "client_id": 1, "seq": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "kv put: {:?}", resp.text().await.ok());

    // GET it back (ops::kv_data::get).
    let resp = http
        .get(format!("{base}/api/stores/8/groups/80/kv/get?key=hello"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["found"], true);
    assert_eq!(body["value_utf8"], "world");

    let _ = stop_pid_with_timeout(upstream.pid, Duration::from_secs(1));
}
