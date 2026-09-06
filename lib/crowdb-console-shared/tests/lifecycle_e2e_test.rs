// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! C3 end-to-end: rack → node → deploy local `crowdb-kv-server` → observe
//! the running instance via `topology::aggregate()`.
//!
//! The test expects the `crowdb-kv-server` binary to be built and available
//! either via `$CROWDB_KV_SERVER_BIN` or as a sibling of the current test
//! executable (the usual `cargo test` layout). If neither resolves, the
//! test is skipped with an `eprintln!` note instead of failing, so this
//! suite stays friendly on first run.

use std::time::Duration;

use crowdb_console_shared::{
    config::{NodeEntry, RackEntry},
    lifecycle::{self, crowdb_kv_server_bin, DeployRequest},
    topology, ConsoleConfig, ServerEntry,
};

fn pick_two_free_ports() -> (u16, u16) {
    let l1 = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let l2 = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let p1 = l1.local_addr().unwrap().port();
    let p2 = l2.local_addr().unwrap().port();
    drop(l1);
    drop(l2);
    (p1, p2)
}

#[cfg(target_os = "linux")]
async fn assert_reaped(pid: u32) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"));
        if stat.is_err() {
            return;
        }
        let state = stat
            .unwrap()
            .rsplit(')')
            .next()
            .and_then(|rest| rest.trim_start().chars().next());
        assert_ne!(state, Some('Z'), "deployed child {pid} became a zombie");
        assert!(
            tokio::time::Instant::now() < deadline,
            "deployed child {pid} was not reaped"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn deploy_local_and_observe_topology() {
    let Some(bin) = crowdb_kv_server_bin() else {
        eprintln!("skipping: crowdb-kv-server binary not found (build it with `cargo build -p crowdb-kv-server` or set $CROWDB_KV_SERVER_BIN)");
        return;
    };
    if !bin.exists() {
        eprintln!(
            "skipping: crowdb-kv-server binary at {} does not exist",
            bin.display()
        );
        return;
    }

    // Build a fresh in-memory config: 1 rack, 1 node.
    let mut cfg = ConsoleConfig::default();
    cfg.add_rack(RackEntry {
        id: 1,
        name: "rack-1".into(),
    })
    .unwrap();
    cfg.add_node(NodeEntry {
        id: 1,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    })
    .unwrap();

    let node = cfg.node(1).unwrap().clone();
    let (rest_port, rpc_port) = pick_two_free_ports();

    let req = DeployRequest {
        server_id: "s1".into(),
        rest_port,
        rpc_port,
        election_profile: Some("e2e".into()),
        binary: Some(bin),
        ..Default::default()
    };

    let deployed = match lifecycle::deploy_local(&req, &node).await {
        Ok(d) => d,
        Err(e) => {
            panic!("deploy_local failed: {e}");
        }
    };

    // Record into the registry as the CLI would.
    cfg.add_server(ServerEntry {
        id: deployed.server_id.clone(),
        url: deployed.mgmt_url.clone(),
        node_id: Some(1),
        rpc_url: Some(deployed.rpc_url.clone()),
        rest_port: Some(rest_port),
        rpc_port: Some(rpc_port),
        auto_start: true,
        binary: None,
        election_profile: Some("e2e".into()),
        pid: None,
        service_type: crowdb_console_shared::config::ServiceType::Kv,
        rpc_workers: None,
        no_fsync: false,
    })
    .unwrap();

    // Aggregate via the same path the CLI uses.
    let snapshot = topology::aggregate(&cfg.server_urls()).await.unwrap();
    let ok = snapshot
        .servers
        .iter()
        .any(|s| s.error.is_none() && s.health.is_some());
    assert!(
        ok,
        "deployed server should appear healthy in the aggregate snapshot: {snapshot:#?}"
    );

    // Clean up: stop the process we spawned so the test doesn't leak.
    let _ = lifecycle::stop_pid(deployed.pid);
    // The console owns the child and must reap it after it exits; otherwise
    // repeated deploy/stop flows accumulate zombies under the web process.
    // Yield so the asynchronous reaper can run after the synchronous stop.
    tokio::time::sleep(Duration::from_millis(50)).await;
    #[cfg(target_os = "linux")]
    assert_reaped(deployed.pid).await;
}
