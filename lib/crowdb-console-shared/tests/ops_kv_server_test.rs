// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for [`ops::kv_server`] validation paths. The actual deploy /
//! stop requires a real `crowdb-kv-server` binary and is covered by E2E
//! tests in Phase 4; here we verify the guard clauses.

use crowdb_console_shared::config::{ConsoleConfig, NodeEntry, ServerEntry, ServiceType};
use crowdb_console_shared::error::Error;
use crowdb_console_shared::lifecycle::DeployRequest;
use crowdb_console_shared::ops::{self, OpContext};

fn ctx_with_node() -> OpContext {
    let mut cfg = ConsoleConfig::default();
    cfg.add_rack(crowdb_console_shared::config::RackEntry {
        id: 1,
        name: "r1".into(),
    })
    .unwrap();
    cfg.add_node(NodeEntry {
        id: 10,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    })
    .unwrap();
    OpContext::new_for_test("127.0.0.1:59999".into(), vec![], cfg)
}

#[tokio::test]
async fn deploy_unknown_node_not_found() {
    let ctx = ctx_with_node();
    let req = DeployRequest {
        server_id: "99".into(),
        rest_port: 10000,
        rpc_port: 10100,
        ..Default::default()
    };
    let err = ops::kv_server::deploy(&ctx, &req, None).await.unwrap_err();
    assert!(matches!(err, Error::NotFound { kind, .. } if kind == "node"));
}

#[tokio::test]
async fn stop_no_server_not_found() {
    let ctx = ctx_with_node();
    let err = ops::kv_server::stop(&ctx, 10, None).await.unwrap_err();
    assert!(matches!(err, Error::NotFound { kind, .. } if kind == "server"));
}

#[tokio::test]
async fn stop_no_pid_not_found() {
    let ctx = ctx_with_node();
    // Manually insert a server entry without a PID.
    {
        let mut cfg = ctx.config_mut();
        cfg.add_server(ServerEntry {
            id: "10".into(),
            url: "http://127.0.0.1:10000".into(),
            node_id: Some(10),
            rpc_url: None,
            rest_port: Some(10000),
            rpc_port: Some(10100),
            auto_start: true,
            binary: None,
            election_profile: None,
            pid: None,
            service_type: ServiceType::Kv,
            rpc_workers: None,
            no_fsync: false,
        })
        .unwrap();
    }
    let err = ops::kv_server::stop(&ctx, 10, None).await.unwrap_err();
    assert!(matches!(err, Error::NotFound { kind, .. } if kind == "server"));
}

#[tokio::test]
async fn list_returns_all_servers() {
    let ctx = ctx_with_node();
    {
        let mut cfg = ctx.config_mut();
        cfg.add_server(ServerEntry {
            id: "10".into(),
            url: "http://127.0.0.1:10000".into(),
            node_id: Some(10),
            rpc_url: None,
            rest_port: Some(10000),
            rpc_port: Some(10100),
            auto_start: true,
            binary: None,
            election_profile: None,
            pid: None,
            service_type: ServiceType::Kv,
            rpc_workers: None,
            no_fsync: false,
        })
        .unwrap();
    }
    let servers = ops::kv_server::list(&ctx);
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].node_id, Some(10));
}

#[tokio::test]
async fn delete_no_server_not_found() {
    let ctx = ctx_with_node();
    let err = ops::kv_server::delete(&ctx, 10).await.unwrap_err();
    assert!(matches!(err, Error::NotFound { kind, .. } if kind == "server"));
}

#[tokio::test]
async fn delete_with_server_succeeds_when_no_replicas() {
    let ctx = ctx_with_node();
    // Insert a server entry with no PID (so no process to stop).
    {
        let mut cfg = ctx.config_mut();
        cfg.add_server(ServerEntry {
            id: "10".into(),
            url: "http://127.0.0.1:10000".into(),
            node_id: Some(10),
            rpc_url: None,
            rest_port: Some(10000),
            rpc_port: Some(10100),
            auto_start: true,
            binary: None,
            election_profile: None,
            pid: None,
            service_type: ServiceType::Kv,
            rpc_workers: None,
            no_fsync: false,
        })
        .unwrap();
    }
    // Delete should succeed — no replicas in sysdata (cluster not init'd).
    ops::kv_server::delete(&ctx, 10).await.unwrap();
    // Server entry should be gone.
    assert!(ctx.config().server_for_node(10).is_none());
}
