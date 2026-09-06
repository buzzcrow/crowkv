// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for [`ops::hardware`] — rack/node CRUD against the local
//! config. The sysdata sync is best-effort (cluster not initialized
//! in these tests), so we verify the config mutations only.

use crowdb_console_shared::config::{ConsoleConfig, NodeEntry};
use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops::{self, OpContext};

fn ctx() -> OpContext {
    OpContext::new_for_test("127.0.0.1:59999".into(), vec![], ConsoleConfig::default())
}

#[tokio::test]
async fn add_rack_and_list() {
    let ctx = ctx();
    let rack = ops::hardware::add_rack(&ctx, 1, "rack-1").await.unwrap();
    assert_eq!(rack.id, 1);
    assert_eq!(rack.name, "rack-1");
    let racks = ops::hardware::list_racks(&ctx);
    assert_eq!(racks.len(), 1);
    assert_eq!(racks[0].id, 1);
}

#[tokio::test]
async fn add_rack_duplicate_conflict() {
    let ctx = ctx();
    ops::hardware::add_rack(&ctx, 1, "r1").await.unwrap();
    let err = ops::hardware::add_rack(&ctx, 1, "r2").await.unwrap_err();
    assert!(matches!(err, Error::Conflict { kind, .. } if kind == "rack"));
}

#[tokio::test]
async fn remove_rack_not_found() {
    let ctx = ctx();
    let err = ops::hardware::remove_rack(&ctx, 99).await.unwrap_err();
    assert!(matches!(err, Error::NotFound { kind, .. } if kind == "rack"));
}

#[tokio::test]
async fn remove_rack_with_nodes_conflict() {
    let ctx = ctx();
    ops::hardware::add_rack(&ctx, 1, "r1").await.unwrap();
    ops::hardware::add_node(
        &ctx,
        NodeEntry {
            id: 10,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
        },
    )
    .await
    .unwrap();
    let err = ops::hardware::remove_rack(&ctx, 1).await.unwrap_err();
    assert!(matches!(err, Error::Conflict { kind, .. } if kind == "rack"));
}

#[tokio::test]
async fn add_node_and_list() {
    let ctx = ctx();
    ops::hardware::add_rack(&ctx, 1, "r1").await.unwrap();
    let node = ops::hardware::add_node(
        &ctx,
        NodeEntry {
            id: 10,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(node.id, 10);
    let nodes = ops::hardware::list_nodes(&ctx, None);
    assert_eq!(nodes.len(), 1);
    let nodes_in_rack = ops::hardware::list_nodes(&ctx, Some(1));
    assert_eq!(nodes_in_rack.len(), 1);
    let nodes_other_rack = ops::hardware::list_nodes(&ctx, Some(2));
    assert_eq!(nodes_other_rack.len(), 0);
}

#[tokio::test]
async fn add_node_unknown_rack_validation() {
    let ctx = ctx();
    let err = ops::hardware::add_node(
        &ctx,
        NodeEntry {
            id: 10,
            rack_id: 99,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, Error::Validation { field, .. } if field == "rack_id"));
}

#[tokio::test]
async fn remove_node_not_found() {
    let ctx = ctx();
    let err = ops::hardware::remove_node(&ctx, 99).await.unwrap_err();
    assert!(matches!(err, Error::NotFound { kind, .. } if kind == "node"));
}

#[tokio::test]
async fn remove_node_with_server_conflict() {
    let ctx = ctx();
    ops::hardware::add_rack(&ctx, 1, "r1").await.unwrap();
    ops::hardware::add_node(
        &ctx,
        NodeEntry {
            id: 10,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
        },
    )
    .await
    .unwrap();
    // Simulate a deployed server on the node.
    {
        let mut cfg = ctx.config_mut();
        cfg.servers.push(crowdb_console_shared::config::ServerEntry {
            id: "10".into(),
            url: "http://127.0.0.1:10000".into(),
            node_id: Some(10),
            rpc_url: None,
            rest_port: None,
            rpc_port: None,
            auto_start: true,
            binary: None,
            election_profile: None,
            pid: None,
            service_type: crowdb_console_shared::config::ServiceType::Kv,
            rpc_workers: None,
            no_fsync: false,
        });
    }
    let err = ops::hardware::remove_node(&ctx, 10).await.unwrap_err();
    assert!(matches!(err, Error::Conflict { kind, .. } if kind == "node"));
}
