// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Construction tests for [`OpContext`]. The sysmd + kv clients share
//! the same underlying `Arc<CrowdbKvClient>`; the config is held behind
//! an `RwLock` and accessible via `config()` / `config_mut()`.

use crowdb_console_shared::config::ConsoleConfig;
use crowdb_console_shared::ops::OpContext;

#[test]
fn op_context_shares_arc_between_sysmd_and_kv() {
    let ctx = OpContext::new(
        "127.0.0.1:10100".into(),
        vec!["http://127.0.0.1:10000".into()],
        ConsoleConfig::default(),
    );
    // Both sysmd and kv wrap the same Arc<CrowdbKvClient>, so the
    // references returned by kv() must point to the same allocation.
    assert!(std::ptr::eq(ctx.sysmd().kv(), ctx.kv()));
}

#[test]
fn op_context_test_scenario_flag_is_explicit() {
    let production = OpContext::new("127.0.0.1:10100".into(), vec![], ConsoleConfig::default());
    let test = OpContext::new_for_test("127.0.0.1:59999".into(), vec![], ConsoleConfig::default());
    assert!(!production.is_test_scenario());
    assert!(test.is_test_scenario());
}

#[test]
fn op_context_config_rw_lock_roundtrip() {
    let ctx = OpContext::new("127.0.0.1:10100".into(), vec![], ConsoleConfig::default());
    // Write a rack into the config.
    {
        let mut cfg = ctx.config_mut();
        cfg.racks.push(crowdb_console_shared::config::RackEntry {
            id: 1,
            name: "r1".into(),
        });
    }
    // Read it back.
    let cfg = ctx.config();
    assert_eq!(cfg.racks.len(), 1);
    assert_eq!(cfg.racks[0].id, 1);
}

#[test]
fn op_context_node_entry_not_found() {
    let ctx = OpContext::new("127.0.0.1:10100".into(), vec![], ConsoleConfig::default());
    let err = ctx.node_entry(42).unwrap_err();
    assert!(matches!(
        err,
        crowdb_console_shared::error::Error::NotFound { kind, id }
        if kind == "node" && id == "42"
    ));
}

#[test]
fn op_context_server_for_node_not_found() {
    let ctx = OpContext::new("127.0.0.1:10100".into(), vec![], ConsoleConfig::default());
    let err = ctx.server_for_node(42).unwrap_err();
    assert!(matches!(
        err,
        crowdb_console_shared::error::Error::NotFound { kind, id }
        if kind == "server" && id == "42"
    ));
}

#[test]
fn op_context_discovery_is_some_when_built_via_new() {
    let ctx = OpContext::new(
        "127.0.0.1:10100".into(),
        vec!["http://127.0.0.1:10000".into()],
        ConsoleConfig::default(),
    );
    // OpContext::new auto-creates a ServiceDiscoveryClient.
    assert!(ctx.discovery().is_some());
}

#[test]
fn op_context_discovery_or_error_returns_client() {
    let ctx = OpContext::new(
        "127.0.0.1:10100".into(),
        vec!["http://127.0.0.1:10000".into()],
        ConsoleConfig::default(),
    );
    assert!(ctx.discovery_or_error().is_ok());
}
