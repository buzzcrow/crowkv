// Copyright 2026-present Gian <crow.db@outlook.com>

//! CLI e2e for the physical lifecycle verbs: `cluster rack/node` and
//! `kv server` round-trips through `--sysmd-ip` / `--sysmd-port` /
//! `--config` against a real `crowdb-kv-server` with group 0
//! initialized — no `crowdb-web` intermediary.

mod common;

use std::time::Duration;

use common::direct::{crowdb_cli_bin, run, spawn_group0};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn rack_node_server_lifecycle() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // rack list — rack 1 already exists (from spawn_group0 config).
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "rack", "list"]);
    assert_eq!(code, 0, "rack list stderr={stderr}");
    assert!(stdout.contains('1'), "stdout={stdout}");

    // rack add — add a second rack.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "rack", "add", "--id", "2", "--name", "rack-two"],
    );
    assert_eq!(code, 0, "rack add stderr={stderr}");
    let (code, stdout, _) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "rack", "list"]);
    assert_eq!(code, 0);
    assert!(stdout.contains('2'), "stdout={stdout}");

    // node add — add node 2 on rack 2.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "node", "add", "--id", "2", "--rack", "2"],
    );
    assert_eq!(code, 0, "node add stderr={stderr}");
    let (code, stdout, _) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "node", "list"]);
    assert_eq!(code, 0);
    assert!(stdout.contains('1') && stdout.contains('2'), "stdout={stdout}");

    // node remove — remove node 2 (no server deployed on it).
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "node", "remove", "--id", "2"],
    );
    assert_eq!(code, 0, "node remove stderr={stderr}");
    let (code, stdout, _) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "node", "list"]);
    assert_eq!(code, 0);
    assert!(
        !stdout.contains("2                 2"),
        "node 2 should be gone: stdout={stdout}"
    );

    // kv server list — server on node 1 already exists.
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["kv", "server", "list"]);
    assert_eq!(code, 0, "server list stderr={stderr}");
    assert!(stdout.contains('1'), "stdout={stdout}");

    // kv server restart — recover node 1 after an out-of-band process exit.
    let pid = g0.pid;
    tokio::task::spawn_blocking(move || {
        let _ = crowdb_console_shared::lifecycle::stop_pid_with_timeout(pid, Duration::from_millis(100));
    })
    .await
    .unwrap();
    let (code, stdout, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["kv", "server", "restart", "--node", "1"],
    );
    assert_eq!(code, 0, "server restart stdout={stdout}\nstderr={stderr}");

    let restarted = crowdb_console_shared::ConsoleConfig::load(&g0.config_path).unwrap();
    let restarted_pid = restarted.server_for_node(1).unwrap().pid.unwrap();
    tokio::task::spawn_blocking(move || {
        let _ = crowdb_console_shared::lifecycle::stop_pid_with_timeout(
            restarted_pid,
            Duration::from_millis(100),
        );
    })
    .await
    .unwrap();

    // kv server stop — clear the deployment state after an out-of-band exit.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["kv", "server", "stop", "--node", "1"],
    );
    assert_eq!(code, 0, "server stop stderr={stderr}");
    let stopped = crowdb_console_shared::ConsoleConfig::load(&g0.config_path).unwrap();
    assert!(stopped.server_for_node(1).unwrap().pid.is_none());

    tokio::time::sleep(Duration::from_millis(100)).await;
}
